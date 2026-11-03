package search

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"

	"github.com/logister/logister/api/internal/db"
)

func ReadAlias(namespace string) string { return namespace + "-read" }
func storageDefinition(alias string) map[string]any {
	return map[string]any{"settings": map[string]any{"number_of_shards": 1, "number_of_replicas": 0}, "aliases": map[string]any{alias: map[string]any{}}, "mappings": map[string]any{
		"_meta": map[string]any{"schema_version": 2, "ready": true}, "properties": map[string]any{
			"@timestamp": map[string]any{"type": "date"}, "event_id": map[string]any{"type": "keyword"}, "message": map[string]any{"type": "text"},
			"source": map[string]any{"type": "keyword"}, "level": map[string]any{"type": "keyword"}, "host": map[string]any{"type": "keyword"}, "stream": map[string]any{"type": "keyword"}, "facility": map[string]any{"type": "keyword"}, "fields": map[string]any{"type": "flat_object"},
			"payload_json": map[string]any{"type": "text", "index": false},
		},
	}}
}
func (c *Client) EnsureReadAlias(ctx context.Context, namespace string) error {
	status, raw, err := c.jsonRequest(ctx, "PUT", "/"+url.PathEscape(namespace)+"/_alias/"+url.PathEscape(ReadAlias(namespace)), map[string]any{})
	if err != nil {
		return err
	}
	if status != 200 {
		return fmt.Errorf("attach read alias: %d %s", status, raw)
	}
	return c.ensurePayloadMapping(ctx, ReadAlias(namespace))
}
func (c *Client) ensurePayloadMapping(ctx context.Context, index string) error {
	status, raw, err := c.jsonRequest(ctx, "PUT", "/"+url.PathEscape(index)+"/_mapping", map[string]any{"properties": map[string]any{"payload_json": map[string]any{"type": "text", "index": false}}})
	if err != nil {
		return err
	}
	if status != 200 {
		return fmt.Errorf("prepare JSON payload mapping: %d %s", status, raw)
	}
	return nil
}
func (c *Client) EnsureStreamIndex(ctx context.Context, namespace, index string) error {
	status, _, err := c.jsonRequest(ctx, "HEAD", "/"+url.PathEscape(index), nil)
	if err != nil {
		return err
	}
	if status == 200 {
		return c.ensurePayloadMapping(ctx, index)
	}
	if status != 404 {
		return fmt.Errorf("check stream index: %d", status)
	}
	status, raw, err := c.jsonRequest(ctx, "PUT", "/"+url.PathEscape(index), storageDefinition(ReadAlias(namespace)))
	if err != nil {
		return err
	}
	if status != 200 {
		// Another publisher can win creation; only accept a ready schema afterward.
		if c.Ready(ctx, index) == nil {
			return nil
		}
		return fmt.Errorf("create stream index: %d %s", status, raw)
	}
	return nil
}

type IndexStatistics struct {
	Documents int64
	SizeBytes int64
}

var ErrIndexNotFound = errors.New("stream index is awaiting creation")

func (c *Client) IndexStats(ctx context.Context, index string) (IndexStatistics, error) {
	status, raw, err := c.jsonRequest(ctx, "GET", "/"+url.PathEscape(index)+"/_stats/docs,store", nil)
	if err != nil {
		return IndexStatistics{}, err
	}
	if status != 200 {
		if status == 404 {
			return IndexStatistics{}, ErrIndexNotFound
		}
		return IndexStatistics{}, fmt.Errorf("index statistics: %d", status)
	}
	var response struct {
		All struct {
			Primaries struct {
				Docs struct {
					Count *int64 `json:"count"`
				} `json:"docs"`
				Store struct {
					Size *int64 `json:"size_in_bytes"`
				} `json:"store"`
			} `json:"primaries"`
		} `json:"_all"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return IndexStatistics{}, err
	}
	if response.All.Primaries.Docs.Count == nil || response.All.Primaries.Store.Size == nil {
		return IndexStatistics{}, fmt.Errorf("incomplete index statistics")
	}
	return IndexStatistics{Documents: *response.All.Primaries.Docs.Count, SizeBytes: *response.All.Primaries.Store.Size}, nil
}
func (c *Client) DeleteStreamIndex(ctx context.Context, index string) error {
	status, raw, err := c.jsonRequest(ctx, "DELETE", "/"+url.PathEscape(index), nil)
	if err != nil {
		return err
	}
	if status != 200 && status != 404 {
		return fmt.Errorf("delete stream index: %d %s", status, raw)
	}
	if status == 200 {
		var response struct {
			Acknowledged bool `json:"acknowledged"`
		}
		if err := json.Unmarshal(raw, &response); err != nil {
			return err
		}
		if !response.Acknowledged {
			return fmt.Errorf("index deletion was not acknowledged")
		}
	}
	return nil
}
func (c *Client) PublishEvents(ctx context.Context, namespace string, events []db.Event) error {
	groups := map[string][]db.Event{}
	for _, event := range events {
		target := event.TargetIndex
		if target == "" {
			target = namespace
		}
		groups[target] = append(groups[target], event)
	}
	for target, items := range groups {
		if target != namespace {
			if err := c.EnsureStreamIndex(ctx, namespace, target); err != nil {
				return err
			}
		}
		if err := c.IndexEvents(ctx, target, items); err != nil {
			return err
		}
	}
	return nil
}
