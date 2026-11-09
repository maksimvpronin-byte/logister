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
func searchMetadataProperties() map[string]any {
	return map[string]any{
		"payload_json": map[string]any{"type": "text", "index": false},
		"service":      map[string]any{"type": "keyword"},
		"request_id":   map[string]any{"type": "keyword"},
		"trace_id":     map[string]any{"type": "keyword"},
		"status":       map[string]any{"type": "long"},
		"ip":           map[string]any{"type": "ip"},
	}
}
func storageDefinition(alias string) map[string]any {
	return map[string]any{"settings": map[string]any{
		"number_of_shards": 1, "number_of_replicas": 0,
		"refresh_interval": "30s", "translog.durability": "async", "translog.sync_interval": "5s",
	}, "aliases": map[string]any{alias: map[string]any{}}, "mappings": map[string]any{
		"_meta": map[string]any{"schema_version": 2, "ready": true}, "properties": map[string]any{
			"@timestamp": map[string]any{"type": "date"}, "event_id": map[string]any{"type": "keyword"}, "message": map[string]any{"type": "text"},
			"source": map[string]any{"type": "keyword"}, "level": map[string]any{"type": "keyword"}, "host": map[string]any{"type": "keyword"}, "stream": map[string]any{"type": "keyword"}, "facility": map[string]any{"type": "keyword"}, "fields": map[string]any{"type": "flat_object"},
			"payload_json": map[string]any{"type": "text", "index": false},
			"service":      map[string]any{"type": "keyword"}, "request_id": map[string]any{"type": "keyword"}, "trace_id": map[string]any{"type": "keyword"}, "status": map[string]any{"type": "long"}, "ip": map[string]any{"type": "ip"},
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
	status, raw, err := c.jsonRequest(ctx, "PUT", "/"+url.PathEscape(index)+"/_mapping", map[string]any{"properties": searchMetadataProperties()})
	if err != nil {
		return err
	}
	if status != 200 {
		return fmt.Errorf("prepare JSON payload mapping: %d %s", status, raw)
	}
	return nil
}
func (c *Client) EnsureStreamIndex(ctx context.Context, namespace, index string) error {
	return c.prepareOnce(ctx, index, func() error { return c.ensureStreamIndex(ctx, namespace, index, false) })
}
func (c *Client) ensureStreamIndex(ctx context.Context, namespace, index string, durable bool) error {
	status, _, err := c.jsonRequest(ctx, "HEAD", "/"+url.PathEscape(index), nil)
	if err != nil {
		return err
	}
	if status == 200 {
		return c.prepareExistingStream(ctx, index, durable)
	}
	if status != 404 {
		return fmt.Errorf("check stream index: %d", status)
	}
	definition := storageDefinition(ReadAlias(namespace))
	settings := definition["settings"].(map[string]any)
	settings["number_of_shards"] = max(1, c.indexShards)
	if durable {
		settings["translog.durability"] = "request"
		delete(settings, "translog.sync_interval")
	}
	if c.indexCodec != "" {
		settings["codec"] = c.indexCodec
		if c.codecLevel > 0 {
			settings["codec.compression_level"] = c.codecLevel
		}
	}
	status, raw, err := c.jsonRequest(ctx, "PUT", "/"+url.PathEscape(index), definition)
	if err != nil {
		return err
	}
	if status != 200 {
		// Another publisher can win creation; only accept a ready schema afterward.
		if c.Ready(ctx, index) == nil {
			if durable {
				return c.prepareExistingStream(ctx, index, true)
			}
			return nil
		}
		return fmt.Errorf("create stream index: %d %s", status, raw)
	}
	return nil
}

func (c *Client) prepareExistingStream(ctx context.Context, index string, durable bool) error {
	if err := c.ensurePayloadMapping(ctx, index); err != nil {
		return err
	}
	if durable {
		return c.setRequestDurability(ctx, index)
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
	c.forgetIndex(index)
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
	return c.publishEvents(ctx, namespace, events, false)
}
func (c *Client) PublishEventsDurable(ctx context.Context, namespace string, events []db.Event) error {
	return c.publishEvents(ctx, namespace, events, true)
}
