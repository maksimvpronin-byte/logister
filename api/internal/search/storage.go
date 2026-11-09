package search

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/logister/logister/api/internal/db"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var errIncompleteShardAcknowledgment = errors.New("bulk item did not reach every shard copy")

// StorageIndex is versioned so upgrading the field representation never deletes
// the original index. The old concrete index remains available as a backup.
func StorageIndex(base string) string { return base + "-v2" }

func (c *Client) jsonRequest(ctx context.Context, method, path string, body any) (int, []byte, error) {
	var encoded []byte
	var err error
	if body != nil {
		encoded, err = json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, path, bytes.NewReader(encoded))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := c.os.Perform(req)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	return response.StatusCode, data, err
}

func (c *Client) Ready(ctx context.Context, index string) error {
	status, raw, err := c.jsonRequest(ctx, http.MethodGet, "/"+url.PathEscape(index)+"/_mapping", nil)
	if err != nil {
		return err
	}
	if status != 200 {
		return fmt.Errorf("index unavailable: %d", status)
	}
	var mapping map[string]struct {
		Mappings struct {
			Meta struct {
				Ready bool `json:"ready"`
			} `json:"_meta"`
		} `json:"mappings"`
	}
	if err := json.Unmarshal(raw, &mapping); err != nil {
		return err
	}
	if len(mapping) == 0 {
		return fmt.Errorf("index mapping is empty")
	}
	for _, entry := range mapping {
		if !entry.Mappings.Meta.Ready {
			return fmt.Errorf("index migration is not complete")
		}
	}
	return nil
}

// PrepareStorage can be retried after interruption: reindexing preserves IDs.
// The caller must quiesce legacy writers before migrating a live installation.
func (c *Client) PrepareStorage(ctx context.Context, base string) error {
	target := StorageIndex(base)
	path := "/" + url.PathEscape(target)
	status, raw, err := c.jsonRequest(ctx, "GET", path+"/_mapping", nil)
	if err != nil {
		return err
	}
	if status == 200 {
		var mapping map[string]struct {
			Mappings struct {
				Meta struct {
					Ready bool `json:"ready"`
				} `json:"_meta"`
			} `json:"mappings"`
		}
		if err := json.Unmarshal(raw, &mapping); err != nil {
			return err
		}
		if mapping[target].Mappings.Meta.Ready {
			return nil
		}
	} else if status == 404 {
		body := map[string]any{"settings": map[string]any{"number_of_shards": 1, "number_of_replicas": 0}, "mappings": map[string]any{
			"_meta": map[string]any{"schema_version": 2, "ready": false}, "properties": map[string]any{
				"@timestamp": map[string]any{"type": "date"}, "event_id": map[string]any{"type": "keyword"},
				"message": map[string]any{"type": "text"}, "source": map[string]any{"type": "keyword"}, "level": map[string]any{"type": "keyword"},
				"host": map[string]any{"type": "keyword"}, "stream": map[string]any{"type": "keyword"}, "facility": map[string]any{"type": "keyword"},
				"fields": map[string]any{"type": "flat_object"},
			}}}
		status, raw, err = c.jsonRequest(ctx, "PUT", path, body)
		if err != nil {
			return err
		}
		if status != 200 {
			return fmt.Errorf("create storage: %d %s", status, raw)
		}
	} else {
		return fmt.Errorf("read storage mapping: %d %s", status, raw)
	}
	legacy, _, err := c.jsonRequest(ctx, "HEAD", "/"+url.PathEscape(base), nil)
	if err != nil {
		return err
	}
	if legacy == 200 {
		status, raw, err = c.jsonRequest(ctx, "POST", "/_reindex?wait_for_completion=true&refresh=true", map[string]any{
			"source": map[string]any{"index": base}, "dest": map[string]any{"index": target},
			"script": map[string]any{"lang": "painless", "source": "ctx._source.event_id = ctx._id"},
		})
		if err != nil {
			return err
		}
		var result struct {
			Failures []json.RawMessage `json:"failures"`
			TimedOut bool              `json:"timed_out"`
		}
		if err := json.Unmarshal(raw, &result); err != nil {
			return err
		}
		if status != 200 || len(result.Failures) > 0 || result.TimedOut {
			return fmt.Errorf("legacy reindex incomplete: %d %s", status, raw)
		}
		oldCount, err := c.Count(ctx, base)
		if err != nil {
			return err
		}
		newCount, err := c.Count(ctx, target)
		if err != nil {
			return err
		}
		if oldCount != newCount {
			return fmt.Errorf("legacy count mismatch: old=%d new=%d", oldCount, newCount)
		}
	} else if legacy != 404 {
		return fmt.Errorf("check legacy index: %d", legacy)
	}
	status, raw, err = c.jsonRequest(ctx, "PUT", path+"/_mapping", map[string]any{"_meta": map[string]any{"schema_version": 2, "ready": true}})
	if err != nil {
		return err
	}
	if status != 200 {
		return fmt.Errorf("finish migration: %d %s", status, raw)
	}
	return nil
}

func (c *Client) IndexEvents(ctx context.Context, index string, events []db.Event) error {
	return c.indexEvents(ctx, index, events, false)
}

func bulkBody(index string, events []db.Event) *bytes.Buffer {
	quotedIndex, _ := json.Marshal(index)
	const actionPrefix = `{"index":{"_id":`
	const indexField = `,"_index":`
	const actionSuffix = "}}\n"
	metadataBytes := len(actionPrefix) + len(indexField) + len(quotedIndex) + len(actionSuffix) + 3
	size := 0
	for _, event := range events {
		size += metadataBytes + len(event.ID) + len(event.Document)
	}
	body := bytes.NewBuffer(make([]byte, 0, size))
	for _, event := range events {
		quotedID, _ := json.Marshal(event.ID)
		body.WriteString(actionPrefix)
		body.Write(quotedID)
		body.WriteString(indexField)
		body.Write(quotedIndex)
		body.WriteString(actionSuffix)
		body.Write(event.Document)
		body.WriteByte('\n')
	}
	return body
}

func (c *Client) indexEvents(ctx context.Context, index string, events []db.Event, durable bool) (err error) {
	started := time.Now()
	var sent time.Time
	status, payloadBytes := 0, 0
	rejected := false
	if c.telemetry != nil {
		defer func() { c.telemetry.Bulk(payloadBytes, time.Since(started), err != nil, rejected) }()
	}
	if c.bulkProfile {
		defer func() {
			finished := time.Now()
			buildMS, requestMS := finished.Sub(started).Seconds()*1000, 0.0
			if !sent.IsZero() {
				buildMS, requestMS = sent.Sub(started).Seconds()*1000, finished.Sub(sent).Seconds()*1000
			}
			raw, _ := json.Marshal(map[string]any{"index": index, "documents": len(events), "bytes": payloadBytes, "build_ms": buildMS, "request_ms": requestMS, "total_ms": finished.Sub(started).Seconds() * 1000, "status": status, "success": err == nil})
			log.Printf("ingest_bulk_profile %s", raw)
		}()
	}
	body := bulkBody(index, events)
	payloadBytes = body.Len()
	path := "/_bulk?filter_path=errors,items.index.status,items.index.error,error"
	if durable {
		path += ",items.index._shards&wait_for_active_shards=all"
	}
	req, err := http.NewRequestWithContext(ctx, "POST", path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	sent = time.Now()
	response, err := c.os.Perform(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	status = response.StatusCode
	rejected = status == 429
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	var result struct {
		Errors bool `json:"errors"`
		Items  []struct {
			Index struct {
				Status int `json:"status"`
				Shards struct {
					Total      int `json:"total"`
					Successful int `json:"successful"`
					Failed     int `json:"failed"`
				} `json:"_shards"`
			} `json:"index"`
		} `json:"items"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return err
	}
	missing := response.StatusCode == 404
	for _, item := range result.Items {
		missing = missing || item.Index.Status == 404
		rejected = rejected || item.Index.Status == 429
	}
	if missing {
		c.forgetIndex(index)
		return fmt.Errorf("%w: %s", ErrIndexNotFound, index)
	}
	if response.StatusCode >= 300 || result.Errors || len(result.Items) != len(events) {
		return fmt.Errorf("bulk publication not acknowledged: %d %s", response.StatusCode, strings.TrimSpace(string(raw)))
	}
	var incomplete error
	for _, item := range result.Items {
		if item.Index.Status < 200 || item.Index.Status >= 300 {
			return fmt.Errorf("bulk item not acknowledged: %d", item.Index.Status)
		}
		if !durable {
			continue
		}
		shards := item.Index.Shards
		if shards.Failed != 0 || shards.Successful <= 0 || shards.Successful > shards.Total {
			return fmt.Errorf("invalid or failed bulk shard acknowledgments: %+v", shards)
		}
		if shards.Successful < shards.Total && incomplete == nil {
			incomplete = fmt.Errorf("%w: %+v", errIncompleteShardAcknowledgment, shards)
		}
	}
	return incomplete
}
