package search

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

var ErrExpiredCursor = errors.New("search session expired or invalid")

type pageCursor struct {
	PIT           string            `json:"pit"`
	After         []json.RawMessage `json:"after,omitempty"`
	Query, Stream string
	From, To      time.Time
	Size, Offset  int
	Reverse       bool
}

func (c *Client) sealCursor(cursor pageCursor) string {
	raw, _ := json.Marshal(cursor)
	mac := hmac.New(sha256.New, c.cursorKey[:])
	mac.Write(raw)
	return base64.RawURLEncoding.EncodeToString(raw) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
func (c *Client) openCursor(token string) (pageCursor, error) {
	var cursor pageCursor
	if len(token) > 16384 {
		return cursor, ErrExpiredCursor
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return cursor, ErrExpiredCursor
	}
	raw, e := base64.RawURLEncoding.DecodeString(parts[0])
	if e != nil {
		return cursor, ErrExpiredCursor
	}
	signature, e := base64.RawURLEncoding.DecodeString(parts[1])
	if e != nil {
		return cursor, ErrExpiredCursor
	}
	mac := hmac.New(sha256.New, c.cursorKey[:])
	mac.Write(raw)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return cursor, ErrExpiredCursor
	}
	if e = json.Unmarshal(raw, &cursor); e != nil || cursor.PIT == "" || cursor.Size < 1 || cursor.Size > MaxPageSize || cursor.Offset < 0 {
		return cursor, ErrExpiredCursor
	}
	return cursor, nil
}

func (c *Client) CloseSearch(ctx context.Context, pit string) error {
	if pit == "" {
		return nil
	}
	if len(pit) > 16384 {
		return ErrExpiredCursor
	}
	status, raw, err := c.jsonRequest(ctx, "DELETE", "/_search/point_in_time", map[string]any{"pit_id": []string{pit}})
	if err != nil {
		return err
	}
	if status >= 300 && status != 404 {
		return fmt.Errorf("close PIT: %d %s", status, raw)
	}
	return nil
}

func (c *Client) SearchPage(ctx context.Context, index string, p SearchParams, token string) (result *SearchResult, err error) {
	var cursor pageCursor
	created := false
	if token != "" {
		cursor, err = c.openCursor(token)
		if err != nil {
			return nil, err
		}
	} else {
		if e := c.Ready(ctx, index); e != nil {
			return nil, e
		}
		status, raw, e := c.jsonRequest(ctx, "POST", "/"+url.PathEscape(index)+"/_search/point_in_time?keep_alive=2m", nil)
		if e != nil {
			return nil, e
		}
		if status != 200 {
			return nil, fmt.Errorf("create search snapshot: %d %s", status, raw)
		}
		var response struct {
			ID string `json:"pit_id"`
		}
		if json.Unmarshal(raw, &response) != nil || response.ID == "" {
			return nil, errors.New("invalid PIT response")
		}
		cursor = pageCursor{PIT: response.ID, Query: p.Query, Stream: p.Stream, From: p.From, To: p.To, Size: p.Size, Offset: p.Offset}
		created = true
	}
	defer func() {
		if created && err != nil {
			cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = c.CloseSearch(cleanup, cursor.PIT)
		}
	}()
	q := strings.TrimSpace(cursor.Query)
	if q == "" {
		q = "*"
	}
	must := []any{map[string]any{"query_string": map[string]any{"query": q, "default_field": "message", "analyze_wildcard": true, "lenient": true}}, map[string]any{"range": map[string]any{"@timestamp": timestampRange(cursor.From, cursor.To)}}}
	if cursor.Stream != "" && cursor.Stream != "all" {
		stream := cursor.Stream
		if stream == "@default" {
			stream = "all"
		}
		must = append(must, map[string]any{"term": map[string]any{"stream": stream}})
	}
	timestampOrder, idOrder := "desc", "asc"
	if cursor.Reverse {
		timestampOrder, idOrder = "asc", "desc"
	}
	body := map[string]any{"pit": map[string]any{"id": cursor.PIT, "keep_alive": "2m"}, "track_total_hits": true, "size": cursor.Size,
		"sort": []any{map[string]any{"@timestamp": timestampOrder}, map[string]any{"event_id": idOrder}}, "query": map[string]any{"bool": map[string]any{"must": must}}}
	if len(cursor.After) > 0 {
		body["search_after"] = cursor.After
	} else if cursor.Offset > 0 {
		body["from"] = cursor.Offset
	}
	status, raw, e := c.jsonRequest(ctx, "POST", "/_search", body)
	if e != nil {
		return nil, e
	}
	if status == 404 {
		return nil, ErrExpiredCursor
	}
	if status == 400 {
		return nil, fmt.Errorf("%w: %s", ErrInvalidQuery, raw)
	}
	if status != 200 {
		return nil, fmt.Errorf("snapshot search: %d %s", status, raw)
	}
	var parsed struct {
		PIT      string `json:"pit_id"`
		Took     int64  `json:"took"`
		TimedOut bool   `json:"timed_out"`
		Shards   struct {
			Failed   int `json:"failed"`
			Failures []struct {
				Reason json.RawMessage `json:"reason"`
			} `json:"failures"`
		} `json:"_shards"`
		Hits struct {
			Total struct {
				Value    int64  `json:"value"`
				Relation string `json:"relation"`
			} `json:"total"`
			Hits []struct {
				ID     string            `json:"_id"`
				Source map[string]any    `json:"_source"`
				Sort   []json.RawMessage `json:"sort"`
			} `json:"hits"`
		} `json:"hits"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if e = decoder.Decode(&parsed); e != nil {
		return nil, e
	}
	if parsed.TimedOut || parsed.Shards.Failed > 0 {
		// Empty shards can succeed while populated shards reject the expression,
		// making OpenSearch return HTTP 200 with partial failures across an alias.
		queryFailure := !parsed.TimedOut && len(parsed.Shards.Failures) > 0
		for _, failure := range parsed.Shards.Failures {
			queryFailure = queryFailure && isQueryShardFailure(failure.Reason)
		}
		if queryFailure {
			return nil, fmt.Errorf("%w: %s", ErrInvalidQuery, raw)
		}
		return nil, errors.New("search returned incomplete results")
	}
	if parsed.PIT != "" {
		cursor.PIT = parsed.PIT
	}
	hits := parsed.Hits.Hits
	if cursor.Reverse {
		for i, j := 0, len(hits)-1; i < j; i, j = i+1, j-1 {
			hits[i], hits[j] = hits[j], hits[i]
		}
	}
	result = &SearchResult{Total: parsed.Hits.Total.Value, TotalRelation: parsed.Hits.Total.Relation, TookMs: parsed.Took, From: cursor.From, To: cursor.To, Messages: make([]Message, 0, len(hits)), SessionID: cursor.PIT, Offset: cursor.Offset}
	for _, hit := range hits {
		result.Messages = append(result.Messages, mapHit(hit.ID, hit.Source))
	}
	if len(hits) > 0 {
		if int64(cursor.Offset+len(hits)) < result.Total {
			next := cursor
			next.Offset += len(hits)
			next.After = hits[len(hits)-1].Sort
			next.Reverse = false
			result.NextCursor = c.sealCursor(next)
		}
		if cursor.Offset > 0 {
			previous := cursor
			previous.Offset -= cursor.Size
			if previous.Offset < 0 {
				previous.Offset = 0
			}
			previous.After = hits[0].Sort
			previous.Reverse = true
			result.PreviousCursor = c.sealCursor(previous)
		}
	} else if created {
		_ = c.CloseSearch(ctx, cursor.PIT)
		result.SessionID = ""
	}
	return result, nil
}

func isQueryShardFailure(raw json.RawMessage) bool {
	var reason struct {
		Type     string          `json:"type"`
		CausedBy json.RawMessage `json:"caused_by"`
	}
	if json.Unmarshal(raw, &reason) != nil {
		return false
	}
	switch reason.Type {
	case "query_shard_exception", "parse_exception", "parsing_exception", "query_parsing_exception":
		return true
	}
	return false
}

func newCursorKey() ([32]byte, error) { var key [32]byte; _, err := rand.Read(key[:]); return key, err }
