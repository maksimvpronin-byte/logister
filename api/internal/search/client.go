package search

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/logister/logister/api/internal/telemetry"
	"golang.org/x/sync/singleflight"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	opensearch "github.com/opensearch-project/opensearch-go/v2"
	"github.com/opensearch-project/opensearch-go/v2/opensearchapi"
)

type Client struct {
	telemetry    *telemetry.Meter
	os           *opensearch.Client
	cursorKey    [32]byte
	preparedMu   sync.Mutex
	prepared     map[string]time.Time
	prepareGroup singleflight.Group
	bulkWorkers  int
	bulkBytes    int
	bulkProfile  bool
	indexShards  int
	indexCodec   string
	codecLevel   int
}

func NewClient(url string) (*Client, error) {
	cfg := opensearch.Config{
		Addresses: []string{url},
		Transport: &http.Transport{
			ResponseHeaderTimeout: 30 * time.Second,
			MaxIdleConns:          64, MaxIdleConnsPerHost: 16, IdleConnTimeout: 90 * time.Second,
		},
	}
	osClient, err := opensearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	key, err := newCursorKey()
	if err != nil {
		return nil, err
	}
	return &Client{os: osClient, cursorKey: key, prepared: map[string]time.Time{}, bulkWorkers: 1, bulkBytes: 5 << 20}, nil
}

func (c *Client) Ping(ctx context.Context) error {
	res, err := c.os.Ping(c.os.Ping.WithContext(ctx))
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("opensearch ping: %s", res.Status())
	}
	return nil
}

func EnsureIndex(ctx context.Context, c *Client, index string) error {
	deadline := time.Now().Add(90 * time.Second)
	for {
		exists := opensearchapi.IndicesExistsRequest{Index: []string{index}}
		res, err := exists.Do(ctx, c.os)
		if err != nil {
			if time.Now().After(deadline) {
				return fmt.Errorf("wait opensearch: %w", err)
			}
			time.Sleep(2 * time.Second)
			continue
		}
		status := res.StatusCode
		_ = res.Body.Close()
		if status == 200 {
			return nil
		}
		break
	}

	body := map[string]any{
		"settings": map[string]any{
			"number_of_shards":   1,
			"number_of_replicas": 0,
		},
		"mappings": map[string]any{
			"properties": map[string]any{
				"@timestamp": map[string]any{"type": "date"},
				"message":    map[string]any{"type": "text"},
				"source":     map[string]any{"type": "keyword"},
				"level":      map[string]any{"type": "keyword"},
				"host":       map[string]any{"type": "keyword"},
				"stream":     map[string]any{"type": "keyword"},
				"facility":   map[string]any{"type": "keyword"},
				"fields":     map[string]any{"type": "object", "enabled": true},
			},
		},
	}
	payload, _ := json.Marshal(body)
	req := opensearchapi.IndicesCreateRequest{
		Index: index,
		Body:  bytes.NewReader(payload),
	}
	res, err := req.Do(ctx, c.os)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.IsError() && res.StatusCode != 400 {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("create index: %s %s", res.Status(), string(b))
	}
	return nil
}

type Message struct {
	Service     string          `json:"service,omitempty"`
	RequestID   string          `json:"request_id,omitempty"`
	TraceID     string          `json:"trace_id,omitempty"`
	Status      *int64          `json:"status,omitempty"`
	IP          string          `json:"ip,omitempty"`
	ID          string          `json:"id"`
	Timestamp   time.Time       `json:"timestamp"`
	Message     string          `json:"message"`
	Source      string          `json:"source"`
	Level       string          `json:"level"`
	Host        string          `json:"host"`
	Stream      string          `json:"stream"`
	Facility    string          `json:"facility,omitempty"`
	Fields      map[string]any  `json:"fields,omitempty"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	PayloadJSON string          `json:"payload_json,omitempty"`
}

type SearchParams struct {
	Query  string
	From   time.Time
	To     time.Time
	Stream string
	Size   int
	Offset int
}

type SearchResult struct {
	Total          int64     `json:"total"`
	TotalRelation  string    `json:"total_relation"`
	TookMs         int64     `json:"took_ms"`
	Messages       []Message `json:"messages"`
	From           time.Time `json:"from"`
	To             time.Time `json:"to"`
	ResultWindow   int       `json:"result_window"`
	NextCursor     string    `json:"next_cursor,omitempty"`
	PreviousCursor string    `json:"previous_cursor,omitempty"`
	SessionID      string    `json:"session_id,omitempty"`
	Offset         int       `json:"offset"`
}

const (
	DefaultPageSize = 50
	MaxPageSize     = 200
	MaxResultWindow = 10000
)

var ErrInvalidQuery = errors.New("invalid search query")

func timestampRange(from, to time.Time) map[string]any {
	// Explicit epoch milliseconds avoid OpenSearch filling omitted fractional
	// seconds up to the end of the second for an inclusive upper bound.
	// Managed and legacy timestamp mappings use the millisecond-resolution date type.
	return map[string]any{"gte": from.UnixMilli(), "lte": to.UnixMilli(), "format": "epoch_millis"}
}

func (c *Client) Search(ctx context.Context, index string, p SearchParams) (*SearchResult, error) {
	if p.Size <= 0 || p.Size > MaxPageSize {
		p.Size = DefaultPageSize
	}
	if p.Offset < 0 {
		p.Offset = 0
	}
	q := strings.TrimSpace(p.Query)
	if q == "" {
		q = "*"
	}

	must := []any{
		map[string]any{
			"query_string": map[string]any{
				"query":            q,
				"default_field":    "message",
				"analyze_wildcard": true,
				"lenient":          true,
			},
		},
		map[string]any{
			"range": map[string]any{
				"@timestamp": timestampRange(p.From, p.To),
			},
		},
	}
	if p.Stream != "" && p.Stream != "all" {
		stream := p.Stream
		if stream == "@default" {
			stream = "all"
		}
		must = append(must, map[string]any{
			"term": map[string]any{"stream": stream},
		})
	}

	body := map[string]any{
		"track_total_hits": true,
		"from":             p.Offset,
		"size":             p.Size,
		"sort": []any{
			map[string]any{"@timestamp": map[string]any{"order": "desc"}},
		},
		"query": map[string]any{
			"bool": map[string]any{"must": must},
		},
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	res, err := c.os.Search(
		c.os.Search.WithContext(ctx),
		c.os.Search.WithIndex(index),
		c.os.Search.WithBody(bytes.NewReader(payload)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	if res.IsError() {
		if res.StatusCode == http.StatusBadRequest {
			return nil, fmt.Errorf("%w: %s", ErrInvalidQuery, string(raw))
		}
		return nil, fmt.Errorf("search: %s %s", res.Status(), string(raw))
	}

	var parsed struct {
		Took int64 `json:"took"`
		Hits struct {
			Total struct {
				Value    int64  `json:"value"`
				Relation string `json:"relation"`
			} `json:"total"`
			Hits []struct {
				ID     string         `json:"_id"`
				Source map[string]any `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&parsed); err != nil {
		return nil, err
	}

	out := &SearchResult{
		Total:         parsed.Hits.Total.Value,
		TotalRelation: parsed.Hits.Total.Relation,
		TookMs:        parsed.Took,
		Messages:      make([]Message, 0, len(parsed.Hits.Hits)),
		From:          p.From, To: p.To, ResultWindow: MaxResultWindow,
	}
	for _, hit := range parsed.Hits.Hits {
		out.Messages = append(out.Messages, mapHit(hit.ID, hit.Source))
	}
	return out, nil
}

func mapHit(id string, src map[string]any) Message {
	m := Message{
		Service: asString(src["service"]), RequestID: asString(src["request_id"]), TraceID: asString(src["trace_id"]), IP: asString(src["ip"]),
		ID:       id,
		Message:  asString(src["message"]),
		Source:   asString(src["source"]),
		Level:    asString(src["level"]),
		Host:     asString(src["host"]),
		Stream:   asString(src["stream"]),
		Facility: asString(src["facility"]),
	}
	switch n := src["status"].(type) {
	case json.Number:
		if value, err := n.Int64(); err == nil {
			m.Status = &value
		}
	case float64:
		value := int64(n)
		m.Status = &value
	}
	if ts, ok := src["@timestamp"].(string); ok {
		if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
			m.Timestamp = t
		} else if t, err := time.Parse(time.RFC3339, ts); err == nil {
			m.Timestamp = t
		}
	}
	if fields, ok := src["fields"].(map[string]any); ok {
		m.Fields = fields
	}
	if payload, ok := src["payload_json"].(string); ok && json.Valid([]byte(payload)) {
		m.Payload = []byte(payload)
		m.PayloadJSON = payload
		var original map[string]any
		decoder := json.NewDecoder(strings.NewReader(payload))
		decoder.UseNumber()
		if decoder.Decode(&original) == nil {
			if fields, ok := original["fields"].(map[string]any); ok {
				m.Fields = fields
			}
		}
	}
	return m
}

func asString(v any) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	default:
		return fmt.Sprint(t)
	}
}

func (c *Client) IndexMessage(ctx context.Context, index string, id string, doc map[string]any) error {
	if id != "" {
		doc["event_id"] = id
	}
	payload, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	req := opensearchapi.IndexRequest{
		Index:      index,
		DocumentID: id,
		Body:       bytes.NewReader(payload),
		Refresh:    "true",
	}
	res, err := req.Do(ctx, c.os)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("index: %s %s", res.Status(), string(b))
	}
	return nil
}

func (c *Client) Count(ctx context.Context, index string) (int64, error) {
	res, err := c.os.Count(
		c.os.Count.WithContext(ctx),
		c.os.Count.WithIndex(index),
	)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	if res.IsError() {
		return 0, fmt.Errorf("count: %s %s", res.Status(), string(raw))
	}
	var parsed struct {
		Count int64 `json:"count"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return 0, err
	}
	return parsed.Count, nil
}

func (c *Client) SetTelemetry(m *telemetry.Meter) { c.telemetry = m }
