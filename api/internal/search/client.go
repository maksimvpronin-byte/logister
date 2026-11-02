package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	opensearch "github.com/opensearch-project/opensearch-go/v2"
	"github.com/opensearch-project/opensearch-go/v2/opensearchapi"
)

type Client struct {
	os *opensearch.Client
}

func NewClient(url string) (*Client, error) {
	cfg := opensearch.Config{
		Addresses: []string{url},
		Transport: &http.Transport{
			ResponseHeaderTimeout: 30 * time.Second,
		},
	}
	osClient, err := opensearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return &Client{os: osClient}, nil
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
	ID        string         `json:"id"`
	Timestamp time.Time      `json:"timestamp"`
	Message   string         `json:"message"`
	Source    string         `json:"source"`
	Level     string         `json:"level"`
	Host      string         `json:"host"`
	Stream    string         `json:"stream"`
	Facility  string         `json:"facility,omitempty"`
	Fields    map[string]any `json:"fields,omitempty"`
}

type SearchParams struct {
	Query    string
	From     time.Time
	To       time.Time
	Stream   string
	Size     int
	Offset   int
}

type SearchResult struct {
	Total    int64     `json:"total"`
	TookMs   int64     `json:"took_ms"`
	Messages []Message `json:"messages"`
}

func (c *Client) Search(ctx context.Context, index string, p SearchParams) (*SearchResult, error) {
	if p.Size <= 0 || p.Size > 200 {
		p.Size = 50
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
				"@timestamp": map[string]any{
					"gte": p.From.UTC().Format(time.RFC3339Nano),
					"lte": p.To.UTC().Format(time.RFC3339Nano),
				},
			},
		},
	}
	if p.Stream != "" && p.Stream != "all" {
		must = append(must, map[string]any{
			"term": map[string]any{"stream": p.Stream},
		})
	}

	body := map[string]any{
		"from": p.Offset,
		"size": p.Size,
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
		return nil, fmt.Errorf("search: %s %s", res.Status(), string(raw))
	}

	var parsed struct {
		Took int64 `json:"took"`
		Hits struct {
			Total struct {
				Value int64 `json:"value"`
			} `json:"total"`
			Hits []struct {
				ID     string         `json:"_id"`
				Source map[string]any `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, err
	}

	out := &SearchResult{
		Total:    parsed.Hits.Total.Value,
		TookMs:   parsed.Took,
		Messages: make([]Message, 0, len(parsed.Hits.Hits)),
	}
	for _, hit := range parsed.Hits.Hits {
		out.Messages = append(out.Messages, mapHit(hit.ID, hit.Source))
	}
	return out, nil
}

func mapHit(id string, src map[string]any) Message {
	m := Message{
		ID:      id,
		Message: asString(src["message"]),
		Source:  asString(src["source"]),
		Level:   asString(src["level"]),
		Host:    asString(src["host"]),
		Stream:  asString(src["stream"]),
		Facility: asString(src["facility"]),
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
