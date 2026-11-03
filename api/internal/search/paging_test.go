package search

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSnapshotCursorForwardBackwardAndTamper(t *testing.T) {
	var requests, closed atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/_mapping") {
			fmt.Fprint(w, `{"logs":{"mappings":{"_meta":{"ready":true}}}}`)
			return
		}
		if strings.Contains(r.URL.Path, "point_in_time") {
			if r.Method == "DELETE" {
				closed.Add(1)
			}
			fmt.Fprint(w, `{"pit_id":"snapshot"}`)
			return
		}
		var body struct {
			Size  int                 `json:"size"`
			After []json.RawMessage   `json:"search_after"`
			Sort  []map[string]string `json:"sort"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		reverse := body.Sort[1]["event_id"] == "desc"
		after := ""
		if len(body.After) > 1 {
			json.Unmarshal(body.After[1], &after)
		}
		hits := []any{}
		for k := 0; k < 25; k++ {
			i := k
			if reverse {
				i = 24 - k
			}
			id := fmt.Sprintf("event-%02d", i)
			if after != "" && ((!reverse && id <= after) || (reverse && id >= after)) {
				continue
			}
			hits = append(hits, map[string]any{"_id": id, "_source": map[string]any{"message": id}, "sort": []any{0, id}})
			if len(hits) == body.Size {
				break
			}
		}
		json.NewEncoder(w).Encode(map[string]any{"hits": map[string]any{"total": map[string]any{"value": 25, "relation": "eq"}, "hits": hits}})
	}))
	defer backend.Close()
	client, err := NewClient(backend.URL)
	if err != nil {
		t.Fatal(err)
	}
	first, err := client.SearchPage(context.Background(), "logs", SearchParams{Size: 3, From: time.Now().Add(-time.Hour), To: time.Now()}, "")
	if err != nil {
		t.Fatal(err)
	}
	if first.Messages[0].ID != "event-00" || first.NextCursor == "" || first.PreviousCursor != "" {
		t.Fatalf("first: %+v", first)
	}
	next, err := client.SearchPage(context.Background(), "logs", SearchParams{}, first.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	if next.Messages[0].ID != "event-03" || next.Offset != 3 {
		t.Fatalf("next: %+v", next)
	}
	previous, err := client.SearchPage(context.Background(), "logs", SearchParams{}, next.PreviousCursor)
	if err != nil {
		t.Fatal(err)
	}
	if previous.Messages[0].ID != "event-00" || previous.Offset != 0 {
		t.Fatalf("previous: %+v", previous)
	}
	before := requests.Load()
	_, err = client.SearchPage(context.Background(), "logs", SearchParams{}, "x"+first.NextCursor)
	if !errors.Is(err, ErrExpiredCursor) || requests.Load() != before {
		t.Fatal("tampered cursor reached OpenSearch")
	}
	if err := client.CloseSearch(context.Background(), first.SessionID); err != nil || closed.Load() != 1 {
		t.Fatalf("close=%d err=%v", closed.Load(), err)
	}
}

func TestFailedInitialQueryClosesSnapshot(t *testing.T) {
	var closed atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/_mapping") {
			fmt.Fprint(w, `{"logs":{"mappings":{"_meta":{"ready":true}}}}`)
			return
		}
		if r.Method == "DELETE" {
			closed.Add(1)
			fmt.Fprint(w, `{}`)
			return
		}
		if strings.Contains(r.URL.Path, "point_in_time") {
			fmt.Fprint(w, `{"pit_id":"snapshot"}`)
			return
		}
		w.WriteHeader(400)
		fmt.Fprint(w, `{"error":"bad query"}`)
	}))
	defer backend.Close()
	client, _ := NewClient(backend.URL)
	_, err := client.SearchPage(context.Background(), "logs", SearchParams{Size: 50}, "")
	if !errors.Is(err, ErrInvalidQuery) || closed.Load() != 1 {
		t.Fatalf("err=%v closed=%d", err, closed.Load())
	}
}

func TestAliasSearchClassifiesPartialQueryFailures(t *testing.T) {
	for _, kind := range []string{"query_shard_exception", "parse_exception", "no_shard_available_action_exception"} {
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/_mapping") {
				fmt.Fprint(w, `{"physical-1":{"mappings":{"_meta":{"ready":true}}}}`)
				return
			}
			if strings.Contains(r.URL.Path, "point_in_time") {
				fmt.Fprint(w, `{"pit_id":"snapshot"}`)
				return
			}
			fmt.Fprintf(w, `{"_shards":{"failed":1,"failures":[{"reason":{"type":%q}}]},"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}`, kind)
		}))
		client, err := NewClient(backend.URL)
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.SearchPage(context.Background(), "read-alias", SearchParams{Size: 10}, "")
		backend.Close()
		if err == nil {
			t.Fatal("partial failure was accepted")
		}
		if errors.Is(err, ErrInvalidQuery) != (kind != "no_shard_available_action_exception") {
			t.Fatal(kind, err)
		}
	}
}
