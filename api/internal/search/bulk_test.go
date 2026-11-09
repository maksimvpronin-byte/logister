package search

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/logister/logister/api/internal/db"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPreparedIndexCacheBulkConcurrencyAndRecovery(t *testing.T) {
	var head, mapping, bulks, active, peak atomic.Int32
	var missing atomic.Bool
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "HEAD":
			head.Add(1)
		case strings.HasSuffix(r.URL.Path, "/_mapping"):
			mapping.Add(1)
			io.WriteString(w, `{}`)
		case r.URL.Path == "/_bulk":
			if got := r.URL.Query().Get("filter_path"); got != "errors,items.index.status,items.index.error,error" {
				t.Errorf("bulk response must retain item statuses and errors, got %q", got)
			}
			bulks.Add(1)
			if missing.Load() {
				w.WriteHeader(404)
				io.WriteString(w, `{}`)
				return
			}
			now := active.Add(1)
			for {
				old := peak.Load()
				if now <= old || peak.CompareAndSwap(old, now) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			defer active.Add(-1)
			raw, _ := io.ReadAll(r.Body)
			count := len(strings.Split(strings.TrimSpace(string(raw)), "\n")) / 2
			items := make([]any, count)
			for i := range items {
				items[i] = map[string]any{"index": map[string]any{"status": 201}}
			}
			json.NewEncoder(w).Encode(map[string]any{"errors": false, "items": items})
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer backend.Close()
	client, _ := NewClient(backend.URL)
	client.ConfigureBulk(2, 1)
	events := []db.Event{{ID: "a", Document: []byte(`{"message":"a"}`), TargetIndex: "index"}, {ID: "b", Document: []byte(`{"message":"b"}`), TargetIndex: "index"}}
	for i := 0; i < 2; i++ {
		if err := client.PublishEvents(context.Background(), "logs", events); err != nil {
			t.Fatal(err)
		}
	}
	if head.Load() != 1 || mapping.Load() != 1 || bulks.Load() != 4 || peak.Load() != 2 {
		t.Fatal(head.Load(), mapping.Load(), bulks.Load(), peak.Load())
	}
	missing.Store(true)
	if err := client.PublishEvents(context.Background(), "logs", events); !errors.Is(err, ErrIndexNotFound) {
		t.Fatal(err)
	}
	missing.Store(false)
	if err := client.PublishEvents(context.Background(), "logs", events); err != nil {
		t.Fatal(err)
	}
	if head.Load() != 2 || mapping.Load() != 2 {
		t.Fatal("deleted index was not re-prepared", head.Load(), mapping.Load())
	}
}

func TestConfiguredShardsApplyOnlyToNewIndices(t *testing.T) {
	for _, exists := range []bool{false, true} {
		t.Run(map[bool]string{false: "new", true: "existing"}[exists], func(t *testing.T) {
			creations := 0
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == "HEAD":
					if !exists {
						w.WriteHeader(404)
					}
				case r.Method == "PUT" && r.URL.Path == "/target":
					creations++
					var body map[string]any
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Fatal(err)
					}
					if body["settings"].(map[string]any)["number_of_shards"] != float64(2) {
						t.Error(body)
					}
					settings := body["settings"].(map[string]any)
					if settings["codec"] != "zstd_no_dict" || settings["codec.compression_level"] != float64(1) {
						t.Error(settings)
					}
					io.WriteString(w, `{"acknowledged":true}`)
				case r.URL.Path == "/target/_mapping":
					io.WriteString(w, `{}`)
				default:
					t.Errorf("unexpected mutation %s %s", r.Method, r.URL.Path)
				}
			}))
			defer backend.Close()
			client, _ := NewClient(backend.URL)
			client.ConfigureShards(2)
			if err := client.ConfigureCodec("zstd_no_dict", 1); err != nil {
				t.Fatal(err)
			}
			if err := client.EnsureStreamIndex(context.Background(), "logs", "target"); err != nil {
				t.Fatal(err)
			}
			want := 1
			if exists {
				want = 0
			}
			if creations != want {
				t.Fatal(creations, want)
			}
		})
	}
}

func TestCodecConfigurationRejectsInvalidValues(t *testing.T) {
	client, _ := NewClient("http://127.0.0.1:9200")
	for _, test := range []struct {
		codec string
		level int
	}{{"unknown", 1}, {"zstd_no_dict", 0}, {"zstd", 7}} {
		if err := client.ConfigureCodec(test.codec, test.level); err == nil {
			t.Fatal(test)
		}
	}
	if err := client.ConfigureCodec("default", 1); err != nil || client.codecLevel != 0 {
		t.Fatal(err, client.codecLevel)
	}
	if err := client.ConfigureCodec("zstd", 3); err != nil || client.codecLevel != 3 {
		t.Fatal(err, client.codecLevel)
	}
}

func TestDurableBulkWaitsForReplicasBeforeWriting(t *testing.T) {
	var health, bulks atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/target":
			if r.Method == http.MethodHead {
				w.WriteHeader(404)
				return
			}
			var body struct{ Settings map[string]any }
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Settings["translog.durability"] != "request" {
				t.Errorf("durable index settings: %v, %v", body, err)
			}
			io.WriteString(w, `{"acknowledged":true}`)
		case "/_cluster/health/target":
			if r.URL.Query().Get("wait_for_status") != "green" || r.URL.Query().Get("wait_for_no_relocating_shards") != "true" {
				t.Error(r.URL)
			}
			if health.Add(1) == 1 {
				io.WriteString(w, `{"status":"yellow","timed_out":true}`)
				return
			}
			io.WriteString(w, `{"status":"green","timed_out":false}`)
		case "/_bulk":
			bulks.Add(1)
			if r.URL.Query().Get("wait_for_active_shards") != "all" {
				t.Error(r.URL)
			}
			io.WriteString(w, `{"items":[{"index":{"status":201,"_shards":{"total":2,"successful":2}}}]}`)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			w.WriteHeader(500)
		}
	}))
	defer backend.Close()
	client, _ := NewClient(backend.URL)
	events := []db.Event{{ID: "a", TargetIndex: "target", Document: []byte(`{"message":"a"}`)}}
	if err := client.PublishEventsDurable(context.Background(), "logs", events); err == nil || bulks.Load() != 0 {
		t.Fatalf("published before shard copies were ready: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := client.PublishEventsDurable(context.Background(), "logs", events); err != nil {
			t.Fatal(err)
		}
	}
	if health.Load() != 2 || bulks.Load() != 2 {
		t.Fatalf("health checks %d, bulk requests %d", health.Load(), bulks.Load())
	}
}

func TestDurableBulkRetriesOnlyTheIncompleteJob(t *testing.T) {
	var retried, unaffected atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_bulk" {
			t.Errorf("unexpected request to %s", r.URL.Path)
			w.WriteHeader(500)
			return
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		switch string(raw) {
		case "{\"index\":{\"_id\":\"retry\",\"_index\":\"target\"}}\n{\"message\":\"first\"}\n":
			if retried.Add(1) <= 2 {
				io.WriteString(w, `{"items":[{"index":{"status":201,"_shards":{"total":3,"successful":2,"failed":0}}}]}`)
				return
			}
		case "{\"index\":{\"_id\":\"other\",\"_index\":\"target\"}}\n{\"message\":\"second\"}\n":
			unaffected.Add(1)
		default:
			t.Errorf("unexpected request body %s", raw)
		}
		io.WriteString(w, `{"items":[{"index":{"status":200,"_shards":{"total":2,"successful":2,"failed":0}}}]}`)
	}))
	defer backend.Close()
	client, _ := NewClient(backend.URL)
	client.ConfigureBulk(2, 1)
	client.prepared["target:durable"] = time.Now().Add(time.Minute)
	events := []db.Event{
		{ID: "retry", Document: []byte(`{"message":"first"}`)},
		{ID: "other", Document: []byte(`{"message":"second"}`)},
	}
	if err := client.PublishEventsDurable(context.Background(), "target", events); err != nil {
		t.Fatal(err)
	}
	if retried.Load() != 3 || unaffected.Load() != 1 {
		t.Fatalf("incomplete job sent %d times, other job sent %d times", retried.Load(), unaffected.Load())
	}
}

func TestDurableBulkReturnsUnconfirmedWrites(t *testing.T) {
	for _, tc := range []struct {
		name, shards string
		wantCalls    int32
		wantError    error
		cancel       bool
	}{
		{"retry limit", `{"total":3,"successful":2,"failed":0}`, 6, errIncompleteShardAcknowledgment, false},
		{"canceled", `{"total":3,"successful":2,"failed":0}`, 1, context.Canceled, true},
		{"explicit failure", `{"total":3,"successful":2,"failed":1}`, 1, nil, false},
		{"missing acknowledgment", `{}`, 1, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if tc.cancel {
					cancel()
				}
				io.WriteString(w, `{"items":[{"index":{"status":201,"_shards":`+tc.shards+`}}]}`)
			}))
			defer backend.Close()
			client, _ := NewClient(backend.URL)
			client.prepared["target:durable"] = time.Now().Add(time.Minute)
			event := db.Event{ID: "a", Document: []byte(`{}`)}
			err := client.PublishEventsDurable(ctx, "target", []db.Event{event})
			if err == nil || tc.wantError != nil && !errors.Is(err, tc.wantError) || calls.Load() != tc.wantCalls {
				t.Fatalf("got %d writes and error %v", calls.Load(), err)
			}
		})
	}
}
