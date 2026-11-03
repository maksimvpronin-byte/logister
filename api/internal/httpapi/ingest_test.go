package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/logister/logister/api/internal/config"
	"github.com/logister/logister/api/internal/db"
)

type ingestStore struct {
	mu       sync.Mutex
	docs     []map[string]any
	attempts int
	failAt   int
	hashes   map[string]string
	keys     map[string]string
}

func (store *ingestStore) Accept(_ context.Context, index, key, hash string, events []db.Event) (db.Receipt, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.failAt > 0 {
		return db.Receipt{}, errors.New("simulated unavailable storage")
	}
	if old, ok := store.keys[key]; key != "" && ok && old != hash {
		return db.Receipt{}, db.ErrIdentityConflict
	}
	for _, event := range events {
		if old, ok := store.hashes[event.ID]; ok && old != event.Hash {
			return db.Receipt{}, db.ErrIdentityConflict
		}
	}
	receipt := db.Receipt{Count: len(events)}
	for _, event := range events {
		if _, ok := store.hashes[event.ID]; ok {
			receipt.Duplicates++
			continue
		}
		var doc map[string]any
		_ = json.Unmarshal(event.Document, &doc)
		store.docs = append(store.docs, doc)
		store.hashes[event.ID] = event.Hash
	}
	if key != "" {
		store.keys[key] = hash
	}
	return receipt, nil
}

func (store *ingestStore) snapshots() []map[string]any {
	store.mu.Lock()
	defer store.mu.Unlock()
	return append([]map[string]any(nil), store.docs...)
}

func ingestHandler(t *testing.T, failAt int) (http.Handler, *ingestStore) {
	t.Helper()
	store := &ingestStore{failAt: failAt, hashes: map[string]string{}, keys: map[string]string{}}
	server := NewServer(config.Config{OpenSearchIndex: "logs"}, nil, nil)
	server.store = store
	return server.Handler(), store
}

func ingestCall(t *testing.T, handler http.Handler, payload []byte, chunked bool) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/ingest/json", bytes.NewReader(payload))
	if chunked {
		req.ContentLength = -1
		req.TransferEncoding = []string{"chunked"}
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v; body=%s", err, recorder.Body.String())
	}
	return recorder, body
}

func TestIngestRejectsWholeInvalidBatch(t *testing.T) {
	handler, store := ingestHandler(t, 0)
	payload := []byte(`[{"message":"valid first message"},null]`)
	for attempt := 0; attempt < 2; attempt++ {
		recorder, _ := ingestCall(t, handler, payload, false)
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("status=%d, want 400; body=%s", recorder.Code, recorder.Body.String())
		}
	}
	if docs := store.snapshots(); len(docs) != 0 {
		t.Fatalf("rejected batch wrote %d documents after retry, want 0", len(docs))
	}
}

func TestIngestBodyLimit(t *testing.T) {
	const limit = 1 << 20
	prefix := []byte(`{"message":"body limit check"}`)
	exactLimit := append(append([]byte{}, prefix...), bytes.Repeat([]byte(" "), limit-len(prefix))...)
	for _, tc := range []struct {
		name    string
		payload []byte
		chunked bool
		status  int
		writes  int
	}{
		{"exact limit accepted", exactLimit, false, http.StatusAccepted, 1},
		{"invalid suffix past limit", append(append([]byte{}, exactLimit...), []byte("garbage")...), false, http.StatusRequestEntityTooLarge, 0},
		{"oversized valid JSON", []byte(`{"message":"` + strings.Repeat("a", limit) + `"}`), false, http.StatusRequestEntityTooLarge, 0},
		{"unknown content length", append(append([]byte{}, exactLimit...), ' '), true, http.StatusRequestEntityTooLarge, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler, store := ingestHandler(t, 0)
			recorder, body := ingestCall(t, handler, tc.payload, tc.chunked)
			if recorder.Code != tc.status {
				t.Errorf("status=%d, want %d; body=%v", recorder.Code, tc.status, body)
			}
			if docs := store.snapshots(); len(docs) != tc.writes {
				t.Errorf("wrote %d documents, want %d", len(docs), tc.writes)
			}
		})
	}
}

func TestIngestPreservesSupportedPayloads(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload string
		count   int
	}{
		{"single", `{"message":"  hello  "}`, 1},
		{"batch", `[{"message":"hello"},{"message":"second"}]`, 2},
		{"fluent bit wrapper", `[{"log":"{\"message\":\"hello\",\"source\":\"nginx\",\"timestamp\":\"2026-11-03T00:00:00Z\"}"}]`, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler, store := ingestHandler(t, 0)
			recorder, body := ingestCall(t, handler, []byte(tc.payload), false)
			if recorder.Code != http.StatusAccepted || body["count"] != float64(tc.count) {
				t.Fatalf("status=%d body=%v", recorder.Code, body)
			}
			docs := store.snapshots()
			if len(docs) != tc.count {
				t.Fatalf("wrote %d docs, want %d", len(docs), tc.count)
			}
			if docs[0]["message"] != "hello" || docs[0]["level"] != "info" || docs[0]["stream"] != "all" {
				t.Errorf("normalization not preserved: %v", docs[0])
			}
			if tc.name == "fluent bit wrapper" && (docs[0]["source"] != "nginx" || docs[0]["@timestamp"] != "2026-11-03T00:00:00Z") {
				t.Errorf("wrapped fields not preserved: %v", docs[0])
			}
		})
	}
}

func TestIngestStorageFailureRejectsWholeBatch(t *testing.T) {
	handler, store := ingestHandler(t, 2)
	recorder, body := ingestCall(t, handler, []byte(`[{"message":"first"},{"message":"second"},{"message":"third"}]`), false)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503", recorder.Code)
	}
	if body["count"] != float64(0) {
		t.Errorf("unexpected partial receipt: %v", body)
	}
	if docs := store.snapshots(); len(docs) != 0 {
		t.Errorf("wrote %d documents, want 0", len(docs))
	}
}

func TestIngestStableIdentitiesAndConflicts(t *testing.T) {
	handler, store := ingestHandler(t, 0)
	for i := 0; i < 2; i++ {
		r, _ := ingestCall(t, handler, []byte(`{"id":"stable","message":"hello"}`), false)
		if r.Code != 202 {
			t.Fatal(r.Body.String())
		}
	}
	if len(store.snapshots()) != 1 {
		t.Fatal("retry duplicated the event")
	}
	r, _ := ingestCall(t, handler, []byte(`{"id":"stable","message":"different"}`), false)
	if r.Code != 409 {
		t.Fatal(r.Body.String())
	}
	for i := 0; i < 2; i++ {
		r, _ := ingestCall(t, handler, []byte(`{"message":"intentional repeat"}`), false)
		if r.Code != 202 {
			t.Fatal(r.Body.String())
		}
	}
	if len(store.snapshots()) != 3 {
		t.Fatal("independent events were collapsed")
	}
}

func TestIngestBatchIdempotencyKey(t *testing.T) {
	handler, store := ingestHandler(t, 0)
	for _, port := range []string{"1000", "2000"} {
		request := httptest.NewRequest("POST", "/api/ingest/json", strings.NewReader(`[{"message":"one"},{"message":"two"}]`))
		request.RemoteAddr = "192.0.2.1:" + port
		request.Header.Set("Idempotency-Key", "lost-ack")
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != 202 {
			t.Fatal(recorder.Body.String())
		}
	}
	if len(store.snapshots()) != 2 {
		t.Fatal("batch retry duplicated records")
	}
}
