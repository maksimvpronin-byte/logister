package httpapi

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/logister/logister/api/internal/config"
	"github.com/logister/logister/api/internal/db"
)

type fakeCatalog struct {
	streams []db.Stream
	saves   int
}

func (c *fakeCatalog) List(context.Context) ([]db.Stream, error) { return c.streams, nil }
func (c *fakeCatalog) Save(_ context.Context, stream db.Stream, create bool) (db.Stream, error) {
	c.saves++
	if create {
		for _, old := range c.streams {
			if old.Name == stream.Name {
				return db.Stream{}, db.ErrStreamExists
			}
		}
	}
	stream.ID = "test-id"
	return stream, nil
}
func TestRoutingPriorityExplicitStreamAndFields(t *testing.T) {
	streams := []db.Stream{{Name: "all"}, {Name: "errors", Routing: db.RoutingConfig{Enabled: true, Priority: 20, Match: "any", Rules: []db.RoutingRule{{Field: "level", Operator: "equals", Value: "error"}}}},
		{Name: "billing", Routing: db.RoutingConfig{Enabled: true, Priority: 10, Match: "all", Rules: []db.RoutingRule{{Field: "source", Operator: "starts_with", Value: "billing"}, {Field: "fields.status", Operator: "equals", Value: "500"}}}}}
	req := ingestRequest{Source: "billing-api", Level: "error", Fields: map[string]any{"status": float64(500)}}
	result, err := routeIngest(req, streams)
	if err != nil || result.Stream != "billing" {
		t.Fatal(result, err)
	}
	req.Stream = "errors"
	result, err = routeIngest(req, streams)
	if err != nil || result.Stream != "errors" {
		t.Fatal(result, err)
	}
	req.Stream = "missing"
	if _, err := routeIngest(req, streams); err == nil {
		t.Fatal("unknown stream was accepted")
	}
	req = ingestRequest{Source: "other", Fields: map[string]any{"nested": map[string]any{"flag": true}}}
	if value, ok := routingValue(req, "fields.nested.flag"); !ok || value != "true" {
		t.Fatal(value, ok)
	}
	result, err = routeIngest(req, streams)
	if err != nil || result.Stream != "all" {
		t.Fatal(result, err)
	}
}
func TestStreamSettingsValidationBeforeSave(t *testing.T) {
	catalog := &fakeCatalog{}
	server := NewServer(config.Config{}, nil, nil)
	server.catalog = catalog
	for _, tc := range []struct {
		method, path, body string
		status             int
	}{
		{"POST", "/api/streams", `{"name":"payments","title":"Payments"}`, 201},
		{"POST", "/api/streams", `{"name":"../logs","title":"Bad"}`, 400},
		{"PUT", "/api/streams/payments", `{"name":"other","title":"Bad"}`, 400},
		{"POST", "/api/streams", `{"name":"payments","title":"Bad","storage":{"max_age_hours":-1}}`, 400},
		{"POST", "/api/streams", `{"name":"payments","title":"Bad","unknown":1}`, 400},
		{"POST", "/api/streams", `{"name":"payments","title":"Bad"} {}`, 400},
	} {
		w := httptest.NewRecorder()
		server.Handler().ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body)))
		if w.Code != tc.status {
			t.Fatalf("%s %s: %d %s", tc.method, tc.body, w.Code, w.Body.String())
		}
	}
	if catalog.saves != 1 {
		t.Fatal("invalid settings reached persistence", catalog.saves)
	}
}
func TestScopedIngestAndUnknownStreamAreAtomic(t *testing.T) {
	store := &ingestStore{hashes: map[string]string{}, keys: map[string]string{}}
	server := NewServer(config.Config{OpenSearchIndex: "logs-v2"}, nil, nil)
	server.store = store
	server.catalog = &fakeCatalog{streams: []db.Stream{{Name: "all"}, {Name: "payments"}}}
	for _, tc := range []struct {
		path, body string
		status     int
	}{
		{"/api/streams/payments/ingest", `{"id":"event","message":"hello"}`, 202},
		{"/api/streams/payments/ingest", `{"id":"event","message":"hello"}`, 202},
		{"/api/streams/payments/ingest", `{"message":"hello","stream":"all"}`, 400},
		{"/api/ingest/json", `[{"message":"valid"},{"message":"bad","stream":"missing"}]`, 400},
	} {
		w := httptest.NewRecorder()
		server.Handler().ServeHTTP(w, httptest.NewRequest("POST", tc.path, strings.NewReader(tc.body)))
		if w.Code != tc.status {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	docs := store.snapshots()
	if len(docs) != 1 || docs[0]["stream"] != "payments" {
		raw, _ := json.Marshal(docs)
		t.Fatal(string(raw))
	}
}

func TestRoutingPreviewUsesDraftAndNeverWrites(t *testing.T) {
	store := &ingestStore{hashes: map[string]string{}, keys: map[string]string{}}
	catalog := &fakeCatalog{streams: []db.Stream{{Name: "all"}, {Name: "web-access", Title: "Web access", Routing: db.RoutingConfig{Enabled: true, Priority: 100, Match: "all", Rules: []db.RoutingRule{{Field: "source", Operator: "equals", Value: "nginx"}}}}}}
	server := NewServer(config.Config{}, nil, nil)
	server.catalog = catalog
	server.store = store
	for _, tc := range []struct {
		body, stream, reason string
		status               int
	}{
		{`{"message":{"source":"nginx","message":"GET /"}}`, "web-access", "rule", 200},
		{`{"message":{"source":"other","message":"hello"}}`, "all", "fallback", 200},
		{`{"message":{"source":"nginx","message":"hello","stream":"all"}}`, "all", "explicit", 200},
		{`{"message":{"source":"nginx","message":"hello"},"draft":{"name":"web-access","title":"Web access","routing":{"enabled":true,"priority":100,"match":"all","rules":[{"field":"source","operator":"equals","value":"custom"}]},"storage":{"max_age_hours":24}}}`, "all", "fallback", 200},
		{`{"message":{"message":"hello","stream":"missing"}}`, "", "", 400},
		{`{"message":{"message":"hello"}} {}`, "", "", 400},
	} {
		w := httptest.NewRecorder()
		server.Handler().ServeHTTP(w, httptest.NewRequest("POST", "/api/streams/preview-routing", strings.NewReader(tc.body)))
		if w.Code != tc.status {
			t.Fatal(w.Code, w.Body.String())
		}
		if tc.status == 200 {
			var decision routingDecision
			if err := json.Unmarshal(w.Body.Bytes(), &decision); err != nil || decision.Stream != tc.stream || decision.Reason != tc.reason {
				t.Fatal(decision, err)
			}
			if tc.reason == "rule" && (!decision.Candidates[0].Selected || decision.Candidates[0].Rules[0].Actual != "nginx") {
				t.Fatal(decision)
			}
		}
	}
	if len(store.snapshots()) != 0 || catalog.saves != 0 {
		t.Fatal("preview mutated saved state")
	}
}
