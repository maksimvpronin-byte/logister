package httpapi

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/logister/logister/api/internal/config"
	"github.com/logister/logister/api/internal/db"
)

func TestAnyJSONIsPreservedAndDisplayMetadataCannotLoseFields(t *testing.T) {
	handler, store := ingestHandler(t, 0)
	raw := `{"id":"arbitrary","tenant":"acme","http":{"status":503},"host":{"name":"node-1"},"timestamp":"not-a-date","fields":[1,null],"large":900719925474099312345,"huge":1e999999,"message":{"text":"structured"},"ключ с пробелом":true}`
	response, _ := ingestCall(t, handler, []byte(raw), false)
	if response.Code != 202 {
		t.Fatal(response.Code, response.Body.String())
	}
	docs := store.snapshots()
	if len(docs) != 1 || docs[0]["message"] == "" {
		t.Fatal(docs)
	}
	original, ok := docs[0]["payload_json"].(string)
	if !ok || !strings.Contains(original, "900719925474099312345") || !strings.Contains(original, "1e999999") {
		t.Fatal(original)
	}
	var input, output map[string]json.RawMessage
	if json.Unmarshal([]byte(raw), &input) != nil || json.Unmarshal([]byte(original), &output) != nil {
		t.Fatal("invalid payload")
	}
	if len(input) != len(output) {
		t.Fatal("fields were lost", input, output)
	}
	for key, wanted := range input {
		var a, b any
		json.Unmarshal(wanted, &a)
		json.Unmarshal(output[key], &b)
		if string(wanted) != string(output[key]) && key != "message" && key != "http" && key != "host" {
			t.Fatal(key, string(wanted), string(output[key]))
		}
	}
	response, _ = ingestCall(t, handler, []byte(strings.Replace(raw, `"tenant":"acme"`, `"tenant":"other"`, 1)), false)
	if response.Code != 409 {
		t.Fatal("extra-field change was not an identity conflict", response.Body.String())
	}
}
func TestLongMessageExcerptKeepsFullPayloadIdentity(t *testing.T) {
	handler, store := ingestHandler(t, 0)
	head, tail := strings.Repeat("a", searchExcerptBytes), strings.Repeat("b", 200)
	raw := `{"id":"long","message":"` + head + tail + `"}`
	response, _ := ingestCall(t, handler, []byte(raw), false)
	if response.Code != 202 {
		t.Fatal(response.Code, response.Body.String())
	}
	doc := store.snapshots()[0]
	message, _ := doc["message"].(string)
	if message != head+"…" {
		t.Fatalf("excerpt = %q", message)
	}
	original, _ := doc["payload_json"].(string)
	if !strings.Contains(original, head+tail) {
		t.Fatal("full message was dropped", original)
	}
	changed := `{"id":"long","message":"` + head + strings.Repeat("c", 200) + `"}`
	response, _ = ingestCall(t, handler, []byte(changed), false)
	if response.Code != 409 {
		t.Fatal("tail change was ignored", response.Code, response.Body.String())
	}
}
func TestRulesCanUseAnyFieldArrayAndExistence(t *testing.T) {
	var req ingestRequest
	if err := json.Unmarshal([]byte(`{"tenant":"acme","event":{"code":503},"tags":["blue","production"],"items":[{"status":500}],"empty":null,"field.with.dot":"x","number":900719925474099312345}`), &req); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		field, op, value string
		expected         bool
	}{
		{"tenant", "equals", "acme", true}, {"event.code", "equals", "503", true}, {"tags", "equals", "production", true}, {"items[0].status", "equals", "500", true},
		{"items[].status", "equals", "500", true}, {"empty", "equals", "null", true}, {"empty", "exists", "", true}, {"missing", "not_exists", "", true},
		{`["field.with.dot"]`, "equals", "x", true}, {"number", "equals", "900719925474099312345", true}, {"source", "exists", "", false},
	} {
		stream := db.Stream{Name: "arbitrary", Routing: db.RoutingConfig{Enabled: true, Match: "all", Rules: []db.RoutingRule{{Field: tc.field, Operator: tc.op, Value: tc.value}}}}
		result, err := decideRouting(req, []db.Stream{{Name: "all"}, stream}, true)
		if err != nil || (result.Stream == "arbitrary") != tc.expected {
			t.Fatal(tc, result, err)
		}
	}
}
func TestPreviewAcceptsRawJSONWithoutMessageAndPreservesNumberPrecision(t *testing.T) {
	server := NewServer(config.Config{}, nil, nil)
	server.catalog = &fakeCatalog{streams: []db.Stream{{Name: "all"}, {Name: "numbers", Routing: db.RoutingConfig{Enabled: true, Match: "all", Rules: []db.RoutingRule{{Field: "number", Operator: "equals", Value: "900719925474099312345"}}}}}}
	request := httptest.NewRequest("POST", "/api/streams/preview-routing", strings.NewReader(`{"message_json":"{\"number\":900719925474099312345,\"random\":true}"}`))
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != 200 || !strings.Contains(response.Body.String(), `"stream":"numbers"`) {
		t.Fatal(response.Code, response.Body.String())
	}
}
