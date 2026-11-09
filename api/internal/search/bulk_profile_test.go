package search

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/logister/logister/api/internal/db"
)

func TestBulkProfileMeasuresConsumedBodyAndKeepsPayloadPrivate(t *testing.T) {
	for _, reject := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "item_failure"}[reject], func(t *testing.T) {
			var received int
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, _ := io.ReadAll(r.Body)
				received = len(raw)
				status := 201
				if reject {
					status = 400
				}
				json.NewEncoder(w).Encode(map[string]any{"errors": reject, "items": []any{map[string]any{"index": map[string]any{"status": status}}}})
			}))
			defer backend.Close()
			client, _ := NewClient(backend.URL)
			client.EnableBulkProfile(true)
			var output bytes.Buffer
			previous := log.Writer()
			log.SetOutput(&output)
			defer log.SetOutput(previous)
			err := client.IndexEvents(context.Background(), "test-index", []db.Event{{ID: "a", Document: []byte(`{"message":"sensitive-payload-marker"}`)}})
			if (err != nil) != reject {
				t.Fatal(err)
			}
			line := output.String()
			start := strings.Index(line, "ingest_bulk_profile ")
			if start < 0 || strings.Contains(line, "sensitive-payload-marker") {
				t.Fatal("missing profile or payload leaked", line)
			}
			var record struct {
				Bytes     int     `json:"bytes"`
				Documents int     `json:"documents"`
				Success   bool    `json:"success"`
				RequestMS float64 `json:"request_ms"`
			}
			if err := json.Unmarshal([]byte(strings.TrimSpace(line[start+len("ingest_bulk_profile "):])), &record); err != nil {
				t.Fatal(err)
			}
			if record.Bytes != received || record.Bytes == 0 || record.Documents != 1 || record.Success == reject || record.RequestMS <= 0 {
				t.Fatal(record, received)
			}
		})
	}
}
