package search

import (
	"encoding/json"
	"testing"
)

func TestSearchMetadataWithBothResponseDecoders(t *testing.T) {
	for _, status := range []any{json.Number("503"), float64(503)} {
		m := mapHit("id", map[string]any{"service": "billing", "request_id": "req", "trace_id": "trace", "status": status, "ip": "192.0.2.10"})
		if m.Service != "billing" || m.RequestID != "req" || m.TraceID != "trace" || m.Status == nil || *m.Status != 503 || m.IP != "192.0.2.10" {
			t.Fatal(m)
		}
	}
}
