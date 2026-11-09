package httpapi

import (
	"encoding/json"
	"net"
	"strconv"
)

// Explicit projections avoid dynamic mapping of arbitrary user payloads.
// Incompatible values remain available unchanged in payload_json.
func searchMetadata(req ingestRequest) map[string]any {
	root := req.Payload
	if req.Lookup != nil {
		root = req.Lookup
	}
	value := func(key string) any {
		if v, ok := root[key]; ok {
			return v
		}
		return req.Fields[key]
	}
	out := map[string]any{}
	for _, key := range []string{"service", "request_id", "trace_id"} {
		if v, ok := value(key).(string); ok && v != "" && len(v) <= 8000 {
			out[key] = v
		}
	}
	switch v := value("status").(type) {
	case json.Number:
		if n, err := v.Int64(); err == nil {
			out["status"] = n
		}
	case string:
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			out["status"] = n
		}
	}
	if v, ok := value("ip").(string); ok && net.ParseIP(v) != nil {
		out["ip"] = v
	}
	return out
}
