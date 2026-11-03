package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode/utf8"
)

type legacyIngestRequest ingestRequest

// Preserve the complete input. Recognized metadata is only a display/routing
// convenience; incompatible types remain intact in Payload rather than failing.
func (req *ingestRequest) UnmarshalJSON(raw []byte) error {
	if !utf8.Valid(raw) {
		return errors.New("JSON must use valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil {
		return err
	}
	if object == nil {
		return errors.New("each message must be a JSON object")
	}
	*req = ingestRequest{Payload: object}
	text := func(key string) string { value, _ := object[key].(string); return value }
	req.ID = text("id")
	req.DeliveryID = text("delivery_id")
	req.Message = text("message")
	req.Source = text("source")
	req.Level = text("level")
	req.Host = text("host")
	req.Stream = text("stream")
	req.Facility = text("facility")
	req.Log = text("log")
	if fields, ok := object["fields"].(map[string]any); ok {
		req.Fields = fields
	}
	if timestamp := text("timestamp"); timestamp != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, timestamp); err == nil {
			req.Timestamp = &parsed
		}
	}
	return nil
}

func trimText(value string, max int) string {
	value = strings.ReplaceAll(value, "\x00", "�")
	if len(value) <= max {
		return value
	}
	value = value[:max]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value + "…"
}

func canonicalPayload(req ingestRequest) []byte {
	if req.Payload == nil {
		content := req
		content.ID = ""
		content.DeliveryID = ""
		content.Log = ""
		raw, _ := json.Marshal(content)
		return raw
	}
	copy := make(map[string]any, len(req.Payload))
	for key, value := range req.Payload {
		if key == "id" || key == "delivery_id" {
			if _, identity := value.(string); identity {
				continue
			}
		}
		copy[key] = value
	}
	// A scoped URL is part of the destination contract even if absent in input.
	if req.ForcedStream != "" {
		copy["stream"] = req.ForcedStream
	}
	raw, _ := json.Marshal(copy)
	return raw
}
func legacyPayloadHash(req ingestRequest) string {
	var old legacyIngestRequest
	if req.Lookup != nil {
		raw, _ := json.Marshal(req.Lookup)
		if json.Unmarshal(raw, &old) != nil {
			return ""
		}
	} else if req.Payload != nil {
		raw, _ := json.Marshal(req.Payload)
		if json.Unmarshal(raw, &old) != nil {
			return ""
		}
	} else {
		old = legacyIngestRequest(req)
	}
	old.ID = ""
	old.DeliveryID = ""
	old.Log = ""
	if req.ForcedStream != "" {
		old.Stream = req.ForcedStream
	}
	raw, _ := json.Marshal(old)
	return digest(raw)
}

// This compatibility search projection is bounded. Full values always remain in
// the unindexed payload, avoiding dynamic mappings and oversized keyword terms.
func indexableValue(value any, depth int) any {
	if depth > 32 {
		return nil
	}
	switch v := value.(type) {
	case string:
		return trimText(v, 8000)
	case json.Number:
		return trimText(string(v), 8000)
	case map[string]any:
		out := map[string]any{}
		for key, item := range v {
			if len(key) <= 1024 && !strings.ContainsRune(key, 0) {
				out[key] = indexableValue(item, depth+1)
			}
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = indexableValue(item, depth+1)
		}
		return out
	default:
		return value
	}
}

// searchExcerptBytes is the searchable and list preview. The complete input stays
// in the unindexed payload, so a long log is not stored and analyzed twice.
const searchExcerptBytes = 500

func payloadMessage(req ingestRequest) string {
	if strings.TrimSpace(req.Message) != "" {
		return trimText(strings.TrimSpace(req.Message), searchExcerptBytes)
	}
	if req.Payload != nil {
		raw, _ := json.Marshal(req.Payload)
		return trimText(string(raw), searchExcerptBytes)
	}
	return ""
}
