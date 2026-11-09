package db

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// KafkaReplayCompatible reports whether a stored original payload is the same
// event as nextHash once the legacy Fluent Bit @timestamp is removed.
func KafkaReplayCompatible(payloadJSON, previous, nextHash string) bool {
	var payload map[string]any
	decoder := json.NewDecoder(bytes.NewBufferString(payloadJSON))
	decoder.UseNumber()
	if decoder.Decode(&payload) != nil {
		return false
	}
	if id, _ := payload["delivery_id"].(string); id == "" {
		return false
	}
	if _, ok := payload["@timestamp"].(json.Number); !ok {
		return false
	}
	for _, key := range []string{"id", "delivery_id"} {
		if _, ok := payload[key].(string); ok {
			delete(payload, key)
		}
	}
	hash := func() string {
		raw, _ := json.Marshal(payload)
		sum := sha256.Sum256(raw)
		return hex.EncodeToString(sum[:])
	}
	if hash() != previous {
		return false
	}
	delete(payload, "@timestamp")
	return hash() == nextHash
}
