package db

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
)

func TestKafkaReplayCompatibleDropsLegacyTimestamp(t *testing.T) {
	hash := func(v any) string {
		raw, _ := json.Marshal(v)
		sum := sha256.Sum256(raw)
		return hex.EncodeToString(sum[:])
	}
	payload := map[string]any{"message": "same", "@timestamp": json.Number("123.456"), "delivery_id": "uuid"}
	raw, _ := json.Marshal(payload)
	previous := hash(map[string]any{"message": "same", "@timestamp": json.Number("123.456")})
	next := hash(map[string]any{"message": "same"})
	if !KafkaReplayCompatible(string(raw), previous, next) {
		t.Fatal("legacy timestamp was not accepted as the same event")
	}
	if KafkaReplayCompatible(string(raw), previous, hash(map[string]any{"message": "other"})) {
		t.Fatal("different payload matched")
	}
}
