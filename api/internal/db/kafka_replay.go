package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/jackc/pgx/v5"
)

// Narrow compatibility for records ingested by the initial Kafka transport,
// whose Fluent Bit timestamp used the default @timestamp key. Validate both
// old and normalized hashes against retained original data before migration.
func normalizeKafkaReplay(ctx context.Context, tx pgx.Tx, index string, event Event, previous string) (bool, error) {
	var raw *string
	if err := tx.QueryRow(ctx, `SELECT document->>'payload_json' FROM ingest_events WHERE index_name=$1 AND event_id=$2 FOR UPDATE`, index, event.ID).Scan(&raw); err != nil {
		return false, err
	}
	if raw == nil || !KafkaReplayCompatible(*raw, previous, event.Hash) {
		return false, nil
	}
	_, err := tx.Exec(ctx, `UPDATE ingest_events SET payload_hash=$3 WHERE index_name=$1 AND event_id=$2`, index, event.ID, event.Hash)
	return err == nil, err
}

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
