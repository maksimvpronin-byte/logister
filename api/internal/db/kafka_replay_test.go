package db

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
)

func TestKafkaTransportReplayDoesNotWeakenPayloadConflictCheck(t *testing.T) {
	ctx, tx := ingestionTransaction(t)
	store := NewIngestStore(tx)
	hash := func(v any) string {
		raw, _ := json.Marshal(v)
		sum := sha256.Sum256(raw)
		return hex.EncodeToString(sum[:])
	}
	old := map[string]any{"message": "same", "@timestamp": json.Number("123.456"), "delivery_id": "uuid"}
	raw, _ := json.Marshal(old)
	document, _ := json.Marshal(map[string]any{"payload_json": string(raw)})
	previous := map[string]any{"message": "same", "@timestamp": json.Number("123.456")}
	original := Event{ID: "record", Hash: hash(previous), HashVersion: 2, Document: document}
	if _, err := store.Accept(ctx, "test", "", "", []Event{original}); err != nil {
		t.Fatal(err)
	}
	normalized := Event{ID: "record", Hash: hash(map[string]any{"message": "same"}), HashVersion: 2, Document: document, KafkaTransport: true}
	if receipt, err := store.Accept(ctx, "test", "", "", []Event{normalized}); err != nil || receipt.Duplicates != 1 {
		t.Fatal(receipt, err)
	}
	if receipt, err := store.Accept(ctx, "test", "", "", []Event{normalized}); err != nil || receipt.Duplicates != 1 {
		t.Fatal(receipt, err)
	}
	changed := normalized
	changed.Hash = hash(map[string]any{"message": "changed"})
	if _, err := store.Accept(ctx, "test", "", "", []Event{changed}); err != ErrIdentityConflict {
		t.Fatal("payload conflict was bypassed", err)
	}
}
