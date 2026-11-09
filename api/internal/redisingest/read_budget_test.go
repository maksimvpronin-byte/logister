package redisingest

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"math/rand"
	"net"
	"strings"
	"testing"

	"github.com/logister/logister/api/internal/db"
	"github.com/redis/go-redis/v9"
)

func TestCompressLargeJournalDocumentWithOriginalPayload(t *testing.T) {
	raw := make([]byte, 5120)
	rand.New(rand.NewSource(1)).Read(raw)
	message := hex.EncodeToString(raw)
	payload, err := json.Marshal(map[string]any{"fields": map[string]any{"sequence": 45}, "message": message})
	if err != nil {
		t.Fatal(err)
	}
	document, err := json.Marshal(map[string]any{"@timestamp": "2026-11-04T00:00:00Z", "message": message, "payload_json": string(payload), "stream": "payments"})
	if err != nil {
		t.Fatal(err)
	}
	body, size := encodeBody(document)
	if size != len(document) || len(body) >= len(document)/2 {
		t.Fatalf("compressed body = %d bytes, original = %d bytes", len(body), len(document))
	}
}

func TestCompressedAndLegacyBodiesKeepLogicalByteBudget(t *testing.T) {
	ctx := context.Background()
	store, _ := testStore(t)
	events := []db.Event{
		event("compressed", "payments", strings.Repeat("same text ", 1024)),
		event("legacy", "payments", strings.Repeat("old text ", 1024)),
	}
	if _, err := store.Accept(ctx, "logs", "", "", events); err != nil {
		t.Fatal(err)
	}
	compressedKey := store.eventKey("logs", events[0].ID)
	if size, err := store.redis.HStrLen(ctx, compressedKey, "body").Result(); err != nil || size <= 0 || size >= int64(len(events[0].Document))/2 {
		t.Fatalf("expected a smaller stored body, got %d bytes, %v", size, err)
	}
	legacyKey := store.eventKey("logs", events[1].ID)
	if _, err := store.redis.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, legacyKey, "body", []byte(events[1].Document))
		pipe.HDel(ctx, legacyKey, "body_size")
		pipe.Del(ctx, store.pendingBytesKey("logs"))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.RestoreCounters(ctx, "logs"); err != nil {
		t.Fatal(err)
	}
	want := int64(len(events[0].Document) + len(events[1].Document))
	if size, err := store.PendingBytes(ctx, "logs"); err != nil || size != want {
		t.Fatalf("restored byte count = %d, want %d, error %v", size, want, err)
	}
	store.SetPublishMaxBytes(len(events[0].Document))
	for _, want := range events {
		n, err := store.Publish(ctx, "logs", func(got []db.Event) error {
			if len(got) != 1 || got[0].ID != want.ID || !bytes.Equal(got[0].Document, want.Document) {
				t.Fatal("publication did not preserve the logical window or document")
			}
			return nil
		})
		if n != 1 || err != nil {
			t.Fatalf("published %d, error %v", n, err)
		}
	}
	if size, err := store.PendingBytes(ctx, "logs"); err != nil || size != 0 {
		t.Fatalf("remaining bytes = %d, error %v", size, err)
	}
	if count, err := store.redis.HExists(ctx, compressedKey, "body_size").Result(); err != nil || count {
		t.Fatalf("published receipt kept a body size, error %v", err)
	}
}

type bodyReadHook struct{ reads *int }

func (h bodyReadHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) { return next(ctx, network, addr) }
}
func (h bodyReadHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }
func (h bodyReadHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, commands []redis.Cmder) error {
		for _, cmd := range commands {
			if cmd.Name() == "hgetall" {
				*h.reads++
			}
		}
		return next(ctx, commands)
	}
}
func TestPublicationTransfersOnlyBodiesWithinSelectedByteWindow(t *testing.T) {
	ctx := context.Background()
	store, catalog := testStore(t)
	info := catalog.streams["payments"]
	info.Storage.MaxDocuments = 100000
	catalog.streams["payments"] = info
	events := []db.Event{event("a", "payments", strings.Repeat("x", 1000)), event("b", "payments", strings.Repeat("y", 1000)), event("c", "payments", strings.Repeat("z", 1000))}
	if _, err := store.Accept(ctx, "logs", "", "", events); err != nil {
		t.Fatal(err)
	}
	store.SetPublishMaxBytes(len(events[0].Document))
	reads := 0
	store.redis.AddHook(bodyReadHook{&reads})
	batch, err := store.readPublication(ctx, "logs", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.events) != 1 || batch.events[0].ID != "a" || reads != 1 {
		t.Fatalf("events=%d body reads=%d", len(batch.events), reads)
	}
	if n, _ := store.Pending(ctx, "logs"); n != 3 {
		t.Fatal("read acknowledged queue", n)
	}
}
func TestPublicationAllowsOneOversizedBodyAndKeepsEmptyReceiptsInPrefix(t *testing.T) {
	ctx := context.Background()
	store, _ := testStore(t)
	if _, err := store.Accept(ctx, "logs", "", "", []db.Event{event("a", "payments", "large body"), event("b", "payments", "next")}); err != nil {
		t.Fatal(err)
	}
	// Recovery may encounter an already-cleared body in the queue prefix.
	if err := store.redis.LPush(ctx, store.pendingKey("logs"), "cleared").Err(); err != nil {
		t.Fatal(err)
	}
	store.SetPublishMaxBytes(1)
	batch, err := store.readPublication(ctx, "logs", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.ids) != 2 || len(batch.events) != 1 || batch.ids[0] != "cleared" || batch.events[0].ID != "a" {
		t.Fatal(batch.ids, len(batch.events))
	}
	if batch.flags[0] != "0" || batch.flags[1] != "1" {
		t.Fatal(batch.flags)
	}
}
