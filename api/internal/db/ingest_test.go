package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"os"
	"testing"
	"time"
)

func ingestionTransaction(t *testing.T) (context.Context, pgx.Tx) {
	t.Helper()
	url := os.Getenv("LOGISTER_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set LOGISTER_TEST_DATABASE_URL for PostgreSQL tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	connection, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close(context.Background()) })
	tx, err := connection.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tx.Rollback(context.Background()) })
	schema := pgx.Identifier{fmt.Sprintf("test_ingest_%d", time.Now().UnixNano())}.Sanitize()
	if _, err := tx.Exec(ctx, "CREATE SCHEMA "+schema+"; SET LOCAL search_path TO "+schema+"; "+ingestSchema); err != nil {
		t.Fatal(err)
	}
	return ctx, tx
}
func testEvent(id, value string) Event {
	doc, _ := json.Marshal(map[string]string{"message": value, "event_id": id})
	return Event{ID: id, Hash: value, Document: doc}
}

func TestAcceptIsAtomicAndIdempotent(t *testing.T) {
	ctx, tx := ingestionTransaction(t)
	store := NewIngestStore(tx)
	events := []Event{testEvent("a", "one"), testEvent("b", "two")}
	receipt, err := store.Accept(ctx, "test", "key", "hash", events)
	if err != nil || receipt.Count != 2 {
		t.Fatalf("%+v %v", receipt, err)
	}
	receipt, err = store.Accept(ctx, "test", "key", "hash", events)
	if err != nil || receipt.Duplicates != 2 {
		t.Fatalf("%+v %v", receipt, err)
	}
	_, err = store.Accept(ctx, "test", "changed", "different", []Event{testEvent("c", "new"), testEvent("b", "changed")})
	if !errors.Is(err, ErrIdentityConflict) {
		t.Fatal(err)
	}
	var count int
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM ingest_events").Scan(&count); err != nil || count != 2 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	_, err = store.Accept(ctx, "test", "key", "different", events)
	if !errors.Is(err, ErrIdentityConflict) {
		t.Fatal(err)
	}
}

func TestFailedWriteRollsBackReceiptAndPrefix(t *testing.T) {
	ctx, tx := ingestionTransaction(t)
	store := NewIngestStore(tx)
	if _, err := tx.Exec(ctx, "ALTER TABLE ingest_events ADD CONSTRAINT force_failure CHECK (event_id <> 'bad')"); err != nil {
		t.Fatal(err)
	}
	_, err := store.Accept(ctx, "test", "batch", "hash", []Event{testEvent("good", "one"), testEvent("bad", "two")})
	if err == nil {
		t.Fatal("expected a storage failure")
	}
	var events, batches int
	if err := tx.QueryRow(ctx, "SELECT (SELECT count(*) FROM ingest_events),(SELECT count(*) FROM ingest_batches)").Scan(&events, &batches); err != nil {
		t.Fatal(err)
	}
	if events != 0 || batches != 0 {
		t.Fatalf("partial commit: events=%d batches=%d", events, batches)
	}
}

func TestPublicationReplaysAfterLostAcknowledgement(t *testing.T) {
	ctx, tx := ingestionTransaction(t)
	store := NewIngestStore(tx)
	if _, err := store.Accept(ctx, "test", "batch", "hash", []Event{testEvent("a", "one"), testEvent("b", "two")}); err != nil {
		t.Fatal(err)
	}
	delivered := map[string]bool{}
	if _, err := store.Publish(ctx, "test", func(events []Event) error {
		for _, event := range events {
			delivered[event.ID] = true
		}
		return errors.New("acknowledgement lost")
	}); err == nil {
		t.Fatal("expected publication failure")
	}
	if pending, err := store.Pending(ctx, "test"); err != nil || pending != 2 {
		t.Fatalf("pending=%d error=%v", pending, err)
	}
	if count, err := store.Publish(ctx, "test", func(events []Event) error {
		for _, event := range events {
			delivered[event.ID] = true
		}
		return nil
	}); err != nil || count != 2 {
		t.Fatalf("count=%d error=%v", count, err)
	}
	if len(delivered) != 2 {
		t.Fatal("duplicate logical records")
	}
	if pending, _ := store.Pending(ctx, "test"); pending != 0 {
		t.Fatal(pending)
	}
}
