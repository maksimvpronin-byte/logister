package db

import (
	"errors"
	"testing"
)

func TestStreamDeleteRestoreAndIngest(t *testing.T) {
	ctx, tx := streamTransaction(t)
	catalog := NewStreamStore(tx)
	if _, err := catalog.Save(ctx, validStream("payments"), true); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SetDeleted(ctx, "all", true); !errors.Is(err, ErrDefaultStream) {
		t.Fatal(err)
	}
	if err := catalog.SetDeleted(ctx, "missing", true); !errors.Is(err, ErrStreamNotFound) {
		t.Fatal(err)
	}
	if err := catalog.SetDeleted(ctx, "payments", true); err != nil {
		t.Fatal(err)
	}
	if rows, err := catalog.List(ctx); err != nil || len(rows) != 0 {
		t.Fatal(rows, err)
	}
	if rows, err := catalog.ListArchived(ctx); err != nil || len(rows) != 1 {
		t.Fatal(rows, err)
	}
	if _, err := catalog.Save(ctx, validStream("payments"), false); !errors.Is(err, ErrStreamNotFound) {
		t.Fatal(err)
	}
	duplicateTx, err := tx.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewStreamStore(duplicateTx).Save(ctx, validStream("payments"), true); !errors.Is(err, ErrStreamExists) {
		t.Fatal(err)
	}
	if err := duplicateTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	// A stale routing snapshot must not accept into a deleted stream.
	event := testEvent("late", "late")
	event.StreamName = "payments"
	if _, err := NewIngestStore(tx).Accept(ctx, "logs-v2", "", "", []Event{event}); !errors.Is(err, ErrStreamNotFound) {
		t.Fatal(err)
	}
	if err := catalog.SetDeleted(ctx, "payments", false); err != nil {
		t.Fatal(err)
	}
	if _, err := NewIngestStore(tx).Accept(ctx, "logs-v2", "", "", []Event{event}); err != nil {
		t.Fatal(err)
	}
	if rows, err := catalog.List(ctx); err != nil || len(rows) != 1 {
		t.Fatal(rows, err)
	}
	if err := catalog.SetDeleted(ctx, "payments", true); err != nil {
		t.Fatal(err)
	}
	stats, err := NewIngestStatsStore(tx).Stats(ctx, "logs-v2", nil)
	if err != nil || len(stats.Streams) != 0 {
		t.Fatal(stats, err)
	}
	// Already accepted records still drain, then reappear on restoration.
	if _, err := NewIngestStore(tx).Publish(ctx, "logs-v2", func([]Event) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SetDeleted(ctx, "payments", false); err != nil {
		t.Fatal(err)
	}
	stats, err = NewIngestStatsStore(tx).Stats(ctx, "logs-v2", nil)
	if err != nil || stats.Totals.Published != 1 {
		t.Fatal(stats, err)
	}
}
