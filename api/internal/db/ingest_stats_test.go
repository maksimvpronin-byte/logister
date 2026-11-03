package db

import (
	"errors"
	"testing"
)

func TestIngestStatsDeliveryRetryAndScope(t *testing.T) {
	ctx, tx := streamTransaction(t)
	catalog := NewStreamStore(tx)
	pay, err := catalog.Save(ctx, validStream("payments"), true)
	if err != nil {
		t.Fatal(err)
	}
	bill, err := catalog.Save(ctx, validStream("billing"), true)
	if err != nil {
		t.Fatal(err)
	}
	ingest := NewIngestStore(tx)
	stats := NewIngestStatsStore(tx)
	events := []Event{testEvent("a", "a"), testEvent("b", "b"), testEvent("c", "c")}
	events[0].StreamName = "payments"
	events[1].StreamName = "payments"
	events[2].StreamName = "billing"
	if _, err := ingest.Accept(ctx, "logs-v2", "", "", events); err != nil {
		t.Fatal(err)
	}
	if _, err := ingest.Accept(ctx, "logs-v2", "", "", events); err != nil {
		t.Fatal(err)
	}
	check := func(ids []string, want IngestCounts, n int) {
		t.Helper()
		got, err := stats.Stats(ctx, "logs-v2", ids)
		if err != nil || got.Totals != want || len(got.Streams) != n || got.CapturedAt.IsZero() {
			t.Fatalf("stats %+v, error %v; want %+v", got, err, want)
		}
	}
	check(nil, IngestCounts{Accepted: 3, Pending: 3}, 2)
	if _, err := ingest.Publish(ctx, "logs-v2", func([]Event) error { return errors.New("offline") }); err == nil {
		t.Fatal("expected delivery failure")
	}
	check(nil, IngestCounts{Accepted: 3, Pending: 3}, 2)
	if _, err := ingest.Publish(ctx, "logs-v2", func([]Event) error { return nil }); err != nil {
		t.Fatal(err)
	}
	e := testEvent("d", "d")
	e.StreamName = "payments"
	if _, err := ingest.Accept(ctx, "logs-v2", "", "", []Event{e}); err != nil {
		t.Fatal(err)
	}
	check(nil, IngestCounts{Accepted: 4, Published: 3, Pending: 1}, 2)
	check([]string{bill.ID}, IngestCounts{Accepted: 1, Published: 1}, 1)
	check([]string{pay.ID}, IngestCounts{Accepted: 3, Published: 2, Pending: 1}, 1)
	check([]string{}, IngestCounts{}, 0)
	if _, err := tx.Exec(ctx, `UPDATE ingest_events SET document=NULL WHERE published_at IS NOT NULL`); err != nil {
		t.Fatal(err)
	}
	check(nil, IngestCounts{Accepted: 4, Published: 3, Pending: 1}, 2)
}
