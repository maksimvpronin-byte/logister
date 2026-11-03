package db

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

func validStream(name string) Stream {
	return Stream{Name: name, Title: "Payments", Routing: RoutingConfig{Priority: 10, Match: "all", Rules: []RoutingRule{}}, Storage: StorageConfig{MaxAgeHours: 24, MaxDocuments: 2}}
}
func streamTransaction(t *testing.T) (context.Context, pgx.Tx) {
	ctx, tx := ingestionTransaction(t)
	if _, err := tx.Exec(ctx, streamSchema); err != nil {
		t.Fatal(err)
	}
	return ctx, tx
}
func TestStreamSettingsValidation(t *testing.T) {
	for _, name := range []string{"payments", "web-access", "team_1"} {
		if err := ValidateStream(validStream(name)); err != nil {
			t.Fatal(err)
		}
	}
	for _, change := range []func(*Stream){
		func(s *Stream) { s.Name = "../logs" }, func(s *Stream) { s.Title = " " }, func(s *Stream) { s.Storage = StorageConfig{} },
		func(s *Stream) { s.Storage.RetentionDays = -1 }, func(s *Stream) { s.Storage.MaxAgeHours = 0; s.Storage.RetentionDays = 7 },
		func(s *Stream) { s.Routing.Enabled = true }, func(s *Stream) { s.Routing.Priority = 1001 },
		func(s *Stream) { s.Routing.Rules = []RoutingRule{{Field: "broken[", Operator: "equals", Value: "x"}} },
	} {
		stream := validStream("payments")
		change(&stream)
		if ValidateStream(stream) == nil {
			t.Fatalf("accepted invalid stream: %+v", stream)
		}
	}
}
func TestStreamSettingsPersistAndCannotOverwriteDuplicate(t *testing.T) {
	ctx, tx := streamTransaction(t)
	store := NewStreamStore(tx)
	stream := validStream("payments")
	stream.Routing = RoutingConfig{Enabled: true, Priority: 5, Match: "any", Rules: []RoutingRule{{Field: "source", Operator: "equals", Value: "billing"}}}
	saved, err := store.Save(ctx, stream, true)
	if err != nil || saved.ID == "" {
		t.Fatalf("%+v %v", saved, err)
	}
	saved.Title = "Updated payments"
	if _, err := store.Save(ctx, saved, false); err != nil {
		t.Fatal(err)
	}
	listed, err := store.List(ctx)
	if err != nil || len(listed) != 1 || listed[0].Title != "Updated payments" || listed[0].Routing.Rules[0].Value != "billing" || listed[0].Storage.MaxDocuments != 2 {
		t.Fatal(listed, err)
	}
	if _, err := store.Save(ctx, stream, true); err != ErrStreamExists {
		t.Fatal(err)
	}
}
func TestStreamTargetsStayFixedAcrossRotationAndReplay(t *testing.T) {
	ctx, tx := streamTransaction(t)
	catalog := NewStreamStore(tx)
	if _, err := catalog.Save(ctx, validStream("payments"), true); err != nil {
		t.Fatal(err)
	}
	events := []Event{}
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		event := testEvent(id, id)
		event.StreamName = "payments"
		events = append(events, event)
	}
	store := NewIngestStore(tx)
	if _, err := store.Accept(ctx, "logs-v2", "batch", "hash", events); err != nil {
		t.Fatal(err)
	}
	indices, err := ListManagedIndices(ctx, tx, "logs-v2", "payments")
	if err != nil || len(indices) != 3 {
		t.Fatalf("%+v %v", indices, err)
	}
	counts := map[int64]int64{}
	for _, index := range indices {
		counts[index.Generation] = index.AcceptedCount
		if index.AcceptedCount > 2 {
			t.Fatal("document threshold exceeded", index)
		}
	}
	if counts[1] != 2 || counts[2] != 2 || counts[3] != 1 {
		t.Fatal("batch did not fill each generation once", indices)
	}
	placed := map[string]string{}
	rows, err := tx.Query(ctx, `SELECT event_id,target_index FROM ingest_events`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id, target string
		if err := rows.Scan(&id, &target); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		placed[id] = target
	}
	rows.Close()
	if err := rows.Err(); err != nil || placed["a"] == "" || placed["a"] != placed["b"] || placed["c"] != placed["d"] || placed["a"] == placed["c"] || placed["c"] == placed["e"] {
		t.Fatal(placed, err)
	}
	var target string
	if err := tx.QueryRow(ctx, `SELECT target_index FROM ingest_events WHERE event_id='a'`).Scan(&target); err != nil {
		t.Fatal(err)
	}
	if rotated, err := RotateStream(ctx, tx, "logs-v2", "payments", ""); err != nil || !rotated {
		t.Fatal(rotated, err)
	}
	receipt, err := store.Accept(ctx, "logs-v2", "", "", events)
	if err != nil || receipt.Duplicates != 5 {
		t.Fatal(receipt, err)
	}
	after, _ := ListManagedIndices(ctx, tx, "logs-v2", "payments")
	if len(after) != 3 {
		t.Fatal("retry created a new generation")
	}
	seen := map[string]string{}
	_, err = store.Publish(ctx, "logs-v2", func(items []Event) error {
		for _, item := range items {
			seen[item.ID] = item.TargetIndex
		}
		return nil
	})
	if err != nil || seen["a"] != target || len(seen) != 5 {
		t.Fatal(seen, err)
	}
}
func TestBatchDuplicateDoesNotTakeAnotherIndexSlot(t *testing.T) {
	ctx, tx := streamTransaction(t)
	if _, err := NewStreamStore(tx).Save(ctx, validStream("payments"), true); err != nil {
		t.Fatal(err)
	}
	store := NewIngestStore(tx)
	first := testEvent("a", "a")
	first.StreamName = "payments"
	if _, err := store.Accept(ctx, "logs-v2", "", "", []Event{first}); err != nil {
		t.Fatal(err)
	}
	second := testEvent("b", "b")
	second.StreamName = "payments"
	receipt, err := store.Accept(ctx, "logs-v2", "", "", []Event{first, second})
	if err != nil || receipt.Count != 2 || receipt.Duplicates != 1 {
		t.Fatalf("%+v %v", receipt, err)
	}
	indices, err := ListManagedIndices(ctx, tx, "logs-v2", "payments")
	if err != nil || len(indices) != 1 || indices[0].AcceptedCount != 2 {
		t.Fatal(indices, err)
	}
}

func TestRetentionProtectsPendingAndKeepsRetryTombstones(t *testing.T) {
	ctx, tx := streamTransaction(t)
	stream := validStream("payments")
	stream.Storage.RetentionDays = 1
	if _, err := NewStreamStore(tx).Save(ctx, stream, true); err != nil {
		t.Fatal(err)
	}
	event := testEvent("a", "a")
	event.StreamName = "payments"
	events := []Event{event}
	store := NewIngestStore(tx)
	if _, err := store.Accept(ctx, "logs-v2", "", "", events); err != nil {
		t.Fatal(err)
	}
	indices, _ := ListManagedIndices(ctx, tx, "logs-v2", "payments")
	target := indices[0].Name
	if _, err := tx.Exec(ctx, `UPDATE stream_indices SET sealed_at=now()-interval '2 days' WHERE index_name=$1`, target); err != nil {
		t.Fatal(err)
	}
	deleted := 0
	remove := func(name string) error {
		deleted++
		if name != target {
			t.Fatal("wrong target")
		}
		return nil
	}
	if ok, err := DeleteExpired(ctx, tx, "logs-v2", target, remove); ok || err != nil || deleted != 0 {
		t.Fatal("pending event was not protected", ok, err)
	}
	if _, err := store.Publish(ctx, "logs-v2", func([]Event) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if ok, err := DeleteExpired(ctx, tx, "logs-v2", target, remove); !ok || err != nil || deleted != 1 {
		t.Fatal(ok, err, deleted)
	}
	var cleared bool
	if err := tx.QueryRow(ctx, `SELECT document IS NULL FROM ingest_events WHERE event_id='a'`).Scan(&cleared); err != nil || !cleared {
		t.Fatal(cleared, err)
	}
	if receipt, err := store.Accept(ctx, "logs-v2", "", "", events); err != nil || receipt.Duplicates != 1 {
		t.Fatal("expired event was reaccepted", receipt, err)
	}
	if count, err := store.Publish(ctx, "logs-v2", func([]Event) error { t.Fatal("expired event was resurrected"); return nil }); err != nil || count != 0 {
		t.Fatal(count, err)
	}
}
