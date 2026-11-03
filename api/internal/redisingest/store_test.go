package redisingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/logister/logister/api/internal/db"
	"github.com/redis/go-redis/v9"
)

type memCatalog struct {
	streams map[string]StreamInfo
	opened  []string
	sealed  map[string]int64
	adopt   *Generation
	totals  map[string]int64
}

func (c *memCatalog) Stream(_ context.Context, name string) (StreamInfo, error) {
	info, ok := c.streams[name]
	if !ok {
		return StreamInfo{}, db.ErrStreamNotFound
	}
	return info, nil
}
func (c *memCatalog) NextGeneration(context.Context, string, string) (int64, error) {
	next := int64(len(c.opened) + 1)
	if c.adopt != nil && c.adopt.Generation >= next {
		next = c.adopt.Generation + 1
	}
	return next, nil
}
func (c *memCatalog) ActiveGeneration(context.Context, string, string) (Generation, bool, error) {
	if c.adopt == nil {
		return Generation{}, false, nil
	}
	if _, sealed := c.sealed[c.adopt.Index]; sealed {
		return Generation{}, false, nil
	}
	return *c.adopt, true, nil
}
func (c *memCatalog) OpenGeneration(_ context.Context, _, _, indexName string, _ int64) error {
	c.opened = append(c.opened, indexName)
	return nil
}
func (c *memCatalog) SealGeneration(_ context.Context, indexName string, accepted int64) error {
	c.sealed[indexName] = accepted
	return nil
}
func (c *memCatalog) AcceptedTotals(context.Context, string) (map[string]int64, error) {
	return c.totals, nil
}
func (c *memCatalog) ListStreams(context.Context) ([]db.Stream, error) {
	out := make([]db.Stream, 0, len(c.streams))
	for name, info := range c.streams {
		out = append(out, db.Stream{ID: info.ID, Name: name, Title: name})
	}
	return out, nil
}

func testStore(t *testing.T) (*Store, *memCatalog) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	catalog := &memCatalog{streams: map[string]StreamInfo{
		"payments": {ID: "11111111-1111-1111-1111-111111111111", Storage: db.StorageConfig{MaxDocuments: 2, MaxAgeHours: 24}},
	}, sealed: map[string]int64{}}
	return New(client, catalog), catalog
}

func event(id, stream, body string) db.Event {
	raw, _ := json.Marshal(map[string]string{"message": body})
	return db.Event{ID: id, Hash: body, HashVersion: 2, Document: raw, StreamName: stream}
}

func TestAcceptPublishesThenDropsTheBody(t *testing.T) {
	ctx := context.Background()
	store, _ := testStore(t)
	receipt, err := store.Accept(ctx, "logs-v2", "batch", "hash", []db.Event{event("a", "payments", "one"), event("b", "payments", "two")})
	if err != nil || receipt.Count != 2 || receipt.Duplicates != 0 {
		t.Fatalf("%+v %v", receipt, err)
	}
	if pending, err := store.Pending(ctx, "logs-v2"); err != nil || pending != 2 {
		t.Fatal(pending, err)
	}
	published := 0
	count, err := store.Publish(ctx, "logs-v2", func(events []db.Event) error {
		published = len(events)
		for _, item := range events {
			if len(item.Document) == 0 || item.TargetIndex == "" {
				t.Fatal(item)
			}
		}
		return nil
	})
	if err != nil || count != 2 || published != 2 {
		t.Fatal(count, published, err)
	}
	if pending, _ := store.Pending(ctx, "logs-v2"); pending != 0 {
		t.Fatal(pending)
	}
	again, err := store.Accept(ctx, "logs-v2", "", "", []db.Event{event("a", "payments", "one")})
	if err != nil || again.Duplicates != 1 {
		t.Fatal(again, err)
	}
	stats, err := store.Stats(ctx, "logs-v2", nil)
	if err != nil || stats.Totals.Accepted != 2 || stats.Totals.Pending != 0 || stats.Totals.Published != 2 {
		t.Fatal(stats, err)
	}
	record, err := store.redis.HGetAll(ctx, store.eventKey("logs-v2", "a")).Result()
	if err != nil || record["body"] != "" || record["hash"] != "one" {
		t.Fatal(record, err)
	}
}

func TestPublishReadsTheWholeBulkAtOnce(t *testing.T) {
	ctx := context.Background()
	store, catalog := testStore(t)
	info := catalog.streams["payments"]
	info.Storage.MaxDocuments = 100000
	catalog.streams["payments"] = info
	events := make([]db.Event, 0, 250)
	for i := 0; i < 250; i++ {
		events = append(events, event(fmt.Sprintf("e-%d", i), "payments", "body"))
	}
	if _, err := store.Accept(ctx, "logs-v2", "", "", events); err != nil {
		t.Fatal(err)
	}
	seen := 0
	count, err := store.Publish(ctx, "logs-v2", func(batch []db.Event) error {
		seen = len(batch)
		return nil
	})
	if err != nil || count != 250 || seen != 250 {
		t.Fatal(count, seen, err)
	}
}

func TestChangedIdentityConflictsAndFailedPublishKeepsTheBody(t *testing.T) {
	ctx := context.Background()
	store, _ := testStore(t)
	if _, err := store.Accept(ctx, "logs-v2", "", "", []db.Event{event("a", "payments", "one")}); err != nil {
		t.Fatal(err)
	}
	_, err := store.Publish(ctx, "logs-v2", func([]db.Event) error { return errors.New("lost") })
	if err == nil {
		t.Fatal("expected publish failure")
	}
	if pending, _ := store.Pending(ctx, "logs-v2"); pending != 1 {
		t.Fatal(pending)
	}
	if _, err := store.Accept(ctx, "logs-v2", "", "", []db.Event{event("a", "payments", "changed"), event("b", "payments", "new")}); !errors.Is(err, db.ErrIdentityConflict) {
		t.Fatal(err)
	}
	stats, err := store.Stats(ctx, "logs-v2", nil)
	if err != nil || stats.Totals.Accepted != 1 {
		t.Fatal(stats, err)
	}
}

func TestBatchFillsEachGenerationOnce(t *testing.T) {
	ctx := context.Background()
	store, catalog := testStore(t)
	events := make([]db.Event, 0, 5)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		events = append(events, event(id, "payments", id))
	}
	if _, err := store.Accept(ctx, "logs-v2", "", "", events); err != nil {
		t.Fatal(err)
	}
	if len(catalog.opened) != 3 || catalog.sealed[catalog.opened[0]] != 2 || catalog.sealed[catalog.opened[1]] != 2 {
		t.Fatal(catalog.opened, catalog.sealed)
	}
	targets := map[string]int{}
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		record, err := store.redis.HGetAll(ctx, store.eventKey("logs-v2", id)).Result()
		if err != nil {
			t.Fatal(err)
		}
		targets[record["target"]]++
	}
	counts := make([]int, 0, 3)
	for _, count := range targets {
		counts = append(counts, count)
	}
	if len(targets) != 3 {
		t.Fatal(targets)
	}
	seen := map[int]int{}
	for _, count := range counts {
		seen[count]++
	}
	if seen[2] != 2 || seen[1] != 1 {
		t.Fatal(targets)
	}
}

func TestEmptyJournalAdoptsTheOpenGeneration(t *testing.T) {
	ctx := context.Background()
	store, catalog := testStore(t)
	catalog.adopt = &Generation{Index: "logs-v2-s-existing-000004", StreamID: catalog.streams["payments"].ID, Generation: 4, Accepted: 1, Created: time.Now().UTC()}
	if _, err := store.Accept(ctx, "logs-v2", "", "", []db.Event{event("a", "payments", "one")}); err != nil {
		t.Fatal(err)
	}
	if len(catalog.opened) != 0 {
		t.Fatal(catalog.opened)
	}
	record, err := store.redis.HGetAll(ctx, store.eventKey("logs-v2", "a")).Result()
	if err != nil || record["target"] != "logs-v2-s-existing-000004" {
		t.Fatal(record, err)
	}
	sealed, err := store.NoteRotation(ctx, "logs-v2", "payments", "")
	if err != nil || !sealed || catalog.sealed["logs-v2-s-existing-000004"] != 2 {
		t.Fatal(sealed, catalog.sealed, err)
	}
	if _, err := store.Accept(ctx, "logs-v2", "", "", []db.Event{event("b", "payments", "two")}); err != nil {
		t.Fatal(err)
	}
	if len(catalog.opened) != 1 {
		t.Fatal(catalog.opened)
	}
}

func TestRestoreCountersDoesNotOverwriteTheJournal(t *testing.T) {
	ctx := context.Background()
	store, catalog := testStore(t)
	catalog.totals = map[string]int64{"payments": 9}
	if err := store.RestoreCounters(ctx, "logs-v2"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Accept(ctx, "logs-v2", "", "", []db.Event{event("a", "payments", "one")}); err != nil {
		t.Fatal(err)
	}
	if err := store.RestoreCounters(ctx, "logs-v2"); err != nil {
		t.Fatal(err)
	}
	stats, err := store.Stats(ctx, "logs-v2", nil)
	if err != nil || stats.Totals.Accepted != 10 {
		t.Fatal(stats, err)
	}
}

func TestUnknownStreamIsRejected(t *testing.T) {
	store, _ := testStore(t)
	_, err := store.Accept(context.Background(), "logs-v2", "", "", []db.Event{event("a", "missing", "x")})
	if !errors.Is(err, db.ErrStreamNotFound) {
		t.Fatal(err)
	}
}
