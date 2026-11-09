package redisingest

import (
	"context"
	"errors"
	"github.com/logister/logister/api/internal/db"
	"sync/atomic"
	"testing"
	"time"
)

func TestPublishBytesFailureAndFencing(t *testing.T) {
	ctx := context.Background()
	store, _ := testStore(t)
	events := []db.Event{event("a", "payments", "one"), event("b", "payments", "two"), event("c", "payments", "three")}
	if _, err := store.Accept(ctx, "logs", "", "", events); err != nil {
		t.Fatal(err)
	}
	store.SetPublishMaxBytes(1) // A single oversized record must still progress.
	if _, err := store.Publish(ctx, "logs", func([]db.Event) error { return errors.New("offline") }); err == nil {
		t.Fatal("write failure hidden")
	}
	if pending, _ := store.Pending(ctx, "logs"); pending != 3 {
		t.Fatal(pending)
	}
	if _, err := store.Publish(ctx, "logs", func(events []db.Event) error {
		if len(events) != 1 {
			t.Fatal(len(events))
		}
		return store.redis.Set(ctx, "logister:publish-lock:logs", "new-owner", time.Minute).Err()
	}); err == nil {
		t.Fatal("expired owner acknowledged work")
	}
	if owner, _ := store.redis.Get(ctx, "logister:publish-lock:logs").Result(); owner != "new-owner" {
		t.Fatal("old release deleted new owner's lease", owner)
	}
	if pending, _ := store.Pending(ctx, "logs"); pending != 3 {
		t.Fatal("lost messages", pending)
	}
	store.redis.Del(ctx, "logister:publish-lock:logs")
	for want := 2; want >= 0; want-- {
		n, err := store.Publish(ctx, "logs", func(events []db.Event) error {
			if len(events) != 1 {
				t.Fatal(len(events))
			}
			return nil
		})
		if err != nil || n != 1 {
			t.Fatal(n, err)
		}
		if pending, _ := store.Pending(ctx, "logs"); pending != int64(want) {
			t.Fatal(pending, want)
		}
	}
	stats, err := store.Stats(ctx, "logs", nil)
	if err != nil || stats.Totals.Published != 3 || stats.Totals.Pending != 0 {
		t.Fatal(stats, err)
	}
}
func TestConcurrentPublishersNeverDoubleAcknowledge(t *testing.T) {
	store, _ := testStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := store.Accept(ctx, "logs", "", "", []db.Event{event("a", "payments", "one"), event("b", "payments", "two")}); err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 2)
	var writes atomic.Int32
	go func() {
		_, err := store.Publish(ctx, "logs", func([]db.Event) error { writes.Add(1); close(started); <-release; return nil })
		done <- err
	}()
	<-started
	go func() {
		_, err := store.Publish(ctx, "logs", func([]db.Event) error { writes.Add(1); return nil })
		done <- err
	}()
	time.Sleep(30 * time.Millisecond)
	if writes.Load() != 1 {
		t.Fatal("two publishers own the same prefix")
	}
	close(release)
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if writes.Load() != 1 {
		t.Fatal(writes.Load())
	}
	stats, err := store.Stats(ctx, "logs", nil)
	if err != nil || stats.Totals.Published != 2 || stats.Totals.Pending != 0 {
		t.Fatal(stats, err)
	}
}
func TestCounterMismatchKeepsBodyAndQueue(t *testing.T) {
	store, _ := testStore(t)
	ctx := context.Background()
	store.Accept(ctx, "logs", "", "", []db.Event{event("a", "payments", "one")})
	store.redis.Set(ctx, store.pendingCountKey("logs", "payments"), 0, 0)
	if _, err := store.Publish(ctx, "logs", func([]db.Event) error { return nil }); err == nil {
		t.Fatal("counter mismatch hidden")
	}
	if body, _ := store.redis.HGet(ctx, store.eventKey("logs", "a"), "body").Result(); body == "" {
		t.Fatal("body removed before validation")
	}
	if n, _ := store.Pending(ctx, "logs"); n != 1 {
		t.Fatal(n)
	}
}
func TestQueueByteBudgetBackpressuresWithoutLosingMessages(t *testing.T) {
	store, _ := testStore(t)
	store.SetQueueMaxBytes(1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	a, b := event("a", "payments", "first"), event("b", "payments", "second")
	if _, err := store.Accept(ctx, "logs", "", "", []db.Event{a}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := store.Accept(ctx, "logs", "", "", []db.Event{b}); done <- err }()
	select {
	case err := <-done:
		t.Fatal("byte budget was bypassed", err)
	case <-time.After(30 * time.Millisecond):
	}
	if _, err := store.Publish(ctx, "logs", func([]db.Event) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	bytes, err := store.PendingBytes(ctx, "logs")
	if err != nil || bytes != int64(len(b.Document)) {
		t.Fatal(bytes, err)
	}
	if _, err := store.Publish(ctx, "logs", func([]db.Event) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if bytes, _ := store.PendingBytes(ctx, "logs"); bytes != 0 {
		t.Fatal(bytes)
	}
}
