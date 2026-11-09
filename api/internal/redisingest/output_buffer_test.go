package redisingest

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/logister/logister/api/internal/db"
	"github.com/redis/go-redis/v9"
)

type windowReadHook struct{ reads chan struct{} }

func (h windowReadHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) { return next(ctx, network, addr) }
}
func (h windowReadHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h windowReadHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "eval" && cmd.Args()[1] == selectPublicationPrefix {
			h.reads <- struct{}{}
		}
		return next(ctx, cmd)
	}
}

func bufferedFixture(t *testing.T) (*Store, context.Context, context.CancelFunc) {
	t.Helper()
	store, _ := testStore(t)
	store.SetPublishMaxBytes(1)   // One oversized event per window is allowed to progress.
	store.SetOutputBufferBytes(2) // Two windows including the one being written.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	events := []db.Event{event("a", "payments", "one"), event("b", "payments", "two"), event("c", "payments", "three"), event("d", "payments", "four"), event("e", "payments", "five")}
	if _, err := store.Accept(ctx, "logs", "", "", events); err != nil {
		t.Fatal(err)
	}
	return store, ctx, cancel
}

func TestOutputBufferPrefetchesButStopsAtCapacity(t *testing.T) {
	store, ctx, cancel := bufferedFixture(t)
	defer cancel()
	reads := make(chan struct{}, 32)
	store.redis.AddHook(windowReadHook{reads})
	writing, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	var ids []string
	go func() {
		n, err := store.Publish(ctx, "logs", func(batch []db.Event) error {
			ids = append(ids, batch[0].ID)
			if len(ids) == 1 {
				close(writing)
				<-release
			}
			return nil
		})
		if err == nil && n != 5 {
			err = errors.New("not all windows published")
		}
		done <- err
	}()
	select {
	case <-writing:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	for i := 0; i < 2; i++ {
		select {
		case <-reads:
		case <-ctx.Done():
			t.Fatal("no read-ahead", ctx.Err())
		}
	}
	select {
	case <-reads:
		t.Fatal("reader exceeded the two-window buffer")
	case <-time.After(30 * time.Millisecond):
	}
	if pending, _ := store.Pending(ctx, "logs"); pending != 5 {
		t.Fatal("queue acknowledged before write", pending)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(ids) != 5 {
		t.Fatal(ids)
	}
	for i, id := range ids {
		if id != string(rune('a'+i)) {
			t.Fatal("out-of-order output", ids)
		}
	}
	if pending, _ := store.Pending(ctx, "logs"); pending != 0 {
		t.Fatal(pending)
	}
}

func TestOutputBufferFailureKeepsUnconfirmedWindows(t *testing.T) {
	store, ctx, cancel := bufferedFixture(t)
	defer cancel()
	calls := 0
	n, err := store.Publish(ctx, "logs", func(batch []db.Event) error {
		calls++
		if calls == 2 {
			return errors.New("OpenSearch offline")
		}
		return nil
	})
	if err == nil || n != 1 {
		t.Fatal(n, err)
	}
	if pending, _ := store.Pending(ctx, "logs"); pending != 4 {
		t.Fatal(pending)
	}
	for _, id := range []string{"b", "c", "d", "e"} {
		if size, _ := store.redis.HStrLen(ctx, store.eventKey("logs", id), "body").Result(); size == 0 {
			t.Fatal("prefetch lost body", id)
		}
	}
	n, err = store.Publish(ctx, "logs", func([]db.Event) error { return nil })
	if err != nil || n != 4 {
		t.Fatal(n, err)
	}
	stats, err := store.Stats(ctx, "logs", nil)
	if err != nil || stats.Totals.Published != 5 || stats.Totals.Pending != 0 {
		t.Fatal(stats, err)
	}
}

func TestOutputBufferLostLeaseCannotAcknowledge(t *testing.T) {
	store, ctx, cancel := bufferedFixture(t)
	defer cancel()
	n, err := store.Publish(ctx, "logs", func([]db.Event) error {
		return store.redis.Set(ctx, "logister:publish-lock:logs", "new-owner", time.Minute).Err()
	})
	if err == nil || n != 0 {
		t.Fatal(n, err)
	}
	if pending, _ := store.Pending(ctx, "logs"); pending != 5 {
		t.Fatal(pending)
	}
	if owner, _ := store.redis.Get(ctx, "logister:publish-lock:logs").Result(); owner != "new-owner" {
		t.Fatal(owner)
	}
}

func TestOutputBufferCancellationJoinsReader(t *testing.T) {
	store, _, cancel := bufferedFixture(t)
	defer cancel()
	ctx, stop := context.WithCancel(context.Background())
	n, err := store.Publish(ctx, "logs", func([]db.Event) error { stop(); return ctx.Err() })
	if !errors.Is(err, context.Canceled) || n != 0 {
		t.Fatal(n, err)
	}
	if pending, _ := store.Pending(context.Background(), "logs"); pending != 5 {
		t.Fatal(pending)
	}
}

func TestOutputBufferSessionsDoNotPublishSamePrefix(t *testing.T) {
	store, ctx, cancel := bufferedFixture(t)
	defer cancel()
	first, release := make(chan struct{}), make(chan struct{})
	results := make(chan int, 2)
	failures := make(chan error, 2)
	var calls atomic.Int32
	write := func([]db.Event) error {
		if calls.Add(1) == 1 {
			close(first)
			<-release
		}
		return nil
	}
	go func() { n, err := store.Publish(ctx, "logs", write); results <- n; failures <- err }()
	<-first
	peer, stop := context.WithTimeout(ctx, 200*time.Millisecond)
	n, err := store.Publish(peer, "logs", write)
	stop()
	close(release)
	if n != 0 || err != nil {
		t.Fatalf("busy publisher should yield without error, got %d, %v", n, err)
	}
	total := <-results
	if err := <-failures; err != nil {
		t.Fatal(err)
	}
	if total != 5 || calls.Load() != 5 {
		t.Fatal(total, calls.Load())
	}
	stats, err := store.Stats(ctx, "logs", nil)
	if err != nil || stats.Totals.Published != 5 || stats.Totals.Pending != 0 {
		t.Fatal(stats, err)
	}
}

func TestOutputBufferKeepsPreparingNewArrivalsDuringBlockedWrite(t *testing.T) {
	store, _ := testStore(t)
	store.SetPublishMaxBytes(1)
	store.SetOutputBufferBytes(2)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	store.Accept(ctx, "logs", "", "", []db.Event{event("a", "payments", "one")})
	blocked, release := make(chan struct{}), make(chan struct{})
	done := make(chan int, 1)
	fail := make(chan error, 1)
	go func() {
		n, err := store.Publish(ctx, "logs", func(batch []db.Event) error {
			if batch[0].ID == "a" {
				close(blocked)
				<-release
			}
			return nil
		})
		done <- n
		fail <- err
	}()
	<-blocked
	time.Sleep(30 * time.Millisecond) // The queue after the reserved first event is empty.
	if _, err := store.Accept(ctx, "logs", "", "", []db.Event{event("b", "payments", "two"), event("c", "payments", "three")}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	close(release)
	if n := <-done; n != 3 {
		t.Fatal("reader stopped while the writer was busy", n)
	}
	if err := <-fail; err != nil {
		t.Fatal(err)
	}
}
