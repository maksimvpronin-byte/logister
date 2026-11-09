package redisingest

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/logister/logister/api/internal/db"
	"github.com/redis/go-redis/v9"
)

type stealingCatalog struct {
	Catalog
	steal func()
}

func (c stealingCatalog) Stream(ctx context.Context, name string) (StreamInfo, error) {
	c.steal()
	return c.Catalog.Stream(ctx, name)
}

type stealOnTransactionHook struct {
	steal func()
	done  bool
}

func (h *stealOnTransactionHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) { return next(ctx, network, addr) }
}
func (h *stealOnTransactionHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }
func (h *stealOnTransactionHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, commands []redis.Cmder) error {
		for _, cmd := range commands {
			if cmd.Name() == "hset" && !h.done {
				h.done = true
				h.steal()
				break
			}
		}
		return next(ctx, commands)
	}
}
func TestExpiredOrReplacedIntakeCannotWriteBodiesCountersOrBatchReceipt(t *testing.T) {
	for _, phase := range []string{"before-watch", "before-exec"} {
		t.Run(phase, func(t *testing.T) {
			ctx := context.Background()
			store, catalog := testStore(t)
			steal := func() {
				if err := store.redis.Set(ctx, "logister:lock:logs", "new-owner", time.Minute).Err(); err != nil {
					t.Fatal(err)
				}
			}
			if phase == "before-watch" {
				store.catalog = stealingCatalog{catalog, steal}
			} else {
				store.redis.AddHook(&stealOnTransactionHook{steal: steal})
			}
			_, err := store.Accept(ctx, "logs", "request-key", "request-hash", []db.Event{event("a", "payments", "original")})
			if !errors.Is(err, errIntakeLeaseLost) {
				t.Fatalf("expected lost lease, got %v", err)
			}
			if n, _ := store.Pending(ctx, "logs"); n != 0 {
				t.Fatal("stale writer appended queue", n)
			}
			for _, key := range []string{store.eventKey("logs", "a"), store.batchKey("logs", "request-key"), store.acceptedKey("logs", "payments")} {
				if n, _ := store.redis.Exists(ctx, key).Result(); n != 0 {
					t.Fatal("stale writer persisted", key)
				}
			}
			if owner, _ := store.redis.Get(ctx, "logister:lock:logs").Result(); owner != "new-owner" {
				t.Fatal("stale release removed replacement lock", owner)
			}
		})
	}
}

func TestConcurrentIntakesCommitTheSameBatchOnlyOnce(t *testing.T) {
	store, catalog := testStore(t)
	info := catalog.streams["payments"]
	info.Storage.MaxDocuments = 1000
	catalog.streams["payments"] = info
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	events := []db.Event{event("a", "payments", "one"), event("b", "payments", "two")}
	type result struct {
		receipt db.Receipt
		err     error
	}
	results := make(chan result, 8)
	for i := 0; i < 8; i++ {
		go func() { r, e := store.Accept(ctx, "logs", "same-key", "same-hash", events); results <- result{r, e} }()
	}
	duplicates := 0
	for i := 0; i < 8; i++ {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		if r.receipt.Count != 2 {
			t.Fatal(r.receipt)
		}
		duplicates += r.receipt.Duplicates
	}
	if duplicates != 14 {
		t.Fatal("duplicate accounting", duplicates)
	}
	if pending, _ := store.Pending(ctx, "logs"); pending != 2 {
		t.Fatal("duplicate queue entries", pending)
	}
	if accepted, _ := store.counter(ctx, store.acceptedKey("logs", "payments")); accepted != 2 {
		t.Fatal("duplicate accepted counts", accepted)
	}
}

type stealingSealCatalog struct {
	Catalog
	steal func()
}

func (c stealingSealCatalog) SealGeneration(ctx context.Context, name string, count int64) error {
	if err := c.Catalog.SealGeneration(ctx, name, count); err != nil {
		return err
	}
	c.steal()
	return nil
}
func TestRotationWithReplacedLeaseKeepsTheActivePointer(t *testing.T) {
	ctx := context.Background()
	store, catalog := testStore(t)
	if _, err := store.Accept(ctx, "logs", "", "", []db.Event{event("a", "payments", "one")}); err != nil {
		t.Fatal(err)
	}
	before, err := store.loadActive(ctx, "logs", "payments")
	if err != nil {
		t.Fatal(err)
	}
	store.catalog = stealingSealCatalog{catalog, func() {
		if err := store.redis.Set(ctx, "logister:lock:logs", "new-owner", time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
	}}
	if _, err := store.NoteRotation(ctx, "logs", "payments", before.Index); !errors.Is(err, errIntakeLeaseLost) {
		t.Fatal("stale rotation succeeded", err)
	}
	after, err := store.loadActive(ctx, "logs", "payments")
	if err != nil || after.Index != before.Index {
		t.Fatal("stale rotation erased active index", after, err)
	}
}
