package ingest

import (
	"context"
	"github.com/logister/logister/api/internal/db"
	"github.com/logister/logister/api/internal/search"
	"log"
	"time"
)

// Run retries both index preparation and publication. The database outbox is the
// durable authority; OpenSearch can be offline without losing accepted records.
func Run(ctx context.Context, store *db.IngestStore, client *search.Client, base string) {
	ready := false
	for ctx.Err() == nil {
		task, cancel := context.WithTimeout(ctx, 30*time.Second)
		var err error
		count := 0
		if !ready {
			err = client.PrepareStorage(task, base)
			ready = err == nil
		}
		if ready && err == nil {
			err = client.Ready(task, search.StorageIndex(base))
			if err != nil {
				ready = false
			}
		}
		if err == nil {
			count, err = store.Publish(task, search.StorageIndex(base), func(events []db.Event) error { return client.IndexEvents(task, search.StorageIndex(base), events) })
		}
		cancel()
		delay := 200 * time.Millisecond
		if err != nil {
			log.Printf("durable publisher retry: %v", err)
			delay = time.Second
		}
		if count == 200 && err == nil {
			continue
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
