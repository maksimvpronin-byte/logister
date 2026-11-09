package ingest

import (
	"context"
	"log"
	"time"

	"github.com/logister/logister/api/internal/db"
	"github.com/logister/logister/api/internal/search"
)

type Publisher interface {
	Publish(context.Context, string, func([]db.Event) error) (int, error)
}

type publicationRetry struct {
	stage   string
	delay   time.Duration
	lastLog time.Time
}

func (r *publicationRetry) failure(stage string, now time.Time) (time.Duration, bool) {
	if r.stage != stage {
		*r = publicationRetry{stage: stage}
	}
	r.delay = min(5*time.Second, max(time.Second, 2*r.delay))
	report := r.lastLog.IsZero() || now.Sub(r.lastLog) >= time.Minute
	if report {
		r.lastLog = now
	}
	return r.delay, report
}

// Run retries both index preparation and publication. The intake journal keeps
// accepted records while OpenSearch is offline.
func Run(ctx context.Context, store Publisher, client *search.Client, base string) {
	ready := false
	var retry publicationRetry
	for ctx.Err() == nil {
		task, cancel := context.WithTimeout(ctx, 30*time.Second)
		var err error
		count := 0
		stage := "OpenSearch"
		if !ready {
			err = client.PrepareStorage(task, base)
			if err == nil {
				err = client.EnsureReadAlias(task, search.StorageIndex(base))
			}
			ready = err == nil
		}

		if err == nil && store == nil {
			cancel()
			return
		}
		if err == nil {
			var writeFailed bool
			count, err = store.Publish(task, search.StorageIndex(base), func(events []db.Event) error {
				err := client.PublishEvents(task, search.StorageIndex(base), events)
				writeFailed = err != nil
				return err
			})
			if writeFailed {
				ready = false
			} else {
				stage = "Redis journal"
			}
		}
		cancel()
		if ctx.Err() != nil {
			return
		}
		delay := 200 * time.Millisecond
		if err != nil {
			var report bool
			delay, report = retry.failure(stage, time.Now())
			if report {
				log.Printf("durable publisher retry (%s): %v", stage, err)
			}
		} else if retry.stage != "" {
			log.Printf("durable publisher recovered")
			retry = publicationRetry{}
		}
		if count > 0 && err == nil {
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
