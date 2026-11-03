package ingest

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/logister/logister/api/internal/db"
	"github.com/logister/logister/api/internal/search"
)

// StreamJournal is the Redis record of the open generation and unpublished targets.
type StreamJournal interface {
	NoteRotation(context.Context, string, string, string) (bool, error)
	TargetPending(context.Context, string, string) (int64, error)
}

// MaintainStreams manages only explicitly registered stream indices. Legacy
// indexes are outside this registry and cannot be deleted by these policies.
func MaintainStreams(ctx context.Context, pool *pgxpool.Pool, client *search.Client, namespace string, journal StreamJournal) error {
	indices, err := db.ListManagedIndices(ctx, pool, namespace, "")
	if err != nil {
		return err
	}
	for _, index := range indices {
		if index.DeletedAt != nil {
			continue
		}
		if index.SealedAt == nil {
			policy := index.Storage
			due := (policy.MaxAgeHours > 0 && time.Since(index.CreatedAt) >= time.Duration(policy.MaxAgeHours)*time.Hour) || (policy.MaxDocuments > 0 && index.AcceptedCount >= policy.MaxDocuments)
			if !due && policy.MaxSizeMB > 0 {
				stats, err := client.IndexStats(ctx, index.Name)
				// An accepted index may not exist in search yet; the publisher creates it.
				if err == nil {
					due = stats.SizeBytes >= policy.MaxSizeMB*1024*1024
				}
			}
			if due {
				if journal != nil {
					if _, err := journal.NoteRotation(ctx, namespace, index.Stream, index.Name); err != nil {
						return err
					}
				}
				if _, err := db.RotateStream(ctx, pool, namespace, index.Stream, index.Name); err != nil {
					if errors.Is(err, db.ErrStreamNotFound) {
						continue
					}
					return err
				}
			}
		} else if index.Storage.RetentionDays > 0 && time.Since(*index.SealedAt) > time.Duration(index.Storage.RetentionDays)*24*time.Hour {
			if journal != nil {
				pending, err := journal.TargetPending(ctx, namespace, index.Name)
				if err != nil {
					return err
				}
				if pending > 0 {
					continue
				}
			}
			if _, err := db.DeleteExpired(ctx, pool, namespace, index.Name, func(name string) error { return client.DeleteStreamIndex(ctx, name) }); err != nil {
				return err
			}
		}
	}
	return nil
}

func RunLifecycle(ctx context.Context, pool *pgxpool.Pool, client *search.Client, namespace string, journal StreamJournal) {
	for ctx.Err() == nil {
		task, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := MaintainStreams(task, pool, client, namespace, journal)
		cancel()
		if err != nil && ctx.Err() == nil {
			log.Printf("stream lifecycle retry: %v", err)
		}
		timer := time.NewTimer(10 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
