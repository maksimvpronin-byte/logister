package ingest

import (
	"context"
	"errors"
	"log"
	"time"

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
func MaintainStreams(ctx context.Context, database *db.DB, client *search.Client, namespace string, journal StreamJournal) error {
	indices, err := db.ListManagedIndices(ctx, database, namespace, "")
	if err != nil {
		return err
	}
	for _, index := range indices {
		if index.DeletedAt != nil {
			continue
		}
		direct := index.Namespace == db.KafkaIndexNamespace(namespace)
		// Without the old journal we cannot verify pending deliveries for
		// legacy generations. Keep them searchable and leave retention alone.
		if journal == nil && !direct {
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
				if journal != nil && !direct {
					if _, err := journal.NoteRotation(ctx, namespace, index.Stream, index.Name); err != nil {
						return err
					}
				}
				if _, err := db.RotateStream(ctx, database, namespace, index.Stream, index.Name); err != nil {
					if errors.Is(err, db.ErrStreamNotFound) {
						continue
					}
					return err
				}
			}
		} else if index.Storage.RetentionDays > 0 && time.Since(*index.SealedAt) > time.Duration(index.Storage.RetentionDays)*24*time.Hour {
			if index.KafkaPending != 0 {
				continue
			}
			if journal != nil && !direct {
				pending, err := journal.TargetPending(ctx, namespace, index.Name)
				if err != nil {
					return err
				}
				if pending > 0 {
					continue
				}
			}
			if _, err := db.DeleteExpired(ctx, database, index.Namespace, index.Name, func(name string) error { return client.DeleteStreamIndex(ctx, name) }); err != nil {
				return err
			}
		}
	}
	return nil
}

func RunLifecycle(ctx context.Context, database *db.DB, client *search.Client, namespace string, journal StreamJournal) {
	for ctx.Err() == nil {
		task, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := MaintainStreams(task, database, client, namespace, journal)
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
