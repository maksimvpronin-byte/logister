package ingest

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/logister/logister/api/internal/db"
	"github.com/logister/logister/api/internal/search"
)

// MaintainStreams manages only explicitly registered stream indices. Legacy
// indexes are outside this registry and cannot be deleted by these policies.
func MaintainStreams(ctx context.Context, pool *pgxpool.Pool, client *search.Client, namespace string) error {
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
				if _, err := db.RotateStream(ctx, pool, namespace, index.Stream, index.Name); err != nil {
					return err
				}
			}
		} else if index.Storage.RetentionDays > 0 && time.Since(*index.SealedAt) > time.Duration(index.Storage.RetentionDays)*24*time.Hour {
			if _, err := db.DeleteExpired(ctx, pool, namespace, index.Name, func(name string) error { return client.DeleteStreamIndex(ctx, name) }); err != nil {
				return err
			}
		}
	}
	return nil
}

func RunLifecycle(ctx context.Context, pool *pgxpool.Pool, client *search.Client, namespace string) {
	for ctx.Err() == nil {
		task, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := MaintainStreams(task, pool, client, namespace)
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
