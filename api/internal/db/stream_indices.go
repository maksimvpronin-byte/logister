package db

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type ManagedIndex struct {
	Name          string        `json:"name"`
	Stream        string        `json:"stream"`
	Generation    int64         `json:"generation"`
	CreatedAt     time.Time     `json:"created_at"`
	SealedAt      *time.Time    `json:"sealed_at"`
	DeletedAt     *time.Time    `json:"deleted_at"`
	AcceptedCount int64         `json:"accepted_count"`
	Storage       StorageConfig `json:"-"`
}

// Lock streams in a stable order before assigning any target. Settings changes
// and rotation serialize with acceptance; one batch remains one transaction.
func lockEventStreams(ctx context.Context, tx pgx.Tx, events []Event) error {
	names := map[string]bool{}
	for _, event := range events {
		if event.StreamName != "" {
			names[event.StreamName] = true
		}
	}
	ordered := make([]string, 0, len(names))
	for name := range names {
		ordered = append(ordered, name)
	}
	sort.Strings(ordered)
	for _, name := range ordered {
		var id string
		if err := tx.QueryRow(ctx, `SELECT id::text FROM streams WHERE name=$1 FOR UPDATE`, name).Scan(&id); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrStreamNotFound
			}
			return err
		}
	}
	return nil
}

func assignStreamIndex(ctx context.Context, tx pgx.Tx, namespace, name string) (string, error) {
	var streamID string
	var storage StorageConfig
	if err := tx.QueryRow(ctx, `SELECT id::text,storage FROM streams WHERE name=$1`, name).Scan(&streamID, &storage); err != nil {
		return "", err
	}
	var target string
	var accepted int64
	var created time.Time
	err := tx.QueryRow(ctx, `SELECT index_name,accepted_count,created_at FROM stream_indices WHERE namespace=$1 AND stream_id=$2 AND sealed_at IS NULL`, namespace, streamID).Scan(&target, &accepted, &created)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return "", err
	}
	due := target != "" && ((storage.MaxDocuments > 0 && accepted >= storage.MaxDocuments) || (storage.MaxAgeHours > 0 && time.Since(created) >= time.Duration(storage.MaxAgeHours)*time.Hour))
	if due {
		if _, err := tx.Exec(ctx, `UPDATE stream_indices SET sealed_at=now() WHERE index_name=$1`, target); err != nil {
			return "", err
		}
		target = ""
	}
	if target == "" {
		var generation int64
		if err := tx.QueryRow(ctx, `SELECT coalesce(max(generation),0)+1 FROM stream_indices WHERE namespace=$1 AND stream_id=$2`, namespace, streamID).Scan(&generation); err != nil {
			return "", err
		}
		target = fmt.Sprintf("%s-s-%s-%06d", namespace, strings.ReplaceAll(streamID, "-", ""), generation)
		if _, err := tx.Exec(ctx, `INSERT INTO stream_indices(namespace,stream_id,index_name,generation) VALUES($1,$2,$3,$4)`, namespace, streamID, target, generation); err != nil {
			return "", err
		}
	}
	_, err = tx.Exec(ctx, `UPDATE stream_indices SET accepted_count=accepted_count+1 WHERE index_name=$1`, target)
	return target, err
}

func ListManagedIndices(ctx context.Context, source streamSource, namespace, name string) ([]ManagedIndex, error) {
	rows, err := source.Query(ctx, `SELECT i.index_name,s.name,i.generation,i.created_at,i.sealed_at,i.deleted_at,i.accepted_count,s.storage
 FROM stream_indices i JOIN streams s ON s.id=i.stream_id WHERE i.namespace=$1 AND ($2='' OR s.name=$2)
 ORDER BY i.created_at DESC,i.index_name DESC`, namespace, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ManagedIndex{}
	for rows.Next() {
		var i ManagedIndex
		if err := rows.Scan(&i.Name, &i.Stream, &i.Generation, &i.CreatedAt, &i.SealedAt, &i.DeletedAt, &i.AcceptedCount, &i.Storage); err != nil {
			return nil, err
		}
		out = append(out, i)
	}
	return out, rows.Err()
}

// expected prevents a delayed worker from sealing a newer generation. No empty
// successor is created: the next accepted event allocates one under the same lock.
func RotateStream(ctx context.Context, source transactionSource, namespace, name, expected string) (bool, error) {
	tx, err := source.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.Background())
	var id string
	if err := tx.QueryRow(ctx, `SELECT id::text FROM streams WHERE name=$1 FOR UPDATE`, name).Scan(&id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, ErrStreamNotFound
		}
		return false, err
	}
	tag, err := tx.Exec(ctx, `UPDATE stream_indices SET sealed_at=now() WHERE namespace=$1 AND stream_id=$2 AND sealed_at IS NULL AND ($3='' OR index_name=$3)`, namespace, id, expected)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, tx.Commit(ctx)
}

// DeleteExpired never deletes an active index or one with unpublished events.
// The durable target is immutable, so retries cannot recreate expired documents.
// Only payloads are cleared; hashes/IDs stay as deduplication tombstones.
func DeleteExpired(ctx context.Context, source transactionSource, namespace, name string, remove func(string) error) (bool, error) {
	tx, err := source.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.Background())
	var streamID string
	var retention int
	if err := tx.QueryRow(ctx, `SELECT s.id::text,(s.storage->>'retention_days')::int FROM streams s JOIN stream_indices i ON i.stream_id=s.id WHERE i.namespace=$1 AND i.index_name=$2 FOR UPDATE OF s`, namespace, name).Scan(&streamID, &retention); err != nil {
		return false, err
	}
	if retention <= 0 {
		return false, nil
	}
	var eligible bool
	if err := tx.QueryRow(ctx, `SELECT deleted_at IS NULL AND sealed_at IS NOT NULL AND sealed_at < now()-make_interval(days=>$2)
 AND NOT EXISTS(SELECT 1 FROM ingest_events WHERE target_index=$1 AND published_at IS NULL)
 FROM stream_indices WHERE index_name=$1 FOR UPDATE`, name, retention).Scan(&eligible); err != nil {
		return false, err
	}
	if !eligible {
		return false, nil
	}
	if err := remove(name); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `UPDATE ingest_events SET document=NULL WHERE index_name=$1 AND target_index=$2 AND published_at IS NOT NULL`, namespace, name); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `UPDATE stream_indices SET deleted_at=now() WHERE index_name=$1`, name); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}
