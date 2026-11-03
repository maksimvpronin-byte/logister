package db

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
)

var ErrDefaultStream = errors.New("the default stream cannot be deleted")

// Deletion is reversible. The row and event tombstones remain: neither delayed
// delivery nor reusing a name may resurrect a removed stream by accident.
func (s *StreamStore) SetDeleted(ctx context.Context, name string, deleted bool) error {
	if name == "all" {
		return ErrDefaultStream
	}
	var id string
	err := s.source.QueryRow(ctx, `UPDATE streams SET deleted_at=CASE WHEN $2 THEN coalesce(deleted_at,now()) ELSE NULL END WHERE name=$1 RETURNING id::text`, name, deleted).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrStreamNotFound
	}
	return err
}
func (s *StreamStore) ListArchived(ctx context.Context) ([]Stream, error) {
	rows, err := s.source.Query(ctx, `SELECT `+streamColumns+` FROM streams WHERE deleted_at IS NOT NULL ORDER BY deleted_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Stream{}
	for rows.Next() {
		stream, err := scanStream(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, stream)
	}
	return out, rows.Err()
}
