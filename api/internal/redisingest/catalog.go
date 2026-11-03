package redisingest

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/logister/logister/api/internal/db"
)

// Catalog reads stream settings from PostgreSQL and records index generations.
// The hot log payload does not pass through it.
type Catalog interface {
	Stream(context.Context, string) (StreamInfo, error)
	NextGeneration(context.Context, string, string) (int64, error)
	ActiveGeneration(context.Context, string, string) (Generation, bool, error)
	OpenGeneration(context.Context, string, string, string, int64) error
	SealGeneration(context.Context, string, int64) error
	ListStreams(context.Context) ([]db.Stream, error)
	AcceptedTotals(context.Context, string) (map[string]int64, error)
}

type StreamInfo struct {
	ID      string
	Storage db.StorageConfig
}

type PGCatalog struct{ pool *pgxpool.Pool }

func NewPGCatalog(pool *pgxpool.Pool) *PGCatalog { return &PGCatalog{pool: pool} }

func (c *PGCatalog) Stream(ctx context.Context, name string) (StreamInfo, error) {
	var info StreamInfo
	err := c.pool.QueryRow(ctx, `SELECT id::text,storage FROM streams WHERE name=$1 AND deleted_at IS NULL`, name).Scan(&info.ID, &info.Storage)
	if errors.Is(err, pgx.ErrNoRows) {
		return StreamInfo{}, db.ErrStreamNotFound
	}
	return info, err
}

func (c *PGCatalog) NextGeneration(ctx context.Context, namespace, streamID string) (int64, error) {
	var generation int64
	err := c.pool.QueryRow(ctx, `SELECT coalesce(max(generation),0)+1 FROM stream_indices WHERE namespace=$1 AND stream_id=$2`, namespace, streamID).Scan(&generation)
	return generation, err
}

func (c *PGCatalog) ActiveGeneration(ctx context.Context, namespace, streamID string) (Generation, bool, error) {
	var gen Generation
	err := c.pool.QueryRow(ctx, `SELECT index_name,generation,accepted_count,created_at FROM stream_indices
 WHERE namespace=$1 AND stream_id=$2 AND sealed_at IS NULL AND deleted_at IS NULL ORDER BY generation DESC LIMIT 1`, namespace, streamID).Scan(&gen.Index, &gen.Generation, &gen.Accepted, &gen.Created)
	if errors.Is(err, pgx.ErrNoRows) {
		return Generation{}, false, nil
	}
	gen.StreamID = streamID
	gen.Created = gen.Created.UTC()
	return gen, err == nil, err
}

func (c *PGCatalog) OpenGeneration(ctx context.Context, namespace, streamID, indexName string, generation int64) error {
	_, err := c.pool.Exec(ctx, `INSERT INTO stream_indices(namespace,stream_id,index_name,generation) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, namespace, streamID, indexName, generation)
	return err
}

func (c *PGCatalog) SealGeneration(ctx context.Context, indexName string, accepted int64) error {
	_, err := c.pool.Exec(ctx, `UPDATE stream_indices SET sealed_at=coalesce(sealed_at,now()), accepted_count=$2 WHERE index_name=$1`, indexName, accepted)
	return err
}

func (c *PGCatalog) ListStreams(ctx context.Context) ([]db.Stream, error) {
	return db.NewStreamStore(c.pool).List(ctx)
}

func (c *PGCatalog) AcceptedTotals(ctx context.Context, namespace string) (map[string]int64, error) {
	rows, err := c.pool.Query(ctx, `SELECT s.name, coalesce(sum(i.accepted_count),0)
 FROM streams s LEFT JOIN stream_indices i ON i.stream_id=s.id AND i.namespace=$1
 WHERE s.deleted_at IS NULL GROUP BY s.name`, namespace)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	totals := map[string]int64{}
	for rows.Next() {
		var name string
		var total int64
		if err := rows.Scan(&name, &total); err != nil {
			return nil, err
		}
		totals[name] = total
	}
	return totals, rows.Err()
}

// Created is the moment a generation became active. Stored in Redis so age
// rotation survives a process restart without a PostgreSQL write per batch.
type Generation struct {
	Index      string
	StreamID   string
	Accepted   int64
	Generation int64
	Created    time.Time
}
