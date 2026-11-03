package db

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

type IngestCounts struct {
	Accepted  int64 `json:"accepted"`
	Published int64 `json:"published"`
	Pending   int64 `json:"pending"`
}
type StreamIngestStats struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Title string `json:"title"`
	IngestCounts
}
type IngestStats struct {
	CapturedAt time.Time           `json:"captured_at"`
	Totals     IngestCounts        `json:"totals"`
	Streams    []StreamIngestStats `json:"streams"`
}
type statsSource interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}
type IngestStatsStore struct{ source statsSource }

func NewIngestStatsStore(source statsSource) *IngestStatsStore {
	return &IngestStatsStore{source: source}
}

// Stats uses durable stream counters plus the partial pending index. It never
// counts/scans all published events. A single SQL snapshot keeps the totals
// coherent while Accept and Publish commit concurrently. Nil IDs means all;
// an explicitly empty list means no streams.
func (s *IngestStatsStore) Stats(ctx context.Context, namespace string, ids []string) (IngestStats, error) {
	result := IngestStats{Streams: []StreamIngestStats{}}
	rows, err := s.source.Query(ctx, `
 WITH allowed AS (
  SELECT id,name,title FROM streams WHERE deleted_at IS NULL AND ($2::uuid[] IS NULL OR id=ANY($2::uuid[]))
 ), accepted AS (
  SELECT i.stream_id,sum(i.accepted_count)::bigint AS count
  FROM stream_indices i JOIN allowed a ON a.id=i.stream_id
  WHERE i.namespace=$1 GROUP BY i.stream_id
 ), pending AS (
  SELECT i.stream_id,count(*) AS count
  FROM ingest_events e JOIN stream_indices i ON i.index_name=e.target_index
  JOIN allowed a ON a.id=i.stream_id
  WHERE e.index_name=$1 AND i.namespace=$1 AND e.published_at IS NULL
  GROUP BY i.stream_id
 )
 SELECT a.id::text,a.name,a.title,coalesce(c.count,0),coalesce(p.count,0),statement_timestamp()
 FROM allowed a LEFT JOIN accepted c ON c.stream_id=a.id LEFT JOIN pending p ON p.stream_id=a.id
 ORDER BY a.name`, namespace, ids)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var stream StreamIngestStats
		if err := rows.Scan(&stream.ID, &stream.Name, &stream.Title, &stream.Accepted, &stream.Pending, &result.CapturedAt); err != nil {
			return result, err
		}
		if stream.Pending > stream.Accepted {
			return result, errors.New("pending stream messages exceed the durable accepted counter")
		}
		stream.Published = stream.Accepted - stream.Pending
		result.Totals.Accepted += stream.Accepted
		result.Totals.Published += stream.Published
		result.Totals.Pending += stream.Pending
		result.Streams = append(result.Streams, stream)
	}
	if result.CapturedAt.IsZero() {
		result.CapturedAt = time.Now().UTC()
	}
	return result, rows.Err()
}
