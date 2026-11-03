package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
)

var ErrIdentityConflict = errors.New("idempotency key or event id was reused with different data")

const ingestSchema = `
CREATE TABLE IF NOT EXISTS ingest_batches (
 index_name text NOT NULL, batch_key text NOT NULL, payload_hash text NOT NULL,
 accepted integer NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(index_name,batch_key)
);
CREATE TABLE IF NOT EXISTS ingest_events (
 index_name text NOT NULL, event_id text NOT NULL, payload_hash text NOT NULL,
 document jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
 published_at timestamptz, PRIMARY KEY(index_name,event_id)
);
CREATE INDEX IF NOT EXISTS ingest_pending ON ingest_events(created_at) WHERE published_at IS NULL;
ALTER TABLE ingest_events ADD COLUMN IF NOT EXISTS target_index text NOT NULL DEFAULT '';
ALTER TABLE ingest_events ALTER COLUMN document DROP NOT NULL;
CREATE INDEX IF NOT EXISTS ingest_target_pending ON ingest_events(target_index) WHERE published_at IS NULL;
ALTER TABLE ingest_events ADD COLUMN IF NOT EXISTS hash_version smallint NOT NULL DEFAULT 1;
`

type Event struct {
	ID, Hash    string
	Document    json.RawMessage
	StreamName  string
	TargetIndex string
	HashVersion int
	LegacyHash  string
}
type Receipt struct {
	Count      int `json:"count"`
	Duplicates int `json:"duplicates"`
}
type transactionSource interface {
	Begin(context.Context) (pgx.Tx, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}
type IngestStore struct{ source transactionSource }

func NewIngestStore(source transactionSource) *IngestStore { return &IngestStore{source: source} }

// Accept commits the entire batch before acknowledging it. Event IDs and batch
// keys remain in PostgreSQL after publication so delayed retries stay idempotent.
func (s *IngestStore) Accept(ctx context.Context, index, key, hash string, events []Event) (Receipt, error) {
	receipt := Receipt{Count: len(events)}
	tx, err := s.source.Begin(ctx)
	if err != nil {
		return Receipt{}, err
	}
	defer tx.Rollback(context.Background())
	if key != "" {
		tag, err := tx.Exec(ctx, `INSERT INTO ingest_batches(index_name,batch_key,payload_hash,accepted) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, index, key, hash, len(events))
		if err != nil {
			return Receipt{}, err
		}
		if tag.RowsAffected() == 0 {
			var previous string
			var count int
			if err := tx.QueryRow(ctx, `SELECT payload_hash,accepted FROM ingest_batches WHERE index_name=$1 AND batch_key=$2`, index, key).Scan(&previous, &count); err != nil {
				return Receipt{}, err
			}
			if previous != hash {
				return Receipt{}, ErrIdentityConflict
			}
			return Receipt{Count: count, Duplicates: count}, tx.Commit(ctx)
		}
	}
	if err := lockEventStreams(ctx, tx, events); err != nil {
		return Receipt{}, err
	}
	for _, event := range events {
		version := event.HashVersion
		if version == 0 {
			version = 1
		}
		tag, err := tx.Exec(ctx, `INSERT INTO ingest_events(index_name,event_id,payload_hash,document,hash_version) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, index, event.ID, event.Hash, event.Document, version)
		if err != nil {
			return Receipt{}, err
		}
		if tag.RowsAffected() == 0 {
			var previous string
			var previousVersion int
			if err := tx.QueryRow(ctx, `SELECT payload_hash,hash_version FROM ingest_events WHERE index_name=$1 AND event_id=$2`, index, event.ID).Scan(&previous, &previousVersion); err != nil {
				return Receipt{}, err
			}
			if previous != event.Hash && !(previousVersion == 1 && event.LegacyHash != "" && previous == event.LegacyHash) {
				return Receipt{}, ErrIdentityConflict
			}
			receipt.Duplicates++
		} else if event.StreamName != "" {
			target, err := assignStreamIndex(ctx, tx, index, event.StreamName)
			if err != nil {
				return Receipt{}, err
			}
			if _, err := tx.Exec(ctx, `UPDATE ingest_events SET target_index=$3 WHERE index_name=$1 AND event_id=$2`, index, event.ID, target); err != nil {
				return Receipt{}, err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Receipt{}, err
	}
	return receipt, nil
}

// Publish holds row locks until a whole projection batch has been acknowledged.
// A crash after OpenSearch writes but before commit simply retries the same IDs.
func (s *IngestStore) Publish(ctx context.Context, index string, write func([]Event) error) (int, error) {
	tx, err := s.source.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.Background())
	rows, err := tx.Query(ctx, `SELECT event_id,document,target_index FROM ingest_events WHERE index_name=$1 AND published_at IS NULL ORDER BY created_at,event_id LIMIT 200 FOR UPDATE SKIP LOCKED`, index)
	if err != nil {
		return 0, err
	}
	var events []Event
	for rows.Next() {
		var event Event
		if err := rows.Scan(&event.ID, &event.Document, &event.TargetIndex); err != nil {
			rows.Close()
			return 0, err
		}
		events = append(events, event)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(events) == 0 {
		return 0, tx.Commit(ctx)
	}
	if err := write(events); err != nil {
		return 0, err
	}
	ids := make([]string, len(events))
	for i, event := range events {
		ids[i] = event.ID
	}
	if _, err := tx.Exec(ctx, `UPDATE ingest_events SET published_at=now() WHERE index_name=$1 AND event_id=ANY($2)`, index, ids); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("publication receipt: %w", err)
	}
	return len(events), nil
}

func (s *IngestStore) Pending(ctx context.Context, index string) (int64, error) {
	var count int64
	err := s.source.QueryRow(ctx, `SELECT count(*) FROM ingest_events WHERE index_name=$1 AND published_at IS NULL`, index).Scan(&count)
	return count, err
}
