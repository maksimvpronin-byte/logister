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
	ID, Hash       string
	Document       json.RawMessage
	StreamName     string
	TargetIndex    string
	HashVersion    int
	LegacyHash     string
	KafkaTransport bool
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
	duplicates, err := acceptEvents(ctx, tx, index, events)
	if err != nil {
		return Receipt{}, err
	}
	receipt.Duplicates = duplicates
	if err := tx.Commit(ctx); err != nil {
		return Receipt{}, err
	}
	return receipt, nil
}

// acceptEvents inserts a batch with one statement, checks every rejected
// identity in one read, then assigns each stream generation once.
func acceptEvents(ctx context.Context, tx pgx.Tx, index string, events []Event) (int, error) {
	if len(events) == 0 {
		return 0, nil
	}
	seen := map[string]bool{}
	insert := make([]Event, 0, len(events))
	replays := make([]Event, 0)
	for _, event := range events {
		if event.HashVersion == 0 {
			event.HashVersion = 1
		}
		if seen[event.ID] {
			replays = append(replays, event)
			continue
		}
		seen[event.ID] = true
		insert = append(insert, event)
	}
	inserted, err := insertEvents(ctx, tx, index, insert)
	if err != nil {
		return 0, err
	}
	fresh := make([]Event, 0, len(inserted))
	conflicts := replays
	for _, event := range insert {
		if _, ok := inserted[event.ID]; ok {
			fresh = append(fresh, event)
		} else {
			conflicts = append(conflicts, event)
		}
	}
	duplicates, err := resolveConflicts(ctx, tx, index, conflicts)
	if err != nil {
		return 0, err
	}
	return duplicates, assignTargets(ctx, tx, index, fresh)
}

func insertEvents(ctx context.Context, tx pgx.Tx, index string, events []Event) (map[string]struct{}, error) {
	inserted := map[string]struct{}{}
	if len(events) == 0 {
		return inserted, nil
	}
	ids := make([]string, len(events))
	hashes := make([]string, len(events))
	documents := make([]string, len(events))
	versions := make([]int32, len(events))
	for i, event := range events {
		ids[i] = event.ID
		hashes[i] = event.Hash
		documents[i] = string(event.Document)
		versions[i] = int32(event.HashVersion)
	}
	rows, err := tx.Query(ctx, `INSERT INTO ingest_events(index_name,event_id,payload_hash,document,hash_version)
SELECT $1,id,hash,NULLIF(doc,'')::jsonb,ver::smallint
FROM unnest($2::text[],$3::text[],$4::text[],$5::int[]) AS t(id,hash,doc,ver)
ON CONFLICT DO NOTHING
RETURNING event_id`, index, ids, hashes, documents, versions)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		inserted[id] = struct{}{}
	}
	return inserted, rows.Err()
}

func resolveConflicts(ctx context.Context, tx pgx.Tx, index string, events []Event) (int, error) {
	if len(events) == 0 {
		return 0, nil
	}
	ids := make([]string, len(events))
	for i, event := range events {
		ids[i] = event.ID
	}
	rows, err := tx.Query(ctx, `SELECT event_id,payload_hash,hash_version FROM ingest_events WHERE index_name=$1 AND event_id=ANY($2)`, index, ids)
	if err != nil {
		return 0, err
	}
	type stored struct {
		hash    string
		version int
	}
	found := map[string]stored{}
	for rows.Next() {
		var id string
		var item stored
		if err := rows.Scan(&id, &item.hash, &item.version); err != nil {
			rows.Close()
			return 0, err
		}
		found[id] = item
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	for _, event := range events {
		previous, ok := found[event.ID]
		if !ok {
			return 0, fmt.Errorf("accepted event %s was not stored", event.ID)
		}
		compatible := false
		if previous.hash != event.Hash && previous.version == 2 && event.KafkaTransport {
			compatible, err = normalizeKafkaReplay(ctx, tx, index, event, previous.hash)
			if err != nil {
				return 0, err
			}
		}
		if previous.hash != event.Hash && !(previous.version == 1 && event.LegacyHash != "" && previous.hash == event.LegacyHash) && !compatible {
			return 0, ErrIdentityConflict
		}
		if compatible {
			previous.hash = event.Hash
			found[event.ID] = previous
		}
	}
	return len(events), nil
}

func assignTargets(ctx context.Context, tx pgx.Tx, namespace string, events []Event) error {
	idsByStream := map[string][]string{}
	order := make([]string, 0)
	for _, event := range events {
		if event.StreamName == "" {
			continue
		}
		if _, ok := idsByStream[event.StreamName]; !ok {
			order = append(order, event.StreamName)
		}
		idsByStream[event.StreamName] = append(idsByStream[event.StreamName], event.ID)
	}
	if len(order) == 0 {
		return nil
	}
	ids := make([]string, 0, len(events))
	targets := make([]string, 0, len(events))
	for _, name := range order {
		placed, err := placeStreamEvents(ctx, tx, namespace, name, len(idsByStream[name]))
		if err != nil {
			return err
		}
		ids = append(ids, idsByStream[name]...)
		targets = append(targets, placed...)
	}
	tag, err := tx.Exec(ctx, `UPDATE ingest_events AS e SET target_index=t.target
FROM unnest($2::text[],$3::text[]) AS t(id,target)
WHERE e.index_name=$1 AND e.event_id=t.id`, namespace, ids, targets)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != int64(len(ids)) {
		return fmt.Errorf("assigned %d stream targets, updated %d events", len(ids), tag.RowsAffected())
	}
	return nil
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
