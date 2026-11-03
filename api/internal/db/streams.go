package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/logister/logister/api/internal/fieldpath"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var ErrStreamNotFound = errors.New("stream not found")
var ErrStreamExists = errors.New("stream name already exists")

type RoutingRule struct {
	Field    string `json:"field"`
	Operator string `json:"operator"`
	Value    string `json:"value"`
}
type RoutingConfig struct {
	Enabled  bool          `json:"enabled"`
	Priority int           `json:"priority"`
	Match    string        `json:"match"`
	Rules    []RoutingRule `json:"rules"`
}
type StorageConfig struct {
	MaxAgeHours   int   `json:"max_age_hours"`
	MaxSizeMB     int64 `json:"max_size_mb"`
	MaxDocuments  int64 `json:"max_documents"`
	RetentionDays int   `json:"retention_days"`
}

const streamSchema = `
CREATE TABLE IF NOT EXISTS streams (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), name text UNIQUE NOT NULL,
 title text NOT NULL, description text NOT NULL DEFAULT '', created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE streams ADD COLUMN IF NOT EXISTS deleted_at timestamptz;
ALTER TABLE streams ADD COLUMN IF NOT EXISTS routing jsonb NOT NULL DEFAULT '{"enabled":false,"priority":100,"match":"all","rules":[]}';
ALTER TABLE streams ADD COLUMN IF NOT EXISTS storage jsonb NOT NULL DEFAULT '{"max_age_hours":24,"max_size_mb":1024,"max_documents":1000000,"retention_days":0}';
CREATE TABLE IF NOT EXISTS stream_indices (
 namespace text NOT NULL, stream_id uuid NOT NULL REFERENCES streams(id),
 index_name text PRIMARY KEY, generation bigint NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(), sealed_at timestamptz,
 deleted_at timestamptz, accepted_count bigint NOT NULL DEFAULT 0,
 UNIQUE(namespace,stream_id,generation)
);
CREATE UNIQUE INDEX IF NOT EXISTS stream_active_index ON stream_indices(namespace,stream_id) WHERE sealed_at IS NULL;
`

var streamName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

func ValidateStream(stream Stream) error {
	if !streamName.MatchString(stream.Name) {
		return errors.New("name must contain 1–63 lowercase letters, digits, hyphens or underscores")
	}
	if strings.TrimSpace(stream.Title) == "" || len(stream.Title) > 160 || len(stream.Description) > 2000 {
		return errors.New("title is required (up to 160 bytes); description must not exceed 2000 bytes")
	}
	r := stream.Routing
	if r.Priority < 0 || r.Priority > 1000 || (r.Match != "all" && r.Match != "any") || len(r.Rules) > 20 {
		return errors.New("invalid routing priority, match mode or number of rules")
	}
	if r.Enabled && (len(r.Rules) == 0 || stream.Name == "all") {
		return errors.New("automatic routing requires rules and a non-default stream")
	}
	for _, rule := range r.Rules {
		if fieldpath.Validate(rule.Field) != nil || (rule.Operator != "equals" && rule.Operator != "contains" && rule.Operator != "starts_with" && rule.Operator != "exists" && rule.Operator != "not_exists") || len(rule.Value) > 4096 {
			return errors.New("invalid routing field, operator or value")
		}
	}
	s := stream.Storage
	if s.MaxAgeHours < 0 || s.MaxAgeHours > 87600 || s.MaxSizeMB < 0 || s.MaxSizeMB > 1048576 || s.MaxDocuments < 0 || s.MaxDocuments > 1000000000 || s.RetentionDays < 0 || s.RetentionDays > 3650 {
		return errors.New("storage settings are outside the allowed range")
	}
	if s.MaxAgeHours == 0 && s.MaxSizeMB == 0 && s.MaxDocuments == 0 {
		return errors.New("enable at least one rotation threshold")
	}
	if s.RetentionDays > 0 && s.MaxAgeHours == 0 {
		return errors.New("retention requires an age rotation threshold so quiet streams can expire")
	}
	return nil
}

type streamSource interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}
type StreamStore struct{ source streamSource }

func NewStreamStore(source streamSource) *StreamStore { return &StreamStore{source: source} }

const streamColumns = `id::text,name,title,description,routing,storage`

func scanStream(row pgx.Row) (Stream, error) {
	var s Stream
	err := row.Scan(&s.ID, &s.Name, &s.Title, &s.Description, &s.Routing, &s.Storage)
	return s, err
}
func (store *StreamStore) List(ctx context.Context) ([]Stream, error) {
	rows, err := store.source.Query(ctx, `SELECT `+streamColumns+` FROM streams WHERE deleted_at IS NULL ORDER BY (name='all') DESC,(routing->>'priority')::int,name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Stream{}
	for rows.Next() {
		s, err := scanStream(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
func (store *StreamStore) Save(ctx context.Context, s Stream, create bool) (Stream, error) {
	if err := ValidateStream(s); err != nil {
		return Stream{}, err
	}
	routing, _ := json.Marshal(s.Routing)
	storage, _ := json.Marshal(s.Storage)
	var row pgx.Row
	if create {
		row = store.source.QueryRow(ctx, `INSERT INTO streams(name,title,description,routing,storage) VALUES($1,$2,$3,$4,$5) RETURNING `+streamColumns, s.Name, strings.TrimSpace(s.Title), s.Description, routing, storage)
	} else {
		row = store.source.QueryRow(ctx, `UPDATE streams SET title=$2,description=$3,routing=$4,storage=$5 WHERE name=$1 AND deleted_at IS NULL RETURNING `+streamColumns, s.Name, strings.TrimSpace(s.Title), s.Description, routing, storage)
	}
	saved, err := scanStream(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Stream{}, ErrStreamNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return Stream{}, ErrStreamExists
	}
	if err != nil {
		return Stream{}, fmt.Errorf("save stream: %w", err)
	}
	return saved, nil
}
