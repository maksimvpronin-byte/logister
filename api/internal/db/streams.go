package db

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/logister/logister/api/internal/fieldpath"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var ErrStreamNotFound = errors.New("stream not found")
var ErrStreamExists = errors.New("stream name already exists")

type Stream struct {
	ID          string        `json:"id"`
	Name        string        `json:"name"`
	Title       string        `json:"title"`
	Description string        `json:"description"`
	Routing     RoutingConfig `json:"routing"`
	Storage     StorageConfig `json:"storage"`
}

type Input struct {
	ID     string         `json:"id"`
	Name   string         `json:"name"`
	Type   string         `json:"type"`
	Config map[string]any `json:"config"`
}

type RoutingRule struct {
	Field    string `json:"field" bson:"field"`
	Operator string `json:"operator" bson:"operator"`
	Value    string `json:"value" bson:"value"`
}
type RoutingConfig struct {
	Enabled  bool          `json:"enabled" bson:"enabled"`
	Priority int           `json:"priority" bson:"priority"`
	Match    string        `json:"match" bson:"match"`
	Rules    []RoutingRule `json:"rules" bson:"rules"`
}
type StorageConfig struct {
	MaxAgeHours   int   `json:"max_age_hours" bson:"max_age_hours"`
	MaxSizeMB     int64 `json:"max_size_mb" bson:"max_size_mb"`
	MaxDocuments  int64 `json:"max_documents" bson:"max_documents"`
	RetentionDays int   `json:"retention_days" bson:"retention_days"`
}

type streamDoc struct {
	ID          string        `bson:"_id"`
	Name        string        `bson:"name"`
	Title       string        `bson:"title"`
	Description string        `bson:"description"`
	CreatedAt   time.Time     `bson:"created_at"`
	DeletedAt   *time.Time    `bson:"deleted_at"`
	Routing     RoutingConfig `bson:"routing"`
	Storage     StorageConfig `bson:"storage"`
}

func (d streamDoc) stream() Stream {
	if d.Routing.Rules == nil {
		d.Routing.Rules = []RoutingRule{}
	}
	return Stream{ID: d.ID, Name: d.Name, Title: d.Title, Description: d.Description, Routing: d.Routing, Storage: d.Storage}
}

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

type StreamStore struct{ db *DB }

func NewStreamStore(database *DB) *StreamStore { return &StreamStore{db: database} }

func (d *DB) streams() *mongo.Collection { return d.coll("streams") }

func (store *StreamStore) List(ctx context.Context) ([]Stream, error) {
	cursor, err := store.db.streams().Find(ctx, bson.M{"deleted_at": nil})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	out := []Stream{}
	for cursor.Next(ctx) {
		var doc streamDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}
		out = append(out, doc.stream())
	}
	if err := cursor.Err(); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		if (out[i].Name == "all") != (out[j].Name == "all") {
			return out[i].Name == "all"
		}
		if out[i].Routing.Priority != out[j].Routing.Priority {
			return out[i].Routing.Priority < out[j].Routing.Priority
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

func (store *StreamStore) Save(ctx context.Context, s Stream, create bool) (Stream, error) {
	if err := ValidateStream(s); err != nil {
		return Stream{}, err
	}
	if s.Routing.Rules == nil {
		s.Routing.Rules = []RoutingRule{}
	}
	title := strings.TrimSpace(s.Title)
	if create {
		doc := streamDoc{ID: newID(), Name: s.Name, Title: title, Description: s.Description, CreatedAt: time.Now().UTC(), Routing: s.Routing, Storage: s.Storage}
		if _, err := store.db.streams().InsertOne(ctx, doc); err != nil {
			if mongo.IsDuplicateKeyError(err) {
				return Stream{}, ErrStreamExists
			}
			return Stream{}, fmt.Errorf("save stream: %w", err)
		}
		return doc.stream(), nil
	}
	var doc streamDoc
	err := store.db.streams().FindOneAndUpdate(ctx,
		bson.M{"name": s.Name, "deleted_at": nil},
		bson.M{"$set": bson.M{"title": title, "description": s.Description, "routing": s.Routing, "storage": s.Storage}},
		options.FindOneAndUpdate().SetReturnDocument(options.After),
	).Decode(&doc)
	if missing(err) {
		return Stream{}, ErrStreamNotFound
	}
	if err != nil {
		return Stream{}, fmt.Errorf("save stream: %w", err)
	}
	return doc.stream(), nil
}

func ListStreams(ctx context.Context, database *DB) ([]Stream, error) {
	return NewStreamStore(database).List(ctx)
}
