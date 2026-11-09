package db

import (
	"context"
	"fmt"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func mongoOptionsUpsert() *options.UpdateOptions {
	return options.Update().SetUpsert(true)
}

func timeNow() time.Time { return time.Now().UTC() }

func sortInputs(in []Input) {
	sort.Slice(in, func(i, j int) bool { return in[i].Name < in[j].Name })
}

// UpgradeMetadata creates indexes and installs built-in settings once.
// Later starts preserve streams, routing and inputs the operator changed.
func UpgradeMetadata(ctx context.Context, database *DB) error {
	if err := database.ensureIndexes(ctx); err != nil {
		return err
	}
	if err := database.seedBuiltin(ctx); err != nil {
		return err
	}
	if err := database.upgradeInputPath(ctx); err != nil {
		return err
	}
	return UpgradeRoutingDefaults(ctx, database)
}

func (d *DB) ensureIndexes(ctx context.Context) error {
	if _, err := d.streams().Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "name", Value: 1}}, Options: options.Index().SetUnique(true)}); err != nil {
		return fmt.Errorf("stream indexes: %w", err)
	}
	if _, err := d.indices().Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "namespace", Value: 1}, {Key: "stream_id", Value: 1}, {Key: "generation", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "namespace", Value: 1}, {Key: "stream_id", Value: 1}}, Options: options.Index().SetUnique(true).SetPartialFilterExpression(bson.M{"sealed_at": nil, "deleted_at": nil})},
	}); err != nil {
		return fmt.Errorf("index catalog indexes: %w", err)
	}
	if _, err := d.kafkaCompletions().Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "index", Value: 1}}}); err != nil {
		return fmt.Errorf("Kafka completion indexes: %w", err)
	}
	if _, err := d.coll("inputs").Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "name", Value: 1}}, Options: options.Index().SetUnique(true)}); err != nil {
		return fmt.Errorf("input indexes: %w", err)
	}
	if _, err := d.coll("auth_sessions").Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "expires_at", Value: 1}}}); err != nil {
		return err
	}
	_, err := d.coll("monitoring_nodes").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "seen_at", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(7 * 24 * 60 * 60),
	})
	return err
}

func defaultRouting() RoutingConfig {
	return RoutingConfig{Enabled: false, Priority: 100, Match: "all", Rules: []RoutingRule{}}
}

func defaultStorage() StorageConfig {
	return StorageConfig{MaxAgeHours: 24, MaxSizeMB: 1024, MaxDocuments: 1000000}
}

func (d *DB) seedBuiltin(ctx context.Context) error {
	builtins := []streamDoc{
		{Name: "all", Title: "All messages", Description: "Default stream for every message"},
		{Name: "web-access", Title: "Web access", Description: "HTTP / nginx access logs"},
		{Name: "system", Title: "System", Description: "Syslog and host messages"},
	}
	for _, stream := range builtins {
		stream.ID = newID()
		stream.Routing = defaultRouting()
		stream.Storage = defaultStorage()
		stream.CreatedAt = timeNow()
		if _, err := d.streams().InsertOne(ctx, stream); err != nil && !mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("seed stream %s: %w", stream.Name, err)
		}
	}
	inputs := []inputDoc{
		{Name: "http-json", Type: "http", Config: bson.M{"path": "/api/ingest/json"}},
		{Name: "sample-seed", Type: "internal", Config: bson.M{"description": "Seeds demo messages on startup"}},
	}
	for _, input := range inputs {
		input.ID = newID()
		if _, err := d.coll("inputs").InsertOne(ctx, input); err != nil && !mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("seed input %s: %w", input.Name, err)
		}
	}
	return nil
}

type inputDoc struct {
	ID     string `bson:"_id"`
	Name   string `bson:"name"`
	Type   string `bson:"type"`
	Config bson.M `bson:"config"`
}

func (d *DB) upgradeInputPath(ctx context.Context) error {
	_, err := d.coll("inputs").UpdateOne(ctx, bson.M{"name": "http-json", "type": "http", "config.path": "/ingest/json"}, bson.M{"$set": bson.M{"config.path": "/api/ingest/json"}})
	if err != nil {
		return fmt.Errorf("upgrade HTTP input path: %w", err)
	}
	return nil
}

func sameRouting(a, b RoutingConfig) bool {
	if a.Enabled != b.Enabled || a.Priority != b.Priority || a.Match != b.Match || len(a.Rules) != len(b.Rules) {
		return false
	}
	for i := range a.Rules {
		if a.Rules[i] != b.Rules[i] {
			return false
		}
	}
	return true
}

// UpgradeRoutingDefaults installs the original rules once and leaves later edits alone.
func UpgradeRoutingDefaults(ctx context.Context, database *DB) error {
	migrations := database.coll("metadata_migrations")
	err := migrations.FindOne(ctx, bson.M{"_id": "routing-defaults-v1"}).Err()
	if err == nil {
		return nil
	}
	if !missing(err) {
		return fmt.Errorf("upgrade default routing rules: %w", err)
	}
	cursor, err := database.streams().Find(ctx, bson.M{"name": bson.M{"$in": []string{"all", "web-access", "system"}}})
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var doc streamDoc
		if err := cursor.Decode(&doc); err != nil {
			return err
		}
		switch doc.Name {
		case "web-access":
			if sameRouting(doc.Routing, defaultRouting()) {
				doc.Routing = RoutingConfig{Enabled: true, Priority: 100, Match: "all", Rules: []RoutingRule{{Field: "source", Operator: "equals", Value: "nginx"}}}
			}
		case "system":
			if sameRouting(doc.Routing, defaultRouting()) {
				doc.Routing = RoutingConfig{Enabled: true, Priority: 100, Match: "all", Rules: []RoutingRule{{Field: "source", Operator: "equals", Value: "cron"}}}
			}
		case "all":
			if doc.Title == "All messages" && doc.Description == "Default stream for every message" {
				doc.Title = "Unmatched messages"
				doc.Description = "Default destination when no routing rule matches"
			}
		}
		if _, err := database.streams().ReplaceOne(ctx, bson.M{"_id": doc.ID}, doc); err != nil {
			return fmt.Errorf("upgrade default routing rules: %w", err)
		}
	}
	if err := cursor.Err(); err != nil {
		return err
	}
	_, err = migrations.InsertOne(ctx, bson.M{"_id": "routing-defaults-v1", "applied_at": timeNow()})
	if mongo.IsDuplicateKeyError(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("upgrade default routing rules: %w", err)
	}
	return nil
}

func ListInputs(ctx context.Context, database *DB) ([]Input, error) {
	cursor, err := database.coll("inputs").Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	out := []Input{}
	for cursor.Next(ctx) {
		var doc inputDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}
		cfg := map[string]any{}
		for key, value := range doc.Config {
			cfg[key] = value
		}
		out = append(out, Input{ID: doc.ID, Name: doc.Name, Type: doc.Type, Config: cfg})
	}
	if err := cursor.Err(); err != nil {
		return nil, err
	}
	sortInputs(out)
	return out, nil
}
