package db

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ErrNotFound is a missing document, including an expired session.
var ErrNotFound = errors.New("not found")

// DB is the configuration replica set. Log bodies stay in Redis.
type DB struct {
	client        *mongo.Client
	database      *mongo.Database
	kafkaWriterID string
}

func Connect(ctx context.Context, databaseURL string) (*DB, error) {
	name := databaseName(databaseURL)
	var last error
	deadline := time.Now().Add(60 * time.Second)
	for {
		client, err := mongo.Connect(ctx, options.Client().ApplyURI(databaseURL).SetServerSelectionTimeout(5*time.Second))
		if err == nil {
			pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = client.Ping(pingCtx, nil)
			cancel()
			if err == nil {
				return &DB{client: client, database: client.Database(name), kafkaWriterID: newID()}, nil
			}
			_ = client.Disconnect(context.Background())
		}
		last = err
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("connect mongo: %w", last)
		}
		time.Sleep(2 * time.Second)
	}
}

func (d *DB) Close() {
	if d != nil && d.client != nil {
		_ = d.client.Disconnect(context.Background())
	}
}

func (d *DB) Ping(ctx context.Context) error {
	if d == nil || d.client == nil {
		return errors.New("mongo is not configured")
	}
	return d.client.Ping(ctx, nil)
}

func databaseName(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "logister"
	}
	name := strings.Trim(parsed.Path, "/")
	if name == "" {
		return "logister"
	}
	return name
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func (d *DB) coll(name string) *mongo.Collection { return d.database.Collection(name) }

func missing(err error) bool { return errors.Is(err, mongo.ErrNoDocuments) }

func asFloat(v any) float64 {
	switch n := v.(type) {
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	case float64:
		return n
	default:
		return 0
	}
}

// ReplicaStatus is the configuration set this process can see.
type ReplicaStatus struct {
	SetName, Primary string
	Members          int
	DataBytes        float64
	Connections      float64
}

func (d *DB) ReplicaStatus(ctx context.Context) (ReplicaStatus, error) {
	var stats bson.M
	if err := d.database.RunCommand(ctx, bson.D{{Key: "dbStats", Value: 1}}).Decode(&stats); err != nil {
		return ReplicaStatus{}, err
	}
	var status struct {
		Connections struct {
			Current int32 `bson:"current"`
		} `bson:"connections"`
		Repl struct {
			SetName string   `bson:"setName"`
			Primary string   `bson:"primary"`
			Hosts   []string `bson:"hosts"`
		} `bson:"repl"`
	}
	if err := d.client.Database("admin").RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&status); err != nil {
		return ReplicaStatus{}, err
	}
	return ReplicaStatus{
		SetName: status.Repl.SetName, Primary: status.Repl.Primary, Members: len(status.Repl.Hosts),
		DataBytes: asFloat(stats["dataSize"]), Connections: float64(status.Connections.Current),
	}, nil
}
