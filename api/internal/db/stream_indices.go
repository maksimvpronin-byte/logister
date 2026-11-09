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

type ManagedIndex struct {
	Name          string        `json:"name"`
	Namespace     string        `json:"-"`
	Stream        string        `json:"stream"`
	Generation    int64         `json:"generation"`
	CreatedAt     time.Time     `json:"created_at"`
	SealedAt      *time.Time    `json:"sealed_at"`
	DeletedAt     *time.Time    `json:"deleted_at"`
	AcceptedCount int64         `json:"accepted_count"`
	KafkaPending  int64         `json:"-"`
	Storage       StorageConfig `json:"-"`
}

type indexDoc struct {
	Name          string     `bson:"_id"`
	Namespace     string     `bson:"namespace"`
	StreamID      string     `bson:"stream_id"`
	Generation    int64      `bson:"generation"`
	CreatedAt     time.Time  `bson:"created_at"`
	SealedAt      *time.Time `bson:"sealed_at"`
	DeletedAt     *time.Time `bson:"deleted_at"`
	AcceptedCount int64      `bson:"accepted_count"`
	// Legacy completions decrement this field. Subtract the completion
	// batches recorded separately to obtain the current pending count.
	KafkaPending   int64     `bson:"kafka_pending,omitempty"`
	KafkaLastWrite time.Time `bson:"kafka_last_write,omitempty"`
}

func (d *DB) indices() *mongo.Collection { return d.coll("stream_indices") }

func (d *DB) StreamInfo(ctx context.Context, name string) (string, StorageConfig, error) {
	var doc streamDoc
	err := d.streams().FindOne(ctx, bson.M{"name": name, "deleted_at": nil}).Decode(&doc)
	if missing(err) {
		return "", StorageConfig{}, ErrStreamNotFound
	}
	return doc.ID, doc.Storage, err
}

func (d *DB) NextGeneration(ctx context.Context, namespace, streamID string) (int64, error) {
	var doc indexDoc
	err := d.indices().FindOne(ctx, bson.M{"namespace": namespace, "stream_id": streamID}, options.FindOne().SetSort(bson.D{{Key: "generation", Value: -1}})).Decode(&doc)
	if missing(err) {
		return 1, nil
	}
	if err != nil {
		return 0, err
	}
	return doc.Generation + 1, nil
}

type GenerationRecord struct {
	Index, StreamID string
	Accepted        int64
	Generation      int64
	Created         time.Time
}

func (d *DB) ActiveGeneration(ctx context.Context, namespace, streamID string) (GenerationRecord, bool, error) {
	var doc indexDoc
	err := d.indices().FindOne(ctx, bson.M{"namespace": namespace, "stream_id": streamID, "sealed_at": nil, "deleted_at": nil}, options.FindOne().SetSort(bson.D{{Key: "generation", Value: -1}})).Decode(&doc)
	if missing(err) {
		return GenerationRecord{}, false, nil
	}
	if err != nil {
		return GenerationRecord{}, false, err
	}
	return GenerationRecord{Index: doc.Name, StreamID: streamID, Accepted: doc.AcceptedCount, Generation: doc.Generation, Created: doc.CreatedAt.UTC()}, true, nil
}

func (d *DB) OpenGeneration(ctx context.Context, namespace, streamID, indexName string, generation int64) error {
	_, err := d.indices().InsertOne(ctx, indexDoc{Name: indexName, Namespace: namespace, StreamID: streamID, Generation: generation, CreatedAt: time.Now().UTC()})
	if mongo.IsDuplicateKeyError(err) {
		return nil
	}
	return err
}

func (d *DB) SealGeneration(ctx context.Context, indexName string, accepted int64) error {
	now := time.Now().UTC()
	res, err := d.indices().UpdateOne(ctx, bson.M{"_id": indexName, "sealed_at": nil}, bson.M{"$set": bson.M{"sealed_at": now, "accepted_count": accepted}})
	if err != nil || res.MatchedCount == 1 {
		return err
	}
	_, err = d.indices().UpdateOne(ctx, bson.M{"_id": indexName}, bson.M{"$set": bson.M{"accepted_count": accepted}})
	return err
}

func (d *DB) AcceptedTotals(ctx context.Context, namespace string) (map[string]int64, error) {
	streams, err := NewStreamStore(d).List(ctx)
	if err != nil {
		return nil, err
	}
	cursor, err := d.indices().Find(ctx, bson.M{"namespace": namespace})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	byID := map[string]int64{}
	for cursor.Next(ctx) {
		var doc indexDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}
		byID[doc.StreamID] += doc.AcceptedCount
	}
	if err := cursor.Err(); err != nil {
		return nil, err
	}
	totals := map[string]int64{}
	for _, stream := range streams {
		totals[stream.Name] = byID[stream.ID]
	}
	return totals, nil
}

func ListManagedIndices(ctx context.Context, database *DB, namespace, name string) ([]ManagedIndex, error) {
	streams, err := database.streams().Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer streams.Close(ctx)
	byID := map[string]streamDoc{}
	for streams.Next(ctx) {
		var doc streamDoc
		if err := streams.Decode(&doc); err != nil {
			return nil, err
		}
		byID[doc.ID] = doc
	}
	if err := streams.Err(); err != nil {
		return nil, err
	}
	cursor, err := database.indices().Find(ctx, bson.M{"namespace": bson.M{"$in": []string{namespace, KafkaIndexNamespace(namespace)}}})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	out := []ManagedIndex{}
	var kafkaIndices []string
	for cursor.Next(ctx) {
		var doc indexDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}
		stream, ok := byID[doc.StreamID]
		if !ok || (name != "" && stream.Name != name) {
			continue
		}
		if doc.Namespace == KafkaIndexNamespace(namespace) {
			kafkaIndices = append(kafkaIndices, doc.Name)
		}
		out = append(out, ManagedIndex{Name: doc.Name, Namespace: doc.Namespace, Stream: stream.Name, Generation: doc.Generation, CreatedAt: doc.CreatedAt, SealedAt: doc.SealedAt, DeletedAt: doc.DeletedAt, AcceptedCount: doc.AcceptedCount, KafkaPending: doc.KafkaPending, Storage: stream.Storage})
	}
	if err := cursor.Err(); err != nil {
		return nil, err
	}
	completed, err := database.kafkaCompletionTotals(ctx, kafkaIndices)
	if err != nil {
		return nil, err
	}
	for i := range out {
		out[i].KafkaPending = max(int64(0), out[i].KafkaPending-completed[out[i].Name].Count)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.After(out[j].CreatedAt)
		}
		return out[i].Name > out[j].Name
	})
	return out, nil
}

// expected prevents a delayed worker from sealing a newer generation. The next
// accepted event allocates the successor.
func RotateStream(ctx context.Context, database *DB, namespace, name, expected string) (bool, error) {
	id, _, err := database.StreamInfo(ctx, name)
	if err != nil {
		return false, err
	}
	filter := bson.M{"namespace": bson.M{"$in": []string{namespace, KafkaIndexNamespace(namespace)}}, "stream_id": id, "sealed_at": nil}
	if expected != "" {
		filter["_id"] = expected
	}
	res, err := database.indices().UpdateMany(ctx, filter, bson.M{"$set": bson.M{"sealed_at": time.Now().UTC()}})
	if err != nil {
		return false, err
	}
	return res.ModifiedCount > 0, nil
}

// DeleteExpired never deletes an active index. Unpublished messages are refused
// by the caller, which checks the Redis journal before asking.
func DeleteExpired(ctx context.Context, database *DB, namespace, name string, remove func(string) error) (bool, error) {
	var doc indexDoc
	err := database.indices().FindOne(ctx, bson.M{"_id": name, "namespace": namespace}).Decode(&doc)
	if missing(err) {
		return false, fmt.Errorf("index %s: %w", name, ErrNotFound)
	}
	if err != nil {
		return false, err
	}
	var stream streamDoc
	if err := database.streams().FindOne(ctx, bson.M{"_id": doc.StreamID}).Decode(&stream); err != nil {
		return false, err
	}
	if stream.Storage.RetentionDays <= 0 || doc.DeletedAt != nil || doc.SealedAt == nil || time.Since(*doc.SealedAt) < time.Duration(stream.Storage.RetentionDays)*24*time.Hour {
		return false, nil
	}
	// Pending placements can still be retried. Give any concurrent duplicate
	// publication a grace interval after the first writer completes.
	pending := doc.KafkaPending
	lastWrite := doc.KafkaLastWrite
	if doc.KafkaPending > 0 {
		completed, err := database.kafkaCompletionTotals(ctx, []string{doc.Name})
		if err != nil {
			return false, err
		}
		record := completed[doc.Name]
		pending -= record.Count
		if record.LastWrite.After(lastWrite) {
			lastWrite = record.LastWrite
		}
	}
	if pending != 0 || time.Since(lastWrite) < time.Minute {
		return false, nil
	}
	if err := remove(name); err != nil {
		return false, err
	}
	_, err = database.indices().UpdateOne(ctx, bson.M{"_id": name}, bson.M{"$set": bson.M{"deleted_at": time.Now().UTC()}})
	return err == nil, err
}
