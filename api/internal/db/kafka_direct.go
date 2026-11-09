package db

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

// Kafka generations use the same stream catalog and read alias, with their own
// allocation counters so the HTTP journal's cached generations stay independent.
func KafkaIndexNamespace(namespace string) string { return namespace + "-kafka" }

type KafkaPlacement struct {
	ID           string `bson:"_id"`
	Hash         string `bson:"hash"`
	Stream       string `bson:"stream"`
	Index        string `bson:"index"`
	DocumentTime string `bson:"document_time"`
	Published    bool   `bson:"published"`
	Fresh        bool   `bson:"-"`
}

func (d *DB) CheckKafkaDirect(ctx context.Context) error {
	var status struct {
		Set string `bson:"setName"`
	}
	if err := d.client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&status); err != nil {
		return err
	}
	if status.Set == "" {
		return fmt.Errorf("direct Kafka ingestion requires a MongoDB replica set")
	}
	return nil
}

func (d *DB) kafkaTransaction(ctx context.Context, run func(context.Context) error) error {
	session, err := d.client.StartSession()
	if err != nil {
		return err
	}
	defer session.EndSession(ctx)
	_, err = session.WithTransaction(ctx, func(tx mongo.SessionContext) (any, error) {
		return nil, run(tx)
	}, options.Transaction().SetWriteConcern(writeconcern.Majority()))
	return err
}

func kafkaReceiptID(namespace, id string) string { return namespace + ":" + id }

type kafkaCompletion struct {
	Index     string    `bson:"index"`
	Count     int64     `bson:"count"`
	LastWrite time.Time `bson:"last_write"`
}

func (d *DB) kafkaCompletions() *mongo.Collection { return d.coll("kafka_completions") }

func (d *DB) kafkaCompletionTotals(ctx context.Context, indices []string) (map[string]kafkaCompletion, error) {
	totals := make(map[string]kafkaCompletion)
	if len(indices) == 0 {
		return totals, nil
	}
	cursor, err := d.kafkaCompletions().Aggregate(ctx, mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"index": bson.M{"$in": indices}}}},
		{{Key: "$group", Value: bson.M{"_id": "$index", "count": bson.M{"$sum": "$count"}, "last_write": bson.M{"$max": "$last_write"}}}},
		{{Key: "$project", Value: bson.M{"_id": 0, "index": "$_id", "count": 1, "last_write": 1}}},
	})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var completion kafkaCompletion
		if err := cursor.Decode(&completion); err != nil {
			return nil, err
		}
		totals[completion.Index] = completion
	}
	return totals, cursor.Err()
}

func (d *DB) kafkaPlacements(ctx context.Context, filter bson.M) ([]KafkaPlacement, error) {
	cursor, err := d.coll("kafka_placements").Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	var found []KafkaPlacement
	err = cursor.All(ctx, &found)
	return found, err
}

// ReserveKafka assigns each identity once, before any external bulk write.
// MongoDB stores only identity and placement metadata. Kafka retains the body.
func (d *DB) ReserveKafka(ctx context.Context, namespace string, events []Event) (map[string]KafkaPlacement, int, error) {
	unique := make(map[string]Event, len(events))
	keys := make([]string, 0, len(events))
	for _, event := range events {
		key := kafkaReceiptID(namespace, event.ID)
		if previous, ok := unique[key]; ok {
			if previous.Hash != event.Hash {
				return nil, 0, ErrIdentityConflict
			}
			continue
		}
		unique[key] = event
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return nil, 0, nil
	}

	var result map[string]KafkaPlacement
	var accepted int
	err := d.kafkaTransaction(ctx, func(tx context.Context) error {
		found, err := d.kafkaPlacements(tx, bson.M{"_id": bson.M{"$in": keys}})
		if err != nil {
			return err
		}
		placements := make(map[string]KafkaPlacement, len(keys))
		for _, placement := range found {
			event := unique[placement.ID]
			if placement.Hash != event.Hash {
				return ErrIdentityConflict
			}
			placements[event.ID] = placement
		}
		byStream := map[string][]Event{}
		for _, key := range keys {
			event := unique[key]
			if _, exists := placements[event.ID]; !exists {
				byStream[event.StreamName] = append(byStream[event.StreamName], event)
			}
		}
		names := make([]string, 0, len(byStream))
		for name := range byStream {
			names = append(names, name)
		}
		sort.Strings(names)
		now := timeNow()
		insert := make([]any, 0, len(keys)-len(found))
		for _, name := range names {
			events := byStream[name]
			targets, err := d.reserveKafkaIndices(tx, KafkaIndexNamespace(namespace), name, len(events), now)
			if err != nil {
				return err
			}
			for i, event := range events {
				placement := KafkaPlacement{ID: kafkaReceiptID(namespace, event.ID), Hash: event.Hash, Stream: name, Index: targets[i], DocumentTime: event.DocumentTime, Fresh: true}
				placements[event.ID] = placement
				insert = append(insert, placement)
			}
		}
		if len(insert) != 0 {
			if _, err := d.coll("kafka_placements").InsertMany(tx, insert); err != nil {
				return err
			}
		}
		result, accepted = placements, len(insert)
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return result, accepted, nil
}

func (d *DB) reserveKafkaIndices(ctx context.Context, namespace, stream string, count int, now time.Time) ([]string, error) {
	id, storage, err := d.StreamInfo(ctx, stream)
	if err != nil {
		return nil, err
	}
	gen, _, err := d.ActiveGeneration(ctx, namespace, id)
	if err != nil {
		return nil, err
	}
	targets := make([]string, 0, count)
	for len(targets) < count {
		ageLimit := storage.MaxAgeHours > 0 && now.Sub(gen.Created) >= time.Duration(storage.MaxAgeHours)*time.Hour
		docLimit := storage.MaxDocuments > 0 && gen.Accepted >= storage.MaxDocuments
		if gen.Index == "" || ageLimit || docLimit {
			if gen.Index != "" {
				if _, err := d.indices().UpdateByID(ctx, gen.Index, bson.M{"$set": bson.M{"sealed_at": now}}); err != nil {
					return nil, err
				}
			}
			next, err := d.NextGeneration(ctx, namespace, id)
			if err != nil {
				return nil, err
			}
			name := fmt.Sprintf("%s-s-%s-%06d", namespace, strings.ReplaceAll(id, "-", ""), next)
			if _, err := d.indices().InsertOne(ctx, indexDoc{Name: name, Namespace: namespace, StreamID: id, Generation: next, CreatedAt: now}); err != nil {
				return nil, err
			}
			gen = GenerationRecord{Index: name, Created: now}
		}
		take := int64(count - len(targets))
		if storage.MaxDocuments > 0 {
			take = min(take, storage.MaxDocuments-gen.Accepted)
		}
		if _, err := d.indices().UpdateByID(ctx, gen.Index, bson.M{"$inc": bson.M{"accepted_count": take, "kafka_pending": take}}); err != nil {
			return nil, err
		}
		for i := int64(0); i < take; i++ {
			targets = append(targets, gen.Index)
		}
		gen.Accepted += take
	}
	return targets, nil
}

// CompleteKafka is idempotent. Only the first successful completion reduces
// the generation's pending count, even when consumers retry the same identity.
func (d *DB) CompleteKafka(ctx context.Context, namespace string, events []Event) (int, error) {
	if len(events) == 0 {
		return 0, nil
	}
	keys := make([]string, 0, len(events))
	target := events[0].TargetIndex
	for _, event := range events {
		keys = append(keys, kafkaReceiptID(namespace, event.ID))
		if event.TargetIndex != target {
			target = ""
		}
	}
	completed := 0
	err := d.kafkaTransaction(ctx, func(tx context.Context) error {
		receipts := d.coll("kafka_placements")
		markPublished := bson.M{"$set": bson.M{"published": true}}
		byIndex := map[string]int64{}
		var total int64
		if target != "" {
			changed, err := receipts.UpdateMany(tx, bson.M{
				"_id": bson.M{"$in": keys}, "index": target, "published": false,
			}, markPublished)
			if err != nil {
				return err
			}
			if changed.ModifiedCount > 0 {
				byIndex[target] = changed.ModifiedCount
				total = changed.ModifiedCount
			}
		}
		// The normal path already has the reserved index. Resolve remaining
		// receipts from MongoDB for callers without a placement or on replay.
		if total < int64(len(keys)) {
			filter := bson.M{"_id": bson.M{"$in": keys}, "published": false}
			cursor, err := receipts.Find(tx, filter, options.Find().SetProjection(bson.M{"_id": 0, "index": 1}))
			if err != nil {
				return err
			}
			var remaining []struct {
				Index string `bson:"index"`
			}
			if err := cursor.All(tx, &remaining); err != nil {
				return err
			}
			if len(remaining) > 0 {
				if _, err := receipts.UpdateMany(tx, filter, markPublished); err != nil {
					return err
				}
				for _, placement := range remaining {
					byIndex[placement.Index]++
				}
				total += int64(len(remaining))
			}
		}
		// Each writer maintains its own completion totals in the same
		// transaction as the receipts, without updating the reservation row.
		batches := make([]mongo.WriteModel, 0, len(byIndex))
		for index, count := range byIndex {
			if err := d.indices().FindOne(tx, bson.M{"_id": index, "kafka_pending": bson.M{"$gte": count}}, options.FindOne().SetProjection(bson.M{"_id": 1})).Err(); err != nil {
				return fmt.Errorf("check Kafka pending count for %s: %w", index, err)
			}
			batches = append(batches, mongo.NewUpdateOneModel().
				SetFilter(bson.M{"_id": bson.D{{Key: "index", Value: index}, {Key: "writer", Value: d.kafkaWriterID}}}).
				SetUpdate(bson.M{"$setOnInsert": bson.M{"index": index}, "$inc": bson.M{"count": count}, "$currentDate": bson.M{"last_write": true}}).
				SetUpsert(true))
		}
		if len(batches) > 0 {
			if _, err := d.kafkaCompletions().BulkWrite(tx, batches); err != nil {
				return err
			}
		}
		completed = int(total)
		return nil
	})
	return completed, err
}

func (d *DB) KafkaIngestCounts(ctx context.Context, namespace string) (map[string]IngestCounts, error) {
	cursor, err := d.indices().Find(ctx, bson.M{"namespace": KafkaIndexNamespace(namespace)}, options.Find().SetProjection(bson.M{
		"stream_id": 1, "accepted_count": 1, "kafka_pending": 1,
	}))
	if err != nil {
		return nil, err
	}
	var generations []indexDoc
	if err := cursor.All(ctx, &generations); err != nil {
		return nil, err
	}
	indices := make([]string, 0, len(generations))
	for _, doc := range generations {
		indices = append(indices, doc.Name)
	}
	totals, err := d.kafkaCompletionTotals(ctx, indices)
	if err != nil {
		return nil, err
	}
	counts := map[string]IngestCounts{}
	for _, doc := range generations {
		pending := max(int64(0), doc.KafkaPending-totals[doc.Name].Count)
		count := counts[doc.StreamID]
		count.Accepted += doc.AcceptedCount
		count.Pending += pending
		count.Published += doc.AcceptedCount - pending
		counts[doc.StreamID] = count
	}
	return counts, nil
}
