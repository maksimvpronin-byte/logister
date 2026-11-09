package db

import (
	"errors"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

func TestKafkaPlacementSurvivesRotationAndCompletion(t *testing.T) {
	ctx, database := testDB(t)
	if err := database.CheckKafkaDirect(ctx); err != nil {
		t.Skipf("Kafka placement test requires a MongoDB replica set: %v", err)
	}
	stream, err := NewStreamStore(database).Save(ctx, Stream{
		Name: "kafka-replay", Title: "Kafka replay", Routing: defaultRouting(),
		Storage: StorageConfig{MaxDocuments: 1, MaxAgeHours: 1, RetentionDays: 1},
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	const namespace = "logs-v2"
	events := []Event{
		{ID: "a", Hash: "first", StreamName: stream.Name, DocumentTime: time.Now().UTC().Format(time.RFC3339Nano)},
		{ID: "b", Hash: "second", StreamName: stream.Name},
	}
	first, accepted, err := database.ReserveKafka(ctx, namespace, events)
	if err != nil || accepted != 2 || first["a"].Index == first["b"].Index {
		t.Fatalf("reserve %+v, %d, %v", first, accepted, err)
	}
	if _, err := RotateStream(ctx, database, namespace, stream.Name, ""); err != nil {
		t.Fatal(err)
	}
	replay := events[0]
	replay.StreamName = "different"
	replay.DocumentTime = "different"
	again, accepted, err := database.ReserveKafka(ctx, namespace, []Event{replay, replay})
	if err != nil || accepted != 0 || again["a"].Index != first["a"].Index || again["a"].Stream != stream.Name || again["a"].DocumentTime != events[0].DocumentTime {
		t.Fatalf("replay %+v, %d, %v", again, accepted, err)
	}
	replay.TargetIndex = first["a"].Index
	if n, err := database.CompleteKafka(ctx, namespace, []Event{replay, replay}); err != nil || n != 1 {
		t.Fatalf("complete %d, %v", n, err)
	}
	if n, err := database.CompleteKafka(ctx, namespace, []Event{replay}); err != nil || n != 0 {
		t.Fatalf("repeat completion %d, %v", n, err)
	}
	counts, err := database.KafkaIngestCounts(ctx, namespace)
	if err != nil || counts[stream.ID] != (IngestCounts{Accepted: 2, Published: 1, Pending: 1}) {
		t.Fatalf("counts %+v, %v", counts, err)
	}
	events[1].TargetIndex = first["a"].Index
	if n, err := database.CompleteKafka(ctx, namespace, events[1:]); err != nil || n != 1 {
		t.Fatalf("complete with stale target %d, %v", n, err)
	}
	counts, err = database.KafkaIngestCounts(ctx, namespace)
	if err != nil || counts[stream.ID] != (IngestCounts{Accepted: 2, Published: 2}) {
		t.Fatalf("counts after stale target %+v, %v", counts, err)
	}
	index := first["a"].Index
	old := time.Now().Add(-48 * time.Hour)
	if _, err := database.indices().UpdateByID(ctx, index, bson.M{"$set": bson.M{"sealed_at": old}}); err != nil {
		t.Fatal(err)
	}
	removed := false
	remove := func(string) error { removed = true; return nil }
	if deleted, err := DeleteExpired(ctx, database, KafkaIndexNamespace(namespace), index, remove); err != nil || deleted || removed {
		t.Fatalf("recent publication did not protect the index: %v, %v", deleted, err)
	}
	if _, err := database.kafkaCompletions().UpdateMany(ctx, bson.M{"index": index}, bson.M{"$set": bson.M{"last_write": old}}); err != nil {
		t.Fatal(err)
	}
	if deleted, err := DeleteExpired(ctx, database, KafkaIndexNamespace(namespace), index, remove); err != nil || !deleted || !removed {
		t.Fatalf("expired and fully published index was not deleted: %v, %v", deleted, err)
	}
	replay.Hash = "conflicting"
	if _, _, err := database.ReserveKafka(ctx, namespace, []Event{replay}); !errors.Is(err, ErrIdentityConflict) {
		t.Fatalf("expected identity conflict, got %v", err)
	}
	fresh := []Event{{ID: "c", Hash: "third", StreamName: stream.Name}}
	placement, _, err := database.ReserveKafka(ctx, namespace, fresh)
	if err != nil {
		t.Fatal(err)
	}
	fresh[0].TargetIndex = placement["c"].Index
	type completion struct {
		count int
		err   error
	}
	results := make(chan completion, 2)
	for i := 0; i < cap(results); i++ {
		go func() {
			n, err := database.CompleteKafka(ctx, namespace, fresh)
			results <- completion{n, err}
		}()
	}
	firstCompletion, secondCompletion := <-results, <-results
	if firstCompletion.err != nil || secondCompletion.err != nil || firstCompletion.count+secondCompletion.count != 1 {
		t.Fatalf("concurrent completion %+v, %+v", firstCompletion, secondCompletion)
	}
}
