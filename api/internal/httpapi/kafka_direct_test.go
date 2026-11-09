package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/logister/logister/api/internal/db"
)

type testKafkaPlacement struct {
	placement db.KafkaPlacement
	fail      bool
}

func (p *testKafkaPlacement) ReserveKafka(context.Context, string, []db.Event) (map[string]db.KafkaPlacement, int, error) {
	return map[string]db.KafkaPlacement{"id": p.placement}, 0, nil
}

func (p *testKafkaPlacement) CompleteKafka(context.Context, string, []db.Event) (int, error) {
	if p.fail {
		return 0, errors.New("API lost its connection before completing the receipt")
	}
	p.placement.Published = true
	return 1, nil
}

type testKafkaWriter struct {
	writes []db.Event
	fail   bool
}

func (w *testKafkaWriter) PublishEventsDurable(_ context.Context, _ string, events []db.Event) error {
	if w.fail {
		return errors.New("bulk write failed")
	}
	w.writes = append(w.writes, events...)
	return nil
}

func TestKafkaDirectReplayAfterBulkWrite(t *testing.T) {
	ctx := context.Background()
	timestamp := time.Now().UTC()
	p := &testKafkaPlacement{placement: db.KafkaPlacement{Index: "before-rotation", Stream: "original", DocumentTime: timestamp.Format(time.RFC3339Nano)}}
	w := &testKafkaWriter{fail: true}
	events := []db.Event{{ID: "id", StreamName: "changed", Document: json.RawMessage(`{"stream":"changed","message":"log"}`)}}
	if err := publishKafkaDirect(ctx, "logs-v2", events, p, w, nil); err == nil || p.placement.Published {
		t.Fatal("bulk failure was treated as published")
	}
	w.fail, p.fail = false, true
	if err := publishKafkaDirect(ctx, "logs-v2", events, p, w, nil); err == nil || p.placement.Published {
		t.Fatal("failure before receipt completion was hidden")
	}
	p.fail = false
	if err := publishKafkaDirect(ctx, "logs-v2", events, p, w, nil); err != nil {
		t.Fatal(err)
	}
	if err := publishKafkaDirect(ctx, "logs-v2", events, p, w, nil); err != nil {
		t.Fatal(err)
	}
	if len(w.writes) != 2 || !p.placement.Published {
		t.Fatalf("expected two writes and a completed receipt, got %d writes", len(w.writes))
	}
	for _, write := range w.writes {
		var projection map[string]string
		if err := json.Unmarshal(write.Document, &projection); err != nil {
			t.Fatal(err)
		}
		if write.ID != "id" || write.TargetIndex != "before-rotation" || projection["stream"] != "original" || projection["@timestamp"] != timestamp.Format(time.RFC3339Nano) {
			t.Fatalf("replay changed the original write: %+v, %+v", write, projection)
		}
	}
}
