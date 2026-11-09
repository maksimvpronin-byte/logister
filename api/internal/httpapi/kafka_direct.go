package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/logister/logister/api/internal/db"
	"github.com/logister/logister/api/internal/telemetry"
)

type kafkaPlacements interface {
	ReserveKafka(context.Context, string, []db.Event) (map[string]db.KafkaPlacement, int, error)
	CompleteKafka(context.Context, string, []db.Event) (int, error)
}

type kafkaBulkWriter interface {
	PublishEventsDurable(context.Context, string, []db.Event) error
}

func publishKafkaDirect(ctx context.Context, namespace string, events []db.Event, placements kafkaPlacements, writer kafkaBulkWriter, meter *telemetry.Meter) error {
	started := time.Now()
	reserved, accepted, err := placements.ReserveKafka(ctx, namespace, events)
	if meter != nil {
		meter.Observe("kafka_reserve", time.Since(started), err != nil)
	}
	if err != nil {
		return err
	}
	pending := make([]db.Event, 0, len(reserved))
	inputBytes := 0
	for _, event := range events {
		placement, ok := reserved[event.ID]
		if !ok {
			return fmt.Errorf("missing placement for Kafka event %s", event.ID)
		}
		if placement.Published {
			continue
		}
		if placement.Fresh {
			inputBytes += event.InputBytes
		}
		// Suppress any later occurrence of this ID in the current batch.
		placement.Published = true
		reserved[event.ID] = placement
		event.TargetIndex = placement.Index
		if placement.Stream != event.StreamName || placement.DocumentTime != event.DocumentTime {
			var document map[string]json.RawMessage
			if err := json.Unmarshal(event.Document, &document); err != nil {
				return err
			}
			if document == nil {
				return fmt.Errorf("missing document for Kafka event %s", event.ID)
			}
			document["stream"], _ = json.Marshal(placement.Stream)
			document["@timestamp"], _ = json.Marshal(placement.DocumentTime)
			event.Document, err = json.Marshal(document)
			if err != nil {
				return err
			}
		}
		pending = append(pending, event)
	}
	if meter != nil {
		meter.Accepted(accepted, inputBytes)
		meter.Duplicates(len(events) - accepted)
	}
	if len(pending) == 0 {
		return nil
	}
	if err := writer.PublishEventsDurable(ctx, namespace, pending); err != nil {
		return err
	}
	started = time.Now()
	completed, err := placements.CompleteKafka(ctx, namespace, pending)
	if meter != nil {
		meter.Observe("kafka_complete", time.Since(started), err != nil)
		if err == nil {
			meter.Published(completed)
		}
	}
	return err
}
