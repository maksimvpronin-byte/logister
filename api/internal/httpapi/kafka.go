package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/logister/logister/api/internal/db"
	"github.com/logister/logister/api/internal/kafkainput"
	"log"
	"net/http"
	"strings"
	"time"
)

func (s *Server) ConfigureKafka(ctx context.Context) (*kafkainput.Consumer, error) {
	if s.cfg.KafkaBrokers == "" {
		return nil, nil
	}
	if s.cfg.KafkaDirect {
		if err := s.database.CheckKafkaDirect(ctx); err != nil {
			return nil, err
		}
	}
	state, err := db.NewKafkaState(ctx, s.database)
	if err != nil {
		return nil, err
	}
	consumer, err := kafkainput.New(ctx, kafkainput.Config{Brokers: strings.Split(s.cfg.KafkaBrokers, ","), Topic: s.cfg.KafkaTopic, Group: s.cfg.KafkaGroup, BatchSize: max(1, s.cfg.KafkaBatchRecords), MaxBytes: s.cfg.KafkaBatchBytes, Profile: s.cfg.StageProfile}, state, s.acceptKafka)
	if err != nil {
		return nil, err
	}
	s.kafka = consumer
	return consumer, nil
}
func (s *Server) decodeKafka(messages []kafkainput.Message) ([]ingestRequest, error) {
	started := time.Now()
	if s.telemetry != nil {
		defer func() { s.telemetry.Observe("decode", time.Since(started), false) }()
	}
	if s.cfg.StageProfile {
		defer func() {
			log.Printf("ingest_stage_profile {\"stage\":\"decode\",\"documents\":%d,\"ms\":%f}", len(messages), time.Since(started).Seconds()*1000)
		}()
	}
	batch := make([]ingestRequest, len(messages))
	err := parallelEvents(len(messages), s.cfg.IngestWorkers, func(i int) error {
		message := messages[i]
		if err := batch[i].UnmarshalJSON(message.Value); err != nil {
			return fmt.Errorf("invalid JSON at %s/%d/%d", message.Topic, message.Partition, message.Offset)
		}
		// Kafka output adds transport time. It is not part of application data
		// identity. Support the old default key only for Fluent Bit envelopes.
		if batch[i].DeliveryID != "" {
			if _, ok := batch[i].Payload["__logister_kafka_timestamp"]; ok {
				delete(batch[i].Payload, "__logister_kafka_timestamp")
				batch[i].PayloadRaw = nil
				batch[i].KafkaTransport = true
			} else if _, ok := batch[i].Payload["@timestamp"].(json.Number); ok && string(message.Key) == batch[i].DeliveryID {
				delete(batch[i].Payload, "@timestamp")
				batch[i].PayloadRaw = nil
				batch[i].KafkaTransport = true
			}
		}
		if batch[i].ID == "" && batch[i].DeliveryID == "" {
			batch[i].DeliveryID = kafkaIdentity(message.Topic, message.Partition, message.Offset)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return batch, nil
}

// PrepareKafka applies the same validation, routing, IDs and document format
// for transport experiments without choosing an acceptance journal.
func (s *Server) PrepareKafka(ctx context.Context, messages []kafkainput.Message) ([]db.Event, error) {
	batch, err := s.decodeKafka(messages)
	if err != nil {
		return nil, err
	}
	return s.prepareIngest(ctx, batch, "", "", "kafka", "", nil)
}
func (s *Server) acceptKafka(ctx context.Context, messages []kafkainput.Message) error {
	if s.cfg.KafkaDirect {
		events, err := s.PrepareKafka(ctx, messages)
		if err != nil {
			return err
		}
		return publishKafkaDirect(ctx, s.cfg.OpenSearchIndex, events, s.database, s.search, s.telemetry)
	}
	batch, err := s.decodeKafka(messages)
	if err != nil {
		return err
	}
	_, err = s.acceptIngest(ctx, batch, "", "", "kafka", "", nil)
	return err
}

func (s *Server) handleKafka(w http.ResponseWriter, r *http.Request) {
	if s.kafka == nil {
		writeJSON(w, 200, kafkainput.Status{CapturedAt: time.Now().UTC()})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if r.Method == http.MethodPut {
		var body struct {
			Paused *bool `json:"paused"`
		}
		if !decodeAuthJSON(w, r, &body) {
			return
		}
		if body.Paused == nil {
			writeError(w, 400, "paused is required")
			return
		}
		if err := s.kafka.SetPaused(ctx, *body.Paused); err != nil {
			writeError(w, 503, "Kafka control unavailable.")
			return
		}
	}
	status, err := s.kafka.Status(ctx)
	if err != nil {
		writeError(w, 503, "Kafka statistics unavailable.")
		return
	}
	writeJSON(w, 200, status)
}
