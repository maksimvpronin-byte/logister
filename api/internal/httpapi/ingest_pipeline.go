package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/logister/logister/api/internal/authz"
	"github.com/logister/logister/api/internal/db"
)

type ingestFailure struct {
	Status  int
	Message string
}

func (e *ingestFailure) Error() string         { return e.Message }
func failure(status int, message string) error { return &ingestFailure{status, message} }

// Both HTTP and Kafka use identical validation, routing, identity and durable
// acceptance. Kafka is a trusted internal input; web roles remain enforced.
func (s *Server) prepareIngest(ctx context.Context, batch []ingestRequest, key, hash, remote, target string, access *authz.Access) ([]db.Event, error) {
	if s.telemetry != nil {
		started := time.Now()
		defer func() { s.telemetry.Observe("prepare", time.Since(started), false) }()
	}
	started := time.Now()
	if s.cfg.StageProfile {
		defer func() {
			log.Printf("ingest_stage_profile {\"stage\":\"prepare\",\"documents\":%d,\"ms\":%f}", len(batch), time.Since(started).Seconds()*1000)
		}()
	}
	var streams []db.Stream
	var err error
	if s.catalog != nil {
		streams, err = s.catalog.List(ctx)
		if err != nil {
			return nil, failure(503, "Stream settings are unavailable. Retry with the same event IDs.")
		}
	}
	events := make([]db.Event, len(batch))
	err = parallelEvents(len(batch), s.cfg.IngestWorkers, func(i int) error {
		item := batch[i]
		item = unwrapIngest(item)
		if target != "" {
			if item.Stream != "" && item.Stream != target {
				return failure(400, "Message stream conflicts with the destination URL.")
			}
			item.Stream = target
			item.ForcedStream = target
		}
		canonical := canonicalPayload(item)
		if access != nil {
			streamID := ""
			for _, stream := range streams {
				if stream.Name == item.Stream {
					streamID = stream.ID
					break
				}
			}
			if !access.AllowIngest(streamID) {
				return failure(403, "Your role cannot ingest into this destination.")
			}
		}
		req, err := normalizeIngest(item, remote)
		if err != nil {
			return failure(400, err.Error())
		}
		if s.catalog != nil {
			req.Stream = item.Stream
			req, err = routeIngest(req, streams)
			// A previously placed Kafka record may name a stream archived
			// since its first delivery. Reservation validates fresh records.
			if err != nil && !(s.cfg.KafkaDirect && remote == "kafka" && item.Stream != "") {
				return failure(400, err.Error())
			}
		}
		id := req.ID
		if id == "" {
			id = req.DeliveryID
		}
		if len(id) > 256 {
			return failure(400, "event id must not exceed 256 bytes")
		}
		if id == "" && key != "" {
			id = "batch:" + key + ":" + strconv.Itoa(i)
		}
		if id == "" {
			id, err = randomIdentity()
			if err != nil {
				return failure(500, "cannot allocate event identity")
			}
		}
		id = digest([]byte(s.cfg.OpenSearchIndex + "\x00" + id))
		var documentTime string
		if s.cfg.KafkaDirect && remote == "kafka" {
			if req.Timestamp == nil {
				now := time.Now().UTC()
				req.Timestamp = &now
			}
			documentTime = req.Timestamp.UTC().Format(time.RFC3339Nano)
		}
		events[i] = db.Event{InputBytes: len(item.PayloadRaw), ID: id, Hash: digest(canonical), Document: ingestDocument(req, id), DocumentTime: documentTime, HashVersion: 2, LegacyHashLazy: sync.OnceValue(func() string { return legacyPayloadHash(item) }), KafkaTransport: item.KafkaTransport}
		if s.catalog != nil {
			events[i].StreamName = req.Stream
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return events, nil
}

func (s *Server) acceptIngest(ctx context.Context, batch []ingestRequest, key, hash, remote, target string, access *authz.Access) (db.Receipt, error) {
	events, err := s.prepareIngest(ctx, batch, key, hash, remote, target, access)
	if err != nil {
		return db.Receipt{}, err
	}

	receipt, err := s.store.Accept(ctx, s.cfg.OpenSearchIndex, key, hash, events)
	if errors.Is(err, db.ErrIdentityConflict) {
		return db.Receipt{}, failure(409, "Event ID or Idempotency-Key was reused with different data.")
	}
	if err != nil {
		log.Printf("durable ingest rejected: %v", err)
		return db.Receipt{}, failure(503, "Ingestion storage is unavailable. Retry with the same event IDs or Idempotency-Key.")
	}
	return receipt, nil
}
func kafkaIdentity(topic string, partition int32, offset int64) string {
	return fmt.Sprintf("kafka:%s:%d:%d", topic, partition, offset)
}
