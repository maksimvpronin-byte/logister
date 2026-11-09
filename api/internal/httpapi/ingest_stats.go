package httpapi

import (
	"context"
	"log"
	"net/http"
	"slices"
	"time"

	"github.com/logister/logister/api/internal/authz"
	"github.com/logister/logister/api/internal/db"
)

type ingestStatsReader interface {
	Stats(context.Context, string, []string) (db.IngestStats, error)
}

func (s *Server) handleIngestStats(w http.ResponseWriter, r *http.Request) {
	var ids []string
	if s.cfg.AuthEnabled {
		info := r.Context().Value(authContextKey{}).(requestIdentity)
		every, allowed := info.access.Streams(authz.StreamsRead)
		if !every {
			if len(allowed) == 0 {
				writeJSON(w, 200, db.IngestStats{CapturedAt: time.Now().UTC(), Streams: []db.StreamIngestStats{}})
				return
			}
			ids = allowed
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	var result db.IngestStats
	var err error
	switch {
	case s.metrics != nil:
		result, err = s.metrics.Stats(ctx, s.cfg.OpenSearchIndex, ids)
	case s.cfg.KafkaOnly() && s.catalog != nil:
		result = db.IngestStats{CapturedAt: time.Now().UTC(), Streams: []db.StreamIngestStats{}}
		var streams []db.Stream
		streams, err = s.catalog.List(ctx)
		for _, stream := range streams {
			if ids == nil || slices.Contains(ids, stream.ID) {
				result.Streams = append(result.Streams, db.StreamIngestStats{ID: stream.ID, Name: stream.Name, Title: stream.Title})
			}
		}
	default:
		writeError(w, 503, "Ingestion statistics are unavailable.")
		return
	}
	if err != nil {
		log.Printf("ingestion stats: %v", err)
		writeError(w, 503, "Ingestion statistics are unavailable.")
		return
	}
	if s.database != nil {
		counts, err := s.database.KafkaIngestCounts(ctx, s.cfg.OpenSearchIndex)
		if err != nil {
			log.Printf("Kafka ingestion stats: %v", err)
			writeError(w, 503, "Ingestion statistics are unavailable.")
			return
		}
		for i := range result.Streams {
			stream := &result.Streams[i]
			count := counts[stream.ID]
			stream.Accepted += count.Accepted
			stream.Published += count.Published
			stream.Pending += count.Pending
			result.Totals.Accepted += count.Accepted
			result.Totals.Published += count.Published
			result.Totals.Pending += count.Pending
		}
	}
	writeJSON(w, 200, result)
}
