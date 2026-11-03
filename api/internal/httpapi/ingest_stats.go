package httpapi

import (
	"context"
	"log"
	"net/http"
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
	if s.metrics == nil {
		writeError(w, 503, "Ingestion statistics are unavailable.")
		return
	}
	result, err := s.metrics.Stats(ctx, s.cfg.OpenSearchIndex, ids)
	if err != nil {
		log.Printf("ingestion stats: %v", err)
		writeError(w, 503, "Ingestion statistics are unavailable.")
		return
	}
	writeJSON(w, 200, result)
}
