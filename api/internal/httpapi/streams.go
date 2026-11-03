package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"

	"github.com/logister/logister/api/internal/db"
	"github.com/logister/logister/api/internal/search"
)

type streamCatalog interface {
	List(context.Context) ([]db.Stream, error)
	Save(context.Context, db.Stream, bool) (db.Stream, error)
}

func (s *Server) readIndex() string {
	if s.cfg.SearchReadIndex != "" {
		return s.cfg.SearchReadIndex
	}
	return s.cfg.OpenSearchIndex
}

func (s *Server) handleSaveStream(w http.ResponseWriter, r *http.Request) {
	stream := db.Stream{Routing: db.RoutingConfig{Priority: 100, Match: "all", Rules: []db.RoutingRule{}}, Storage: db.StorageConfig{MaxAgeHours: 24, MaxSizeMB: 1024, MaxDocuments: 1000000}}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 65536))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&stream); err != nil {
		writeError(w, 400, "Invalid stream settings JSON.")
		return
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		writeError(w, 400, "Only one JSON object is allowed.")
		return
	}
	create := r.Method == http.MethodPost
	if !create && stream.Name != r.PathValue("name") {
		writeError(w, 400, "Stream names cannot be changed.")
		return
	}
	if err := db.ValidateStream(stream); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	saved, err := s.catalog.Save(r.Context(), stream, create)
	if err != nil {
		switch {
		case errors.Is(err, db.ErrStreamExists):
			writeError(w, 409, "A stream with this name already exists.")
		case errors.Is(err, db.ErrStreamNotFound):
			writeError(w, 404, "Stream not found.")
		default:
			log.Printf("save stream: %v", err)
			writeError(w, 503, "Stream settings could not be saved. Please retry.")
		}
		return
	}
	status := 200
	if create {
		status = 201
	}
	writeJSON(w, status, map[string]any{"stream": saved})
}

func (s *Server) knownStream(ctx context.Context, name string) (bool, error) {
	streams, err := s.catalog.List(ctx)
	if err != nil {
		return false, err
	}
	for _, stream := range streams {
		if stream.Name == name {
			return true, nil
		}
	}
	return false, nil
}
func (s *Server) handleStreamIndices(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	exists, err := s.knownStream(r.Context(), name)
	if err != nil {
		writeError(w, 503, "Stream settings are unavailable.")
		return
	}
	if !exists {
		writeError(w, 404, "Stream not found.")
		return
	}
	indices, err := db.ListManagedIndices(r.Context(), s.pool, s.cfg.OpenSearchIndex, name)
	if err != nil {
		log.Printf("stream indices: %v", err)
		writeError(w, 503, "Index status is unavailable.")
		return
	}
	// A bounded status view avoids one HTTP request for every historical index.
	if len(indices) > 100 {
		indices = indices[:100]
	}
	out := []map[string]any{}
	for _, index := range indices {
		row := map[string]any{"name": index.Name, "generation": index.Generation, "created_at": index.CreatedAt, "sealed_at": index.SealedAt, "deleted_at": index.DeletedAt, "accepted_count": index.AcceptedCount}
		if index.DeletedAt == nil {
			stats, err := s.search.IndexStats(r.Context(), index.Name)
			if err == nil {
				row["documents"] = stats.Documents
				row["size_bytes"] = stats.SizeBytes
			} else if errors.Is(err, search.ErrIndexNotFound) {
				row["preparing"] = true
			} else {
				row["stats_error"] = "Search statistics are temporarily unavailable."
			}
		}
		out = append(out, row)
	}
	writeJSON(w, 200, map[string]any{"indices": out, "legacy_preserved": true})
}
func (s *Server) handleRotateStream(w http.ResponseWriter, r *http.Request) {
	rotated, err := db.RotateStream(r.Context(), s.pool, s.cfg.OpenSearchIndex, r.PathValue("name"), "")
	if err != nil {
		if errors.Is(err, db.ErrStreamNotFound) {
			writeError(w, 404, "Stream not found.")
		} else {
			log.Printf("rotate stream: %v", err)
			writeError(w, 503, "Could not rotate this stream. Please retry.")
		}
		return
	}
	writeJSON(w, 200, map[string]any{"rotated": rotated})
}
