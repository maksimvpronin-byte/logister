package httpapi

import (
	"context"
	"errors"
	"github.com/logister/logister/api/internal/db"
	"net/http"
)

type archivedCatalog interface {
	ListArchived(context.Context) ([]db.Stream, error)
	SetDeleted(context.Context, string, bool) error
}

func (s *Server) handleArchivedStreams(w http.ResponseWriter, r *http.Request) {
	catalog, ok := s.catalog.(archivedCatalog)
	if !ok {
		writeError(w, 503, "Stream settings unavailable.")
		return
	}
	streams, err := catalog.ListArchived(r.Context())
	if err != nil {
		writeError(w, 503, "Stream settings unavailable.")
		return
	}
	writeJSON(w, 200, map[string]any{"streams": streams})
}
func (s *Server) handleDeleteStream(w http.ResponseWriter, r *http.Request) {
	catalog, ok := s.catalog.(archivedCatalog)
	if !ok {
		writeError(w, 503, "Stream settings unavailable.")
		return
	}
	err := catalog.SetDeleted(r.Context(), r.PathValue("name"), r.Method == http.MethodDelete)
	switch {
	case errors.Is(err, db.ErrDefaultStream):
		writeError(w, 400, "The default stream cannot be deleted.")
	case errors.Is(err, db.ErrStreamNotFound):
		writeError(w, 404, "Stream not found.")
	case err != nil:
		writeError(w, 503, "Could not change stream deletion state.")
	default:
		writeJSON(w, 200, map[string]bool{"deleted": r.Method == http.MethodDelete})
	}
}
