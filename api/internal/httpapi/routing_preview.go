package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/logister/logister/api/internal/db"
)

// Preview uses the same evaluator as intake, with an optional unsaved stream
// draft replacing its saved counterpart. It never writes or indexes the sample.
func (s *Server) handleRoutingPreview(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Message     json.RawMessage `json:"message"`
		MessageJSON string          `json:"message_json,omitempty"`
		Draft       *db.Stream      `json:"draft,omitempty"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 65536))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&body) != nil {
		writeError(w, 400, "Provide a JSON message and optional stream settings.")
		return
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		writeError(w, 400, "Only one preview request is allowed.")
		return
	}
	streams, err := s.catalog.List(r.Context())
	if err != nil {
		writeError(w, 503, "Stream settings are unavailable.")
		return
	}
	if body.Draft != nil {
		if err := db.ValidateStream(*body.Draft); err != nil {
			writeError(w, 400, err.Error())
			return
		}
		candidates := make([]db.Stream, 0, len(streams)+1)
		for _, stream := range streams {
			if stream.Name != body.Draft.Name {
				candidates = append(candidates, stream)
			}
		}
		streams = append(candidates, *body.Draft)
	}
	if body.MessageJSON != "" {
		if len(body.Message) > 0 {
			writeError(w, 400, "Use message or message_json, not both.")
			return
		}
		body.Message = []byte(body.MessageJSON)
	}
	var message ingestRequest
	if json.Unmarshal(body.Message, &message) != nil {
		writeError(w, 400, "The sample must be a JSON object.")
		return
	}
	item := unwrapIngest(message)
	req, err := normalizeIngest(item, r.RemoteAddr)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	req.Stream = item.Stream
	decision, err := decideRouting(req, streams, true)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	writeJSON(w, 200, decision)
}
