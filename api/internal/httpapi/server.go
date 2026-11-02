package httpapi

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/logister/logister/api/internal/config"
	"github.com/logister/logister/api/internal/db"
	"github.com/logister/logister/api/internal/search"
)

type Server struct {
	cfg    config.Config
	pool   *pgxpool.Pool
	search *search.Client
	store  batchAcceptor
}

type batchAcceptor interface {
	Accept(context.Context, string, string, string, []db.Event) (db.Receipt, error)
}

func NewServer(cfg config.Config, pool *pgxpool.Pool, searchClient *search.Client) *Server {
	return &Server{cfg: cfg, pool: pool, search: searchClient, store: db.NewIngestStore(pool)}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", s.handleHealth)
	mux.HandleFunc("GET /api/streams", s.handleStreams)
	mux.HandleFunc("GET /api/inputs", s.handleInputs)
	mux.HandleFunc("GET /api/search", s.handleSearch)
	mux.HandleFunc("DELETE /api/search/session", s.handleCloseSearch)
	mux.HandleFunc("POST /api/ingest/json", s.handleIngestJSON)
	mux.HandleFunc("POST /api/ingest/fluent-bit", func(w http.ResponseWriter, r *http.Request) { s.handleIngest(w, r, 16<<20, 10000) })
	return withCORS(s.cfg.CORSOrigin, mux)
}

func withCORS(origin string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, Idempotency-Key")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func parseRelative(v string) (time.Duration, error) {
	v = strings.TrimSpace(strings.ToLower(v))
	if strings.HasSuffix(v, "d") {
		days, err := strconv.Atoi(strings.TrimSuffix(v, "d"))
		if err != nil {
			return 0, err
		}
		if days <= 0 || int64(days) > math.MaxInt64/int64(24*time.Hour) {
			return 0, strconv.ErrSyntax
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	duration, err := time.ParseDuration(v)
	if err != nil {
		return 0, err
	}
	if duration <= 0 {
		return 0, strconv.ErrSyntax
	}
	return duration, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	pgOK := db.Ping(ctx, s.pool) == nil
	osOK := s.search.Ready(ctx, s.cfg.OpenSearchIndex) == nil
	var pending any
	if pgOK && s.pool != nil {
		if count, err := db.NewIngestStore(s.pool).Pending(ctx, s.cfg.OpenSearchIndex); err == nil {
			pending = count
		}
	}
	status := http.StatusOK
	if !pgOK || !osOK {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]any{
		"status":           map[bool]string{true: "ok", false: "degraded"}[pgOK && osOK],
		"postgres":         pgOK,
		"opensearch":       osOK,
		"time":             time.Now().UTC(),
		"pending_messages": pending,
		"index":            s.cfg.OpenSearchIndex,
	})
}

func (s *Server) handleStreams(w http.ResponseWriter, r *http.Request) {
	streams, err := db.ListStreams(r.Context(), s.pool)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if streams == nil {
		streams = []db.Stream{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"streams": streams})
}

func (s *Server) handleInputs(w http.ResponseWriter, r *http.Request) {
	inputs, err := db.ListInputs(r.Context(), s.pool)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if inputs == nil {
		inputs = []db.Input{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"inputs": inputs})
}

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if q.Has("cursor") {
		if len(q) != 1 || q.Get("cursor") == "" {
			writeError(w, 400, "cursor must be used without other search parameters")
			return
		}
		result, err := s.search.SearchPage(r.Context(), s.cfg.OpenSearchIndex, search.SearchParams{}, q.Get("cursor"))
		if err != nil {
			s.searchError(w, err)
			return
		}
		writeJSON(w, 200, result)
		return
	}
	now := time.Now().UTC()
	from := now.Add(-24 * time.Hour)
	to := now

	if v := q.Get("from"); q.Has("from") {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid from (use RFC3339)")
			return
		}
		from = t.UTC()
	}
	if v := q.Get("to"); q.Has("to") {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid to (use RFC3339)")
			return
		}
		to = t.UTC()
		if !q.Has("from") {
			from = to.Add(-24 * time.Hour)
		}
	}
	if q.Has("relative") {
		if q.Has("from") || q.Has("to") {
			writeError(w, http.StatusBadRequest, "use relative or an absolute time range, not both")
			return
		}
		rel := q.Get("relative")
		d, err := parseRelative(rel)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid relative duration (e.g. 15m, 1h, 24h, 7d)")
			return
		}
		to = now
		from = now.Add(-d)
	}
	if !from.Before(to) {
		writeError(w, http.StatusBadRequest, "from must be earlier than to")
		return
	}

	size := search.DefaultPageSize
	if q.Has("size") {
		value, err := strconv.Atoi(q.Get("size"))
		if err != nil || value < 1 || value > search.MaxPageSize {
			writeError(w, http.StatusBadRequest, "size must be an integer between 1 and 200")
			return
		}
		size = value
	}
	offset := 0
	if q.Has("offset") {
		value, err := strconv.Atoi(q.Get("offset"))
		if err != nil || value < 0 || value > search.MaxResultWindow-size {
			writeError(w, http.StatusBadRequest, "offset must be a non-negative integer and offset + size must not exceed 10000; narrow the search range")
			return
		}
		offset = value
	}

	result, err := s.search.SearchPage(r.Context(), s.cfg.OpenSearchIndex, search.SearchParams{
		Query:  q.Get("q"),
		From:   from,
		To:     to,
		Stream: q.Get("stream"),
		Size:   size,
		Offset: offset,
	}, "")
	if err != nil {
		s.searchError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) searchError(w http.ResponseWriter, err error) {
	log.Printf("search failed: %v", err)
	switch {
	case errors.Is(err, search.ErrInvalidQuery):
		writeError(w, 400, "Invalid search query. Check the syntax and field values.")
	case errors.Is(err, search.ErrExpiredCursor):
		writeError(w, 410, "Search session expired. Run Search again.")
	default:
		writeError(w, 503, "Search is temporarily unavailable. Please try again.")
	}
}

func (s *Server) handleCloseSearch(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"session_id"`
	}
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 32768)).Decode(&body) != nil || body.SessionID == "" {
		writeError(w, 400, "session_id is required")
		return
	}
	if err := s.search.CloseSearch(r.Context(), body.SessionID); err != nil {
		writeError(w, 503, "Could not close search session.")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type ingestRequest struct {
	ID         string         `json:"id,omitempty"`
	DeliveryID string         `json:"delivery_id,omitempty"`
	Message    string         `json:"message"`
	Source     string         `json:"source"`
	Level      string         `json:"level"`
	Host       string         `json:"host"`
	Stream     string         `json:"stream"`
	Facility   string         `json:"facility"`
	Timestamp  *time.Time     `json:"timestamp"`
	Fields     map[string]any `json:"fields"`
	// Fluent Bit tail without parser keeps the line here.
	Log string `json:"log"`
}

func unwrapIngest(req ingestRequest) ingestRequest {
	if strings.TrimSpace(req.Message) == "" && strings.TrimSpace(req.Log) != "" {
		var nested ingestRequest
		if err := json.Unmarshal([]byte(req.Log), &nested); err == nil && strings.TrimSpace(nested.Message) != "" {
			if nested.ID == "" {
				nested.ID = req.ID
			}
			if nested.DeliveryID == "" {
				nested.DeliveryID = req.DeliveryID
			}
			req = nested
		} else {
			req.Message = req.Log
		}
	}
	return req
}

func normalizeIngest(req ingestRequest, remote string) (ingestRequest, error) {
	req = unwrapIngest(req)
	req.Message = strings.TrimSpace(req.Message)
	if req.Message == "" {
		return req, errMessageRequired
	}
	if req.Source == "" {
		req.Source = "http"
	}
	if req.Level == "" {
		req.Level = "info"
	}
	if req.Host == "" {
		req.Host = remote
	}
	if req.Stream == "" {
		req.Stream = "all"
	}
	return req, nil
}

var errMessageRequired = jsonError("message is required")

type jsonError string

func (e jsonError) Error() string { return string(e) }

func ingestDocument(req ingestRequest, id string) json.RawMessage {
	ts := time.Now().UTC()
	if req.Timestamp != nil {
		ts = req.Timestamp.UTC()
	}
	doc := map[string]any{
		"event_id":   id,
		"@timestamp": ts.Format(time.RFC3339Nano),
		"message":    req.Message,
		"source":     req.Source,
		"level":      req.Level,
		"host":       req.Host,
		"stream":     req.Stream,
		"facility":   req.Facility,
		"fields":     req.Fields,
	}
	encoded, _ := json.Marshal(doc)
	return encoded
}

func digest(data []byte) string { value := sha256.Sum256(data); return hex.EncodeToString(value[:]) }

func randomIdentity() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

const maxIngestBodyBytes = 1 << 20

func (s *Server) handleIngestJSON(w http.ResponseWriter, r *http.Request) {
	s.handleIngest(w, r, maxIngestBodyBytes, 1000)
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request, limit int64, maxBatch int) {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var limitErr *http.MaxBytesError
		if errors.As(err, &limitErr) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body exceeds configured limit")
			return
		}
		writeError(w, http.StatusBadRequest, "cannot read body")
		return
	}
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		writeError(w, http.StatusBadRequest, "empty body")
		return
	}

	var batch []ingestRequest
	switch body[0] {
	case '[':
		if err := json.Unmarshal(body, &batch); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json array")
			return
		}
	case '{':
		var one ingestRequest
		if err := json.Unmarshal(body, &one); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json")
			return
		}
		batch = []ingestRequest{one}
	default:
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if len(batch) == 0 || len(batch) > maxBatch {
		writeError(w, http.StatusBadRequest, "batch size exceeds allowed message count or is empty")
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if len(key) > 200 {
		writeError(w, http.StatusBadRequest, "Idempotency-Key must not exceed 200 bytes")
		return
	}
	events := make([]db.Event, len(batch))

	// Validate every item before writing anything. A client must be able to
	// correct and retry a malformed batch without duplicating a valid prefix.
	for i, item := range batch {
		item = unwrapIngest(item)
		content := item
		content.ID = ""
		content.DeliveryID = ""
		content.Log = ""
		canonical, _ := json.Marshal(content)
		req, err := normalizeIngest(item, r.RemoteAddr)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		id := req.ID
		if id == "" {
			id = req.DeliveryID
		}
		if len(id) > 256 {
			writeError(w, http.StatusBadRequest, "event id must not exceed 256 bytes")
			return
		}
		if id == "" && key != "" {
			id = "batch:" + key + ":" + strconv.Itoa(i)
		}
		if id == "" {
			id, err = randomIdentity()
			if err != nil {
				writeError(w, http.StatusInternalServerError, "cannot allocate event identity")
				return
			}
		}
		id = digest([]byte(s.cfg.OpenSearchIndex + "\x00" + id))
		events[i] = db.Event{ID: id, Hash: digest(canonical), Document: ingestDocument(req, id)}
	}
	receipt, err := s.store.Accept(r.Context(), s.cfg.OpenSearchIndex, key, digest(body), events)
	if err != nil {
		if errors.Is(err, db.ErrIdentityConflict) {
			writeError(w, http.StatusConflict, "Event ID or Idempotency-Key was reused with different data.")
			return
		}
		log.Printf("durable ingest rejected: %v", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "Ingestion storage is unavailable. Retry with the same event IDs or Idempotency-Key.", "count": 0})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"status": "accepted", "count": receipt.Count, "duplicates": receipt.Duplicates})
}
