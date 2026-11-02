package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
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
}

func NewServer(cfg config.Config, pool *pgxpool.Pool, searchClient *search.Client) *Server {
	return &Server{cfg: cfg, pool: pool, search: searchClient}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", s.handleHealth)
	mux.HandleFunc("GET /api/streams", s.handleStreams)
	mux.HandleFunc("GET /api/inputs", s.handleInputs)
	mux.HandleFunc("GET /api/search", s.handleSearch)
	mux.HandleFunc("POST /api/ingest/json", s.handleIngestJSON)
	return withCORS(s.cfg.CORSOrigin, mux)
}

func withCORS(origin string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
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
		if days <= 0 {
			return 0, strconv.ErrSyntax
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	return time.ParseDuration(v)
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
	osOK := s.search.Ping(ctx) == nil
	status := http.StatusOK
	if !pgOK || !osOK {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]any{
		"status":     map[bool]string{true: "ok", false: "degraded"}[pgOK && osOK],
		"postgres":   pgOK,
		"opensearch": osOK,
		"time":       time.Now().UTC(),
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
	now := time.Now().UTC()
	from := now.Add(-24 * time.Hour)
	to := now

	if v := q.Get("from"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid from (use RFC3339)")
			return
		}
		from = t.UTC()
	}
	if v := q.Get("to"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid to (use RFC3339)")
			return
		}
		to = t.UTC()
	}
	if rel := q.Get("relative"); rel != "" {
		d, err := parseRelative(rel)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid relative duration (e.g. 15m, 1h, 24h, 7d)")
			return
		}
		to = now
		from = now.Add(-d)
	}

	size, _ := strconv.Atoi(q.Get("size"))
	offset, _ := strconv.Atoi(q.Get("offset"))

	result, err := s.search.Search(r.Context(), s.cfg.OpenSearchIndex, search.SearchParams{
		Query:  q.Get("q"),
		From:   from,
		To:     to,
		Stream: q.Get("stream"),
		Size:   size,
		Offset: offset,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

type ingestRequest struct {
	Message   string         `json:"message"`
	Source    string         `json:"source"`
	Level     string         `json:"level"`
	Host      string         `json:"host"`
	Stream    string         `json:"stream"`
	Facility  string         `json:"facility"`
	Timestamp *time.Time     `json:"timestamp"`
	Fields    map[string]any `json:"fields"`
	// Fluent Bit tail without parser keeps the line here.
	Log string `json:"log"`
}

func normalizeIngest(req ingestRequest, remote string) (ingestRequest, error) {
	if strings.TrimSpace(req.Message) == "" && strings.TrimSpace(req.Log) != "" {
		var nested ingestRequest
		if err := json.Unmarshal([]byte(req.Log), &nested); err == nil && strings.TrimSpace(nested.Message) != "" {
			req = nested
		} else {
			req.Message = req.Log
		}
	}
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

func (s *Server) indexIngest(ctx context.Context, req ingestRequest) error {
	ts := time.Now().UTC()
	if req.Timestamp != nil {
		ts = req.Timestamp.UTC()
	}
	doc := map[string]any{
		"@timestamp": ts.Format(time.RFC3339Nano),
		"message":    req.Message,
		"source":     req.Source,
		"level":      req.Level,
		"host":       req.Host,
		"stream":     req.Stream,
		"facility":   req.Facility,
		"fields":     req.Fields,
	}
	return s.search.IndexMessage(ctx, s.cfg.OpenSearchIndex, "", doc)
}

func (s *Server) handleIngestJSON(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
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

	accepted := 0
	for _, item := range batch {
		req, err := normalizeIngest(item, r.RemoteAddr)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := s.indexIngest(r.Context(), req); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		accepted++
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"status": "accepted", "count": accepted})
}
