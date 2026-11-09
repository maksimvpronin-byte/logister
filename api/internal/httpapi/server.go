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

	"github.com/logister/logister/api/internal/authz"
	"github.com/logister/logister/api/internal/config"
	"github.com/logister/logister/api/internal/db"
	"github.com/logister/logister/api/internal/kafkainput"
	"github.com/logister/logister/api/internal/search"
	"github.com/logister/logister/api/internal/telemetry"
)

type Server struct {
	telemetry *telemetry.Meter
	monitor   *monitoringStore
	kafka     *kafkainput.Consumer
	cfg       config.Config
	database  *db.DB
	search    *search.Client
	store     batchAcceptor
	catalog   streamCatalog
	auth      *authentication
	metrics   ingestStatsReader
	intake    intakeJournal
}

type intakeJournal interface {
	Pending(context.Context, string) (int64, error)
	Ping(context.Context) error
	ActiveAccepted(context.Context, string, string) (string, int64, bool)
	NoteRotation(context.Context, string, string, string) (bool, error)
	TargetPending(context.Context, string, string) (int64, error)
}

func (s *Server) UseJournal(journal interface {
	batchAcceptor
	ingestStatsReader
	intakeJournal
}) {
	s.store = journal
	s.metrics = journal
	s.intake = journal
}

type batchAcceptor interface {
	Accept(context.Context, string, string, string, []db.Event) (db.Receipt, error)
}

func NewServer(cfg config.Config, database *db.DB, searchClient *search.Client) *Server {
	server := &Server{cfg: cfg, database: database, search: searchClient}
	if database != nil {
		server.catalog = db.NewStreamStore(database)
	}
	return server
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", s.handleHealth)
	mux.HandleFunc("GET /api/monitoring", s.handleMonitoring)
	mux.HandleFunc("GET /api/auth/status", s.handleAuthStatus)
	mux.HandleFunc("POST /api/auth/login", s.handleLogin)
	mux.HandleFunc("POST /api/auth/logout", func(w http.ResponseWriter, r *http.Request) { writeError(w, 404, "No session.") })
	mux.HandleFunc("GET /api/auth/me", s.handleMe)
	mux.HandleFunc("GET /api/auth/directory", s.require(authz.SystemGrants, s.handleDirectory))
	mux.HandleFunc("GET /api/auth/grants", s.require(authz.SystemGrants, s.handleGrants))
	mux.HandleFunc("PUT /api/auth/grants", s.require(authz.SystemGrants, s.handleGrants))
	mux.HandleFunc("GET /api/auth/ldap", s.require(authz.SystemGrants, s.handleLDAPSettings))
	mux.HandleFunc("PUT /api/auth/ldap", s.require(authz.SystemGrants, s.handleLDAPSettings))
	mux.HandleFunc("POST /api/auth/ldap/test", s.require(authz.SystemGrants, s.handleLDAPSettings))
	mux.HandleFunc("GET /api/streams", s.handleStreams)
	mux.HandleFunc("GET /api/ingest/kafka", s.require(authz.SystemGrants, s.handleKafka))
	mux.HandleFunc("PUT /api/ingest/kafka", s.require(authz.SystemGrants, s.handleKafka))
	mux.HandleFunc("GET /api/ingest/stats", s.handleIngestStats)
	mux.HandleFunc("GET /api/streams/deleted", s.require(authz.StreamsDelete, s.handleArchivedStreams))
	mux.HandleFunc("DELETE /api/streams/{name}", s.require(authz.StreamsDelete, s.handleDeleteStream))
	mux.HandleFunc("POST /api/streams/{name}/restore", s.require(authz.StreamsDelete, s.handleDeleteStream))
	mux.HandleFunc("POST /api/streams", s.require(authz.StreamsCreate, s.handleSaveStream))
	mux.HandleFunc("POST /api/streams/preview-routing", s.require(authz.StreamsEdit, s.handleRoutingPreview))
	mux.HandleFunc("PUT /api/streams/{name}", s.require(authz.StreamsEdit, s.handleSaveStream))
	mux.HandleFunc("GET /api/streams/{name}/indices", s.require(authz.StreamsEdit, s.handleStreamIndices))
	mux.HandleFunc("POST /api/streams/{name}/rotate", s.require(authz.StreamsRotate, s.handleRotateStream))
	mux.HandleFunc("POST /api/streams/{name}/ingest", s.handleIngestJSON)
	mux.HandleFunc("GET /api/inputs", s.require(authz.InputsRead, s.handleInputs))
	mux.HandleFunc("GET /api/search", s.handleSearch)
	mux.HandleFunc("DELETE /api/search/session", s.handleCloseSearch)
	mux.HandleFunc("POST /api/ingest/json", s.handleIngestJSON)
	mux.HandleFunc("POST /api/ingest/fluent-bit", func(w http.ResponseWriter, r *http.Request) { s.handleIngest(w, r, 16<<20, 10000) })
	return withCORS(s.cfg.CORSOrigin, s.authenticate(mux))
}

func withCORS(origin string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
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
	mongoOK := s.database != nil && s.database.Ping(ctx) == nil
	osOK := s.search.Ready(ctx, s.readIndex()) == nil
	backendOK := true
	backends := map[string]any{}
	var pending any
	if s.cfg.KafkaOnly() {
		task, cancel := context.WithTimeout(ctx, 3*time.Second)
		backendOK = s.kafka != nil && s.kafka.Ping(task) == nil
		cancel()
		backends["kafka"] = backendOK
	} else {
		backendOK = s.intake == nil || s.intake.Ping(ctx) == nil
		backends["redis"] = backendOK
		if s.intake != nil && backendOK {
			if count, err := s.intake.Pending(ctx, s.cfg.OpenSearchIndex); err == nil {
				pending = count
			}
		}
	}
	ready := mongoOK && osOK && backendOK
	status := http.StatusOK
	if !ready {
		status = http.StatusServiceUnavailable
	}
	backends["status"] = map[bool]string{true: "ok", false: "degraded"}[ready]
	backends["mongo"] = mongoOK
	backends["opensearch"] = osOK
	backends["time"] = time.Now().UTC()
	backends["pending_messages"] = pending
	backends["index"] = s.cfg.OpenSearchIndex
	backends["search_index"] = s.readIndex()
	writeJSON(w, status, backends)
}

func (s *Server) handleStreams(w http.ResponseWriter, r *http.Request) {
	streams, err := s.catalog.List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if streams == nil {
		streams = []db.Stream{}
	}
	if s.cfg.AuthEnabled {
		info := r.Context().Value(authContextKey{}).(requestIdentity)
		filtered := []db.Stream{}
		for _, stream := range streams {
			if info.access.Allowed(authz.StreamsRead, stream.ID) {
				filtered = append(filtered, stream)
			}
		}
		streams = filtered
	}
	writeJSON(w, http.StatusOK, map[string]any{"streams": streams})
}

func (s *Server) handleInputs(w http.ResponseWriter, r *http.Request) {
	inputs, err := db.ListInputs(r.Context(), s.database)
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
	var permitted bool
	r, permitted = s.allowedSearch(w, r)
	if !permitted {
		return
	}
	q := r.URL.Query()
	if q.Has("cursor") {
		if len(q) != 1 || q.Get("cursor") == "" {
			writeError(w, 400, "cursor must be used without other search parameters")
			return
		}
		result, err := s.search.SearchPage(r.Context(), s.readIndex(), search.SearchParams{}, q.Get("cursor"))
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

	result, err := s.search.SearchPage(r.Context(), s.readIndex(), search.SearchParams{
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
	var permitted bool
	r, permitted = s.allowedSearch(w, r)
	if !permitted {
		return
	}
	var body struct {
		SessionID string `json:"session_id"`
	}
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, search.MaxSearchSessionBytes+1024)).Decode(&body) != nil || body.SessionID == "" {
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
	Log            string          `json:"log"`
	Payload        map[string]any  `json:"-"`
	PayloadRaw     json.RawMessage `json:"-"`
	Lookup         map[string]any  `json:"-"`
	ForcedStream   string          `json:"-"`
	KafkaTransport bool            `json:"-"`
}

func unwrapIngest(req ingestRequest) ingestRequest {
	if strings.TrimSpace(req.Message) == "" && strings.TrimSpace(req.Log) != "" {
		var nested ingestRequest
		if err := json.Unmarshal([]byte(req.Log), &nested); err == nil {
			if nested.ID == "" {
				nested.ID = req.ID
			}
			if nested.DeliveryID == "" {
				nested.DeliveryID = req.DeliveryID
			}
			lookup := map[string]any{}
			for key, value := range req.Payload {
				lookup[key] = value
			}
			for key, value := range nested.Payload {
				lookup[key] = value
			}
			nested.KafkaTransport = req.KafkaTransport
			nested.Lookup = lookup
			nested.Payload = req.Payload
			nested.PayloadRaw = req.PayloadRaw
			req = nested
		} else {
			req.Message = req.Log
		}
	}
	return req
}

func normalizeIngest(req ingestRequest, remote string) (ingestRequest, error) {
	req = unwrapIngest(req)
	req.Message = payloadMessage(req)
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
	req.Source = trimText(req.Source, 8000)
	req.Host = trimText(req.Host, 8000)
	req.Level = trimText(req.Level, 8000)
	req.Facility = trimText(req.Facility, 8000)
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
		"fields":     indexableValue(req.Fields, 0),
	}
	for key, value := range searchMetadata(req) {
		doc[key] = value
	}
	if req.Payload != nil {
		raw := req.PayloadRaw
		if len(raw) == 0 {
			raw, _ = json.Marshal(req.Payload)
		}
		doc["payload_json"] = string(raw)
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
	if s.cfg.KafkaOnly() {
		writeError(w, http.StatusNotFound, "HTTP ingestion is disabled. Send events to Kafka.")
		return
	}
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
	var access *authz.Access
	if s.cfg.AuthEnabled && !isFluentBitIngest(r) {
		info := r.Context().Value(authContextKey{}).(requestIdentity)
		access = &info.access
	}
	batchBody := body
	if target := r.PathValue("name"); target != "" {
		batchBody = append(append([]byte(target+"\x00"), body...), byte(0))
	}
	receipt, err := s.acceptIngest(r.Context(), batch, key, digest(batchBody), r.RemoteAddr, r.PathValue("name"), access)
	if err != nil {
		var problem *ingestFailure
		if errors.As(err, &problem) {
			writeJSON(w, problem.Status, map[string]any{"error": problem.Message, "count": 0})
		} else {
			writeError(w, 503, "Ingestion storage is unavailable.")
		}
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"status": "accepted", "count": receipt.Count, "duplicates": receipt.Duplicates})
}

func (s *Server) UseTelemetry(m *telemetry.Meter) { s.telemetry = m }
