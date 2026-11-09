package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/logister/logister/api/internal/authz"
	"github.com/logister/logister/api/internal/config"
	"github.com/logister/logister/api/internal/db"
	"github.com/logister/logister/api/internal/directory"
	"github.com/logister/logister/api/internal/search"
	"golang.org/x/crypto/bcrypt"
)

type memoryAuth struct {
	catalog  db.GrantCatalog
	sessions map[string]db.AuthSession
}

func (m *memoryAuth) Grants(context.Context) (db.GrantCatalog, error) { return m.catalog, nil }
func (m *memoryAuth) SaveGrants(_ context.Context, c db.GrantCatalog) error {
	m.catalog = c
	return nil
}
func (m *memoryAuth) CreateSession(_ context.Context, hash string, s db.AuthSession) error {
	m.sessions[hash] = s
	return nil
}
func (m *memoryAuth) Session(_ context.Context, hash string) (db.AuthSession, error) {
	s, ok := m.sessions[hash]
	if !ok || s.Expires.Before(time.Now()) {
		return s, db.ErrNotFound
	}
	return s, nil
}
func (m *memoryAuth) DeleteSession(_ context.Context, hash string) error {
	delete(m.sessions, hash)
	return nil
}

type fakeDirectory struct {
	groups      []string
	unavailable bool
}

func (d *fakeDirectory) Authenticate(_ context.Context, user, password string) (directory.Identity, error) {
	if d.unavailable {
		return directory.Identity{}, errors.New("offline")
	}
	if password != "correct" || user == "disabled" || user == "" {
		return directory.Identity{}, directory.ErrCredentials
	}
	return directory.Identity{Subject: authz.Subject{UserID: "uuid-" + user, GroupIDs: d.groups}, Username: user}, nil
}
func (d *fakeDirectory) Refresh(ctx context.Context, id string) (directory.Identity, error) {
	return d.Authenticate(ctx, strings.TrimPrefix(id, "uuid-"), "correct")
}
func (d *fakeDirectory) List(context.Context, string) ([]directory.Entry, error) { return nil, nil }

type authStreams struct{}

func (authStreams) List(context.Context) ([]db.Stream, error) {
	return []db.Stream{{ID: "pay-id", Name: "payments", Title: "Payments"}, {ID: "bill-id", Name: "billing", Title: "Billing"}}, nil
}
func (authStreams) Save(context.Context, db.Stream, bool) (db.Stream, error) { return db.Stream{}, nil }

func authFixture(t *testing.T) (*Server, *memoryAuth, *fakeDirectory) {
	t.Helper()
	store := &memoryAuth{catalog: db.GrantCatalog{Grants: []authz.Grant{db.LocalAdminGrant(), {PrincipalKind: authz.PrincipalGroup, PrincipalID: "operators", Role: authz.RoleOperator, Scope: authz.ScopeStream, StreamID: "pay-id"}}}, sessions: map[string]db.AuthSession{}}
	d := &fakeDirectory{groups: []string{"operators"}}
	hash, _ := bcrypt.GenerateFromPassword([]byte("local-test-password"), bcrypt.MinCost)
	server := NewServer(config.Config{AuthEnabled: true, CORSOrigin: "http://localhost:3000"}, nil, nil)
	server.catalog = authStreams{}
	server.auth = &authentication{directory: d, store: store, localHash: hash, failures: map[string]loginFailures{}}
	return server, store, d
}
func authRequest(handler http.Handler, method, path, body string, cookie *http.Cookie) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Origin", "http://localhost:3000")
	if cookie != nil {
		r.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
}
func loginCookie(t *testing.T, h http.Handler, user, source, password string) *http.Cookie {
	t.Helper()
	raw, _ := json.Marshal(map[string]string{"username": user, "source": source, "password": password})
	w := authRequest(h, "POST", "/api/auth/login", string(raw), nil)
	if w.Code != 200 {
		t.Fatalf("login: %d %s", w.Code, w.Body.String())
	}
	cookies := w.Result().Cookies()
	if len(cookies) != 1 || !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteStrictMode {
		t.Fatalf("unsafe cookie: %v", cookies)
	}
	return cookies[0]
}
func TestAuthenticationEnforcesRolesAndRechecksMembership(t *testing.T) {
	s, _, d := authFixture(t)
	h := s.Handler()
	if w := authRequest(h, "GET", "/api/streams", "", nil); w.Code != 401 {
		t.Fatal(w.Code)
	}
	c := loginCookie(t, h, "operator", "ldap", "correct")
	w := authRequest(h, "GET", "/api/streams", "", c)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "payments") || strings.Contains(w.Body.String(), "billing") {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, path := range []string{"/api/inputs", "/api/auth/grants", "/api/auth/directory", "/api/streams/billing/indices"} {
		if w := authRequest(h, "GET", path, "", c); w.Code != 403 {
			t.Fatal(path, w.Code, w.Body.String())
		}
	}
	if w := authRequest(h, "POST", "/api/streams/payments/ingest", `{"message":"forbidden"}`, c); w.Code != 403 {
		t.Fatal(w.Code, w.Body.String())
	}
	d.groups = nil // Same session, next request; no re-login or cache expiry.
	w = authRequest(h, "GET", "/api/streams", "", c)
	if w.Code != 200 || strings.Contains(w.Body.String(), "payments") {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := authRequest(h, "GET", "/api/search", "", c); w.Code != 403 {
		t.Fatal(w.Code, w.Body.String())
	}
	d.unavailable = true
	if w := authRequest(h, "GET", "/api/streams", "", c); w.Code != 503 {
		t.Fatal(w.Code)
	}
	local := loginCookie(t, h, "local-admin", "local", "local-test-password")
	if w := authRequest(h, "GET", "/api/auth/grants", "", local); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := authRequest(h, "POST", "/api/auth/logout", "", c); w.Code != 204 {
		t.Fatal(w.Code)
	}
	d.unavailable = false
	if w := authRequest(h, "GET", "/api/streams", "", c); w.Code != 401 {
		t.Fatal(w.Code)
	}
}
func TestLoginInvalidInputsCSRFAndSessionExpiry(t *testing.T) {
	s, store, _ := authFixture(t)
	s.cfg.AuthCookieSecure = true
	h := s.Handler()
	for _, body := range []string{`{"username":"operator","password":""}`, `{"username":"operator","password":"wrong"}`, `{"username":"disabled","password":"correct"}`, `{"username":"*)(uid=*)","password":"wrong"}`} {
		if w := authRequest(h, "POST", "/api/auth/login", body, nil); w.Code != 401 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	c := loginCookie(t, h, "operator", "ldap", "correct")
	if !c.Secure {
		t.Fatal("cookie lacks Secure")
	}
	r := httptest.NewRequest("POST", "/api/auth/logout", nil)
	r.AddCookie(c)
	r.Header.Set("Origin", "https://untrusted.example")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal(w.Code)
	}
	for hash, session := range store.sessions {
		session.Expires = time.Now().Add(-time.Second)
		store.sessions[hash] = session
	}
	if w := authRequest(h, "GET", "/api/auth/me", "", c); w.Code != 401 {
		t.Fatal(w.Code)
	}
	for i := 0; i < 10; i++ {
		authRequest(h, "POST", "/api/auth/login", `{"username":"operator","password":"wrong"}`, nil)
	}
	if w := authRequest(h, "POST", "/api/auth/login", `{"username":"operator","password":"correct"}`, nil); w.Code != 429 {
		t.Fatal(w.Code)
	}
}
func TestAuthenticatedSearchScopesQueryAndRejectsOtherStreams(t *testing.T) {
	s, _, _ := authFixture(t)
	var queries int
	var queryBody string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/_mapping"):
			w.Write([]byte(`{"logs":{"mappings":{"_meta":{"ready":true}}}}`))
		case strings.Contains(r.URL.Path, "point_in_time"):
			w.Write([]byte(`{"pit_id":"pit"}`))
		default:
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			raw, _ := json.Marshal(body)
			queryBody = string(raw)
			queries++
			w.Write([]byte(`{"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}`))
		}
	}))
	defer backend.Close()
	s.search, _ = search.NewClient(backend.URL)
	s.cfg.OpenSearchIndex = "logs"
	h := s.Handler()
	c := loginCookie(t, h, "operator", "ldap", "correct")
	w := authRequest(h, "GET", "/api/search?stream=all&q=stream:billing%20OR%20*", "", c)
	if w.Code != 200 || !strings.Contains(queryBody, `"terms":{"stream":["payments"]}`) {
		t.Fatal(w.Code, queryBody)
	}
	for _, stream := range []string{"billing", "@default"} {
		w = authRequest(h, "GET", "/api/search?stream="+stream, "", c)
		if w.Code != 403 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if queries != 1 {
		t.Fatal("forbidden requests reached search", queries)
	}
}

func TestScopedServiceCannotRouteOrPartiallyCommitForbiddenBatch(t *testing.T) {
	s, store, d := authFixture(t)
	d.groups = []string{"service-group"}
	store.catalog.Grants = append(store.catalog.Grants, authz.Grant{PrincipalKind: authz.PrincipalGroup, PrincipalID: "service-group", Role: authz.RoleService, Scope: authz.ScopeStream, StreamID: "pay-id"})
	intake := &ingestStore{hashes: map[string]string{}, keys: map[string]string{}}
	s.store = intake
	h := s.Handler()
	for _, test := range []struct {
		path, body string
		status     int
	}{
		{"/api/ingest/json", `{"message":"implicit route"}`, 403},
		{"/api/ingest/json", `[{"message":"permitted","stream":"payments"},{"message":"forbidden","stream":"billing"}]`, 403},
		{"/api/streams/billing/ingest", `{"message":"forbidden"}`, 403},
		{"/api/streams/payments/ingest", `{"message":"explicit route"}`, 202},
	} {
		r := httptest.NewRequest("POST", test.path, strings.NewReader(test.body))
		r.SetBasicAuth("service", "correct")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != test.status {
			t.Fatal(test.path, w.Code, w.Body.String())
		}
	}
	if len(intake.snapshots()) != 1 {
		t.Fatal("unauthorized batch partially committed", intake.snapshots())
	}
	r := httptest.NewRequest("GET", "/api/streams", nil)
	r.SetBasicAuth("service", "correct")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal("Basic credentials granted browser API access", w.Code)
	}
}

func TestFluentBitReceiverDoesNotRequireIdentityProvider(t *testing.T) {
	s, _, directory := authFixture(t)
	directory.unavailable = true
	intake := &ingestStore{hashes: map[string]string{}, keys: map[string]string{}}
	s.store = intake
	h := s.Handler()
	for _, credentials := range []bool{false, true} {
		r := httptest.NewRequest("POST", "/api/ingest/fluent-bit", strings.NewReader(`[{"message":"shipper message","stream":"payments"}]`))
		if credentials {
			r.SetBasicAuth("obsolete-shipper-user", "obsolete-password")
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 202 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if len(intake.snapshots()) != 2 {
		t.Fatal("shipper events not accepted", intake.snapshots())
	}
	for _, path := range []string{"/api/ingest/json", "/api/streams/payments/ingest", "/api/ingest/fluent-bit/", "/api/streams", "/api/auth/grants"} {
		w := authRequest(h, "POST", path, `{"message":"unauthorized"}`, nil)
		if w.Code != 401 {
			t.Fatal(path, w.Code, w.Body.String())
		}
	}
	if w := authRequest(h, "GET", "/api/ingest/fluent-bit", "", nil); w.Code != 401 {
		t.Fatal(w.Code)
	}
}
