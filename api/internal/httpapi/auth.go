package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/logister/logister/api/internal/authz"
	"github.com/logister/logister/api/internal/db"
	"github.com/logister/logister/api/internal/directory"
	"github.com/logister/logister/api/internal/search"
	"golang.org/x/crypto/bcrypt"
)

type directoryClient interface {
	Authenticate(context.Context, string, string) (directory.Identity, error)
	Refresh(context.Context, string) (directory.Identity, error)
	List(context.Context, string) ([]directory.Entry, error)
}
type authStorage interface {
	Grants(context.Context) (db.GrantCatalog, error)
	SaveGrants(context.Context, db.GrantCatalog) error
	CreateSession(context.Context, string, db.AuthSession) error
	Session(context.Context, string) (db.AuthSession, error)
	DeleteSession(context.Context, string) error
}
type loginFailures struct {
	Count int
	Until time.Time
}
type authentication struct {
	directory directoryClient
	ldap      *ldapManager
	store     authStorage
	localHash []byte
	mu        sync.Mutex
	failures  map[string]loginFailures
}
type authContextKey struct{}
type requestIdentity struct {
	identity directory.Identity
	access   authz.Access
	version  string
}

const sessionCookie = "logister_session"

func (s *Server) ConfigureAuth(ctx context.Context) error {
	if !s.cfg.AuthEnabled {
		return nil
	}
	if len(s.cfg.AuthLocalPassword) < 16 || len(s.cfg.AuthLocalPassword) > 72 {
		return errors.New("AUTH_LOCAL_PASSWORD must have 16–72 bytes")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(s.cfg.AuthLocalPassword), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	store := db.NewAuthStore(s.pool)
	if err = store.Initialize(ctx); err != nil {
		return err
	}
	manager, err := newLDAPManager(store, s.cfg.AuthSettingsKey)
	if err != nil {
		return err
	}
	// Environment settings only seed an installation without a stored record.
	// An empty environment allows first-time setup through the emergency account.
	secret, err := manager.encrypt(s.cfg.LDAPBindPassword)
	if err != nil {
		return err
	}
	if err = store.InitializeLDAP(ctx, db.LDAPSettings{URL: s.cfg.LDAPURL, BindDN: s.cfg.LDAPBindDN, UsersDN: s.cfg.LDAPUsersDN, GroupsDN: s.cfg.LDAPGroupsDN, PasswordCipher: secret}); err != nil {
		return err
	}
	s.auth = &authentication{directory: manager, ldap: manager, store: store, localHash: hash, failures: map[string]loginFailures{}}
	return nil
}

func (a *authentication) attempt(remote string, failed *bool) bool {
	host, _, _ := net.SplitHostPort(remote)
	if host == "" {
		host = remote
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	for key, value := range a.failures {
		if now.After(value.Until) {
			delete(a.failures, key)
		}
	}
	value := a.failures[host]
	if failed == nil {
		return value.Count < 10 && len(a.failures) < 2048
	}
	if !*failed {
		delete(a.failures, host)
		return true
	}
	if value.Count == 0 {
		value.Until = now.Add(time.Minute)
	}
	value.Count++
	a.failures[host] = value
	return true
}

func decodeAuthJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 65536))
	decoder.DisallowUnknownFields()
	if decoder.Decode(v) != nil || decoder.Decode(new(any)) != io.EOF {
		writeError(w, 400, "Invalid request JSON.")
		return false
	}
	return true
}

func (s *Server) handleAuthStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]bool{"enabled": s.cfg.AuthEnabled})
}
func (s *Server) cookie(w http.ResponseWriter, token string, age int) {
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: token, Path: "/api", HttpOnly: true, Secure: s.cfg.AuthCookieSecure, SameSite: http.SameSiteStrictMode, MaxAge: age})
}
func (s *Server) validOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return r.Header.Get("Sec-Fetch-Site") == "" || r.Header.Get("Sec-Fetch-Site") == "same-origin"
	}
	// Compare exact origins, including port. Reverse proxies must preserve Host.
	u, err := url.Parse(origin)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Path != "" {
		return false
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return origin == scheme+"://"+r.Host || origin == s.cfg.CORSOrigin
}
func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	if s.auth == nil {
		writeError(w, 404, "Authentication is disabled.")
		return
	}
	if !s.validOrigin(r) {
		writeError(w, 403, "Cross-origin sign-in is not allowed.")
		return
	}
	if !s.auth.attempt(r.RemoteAddr, nil) {
		writeError(w, 429, "Too many failed sign-in attempts. Try again in one minute.")
		return
	}
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
		Source   string `json:"source"`
	}
	if !decodeAuthJSON(w, r, &body) {
		return
	}
	var identity directory.Identity
	var err error
	local := body.Source == "local"
	if local {
		err = directory.ErrCredentials
		if body.Username == db.LocalAdminID && bcrypt.CompareHashAndPassword(s.auth.localHash, []byte(body.Password)) == nil {
			identity = directory.Identity{Subject: authz.Subject{UserID: db.LocalAdminID}, Username: db.LocalAdminID}
			err = nil
		}
	} else if body.Source == "" || body.Source == "ldap" {
		identity, err = s.auth.directory.Authenticate(r.Context(), body.Username, body.Password)
	} else {
		err = directory.ErrCredentials
	}
	if err != nil {
		failed := true
		s.auth.attempt(r.RemoteAddr, &failed)
		s.identityError(w, err)
		return
	}
	failed := false
	s.auth.attempt(r.RemoteAddr, &failed)
	var raw [32]byte
	if _, err = rand.Read(raw[:]); err != nil {
		writeError(w, 503, "Sign-in unavailable.")
		return
	}
	token := hex.EncodeToString(raw[:])
	session := db.AuthSession{UserID: identity.UserID, Username: identity.Username, Local: local, Expires: time.Now().Add(8 * time.Hour)}
	if err = s.auth.store.CreateSession(r.Context(), digest([]byte(token)), session); err != nil {
		writeError(w, 503, "Sign-in unavailable.")
		return
	}
	// Rotate the previous browser session as part of signing in.
	if old, e := r.Cookie(sessionCookie); e == nil {
		_ = s.auth.store.DeleteSession(r.Context(), digest([]byte(old.Value)))
	}
	s.cookie(w, token, 8*3600)
	writeJSON(w, 200, map[string]string{"username": identity.Username})
}
func (s *Server) identityError(w http.ResponseWriter, err error) {
	if errors.Is(err, directory.ErrCredentials) || errors.Is(err, pgx.ErrNoRows) {
		writeError(w, 401, "Invalid credentials or expired session.")
		return
	}
	log.Printf("identity provider unavailable: %v", err)
	writeError(w, 503, "Identity provider is unavailable. Try again or use the local emergency administrator.")
}

func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		public := r.URL.Path == "/api/health" || r.URL.Path == "/api/auth/status" || r.URL.Path == "/api/auth/login" || isFluentBitIngest(r)
		if !s.cfg.AuthEnabled || public {
			next.ServeHTTP(w, r)
			return
		}
		if s.auth == nil {
			writeError(w, 503, "Authentication is not configured.")
			return
		}
		var identity directory.Identity
		var err error
		username, password, basic := r.BasicAuth()
		ingest := r.Method == "POST" && (r.URL.Path == "/api/ingest/json" || strings.HasPrefix(r.URL.Path, "/api/streams/") && strings.HasSuffix(r.URL.Path, "/ingest"))
		if basic {
			if !ingest {
				writeError(w, 401, "Use a browser session for this endpoint.")
				return
			}
			if !s.auth.attempt(r.RemoteAddr, nil) {
				writeError(w, 429, "Too many failed authentication attempts.")
				return
			}
			identity, err = s.auth.directory.Authenticate(r.Context(), username, password)
			failed := err != nil
			s.auth.attempt(r.RemoteAddr, &failed)
		} else {
			cookie, e := r.Cookie(sessionCookie)
			if e != nil || len(cookie.Value) != 64 {
				writeError(w, 401, "Sign in to continue.")
				return
			}
			if r.Method != "GET" && r.Method != "HEAD" && !s.validOrigin(r) {
				writeError(w, 403, "Cross-origin requests are not allowed.")
				return
			}
			if r.URL.Path == "/api/auth/logout" && r.Method == "POST" {
				if e = s.auth.store.DeleteSession(r.Context(), digest([]byte(cookie.Value))); e != nil {
					writeError(w, 503, "Could not sign out.")
					return
				}
				s.cookie(w, "", -1)
				w.WriteHeader(204)
				return
			}
			session, e := s.auth.store.Session(r.Context(), digest([]byte(cookie.Value)))
			err = e
			if err == nil {
				if session.Local {
					identity = directory.Identity{Subject: authz.Subject{UserID: db.LocalAdminID}, Username: db.LocalAdminID}
				} else {
					identity, err = s.auth.directory.Refresh(r.Context(), session.UserID)
				}
			}
		}
		if err != nil {
			s.identityError(w, err)
			return
		}
		grants, err := s.auth.store.Grants(r.Context())
		if err != nil {
			writeError(w, 503, "Role mappings are unavailable.")
			return
		}
		access, err := authz.Resolve(identity.Subject, grants.Grants)
		if err != nil {
			writeError(w, 503, "Invalid role mappings.")
			return
		}
		capabilities := []any{}
		for _, permission := range authz.AllPermissions() {
			every, ids := access.Streams(permission)
			capabilities = append(capabilities, struct {
				Permission authz.Permission
				Every      bool
				Streams    []string
			}{permission, every, ids})
		}
		version, _ := json.Marshal(capabilities)
		ctx := context.WithValue(r.Context(), authContextKey{}, requestIdentity{identity: identity, access: access, version: digest(version)})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// The dedicated Fluent Bit receiver accepts unauthenticated shipper traffic.
// Other ingest and application endpoints retain their authentication policy.
func isFluentBitIngest(r *http.Request) bool {
	return r.Method == http.MethodPost && r.URL.Path == "/api/ingest/fluent-bit"
}

func (s *Server) require(permission authz.Permission, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.cfg.AuthEnabled {
			next(w, r)
			return
		}
		info, ok := r.Context().Value(authContextKey{}).(requestIdentity)
		if !ok {
			writeError(w, 401, "Sign in to continue.")
			return
		}
		id := ""
		if name := r.PathValue("name"); name != "" && permission.StreamScoped() {
			streams, err := s.catalog.List(r.Context())
			if err != nil {
				writeError(w, 503, "Stream settings unavailable.")
				return
			}
			for _, stream := range streams {
				if stream.Name == name {
					id = stream.ID
					break
				}
			}
		}
		if !info.access.Allowed(permission, id) {
			writeError(w, 403, "Your role does not permit this action.")
			return
		}
		next(w, r)
	}
}
func (s *Server) handleMe(w http.ResponseWriter, r *http.Request) {
	if !s.cfg.AuthEnabled {
		writeJSON(w, 200, map[string]bool{"enabled": false})
		return
	}
	info := r.Context().Value(authContextKey{}).(requestIdentity)
	permissions := []authz.Permission{}
	for _, p := range authz.AllPermissions() {
		every, ids := info.access.Streams(p)
		if every || len(ids) > 0 {
			permissions = append(permissions, p)
		}
	}
	writeJSON(w, 200, map[string]any{"enabled": true, "username": info.identity.Username, "user_id": info.identity.UserID, "permissions": permissions, "administrator": info.access.Allowed(authz.SystemGrants, ""), "access_version": info.version})
}
func (s *Server) handleDirectory(w http.ResponseWriter, r *http.Request) {
	if s.auth == nil {
		writeError(w, 404, "LDAP is disabled.")
		return
	}
	q := r.URL.Query().Get("q")
	if len(q) > 128 {
		writeError(w, 400, "Search text is too long.")
		return
	}
	entries, err := s.auth.directory.List(r.Context(), q)
	if err != nil {
		writeError(w, 503, "Directory search failed. Try a narrower search.")
		return
	}
	writeJSON(w, 200, map[string]any{"entries": entries})
}
func (s *Server) handleGrants(w http.ResponseWriter, r *http.Request) {
	if s.auth == nil {
		writeError(w, 404, "Authentication is disabled.")
		return
	}
	if r.Method == "GET" {
		catalog, err := s.auth.store.Grants(r.Context())
		if err != nil {
			writeError(w, 503, "Role mappings unavailable.")
			return
		}
		writeJSON(w, 200, catalog)
		return
	}
	var catalog db.GrantCatalog
	if !decodeAuthJSON(w, r, &catalog) {
		return
	}
	if len(catalog.Grants) > 1000 {
		writeError(w, 400, "Too many role mappings.")
		return
	}
	streams, err := s.catalog.List(r.Context())
	if err != nil {
		writeError(w, 503, "Stream settings unavailable.")
		return
	}
	if archived, ok := s.catalog.(archivedCatalog); ok {
		hidden, err := archived.ListArchived(r.Context())
		if err != nil {
			writeError(w, 503, "Stream settings unavailable.")
			return
		}
		streams = append(streams, hidden...)
	}
	for _, g := range catalog.Grants {
		if err = authz.ValidateGrant(g); err != nil {
			writeError(w, 400, err.Error())
			return
		}
		if g.Scope == authz.ScopeStream {
			found := false
			for _, stream := range streams {
				found = found || stream.ID == g.StreamID
			}
			if !found {
				writeError(w, 400, "Role mapping references an unknown stream.")
				return
			}
		}
	}
	if err = s.auth.store.SaveGrants(r.Context(), catalog); err != nil {
		if errors.Is(err, db.ErrGrantConflict) {
			writeError(w, 409, err.Error())
		} else {
			log.Printf("save grants: %v", err)
			writeError(w, 400, "Could not save mappings. Preserve the local emergency administrator.")
		}
		return
	}
	writeJSON(w, 200, map[string]string{"status": "saved"})
}

// allowedSearch applies the current membership before OpenSearch sees any query,
// including a query_string expression or a signed cursor from an earlier request.
func (s *Server) allowedSearch(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	if archived, ok := s.catalog.(archivedCatalog); ok {
		hidden, err := archived.ListArchived(r.Context())
		if err != nil {
			writeError(w, 503, "Stream settings unavailable.")
			return r, false
		}
		names := []string{}
		for _, stream := range hidden {
			names = append(names, stream.Name)
		}
		r = r.WithContext(search.WithExcludedStreams(r.Context(), names))
	}
	if !s.cfg.AuthEnabled {
		return r, true
	}
	info := r.Context().Value(authContextKey{}).(requestIdentity)
	every, ids := info.access.Streams(authz.MessagesSearch)
	if !every && len(ids) == 0 {
		writeError(w, 403, "No streams are available for search.")
		return r, false
	}
	var names []string
	if !every {
		names = []string{}
		streams, err := s.catalog.List(r.Context())
		if err != nil {
			writeError(w, 503, "Stream settings unavailable.")
			return r, false
		}
		for _, stream := range streams {
			if info.access.Allowed(authz.MessagesSearch, stream.ID) {
				names = append(names, stream.Name)
			}
		}
	}
	target := r.URL.Query().Get("stream")
	if target == "@default" {
		target = "all"
	} else if target == "all" {
		target = ""
	}
	if !every && target != "" {
		found := false
		for _, name := range names {
			found = found || name == target
		}
		if !found {
			writeError(w, 403, "This stream is not available to your role.")
			return r, false
		}
	}
	return r.WithContext(search.WithAccess(r.Context(), info.identity.UserID, names)), true
}
