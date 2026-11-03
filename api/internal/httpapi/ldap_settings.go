package httpapi

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"

	"github.com/logister/logister/api/internal/config"
	"github.com/logister/logister/api/internal/db"
	"github.com/logister/logister/api/internal/directory"
)

type ldapSettingsStore interface {
	LDAPSettings(context.Context) (db.LDAPSettings, error)
	SaveLDAPSettings(context.Context, db.LDAPSettings) error
}
type checkedDirectory interface {
	directoryClient
	Check(context.Context) (directory.CheckResult, error)
}
type ldapManager struct {
	store   ldapSettingsStore
	aead    cipher.AEAD
	factory func(config.Config) (checkedDirectory, error)
}

func newLDAPManager(store ldapSettingsStore, key string) (*ldapManager, error) {
	raw, err := hex.DecodeString(key)
	if err != nil || len(raw) != 32 {
		return nil, errors.New("AUTH_SETTINGS_KEY must be a 64-character hex encryption key")
	}
	block, err := aes.NewCipher(raw)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &ldapManager{store: store, aead: aead, factory: func(cfg config.Config) (checkedDirectory, error) { return directory.New(cfg) }}, nil
}
func (m *ldapManager) encrypt(password string) (string, error) {
	if password == "" {
		return "", nil
	}
	nonce := make([]byte, m.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(m.aead.Seal(nonce, nonce, []byte(password), []byte("logister:ldap-bind:v1"))), nil
}
func (m *ldapManager) decrypt(value string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(value)
	n := m.aead.NonceSize()
	if err != nil || len(raw) < n {
		return "", errors.New("Stored LDAP password cannot be decrypted. Enter the service account password again.")
	}
	plain, err := m.aead.Open(nil, raw[:n], raw[n:], []byte("logister:ldap-bind:v1"))
	if err != nil {
		return "", errors.New("Stored LDAP password cannot be decrypted. Enter the service account password again.")
	}
	return string(plain), nil
}
func (m *ldapManager) client(ctx context.Context) (checkedDirectory, error) {
	saved, err := m.store.LDAPSettings(ctx)
	if err != nil {
		return nil, err
	}
	if saved.URL == "" || saved.PasswordCipher == "" {
		return nil, errors.New("LDAP is not configured. Use the local emergency administrator to configure it.")
	}
	password, err := m.decrypt(saved.PasswordCipher)
	if err != nil {
		return nil, err
	}
	return m.factory(config.Config{LDAPURL: saved.URL, LDAPBindDN: saved.BindDN, LDAPBindPassword: password, LDAPUsersDN: saved.UsersDN, LDAPGroupsDN: saved.GroupsDN})
}

// Read the saved settings per operation so all API instances use the same config.
func (m *ldapManager) Authenticate(ctx context.Context, u, p string) (directory.Identity, error) {
	c, err := m.client(ctx)
	if err != nil {
		return directory.Identity{}, err
	}
	return c.Authenticate(ctx, u, p)
}
func (m *ldapManager) Refresh(ctx context.Context, id string) (directory.Identity, error) {
	c, err := m.client(ctx)
	if err != nil {
		return directory.Identity{}, err
	}
	return c.Refresh(ctx, id)
}
func (m *ldapManager) List(ctx context.Context, q string) ([]directory.Entry, error) {
	c, err := m.client(ctx)
	if err != nil {
		return nil, err
	}
	return c.List(ctx, q)
}

type ldapSettingsInput struct {
	URL          string `json:"url"`
	BindDN       string `json:"bind_dn"`
	BindPassword string `json:"bind_password,omitempty"`
	UsersDN      string `json:"users_dn"`
	GroupsDN     string `json:"groups_dn"`
	Revision     int64  `json:"revision"`
}
type ldapSettingsView struct {
	URL         string `json:"url"`
	BindDN      string `json:"bind_dn"`
	UsersDN     string `json:"users_dn"`
	GroupsDN    string `json:"groups_dn"`
	PasswordSet bool   `json:"password_set"`
	Revision    int64  `json:"revision"`
}

func ldapView(v db.LDAPSettings) ldapSettingsView {
	return ldapSettingsView{URL: v.URL, BindDN: v.BindDN, UsersDN: v.UsersDN, GroupsDN: v.GroupsDN, PasswordSet: v.PasswordCipher != "", Revision: v.Revision}
}

func (s *Server) handleLDAPSettings(w http.ResponseWriter, r *http.Request) {
	if s.auth == nil || s.auth.ldap == nil {
		writeError(w, 503, "LDAP settings are unavailable.")
		return
	}
	manager := s.auth.ldap
	saved, err := manager.store.LDAPSettings(r.Context())
	if err != nil {
		writeError(w, 503, "Could not read LDAP settings.")
		return
	}
	if r.Method == http.MethodGet {
		writeJSON(w, 200, ldapView(saved))
		return
	}
	var input ldapSettingsInput
	if !decodeAuthJSON(w, r, &input) {
		return
	}
	if input.Revision != saved.Revision {
		writeError(w, 409, db.ErrLDAPConflict.Error())
		return
	}
	input.URL = strings.TrimSpace(input.URL)
	input.BindDN = strings.TrimSpace(input.BindDN)
	input.UsersDN = strings.TrimSpace(input.UsersDN)
	input.GroupsDN = strings.TrimSpace(input.GroupsDN)
	password := input.BindPassword
	if password == "" && saved.PasswordCipher != "" {
		password, err = manager.decrypt(saved.PasswordCipher)
		if err != nil {
			writeError(w, 400, err.Error())
			return
		}
	}
	cfg := config.Config{LDAPURL: input.URL, LDAPBindDN: input.BindDN, LDAPBindPassword: password, LDAPUsersDN: input.UsersDN, LDAPGroupsDN: input.GroupsDN}
	// Always validate independently of the injectable connection implementation.
	if _, err = directory.New(cfg); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	client, err := manager.factory(cfg)
	if err != nil {
		writeError(w, 400, "Invalid LDAP configuration.")
		return
	}
	check, err := client.Check(r.Context())
	if err != nil {
		writeError(w, 422, err.Error())
		return
	}
	if r.Method == http.MethodPost {
		writeJSON(w, 200, check)
		return
	}
	encrypted := saved.PasswordCipher
	if input.BindPassword != "" {
		encrypted, err = manager.encrypt(input.BindPassword)
		if err != nil {
			writeError(w, 503, "Could not protect the LDAP password.")
			return
		}
	}
	next := db.LDAPSettings{URL: input.URL, BindDN: input.BindDN, UsersDN: input.UsersDN, GroupsDN: input.GroupsDN, PasswordCipher: encrypted, Revision: input.Revision}
	if err = manager.store.SaveLDAPSettings(r.Context(), next); err != nil {
		if errors.Is(err, db.ErrLDAPConflict) {
			writeError(w, 409, err.Error())
		} else {
			writeError(w, 503, "Could not save LDAP settings.")
		}
		return
	}
	next.Revision++
	writeJSON(w, 200, ldapView(next))
}
