package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/logister/logister/api/internal/config"
	"github.com/logister/logister/api/internal/db"
	"github.com/logister/logister/api/internal/directory"
)

type memoryLDAPSettings struct {
	value  db.LDAPSettings
	writes int
}

func (m *memoryLDAPSettings) LDAPSettings(context.Context) (db.LDAPSettings, error) {
	return m.value, nil
}
func (m *memoryLDAPSettings) SaveLDAPSettings(_ context.Context, v db.LDAPSettings) error {
	if v.Revision != m.value.Revision {
		return db.ErrLDAPConflict
	}
	v.Revision++
	m.value = v
	m.writes++
	return nil
}

type checkedFake struct {
	fakeDirectory
	checkErr error
}

func (f *checkedFake) Check(context.Context) (directory.CheckResult, error) {
	return directory.CheckResult{UsersFound: true, GroupsFound: true}, f.checkErr
}

func ldapSettingsFixture(t *testing.T) (*Server, *memoryLDAPSettings, *ldapManager, *[]config.Config) {
	t.Helper()
	s, _, _ := authFixture(t)
	store := &memoryLDAPSettings{}
	manager, err := newLDAPManager(store, strings.Repeat("a1", 32))
	if err != nil {
		t.Fatal(err)
	}
	cipher, err := manager.encrypt("private-bind-password")
	if err != nil {
		t.Fatal(err)
	}
	store.value = db.LDAPSettings{URL: "ldap://directory:389", BindDN: "uid=reader,dc=test", UsersDN: "ou=people,dc=test", GroupsDN: "ou=groups,dc=test", PasswordCipher: cipher, Revision: 1}
	seen := []config.Config{}
	manager.factory = func(cfg config.Config) (checkedDirectory, error) {
		seen = append(seen, cfg)
		return &checkedFake{}, nil
	}
	s.auth.ldap = manager
	return s, store, manager, &seen
}
func inputFrom(v db.LDAPSettings) ldapSettingsInput {
	return ldapSettingsInput{URL: v.URL, BindDN: v.BindDN, UsersDN: v.UsersDN, GroupsDN: v.GroupsDN, Revision: v.Revision}
}
func TestLDAPSettingsAreAdminOnlyAndNeverExposePassword(t *testing.T) {
	s, store, _, _ := ldapSettingsFixture(t)
	h := s.Handler()
	for _, route := range []struct{ method, path string }{{"GET", "/api/auth/ldap"}, {"PUT", "/api/auth/ldap"}, {"POST", "/api/auth/ldap/test"}} {
		if w := authRequest(h, route.method, route.path, `{}`, nil); w.Code != 401 {
			t.Fatal(route, w.Code)
		}
	}
	operator := loginCookie(t, h, "operator", "ldap", "correct")
	for _, route := range []struct{ method, path string }{{"GET", "/api/auth/ldap"}, {"PUT", "/api/auth/ldap"}, {"POST", "/api/auth/ldap/test"}} {
		if w := authRequest(h, route.method, route.path, `{}`, operator); w.Code != 403 {
			t.Fatal(route, w.Code)
		}
	}
	admin := loginCookie(t, h, "local-admin", "local", "local-test-password")
	w := authRequest(h, "GET", "/api/auth/ldap", "", admin)
	if w.Code != 200 || strings.Contains(w.Body.String(), "private-bind-password") || strings.Contains(w.Body.String(), store.value.PasswordCipher) || strings.Contains(w.Body.String(), "bind_password") {
		t.Fatal(w.Code, w.Body.String())
	}
	var v ldapSettingsView
	if err := json.Unmarshal(w.Body.Bytes(), &v); err != nil || !v.PasswordSet {
		t.Fatal(v, err)
	}
}
func TestLDAPDraftTestingDoesNotSaveAndSavePreservesBlankPassword(t *testing.T) {
	s, store, manager, seen := ldapSettingsFixture(t)
	h := s.Handler()
	admin := loginCookie(t, h, "local-admin", "local", "local-test-password")
	input := inputFrom(store.value)
	input.UsersDN = "ou=newpeople,dc=test"
	raw, _ := json.Marshal(input)
	w := authRequest(h, "POST", "/api/auth/ldap/test", string(raw), admin)
	if w.Code != 200 || store.writes != 0 || (*seen)[0].LDAPBindPassword != "private-bind-password" {
		t.Fatal(w.Code, store.writes)
	}
	w = authRequest(h, "PUT", "/api/auth/ldap", string(raw), admin)
	if w.Code != 200 || store.writes != 1 || store.value.UsersDN != input.UsersDN || store.value.Revision != 2 {
		t.Fatal(w.Code, store.value)
	}
	password, err := manager.decrypt(store.value.PasswordCipher)
	if err != nil || password != "private-bind-password" {
		t.Fatal("blank password was not preserved", err)
	}
	if w = authRequest(h, "PUT", "/api/auth/ldap", string(raw), admin); w.Code != 409 {
		t.Fatal(w.Code)
	}
	input.Revision = 2
	input.BindPassword = "replacement-password"
	raw, _ = json.Marshal(input)
	w = authRequest(h, "PUT", "/api/auth/ldap", string(raw), admin)
	password, err = manager.decrypt(store.value.PasswordCipher)
	if w.Code != 200 || err != nil || password != "replacement-password" || strings.Contains(w.Body.String(), input.BindPassword) {
		t.Fatal(w.Code, err)
	}
}
func TestLDAPRejectsBadConnectionAndInvalidDNWithoutSaving(t *testing.T) {
	s, store, manager, _ := ldapSettingsFixture(t)
	h := s.Handler()
	admin := loginCookie(t, h, "local-admin", "local", "local-test-password")
	input := inputFrom(store.value)
	raw, _ := json.Marshal(input)
	manager.factory = func(config.Config) (checkedDirectory, error) {
		return &checkedFake{checkErr: errors.New("connection failed")}, nil
	}
	if w := authRequest(h, "PUT", "/api/auth/ldap", string(raw), admin); w.Code != 422 || store.writes != 0 {
		t.Fatal(w.Code, store.writes)
	}
	input.UsersDN = "not-a-dn"
	raw, _ = json.Marshal(input)
	if w := authRequest(h, "PUT", "/api/auth/ldap", string(raw), admin); w.Code != 400 || store.writes != 0 {
		t.Fatal(w.Code, store.writes)
	}
}
func TestLDAPPasswordEncryptionAndRuntimeSettingsRefresh(t *testing.T) {
	_, store, manager, seen := ldapSettingsFixture(t)
	a, _ := manager.encrypt("secret")
	b, _ := manager.encrypt("secret")
	if a == b {
		t.Fatal("nonce reused")
	}
	if _, err := manager.decrypt(a[:len(a)-4] + "AAAA"); err == nil {
		t.Fatal("accepted damaged ciphertext")
	}
	wrong, _ := newLDAPManager(store, strings.Repeat("b2", 32))
	if _, err := wrong.decrypt(a); err == nil {
		t.Fatal("accepted wrong key")
	}
	if _, err := newLDAPManager(store, "short-key"); err == nil {
		t.Fatal("accepted invalid key")
	}
	if _, err := manager.List(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	store.value.URL = "ldaps://new-directory:636"
	if _, err := manager.Refresh(context.Background(), "uuid-operator"); err != nil {
		t.Fatal(err)
	}
	if (*seen)[1].LDAPURL != "ldaps://new-directory:636" {
		t.Fatal("runtime change not applied")
	}
}

func TestUnconfiguredOrUnreadableLDAPDoesNotBlockEmergencySettings(t *testing.T) {
	s, store, manager, _ := ldapSettingsFixture(t)
	s.auth.directory = manager
	store.value.URL = ""
	store.value.PasswordCipher = ""
	h := s.Handler()
	admin := loginCookie(t, h, "local-admin", "local", "local-test-password")
	w := authRequest(h, "GET", "/api/auth/ldap", "", admin)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"password_set":false`) {
		t.Fatal(w.Code, w.Body.String())
	}
	w = authRequest(h, "POST", "/api/auth/login", `{"username":"operator","password":"correct"}`, nil)
	if w.Code != 503 {
		t.Fatal(w.Code, w.Body.String())
	}
	store.value.URL = "ldap://directory:389"
	store.value.PasswordCipher = "unreadable-ciphertext"
	if w = authRequest(h, "GET", "/api/auth/ldap", "", admin); w.Code != 200 {
		t.Fatal(w.Code)
	}
	input := inputFrom(store.value)
	input.BindPassword = "new-service-password"
	raw, _ := json.Marshal(input)
	if w = authRequest(h, "PUT", "/api/auth/ldap", string(raw), admin); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
}
