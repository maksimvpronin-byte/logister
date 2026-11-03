package db

import (
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/logister/logister/api/internal/authz"
	"testing"
	"time"
)

func TestAuthStoreKeepsEmergencyAdminAndRejectsConcurrentEdits(t *testing.T) {
	ctx, tx := ingestionTransaction(t)
	store := NewAuthStore(tx)
	if err := store.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	c, err := store.Grants(ctx)
	if err != nil || len(c.Grants) != 1 || c.Grants[0] != LocalAdminGrant() {
		t.Fatal(c, err)
	}
	c.Grants = append(c.Grants, authz.Grant{PrincipalKind: authz.PrincipalGroup, PrincipalID: "ldap-group-uuid", Role: authz.RoleOperator, Scope: authz.ScopeStream, StreamID: "stream-uuid"})
	if err = store.SaveGrants(ctx, c); err != nil {
		t.Fatal(err)
	}
	if err = store.SaveGrants(ctx, c); !errors.Is(err, ErrGrantConflict) {
		t.Fatal(err)
	}
	next, _ := store.Grants(ctx)
	next.Grants = next.Grants[1:]
	if err = store.SaveGrants(ctx, next); err == nil {
		t.Fatal("removed emergency administrator")
	}
	if err = store.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	next, _ = store.Grants(ctx)
	if len(next.Grants) != 2 {
		t.Fatal("startup overwrote mappings")
	}
	session := AuthSession{UserID: "ldap-user-uuid", Username: "operator", Expires: time.Now().Add(time.Hour)}
	if err = store.CreateSession(ctx, "token-hash", session); err != nil {
		t.Fatal(err)
	}
	got, err := store.Session(ctx, "token-hash")
	if err != nil || got.UserID != session.UserID || got.Local {
		t.Fatal(got, err)
	}
	if err = store.DeleteSession(ctx, "token-hash"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Session(ctx, "token-hash"); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal(err)
	}
	session.Expires = time.Now().Add(-time.Hour)
	if err = store.CreateSession(ctx, "expired-hash", session); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Session(ctx, "expired-hash"); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal(err)
	}
}
