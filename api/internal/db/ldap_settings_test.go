package db

import (
	"errors"
	"testing"
)

func TestLDAPSettingsPersistAndEnvironmentDoesNotOverwrite(t *testing.T) {
	ctx, tx := ingestionTransaction(t)
	store := NewAuthStore(tx)
	if err := store.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	initial := LDAPSettings{URL: "ldap://first", BindDN: "uid=reader,dc=test", UsersDN: "ou=people,dc=test", GroupsDN: "ou=groups,dc=test", PasswordCipher: "encrypted-password"}
	if err := store.InitializeLDAP(ctx, initial); err != nil {
		t.Fatal(err)
	}
	current, err := store.LDAPSettings(ctx)
	if err != nil || current.Revision != 1 || current.URL != initial.URL {
		t.Fatal(current, err)
	}
	current.URL = "ldaps://changed"
	if err = store.SaveLDAPSettings(ctx, current); err != nil {
		t.Fatal(err)
	}
	if err = store.SaveLDAPSettings(ctx, current); !errors.Is(err, ErrLDAPConflict) {
		t.Fatal(err)
	}
	if err = store.InitializeLDAP(ctx, initial); err != nil {
		t.Fatal(err)
	}
	got, err := store.LDAPSettings(ctx)
	if err != nil || got.URL != current.URL || got.Revision != 2 || got.PasswordCipher != initial.PasswordCipher {
		t.Fatal(got, err)
	}
}
