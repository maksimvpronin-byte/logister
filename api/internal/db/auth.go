package db

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/logister/logister/api/internal/authz"
)

const LocalAdminID = "local-admin"
const authSchema = `
CREATE TABLE IF NOT EXISTS auth_sessions (
 token_hash text PRIMARY KEY, user_id text NOT NULL, username text NOT NULL,
 local_user boolean NOT NULL DEFAULT false, expires_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS auth_sessions_expiry ON auth_sessions(expires_at);
CREATE TABLE IF NOT EXISTS auth_grant_catalog (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 grants jsonb NOT NULL, revision bigint NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS auth_ldap_settings (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 server_url text NOT NULL, bind_dn text NOT NULL, users_dn text NOT NULL,
 groups_dn text NOT NULL, password_cipher text NOT NULL,
 revision bigint NOT NULL DEFAULT 1
);
`

type authDB interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	Begin(context.Context) (pgx.Tx, error)
}
type AuthStore struct{ pool authDB }

func NewAuthStore(pool authDB) *AuthStore { return &AuthStore{pool: pool} }
func LocalAdminGrant() authz.Grant {
	return authz.Grant{PrincipalKind: authz.PrincipalUser, PrincipalID: LocalAdminID, Role: authz.RoleAdministrator, Scope: authz.ScopeSystem}
}
func (s *AuthStore) Initialize(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, authSchema); err != nil {
		return err
	}
	raw, _ := json.Marshal([]authz.Grant{LocalAdminGrant()})
	_, err := s.pool.Exec(ctx, `INSERT INTO auth_grant_catalog(singleton,grants) VALUES(true,$1) ON CONFLICT DO NOTHING`, raw)
	return err
}

type GrantCatalog struct {
	Grants   []authz.Grant `json:"grants"`
	Revision int64         `json:"revision"`
}

func (s *AuthStore) Grants(ctx context.Context) (GrantCatalog, error) {
	var c GrantCatalog
	var raw []byte
	err := s.pool.QueryRow(ctx, `SELECT grants,revision FROM auth_grant_catalog WHERE singleton`).Scan(&raw, &c.Revision)
	if err == nil {
		err = json.Unmarshal(raw, &c.Grants)
	}
	return c, err
}

var ErrGrantConflict = errors.New("role mappings changed; reload before saving")

func (s *AuthStore) SaveGrants(ctx context.Context, c GrantCatalog) error {
	local := false
	for _, g := range c.Grants {
		if err := authz.ValidateGrant(g); err != nil {
			return err
		}
		if g == LocalAdminGrant() {
			local = true
		}
	}
	if !local || authz.SystemAdministrators(c.Grants) == 0 {
		return errors.New("the local emergency administrator grant must be preserved")
	}
	raw, err := json.Marshal(c.Grants)
	if err != nil {
		return err
	}
	result, err := s.pool.Exec(ctx, `UPDATE auth_grant_catalog SET grants=$1,revision=revision+1 WHERE singleton AND revision=$2`, raw, c.Revision)
	if err == nil && result.RowsAffected() != 1 {
		return ErrGrantConflict
	}
	return err
}

type AuthSession struct {
	UserID, Username string
	Local            bool
	Expires          time.Time
}

func (s *AuthStore) CreateSession(ctx context.Context, hash string, session AuthSession) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `DELETE FROM auth_sessions WHERE expires_at<=now()`); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO auth_sessions(token_hash,user_id,username,local_user,expires_at) VALUES($1,$2,$3,$4,$5)`, hash, session.UserID, session.Username, session.Local, session.Expires)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *AuthStore) Session(ctx context.Context, hash string) (AuthSession, error) {
	var session AuthSession
	err := s.pool.QueryRow(ctx, `SELECT user_id,username,local_user,expires_at FROM auth_sessions WHERE token_hash=$1 AND expires_at>now()`, hash).Scan(&session.UserID, &session.Username, &session.Local, &session.Expires)
	return session, err
}
func (s *AuthStore) DeleteSession(ctx context.Context, hash string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM auth_sessions WHERE token_hash=$1`, hash)
	return err
}
