package db

import (
	"context"
	"errors"
	"time"

	"github.com/logister/logister/api/internal/authz"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const LocalAdminID = "local-admin"

type AuthStore struct{ db *DB }

func NewAuthStore(database *DB) *AuthStore { return &AuthStore{db: database} }

func LocalAdminGrant() authz.Grant {
	return authz.Grant{PrincipalKind: authz.PrincipalUser, PrincipalID: LocalAdminID, Role: authz.RoleAdministrator, Scope: authz.ScopeSystem}
}

type grantDoc struct {
	ID       string        `bson:"_id"`
	Grants   []authz.Grant `bson:"grants"`
	Revision int64         `bson:"revision"`
}

func (s *AuthStore) grants() *mongo.Collection { return s.db.coll("auth_grant_catalog") }
func (s *AuthStore) sessions() *mongo.Collection {
	return s.db.coll("auth_sessions")
}

func (s *AuthStore) Initialize(ctx context.Context) error {
	_, err := s.grants().InsertOne(ctx, grantDoc{ID: "singleton", Grants: []authz.Grant{LocalAdminGrant()}, Revision: 1})
	if mongo.IsDuplicateKeyError(err) {
		return nil
	}
	return err
}

type GrantCatalog struct {
	Grants   []authz.Grant `json:"grants"`
	Revision int64         `json:"revision"`
}

func (s *AuthStore) Grants(ctx context.Context) (GrantCatalog, error) {
	var doc grantDoc
	err := s.grants().FindOne(ctx, bson.M{"_id": "singleton"}).Decode(&doc)
	if err != nil {
		return GrantCatalog{}, err
	}
	if doc.Grants == nil {
		doc.Grants = []authz.Grant{}
	}
	return GrantCatalog{Grants: doc.Grants, Revision: doc.Revision}, nil
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
	res, err := s.grants().UpdateOne(ctx, bson.M{"_id": "singleton", "revision": c.Revision}, bson.M{"$set": bson.M{"grants": c.Grants}, "$inc": bson.M{"revision": 1}})
	if err != nil {
		return err
	}
	if res.MatchedCount != 1 {
		return ErrGrantConflict
	}
	return nil
}

type AuthSession struct {
	UserID, Username string
	Local            bool
	Expires          time.Time
}

type sessionDoc struct {
	Hash     string    `bson:"_id"`
	UserID   string    `bson:"user_id"`
	Username string    `bson:"username"`
	Local    bool      `bson:"local_user"`
	Expires  time.Time `bson:"expires_at"`
}

func (s *AuthStore) CreateSession(ctx context.Context, hash string, session AuthSession) error {
	if _, err := s.sessions().DeleteMany(ctx, bson.M{"expires_at": bson.M{"$lte": time.Now().UTC()}}); err != nil {
		return err
	}
	_, err := s.sessions().InsertOne(ctx, sessionDoc{Hash: hash, UserID: session.UserID, Username: session.Username, Local: session.Local, Expires: session.Expires.UTC()})
	return err
}

func (s *AuthStore) Session(ctx context.Context, hash string) (AuthSession, error) {
	var doc sessionDoc
	err := s.sessions().FindOne(ctx, bson.M{"_id": hash, "expires_at": bson.M{"$gt": time.Now().UTC()}}).Decode(&doc)
	if missing(err) {
		return AuthSession{}, ErrNotFound
	}
	if err != nil {
		return AuthSession{}, err
	}
	return AuthSession{UserID: doc.UserID, Username: doc.Username, Local: doc.Local, Expires: doc.Expires}, nil
}

func (s *AuthStore) DeleteSession(ctx context.Context, hash string) error {
	_, err := s.sessions().DeleteOne(ctx, bson.M{"_id": hash})
	return err
}
