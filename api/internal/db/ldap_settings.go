package db

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// LDAPSettings is internal storage. Never serialize PasswordCipher to the API.
type LDAPSettings struct {
	URL, BindDN, UsersDN, GroupsDN, PasswordCipher string
	Revision                                       int64
}

type ldapDoc struct {
	ID             string `bson:"_id"`
	URL            string `bson:"server_url"`
	BindDN         string `bson:"bind_dn"`
	UsersDN        string `bson:"users_dn"`
	GroupsDN       string `bson:"groups_dn"`
	PasswordCipher string `bson:"password_cipher"`
	Revision       int64  `bson:"revision"`
}

var ErrLDAPConflict = errors.New("LDAP settings changed; reload before saving")

func (s *AuthStore) ldap() *mongo.Collection { return s.db.coll("auth_ldap_settings") }

func (s *AuthStore) InitializeLDAP(ctx context.Context, v LDAPSettings) error {
	_, err := s.ldap().InsertOne(ctx, ldapDoc{ID: "singleton", URL: v.URL, BindDN: v.BindDN, UsersDN: v.UsersDN, GroupsDN: v.GroupsDN, PasswordCipher: v.PasswordCipher, Revision: 1})
	if mongo.IsDuplicateKeyError(err) {
		return nil
	}
	return err
}

func (s *AuthStore) LDAPSettings(ctx context.Context) (LDAPSettings, error) {
	var doc ldapDoc
	err := s.ldap().FindOne(ctx, bson.M{"_id": "singleton"}).Decode(&doc)
	if err != nil {
		return LDAPSettings{}, err
	}
	return LDAPSettings{URL: doc.URL, BindDN: doc.BindDN, UsersDN: doc.UsersDN, GroupsDN: doc.GroupsDN, PasswordCipher: doc.PasswordCipher, Revision: doc.Revision}, nil
}

func (s *AuthStore) SaveLDAPSettings(ctx context.Context, v LDAPSettings) error {
	res, err := s.ldap().UpdateOne(ctx, bson.M{"_id": "singleton", "revision": v.Revision}, bson.M{"$set": bson.M{
		"server_url": v.URL, "bind_dn": v.BindDN, "users_dn": v.UsersDN, "groups_dn": v.GroupsDN, "password_cipher": v.PasswordCipher,
	}, "$inc": bson.M{"revision": 1}})
	if err != nil {
		return err
	}
	if res.MatchedCount != 1 {
		return ErrLDAPConflict
	}
	return nil
}
