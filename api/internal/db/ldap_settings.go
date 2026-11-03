package db

import (
	"context"
	"errors"
)

// LDAPSettings is internal storage. Never serialize PasswordCipher to the API.
type LDAPSettings struct {
	URL, BindDN, UsersDN, GroupsDN, PasswordCipher string
	Revision                                       int64
}

var ErrLDAPConflict = errors.New("LDAP settings changed; reload before saving")

func (s *AuthStore) InitializeLDAP(ctx context.Context, v LDAPSettings) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO auth_ldap_settings(singleton,server_url,bind_dn,users_dn,groups_dn,password_cipher) VALUES(true,$1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, v.URL, v.BindDN, v.UsersDN, v.GroupsDN, v.PasswordCipher)
	return err
}
func (s *AuthStore) LDAPSettings(ctx context.Context) (LDAPSettings, error) {
	var v LDAPSettings
	err := s.pool.QueryRow(ctx, `SELECT server_url,bind_dn,users_dn,groups_dn,password_cipher,revision FROM auth_ldap_settings WHERE singleton`).Scan(&v.URL, &v.BindDN, &v.UsersDN, &v.GroupsDN, &v.PasswordCipher, &v.Revision)
	return v, err
}
func (s *AuthStore) SaveLDAPSettings(ctx context.Context, v LDAPSettings) error {
	result, err := s.pool.Exec(ctx, `UPDATE auth_ldap_settings SET server_url=$1,bind_dn=$2,users_dn=$3,groups_dn=$4,password_cipher=$5,revision=revision+1 WHERE singleton AND revision=$6`, v.URL, v.BindDN, v.UsersDN, v.GroupsDN, v.PasswordCipher, v.Revision)
	if err == nil && result.RowsAffected() != 1 {
		return ErrLDAPConflict
	}
	return err
}
