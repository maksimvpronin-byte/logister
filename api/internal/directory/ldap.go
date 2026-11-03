// Package directory implements ordinary LDAP identities with stable entryUUIDs.
package directory

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	ldap "github.com/go-ldap/ldap/v3"
	"github.com/logister/logister/api/internal/authz"
	"github.com/logister/logister/api/internal/config"
)

var ErrCredentials = errors.New("invalid username or password")

type Entry struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
	Name string `json:"name"`
	DN   string `json:"dn"`
}
type Identity struct {
	authz.Subject
	Username string
}
type Client struct{ cfg config.Config }

func New(cfg config.Config) (*Client, error) {
	u, err := url.Parse(cfg.LDAPURL)
	if err != nil || u == nil || (u.Scheme != "ldap" && u.Scheme != "ldaps") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("LDAP_URL must be an ldap:// or ldaps:// server URL")
	}
	if cfg.LDAPBindDN == "" || cfg.LDAPBindPassword == "" || cfg.LDAPUsersDN == "" || cfg.LDAPGroupsDN == "" {
		return nil, errors.New("LDAP bind credentials and search bases are required")
	}
	if len(cfg.LDAPURL) > 2048 || len(cfg.LDAPBindPassword) > 4096 {
		return nil, errors.New("LDAP server URL or password is too long")
	}
	for _, value := range []string{cfg.LDAPBindDN, cfg.LDAPUsersDN, cfg.LDAPGroupsDN} {
		if len(value) > 2048 {
			return nil, errors.New("LDAP DN is too long")
		}
		if _, err := ldap.ParseDN(value); err != nil {
			return nil, errors.New("Bind DN and search bases must be valid LDAP distinguished names")
		}
	}
	return &Client{cfg: cfg}, nil
}

type CheckResult struct {
	UsersFound  bool `json:"users_found"`
	GroupsFound bool `json:"groups_found"`
}

// Check verifies bind, both search bases and representative readable records.
// A server size limit is expected when sampling a populated directory.
func (c *Client) Check(ctx context.Context) (CheckResult, error) {
	conn, err := c.connect(ctx)
	if err != nil {
		return CheckResult{}, errors.New("Could not connect or bind to LDAP. Check the server, certificate and service account credentials.")
	}
	defer conn.Close()
	var check CheckResult
	for _, kind := range []string{"user", "group"} {
		base, class, attr := c.cfg.LDAPUsersDN, "inetOrgPerson", "uid"
		if kind == "group" {
			base, class, attr = c.cfg.LDAPGroupsDN, "groupOfNames", "cn"
		}
		result, err := conn.Search(ldap.NewSearchRequest(base, ldap.ScopeBaseObject, ldap.NeverDerefAliases, 1, 5, false, "(objectClass=*)", []string{"1.1"}, nil))
		if err != nil || len(result.Entries) != 1 {
			return check, fmt.Errorf("Cannot read the %s search base. Check the DN and reader permissions.", kind)
		}
		result, err = conn.Search(ldap.NewSearchRequest(base, ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 1, 5, false, "(objectClass="+class+")", []string{attr, "entryUUID"}, nil))
		if err != nil && !ldap.IsErrorWithCode(err, ldap.LDAPResultSizeLimitExceeded) {
			return check, fmt.Errorf("Cannot search LDAP %ss with this service account.", kind)
		}
		found := result != nil && len(result.Entries) > 0
		if found && (result.Entries[0].GetAttributeValue("entryUUID") == "" || result.Entries[0].GetAttributeValue(attr) == "") {
			return check, fmt.Errorf("LDAP %s records must expose %s and entryUUID.", kind, attr)
		}
		if kind == "user" {
			check.UsersFound = found
		} else {
			check.GroupsFound = found
		}
	}
	return check, nil
}

func (c *Client) connect(ctx context.Context) (*ldap.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := ldap.DialURL(c.cfg.LDAPURL, ldap.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}))
	if err != nil {
		return nil, err
	}
	conn.SetTimeout(5 * time.Second)
	if err = conn.Bind(c.cfg.LDAPBindDN, c.cfg.LDAPBindPassword); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func lookup(conn *ldap.Conn, base, filter string, attrs ...string) ([]*ldap.Entry, error) {
	result, err := conn.Search(ldap.NewSearchRequest(base, ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 200, 5, false, filter, attrs, nil))
	if err != nil {
		return nil, err
	}
	return result.Entries, nil
}

func (c *Client) Authenticate(ctx context.Context, username, password string) (Identity, error) {
	if strings.TrimSpace(username) == "" || password == "" || len(username) > 256 || len(password) > 4096 {
		return Identity{}, ErrCredentials
	}
	conn, err := c.connect(ctx)
	if err != nil {
		return Identity{}, err
	}
	defer conn.Close()
	entries, err := lookup(conn, c.cfg.LDAPUsersDN, "(&(objectClass=inetOrgPerson)(uid="+ldap.EscapeFilter(username)+"))", "uid", "entryUUID")
	if err != nil {
		return Identity{}, err
	}
	if len(entries) != 1 {
		return Identity{}, ErrCredentials
	}
	// A separate connection verifies the password without changing the reader bind.
	user, err := ldap.DialURL(c.cfg.LDAPURL, ldap.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}))
	if err != nil {
		return Identity{}, err
	}
	defer user.Close()
	user.SetTimeout(5 * time.Second)
	if err = user.Bind(entries[0].DN, password); err != nil {
		if ldap.IsErrorWithCode(err, ldap.LDAPResultInvalidCredentials) || ldap.IsErrorWithCode(err, ldap.LDAPResultUnwillingToPerform) {
			return Identity{}, ErrCredentials
		}
		return Identity{}, err
	}
	return c.identity(conn, entries[0])
}

func (c *Client) identity(conn *ldap.Conn, user *ldap.Entry) (Identity, error) {
	id := user.GetAttributeValue("entryUUID")
	if id == "" {
		return Identity{}, errors.New("LDAP user has no entryUUID")
	}
	groups, err := lookup(conn, c.cfg.LDAPGroupsDN, "(&(objectClass=groupOfNames)(member="+ldap.EscapeFilter(user.DN)+"))", "entryUUID")
	if err != nil {
		return Identity{}, err
	}
	identity := Identity{Subject: authz.Subject{UserID: id}, Username: user.GetAttributeValue("uid")}
	for _, group := range groups {
		gid := group.GetAttributeValue("entryUUID")
		if gid == "" {
			return Identity{}, errors.New("LDAP group has no entryUUID")
		}
		identity.GroupIDs = append(identity.GroupIDs, gid)
	}
	return identity, nil
}

func (c *Client) Refresh(ctx context.Context, id string) (Identity, error) {
	conn, err := c.connect(ctx)
	if err != nil {
		return Identity{}, err
	}
	defer conn.Close()
	entries, err := lookup(conn, c.cfg.LDAPUsersDN, "(&(objectClass=inetOrgPerson)(entryUUID="+ldap.EscapeFilter(id)+"))", "uid", "entryUUID")
	if err != nil {
		return Identity{}, err
	}
	if len(entries) != 1 {
		return Identity{}, ErrCredentials
	}
	return c.identity(conn, entries[0])
}

func (c *Client) List(ctx context.Context, query string) ([]Entry, error) {
	conn, err := c.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	out := []Entry{}
	for _, kind := range []string{"user", "group"} {
		base, filter, attr := c.cfg.LDAPUsersDN, "inetOrgPerson", "uid"
		if kind == "group" {
			base, filter, attr = c.cfg.LDAPGroupsDN, "groupOfNames", "cn"
		}
		filter = directoryFilter(filter, attr, query)
		entries, err := lookup(conn, base, filter, attr, "entryUUID")
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			id := entry.GetAttributeValue("entryUUID")
			if id != "" {
				out = append(out, Entry{ID: id, Kind: kind, Name: entry.GetAttributeValue(attr), DN: entry.DN})
			}
		}
	}
	return out, nil
}

func directoryFilter(class, attribute, query string) string {
	value := "*"
	if query != "" {
		value = "*" + ldap.EscapeFilter(query) + "*"
	}
	return fmt.Sprintf("(&(objectClass=%s)(%s=%s))", class, attribute, value)
}
