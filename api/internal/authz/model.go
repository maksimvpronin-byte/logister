// Package authz defines Logister's role model.
// A directory group is not a permission. A grant binds a user or a
// directory group to a built-in role, either for the whole installation or
// for one stream.
package authz

import (
	"errors"
	"fmt"
	"regexp"
)

// Permission is a stable capability code. Directory groups never receive
// these codes directly; roles bundle them.
type Permission string

const (
	MessagesSearch   Permission = "messages:search"
	StreamsRead      Permission = "streams:read"
	StreamsEdit      Permission = "streams:edit"
	StreamsRotate    Permission = "streams:rotate"
	StreamsRetention Permission = "streams:retention"
	IngestWrite      Permission = "ingest:write"
	StreamsCreate    Permission = "streams:create"
	StreamsDelete    Permission = "streams:delete"
	InputsRead       Permission = "inputs:read"
	SystemGrants     Permission = "system:grants"
)

// AllPermissions is every capability a role may grant.
func AllPermissions() []Permission {
	return []Permission{
		MessagesSearch, StreamsRead, StreamsEdit, StreamsRotate, StreamsRetention, IngestWrite,
		StreamsCreate, StreamsDelete, InputsRead, SystemGrants,
	}
}

// StreamScoped reports whether the permission can apply to one stream.
// A system grant of a stream-scoped permission covers every stream,
// including streams created later. The other permissions exist only on a
// system grant.
func (p Permission) StreamScoped() bool {
	switch p {
	case MessagesSearch, StreamsRead, StreamsEdit, StreamsRotate, StreamsRetention, IngestWrite:
		return true
	default:
		return false
	}
}

type Scope string

const (
	ScopeSystem Scope = "system"
	ScopeStream Scope = "stream"
)

type PrincipalKind string

const (
	PrincipalUser  PrincipalKind = "user"
	PrincipalGroup PrincipalKind = "group"
)

const (
	RoleOperator      = "operator"
	RoleService       = "service"
	RoleAdministrator = "administrator"
)

// Role is a fixed bundle of permissions. Built-in roles are the only roles
// a directory mapping can name until custom roles exist.
type Role struct {
	Code        string
	Permissions []Permission
	Scopes      []Scope
}

func (r Role) Allows(scope Scope) bool {
	for _, candidate := range r.Scopes {
		if candidate == scope {
			return true
		}
	}
	return false
}

func (r Role) Has(permission Permission) bool {
	for _, candidate := range r.Permissions {
		if candidate == permission {
			return true
		}
	}
	return false
}

var builtinRoles = []Role{
	{Code: RoleOperator, Permissions: []Permission{MessagesSearch, StreamsRead}, Scopes: []Scope{ScopeSystem, ScopeStream}},
	{Code: RoleService, Permissions: []Permission{IngestWrite}, Scopes: []Scope{ScopeSystem, ScopeStream}},
	{Code: RoleAdministrator, Permissions: AllPermissions(), Scopes: []Scope{ScopeSystem}},
}

// BuiltinRoles returns the installation roles in display order.
// The result is a copy; changing it does not change later lookups.
func BuiltinRoles() []Role {
	out := make([]Role, len(builtinRoles))
	for i, role := range builtinRoles {
		out[i] = cloneRole(role)
	}
	return out
}

// RoleByCode returns a built-in role. The boolean is false for any other code.
func RoleByCode(code string) (Role, bool) {
	for _, role := range builtinRoles {
		if role.Code == code {
			return cloneRole(role), true
		}
	}
	return Role{}, false
}

func cloneRole(role Role) Role {
	role.Permissions = append([]Permission(nil), role.Permissions...)
	role.Scopes = append([]Scope(nil), role.Scopes...)
	return role
}

// Grant assigns a role to a local user or a directory group.
// PrincipalID is a local user id or a directory entryUUID.
// Store the stable ID, not the group name: renames in the directory must not
// drop access. StreamID is the stream primary key, and only for ScopeStream.
type Grant struct {
	PrincipalKind PrincipalKind `json:"principal_kind"`
	PrincipalID   string        `json:"principal_id"`
	Role          string        `json:"role"`
	Scope         Scope         `json:"scope"`
	StreamID      string        `json:"stream_id,omitempty"`
}

// Subject is the identity established at sign-in.
// GroupIDs are direct LDAP group entryUUIDs, refreshed on every request.
type Subject struct {
	UserID   string
	GroupIDs []string
}

var (
	ErrUnknownRole  = errors.New("unknown role")
	ErrInvalidGrant = errors.New("invalid grant")
)

var principalID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,256}$`)
var streamIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// ValidateGrant checks a mapping before it is stored.
// Administrator is installation-wide: a stream-scoped administrator would
// silently lose user management, stream creation and retention.
func ValidateGrant(grant Grant) error {
	if grant.PrincipalKind != PrincipalUser && grant.PrincipalKind != PrincipalGroup {
		return fmt.Errorf("%w: principal kind must be user or group", ErrInvalidGrant)
	}
	if !principalID.MatchString(grant.PrincipalID) {
		return fmt.Errorf("%w: principal id is empty or not a stable directory or local id", ErrInvalidGrant)
	}
	role, ok := RoleByCode(grant.Role)
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownRole, grant.Role)
	}
	if !role.Allows(grant.Scope) {
		return fmt.Errorf("%w: role %s cannot be granted on %s", ErrInvalidGrant, grant.Role, grant.Scope)
	}
	switch grant.Scope {
	case ScopeSystem:
		if grant.StreamID != "" {
			return fmt.Errorf("%w: system grant must not name a stream", ErrInvalidGrant)
		}
	case ScopeStream:
		if !streamIDPattern.MatchString(grant.StreamID) {
			return fmt.Errorf("%w: stream grant requires a stream id", ErrInvalidGrant)
		}
	default:
		return fmt.Errorf("%w: scope must be system or stream", ErrInvalidGrant)
	}
	return nil
}
