package authz

import (
	"sort"
	"strings"
)

// Access is the union of every grant that matches a subject.
// There are no deny grants. Removing access means removing the grant or
// removing the user from the directory group.
type Access struct {
	system   map[Permission]struct{}
	byStream map[string]map[Permission]struct{}
}

// Allowed reports whether permission is held.
// For a stream-scoped permission, a system grant matches any streamID,
// including a stream that did not exist when the grant was written.
// Pass an empty streamID to test an installation permission such as
// streams:create. System grants satisfy that test; stream grants do not.
func (a Access) Allowed(permission Permission, streamID string) bool {
	if _, ok := a.system[permission]; ok {
		return true
	}
	if streamID == "" {
		return false
	}
	_, ok := a.byStream[streamID][permission]
	return ok
}

// Streams lists where a stream-scoped permission is held.
// every is true when a system grant covers all streams, including future
// ones; ids is then nil. An empty ids with every false means no access.
// Callers must not treat that as "no filter": a search without a stream
// filter would return messages from streams the subject cannot read.
func (a Access) Streams(permission Permission) (every bool, ids []string) {
	if _, ok := a.system[permission]; ok {
		return true, nil
	}
	for id, held := range a.byStream {
		if _, ok := held[permission]; ok {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return false, ids
}

// AllowIngest reports whether a write may be accepted.
// An empty explicitStreamID means the routing rules choose the destination.
// Only a system grant of ingest:write may use routing, because the fallback
// stream would otherwise accept messages into a stream the caller cannot name.
// A stream grant may write only when the request names that stream.
func (a Access) AllowIngest(explicitStreamID string) bool {
	if _, ok := a.system[IngestWrite]; ok {
		return true
	}
	if explicitStreamID == "" {
		return false
	}
	_, ok := a.byStream[explicitStreamID][IngestWrite]
	return ok
}

// Resolve unions the grants that match subject.
// Grants for other principals are ignored. A matching grant that is no
// longer valid fails the whole resolution, so a bad row cannot grant a
// subset and look like success.
func Resolve(subject Subject, grants []Grant) (Access, error) {
	access := Access{
		system:   map[Permission]struct{}{},
		byStream: map[string]map[Permission]struct{}{},
	}
	for _, grant := range grants {
		if !grant.matches(subject) {
			continue
		}
		if err := ValidateGrant(grant); err != nil {
			return Access{}, err
		}
		role, _ := RoleByCode(grant.Role)
		if grant.Scope == ScopeSystem {
			for _, permission := range role.Permissions {
				access.system[permission] = struct{}{}
			}
			continue
		}
		held := access.byStream[grant.StreamID]
		if held == nil {
			held = map[Permission]struct{}{}
			access.byStream[grant.StreamID] = held
		}
		for _, permission := range role.Permissions {
			if permission.StreamScoped() {
				held[permission] = struct{}{}
			}
		}
	}
	return access, nil
}

func (g Grant) matches(subject Subject) bool {
	switch g.PrincipalKind {
	case PrincipalUser:
		return sameID(g.PrincipalID, subject.UserID)
	case PrincipalGroup:
		for _, id := range subject.GroupIDs {
			if sameID(g.PrincipalID, id) {
				return true
			}
		}
	}
	return false
}

func sameID(left, right string) bool {
	return left != "" && strings.EqualFold(left, right)
}

// SystemAdministrators counts distinct principals that hold administrator
// on the whole installation. The store must refuse a change that would
// leave this count at zero, including deletion of the local break-glass user
// while no directory group is mapped to the same role.
func SystemAdministrators(grants []Grant) int {
	seen := map[string]struct{}{}
	for _, grant := range grants {
		if grant.Role != RoleAdministrator || grant.Scope != ScopeSystem || grant.PrincipalID == "" {
			continue
		}
		seen[string(grant.PrincipalKind)+"\x00"+strings.ToLower(grant.PrincipalID)] = struct{}{}
	}
	return len(seen)
}
