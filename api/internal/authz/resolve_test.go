package authz

import (
	"errors"
	"slices"
	"testing"
)

const (
	payments = "11111111-1111-1111-1111-111111111111"
	billing  = "22222222-2222-2222-2222-222222222222"
)

func grant(kind PrincipalKind, id, role string, scope Scope, stream string) Grant {
	return Grant{PrincipalKind: kind, PrincipalID: id, Role: role, Scope: scope, StreamID: stream}
}

func TestBuiltinRoles(t *testing.T) {
	if got := BuiltinRoles(); len(got) != 3 {
		t.Fatalf("roles = %d", len(got))
	}
	operator, _ := RoleByCode(RoleOperator)
	service, _ := RoleByCode(RoleService)
	admin, _ := RoleByCode(RoleAdministrator)

	if !operator.Has(MessagesSearch) || !operator.Has(StreamsRead) || len(operator.Permissions) != 2 {
		t.Fatal("operator must only read messages and see the stream")
	}
	for _, permission := range []Permission{StreamsEdit, StreamsRotate, StreamsRetention, IngestWrite, StreamsCreate, InputsRead, SystemGrants} {
		if operator.Has(permission) || service.Has(permission) && permission != IngestWrite {
			t.Fatalf("narrow role holds %s", permission)
		}
	}
	if !service.Has(IngestWrite) || len(service.Permissions) != 1 || service.Has(MessagesSearch) {
		t.Fatal("service account must only write through the ingest API")
	}
	for _, permission := range AllPermissions() {
		if !admin.Has(permission) {
			t.Fatalf("administrator is missing %s", permission)
		}
	}
	if admin.Allows(ScopeStream) || !operator.Allows(ScopeStream) || !service.Allows(ScopeSystem) {
		t.Fatal("administrator is installation-wide; operator and service allow a stream or the installation")
	}
	for _, code := range []string{"reader", "editor", "ingest", "owner"} {
		if _, ok := RoleByCode(code); ok {
			t.Fatalf("%s is still a built-in role", code)
		}
	}
	copied := BuiltinRoles()
	copied[0].Permissions[0] = IngestWrite
	fresh, _ := RoleByCode(RoleOperator)
	if fresh.Has(IngestWrite) {
		t.Fatal("role catalog was mutated through a returned slice")
	}
}

func TestOperatorReadsOnlyTheGrantedStream(t *testing.T) {
	subject := Subject{UserID: "anna", GroupIDs: []string{"S-1-5-21-100-1101"}}
	grants := []Grant{
		grant(PrincipalGroup, "s-1-5-21-100-1101", RoleOperator, ScopeStream, payments),
		grant(PrincipalGroup, "S-1-5-21-100-9999", RoleOperator, ScopeStream, billing),
	}
	access, err := Resolve(subject, grants)
	if err != nil {
		t.Fatal(err)
	}
	if !access.Allowed(MessagesSearch, payments) || !access.Allowed(StreamsRead, payments) {
		t.Fatal("payments operator cannot read payments")
	}
	if access.Allowed(MessagesSearch, billing) || access.Allowed(StreamsEdit, payments) || access.AllowIngest(payments) {
		t.Fatal("payments operator gained another stream, settings, or ingest")
	}
	every, ids := access.Streams(MessagesSearch)
	if every || !slices.Equal(ids, []string{payments}) {
		t.Fatalf("search filter = every:%v ids:%v", every, ids)
	}
}

func TestOperatorAndServiceUnionOnOneStream(t *testing.T) {
	subject := Subject{GroupIDs: []string{"S-1-5-21-100-1101", "S-1-5-21-100-3001"}}
	grants := []Grant{
		grant(PrincipalGroup, "S-1-5-21-100-1101", RoleOperator, ScopeStream, payments),
		grant(PrincipalGroup, "S-1-5-21-100-3001", RoleService, ScopeStream, payments),
		grant(PrincipalGroup, "S-1-5-21-100-1101", RoleOperator, ScopeStream, billing),
	}
	access, err := Resolve(subject, grants)
	if err != nil {
		t.Fatal(err)
	}
	if !access.Allowed(MessagesSearch, payments) || !access.AllowIngest(payments) || !access.Allowed(MessagesSearch, billing) {
		t.Fatal("group grants did not union")
	}
	if access.AllowIngest(billing) || access.Allowed(StreamsRetention, payments) {
		t.Fatal("union leaked ingest or retention onto the wrong stream")
	}
}

func TestSystemOperatorCoversEveryStream(t *testing.T) {
	access, err := Resolve(Subject{GroupIDs: []string{"S-1-5-21-100-2000"}}, []Grant{
		grant(PrincipalGroup, "S-1-5-21-100-2000", RoleOperator, ScopeSystem, ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !access.Allowed(MessagesSearch, "stream-created-later") || !access.Allowed(StreamsRead, payments) {
		t.Fatal("system operator does not cover every stream")
	}
	if access.Allowed(StreamsEdit, payments) || access.AllowIngest("") || access.Allowed(SystemGrants, "") {
		t.Fatal("system operator can change the installation or write logs")
	}
	every, ids := access.Streams(MessagesSearch)
	if !every || ids != nil {
		t.Fatalf("system coverage = every:%v ids:%v", every, ids)
	}
}

func TestServiceRoutingRequiresSystemGrant(t *testing.T) {
	streamWriter, err := Resolve(Subject{GroupIDs: []string{"S-1-5-21-100-3001"}}, []Grant{
		grant(PrincipalGroup, "S-1-5-21-100-3001", RoleService, ScopeStream, payments),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !streamWriter.AllowIngest(payments) || streamWriter.AllowIngest(billing) || streamWriter.AllowIngest("") {
		t.Fatal("stream service account must require an explicit matching destination")
	}
	if streamWriter.Allowed(MessagesSearch, payments) {
		t.Fatal("service account can read messages")
	}

	router, err := Resolve(Subject{UserID: "fluent-bit"}, []Grant{
		grant(PrincipalUser, "fluent-bit", RoleService, ScopeSystem, ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !router.AllowIngest("") || !router.AllowIngest(billing) || router.Allowed(MessagesSearch, billing) {
		t.Fatal("system service account must write through routing without reading messages")
	}
}

func TestAdministratorIsInstallationWide(t *testing.T) {
	access, err := Resolve(Subject{UserID: "local-admin", GroupIDs: []string{"S-1-5-21-100-9000"}}, []Grant{
		grant(PrincipalGroup, "S-1-5-21-100-9000", RoleAdministrator, ScopeSystem, ""),
		grant(PrincipalUser, "someone-else", RoleAdministrator, ScopeSystem, ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !access.Allowed(StreamsRetention, payments) || !access.Allowed(StreamsDelete, "") || !access.Allowed(SystemGrants, "") || !access.AllowIngest("") {
		t.Fatal("administrator is missing retention, delete, grants or ingest")
	}
	if err := ValidateGrant(grant(PrincipalGroup, "S-1-5-21-100-9000", RoleAdministrator, ScopeStream, payments)); err == nil {
		t.Fatal("accepted a stream-scoped administrator")
	}
}

func TestInvalidMatchingGrantFailsClosed(t *testing.T) {
	_, err := Resolve(Subject{GroupIDs: []string{"S-1-5-21-100-1101"}}, []Grant{
		grant(PrincipalGroup, "S-1-5-21-100-1101", "reader", ScopeStream, payments),
	})
	if !errors.Is(err, ErrUnknownRole) {
		t.Fatalf("err = %v", err)
	}
	access, err := Resolve(Subject{GroupIDs: []string{"S-1-5-21-100-1101"}}, []Grant{
		grant(PrincipalGroup, "S-1-5-21-100-9999", "reader", ScopeStream, payments),
	})
	if err != nil || access.Allowed(MessagesSearch, payments) {
		t.Fatalf("unrelated bad grant affected the subject: access=%v err=%v", access, err)
	}
}

func TestNoGrantsSearchNothing(t *testing.T) {
	access, err := Resolve(Subject{UserID: "anna", GroupIDs: nil}, nil)
	if err != nil {
		t.Fatal(err)
	}
	every, ids := access.Streams(MessagesSearch)
	if every || len(ids) != 0 || access.AllowIngest("") || access.Allowed(MessagesSearch, payments) {
		t.Fatal("empty access must not become an unfiltered search or ingest")
	}
}

func TestGrantShape(t *testing.T) {
	valid := []Grant{
		grant(PrincipalGroup, "S-1-5-21-100-1101", RoleOperator, ScopeStream, payments),
		grant(PrincipalUser, "fluent-bit", RoleService, ScopeSystem, ""),
		grant(PrincipalUser, "local-admin", RoleAdministrator, ScopeSystem, ""),
	}
	for _, grant := range valid {
		if err := ValidateGrant(grant); err != nil {
			t.Fatal(err)
		}
	}
	invalid := []Grant{
		grant(PrincipalKind("team"), "S-1-5-21-100-1101", RoleOperator, ScopeStream, payments),
		grant(PrincipalGroup, "Payments Operators", RoleOperator, ScopeStream, payments),
		grant(PrincipalGroup, "S-1-5-21-100-1101", RoleOperator, ScopeStream, ""),
		grant(PrincipalGroup, "S-1-5-21-100-1101", RoleService, ScopeSystem, payments),
		grant(PrincipalGroup, "S-1-5-21-100-1101", RoleOperator, Scope("folder"), ""),
	}
	for _, grant := range invalid {
		if err := ValidateGrant(grant); !errors.Is(err, ErrInvalidGrant) {
			t.Fatalf("grant %+v err = %v", grant, err)
		}
	}
}

func TestLastSystemAdministrator(t *testing.T) {
	grants := []Grant{
		grant(PrincipalUser, "local-admin", RoleAdministrator, ScopeSystem, ""),
		grant(PrincipalGroup, "S-1-5-21-100-9000", RoleAdministrator, ScopeSystem, ""),
		grant(PrincipalUser, "local-admin", RoleAdministrator, ScopeSystem, ""),
		grant(PrincipalGroup, "S-1-5-21-100-1101", RoleOperator, ScopeSystem, ""),
	}
	if SystemAdministrators(grants) != 2 {
		t.Fatalf("count = %d", SystemAdministrators(grants))
	}
	if SystemAdministrators(grants[:1]) != 1 || SystemAdministrators(nil) != 0 {
		t.Fatal("administrator count did not track remaining installation grants")
	}
}
