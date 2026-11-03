package directory

import "testing"

func TestDirectoryFilters(t *testing.T) {
	if got := directoryFilter("groupOfNames", "cn", ""); got != "(&(objectClass=groupOfNames)(cn=*))" {
		t.Fatal(got)
	}
	if got := directoryFilter("groupOfNames", "cn", "*)(uid=*)"); got != `(&(objectClass=groupOfNames)(cn=*\2a\29\28uid=\2a\29*))` {
		t.Fatal(got)
	}
}
