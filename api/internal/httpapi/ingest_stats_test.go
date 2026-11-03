package httpapi

import (
	"context"
	"errors"
	"github.com/logister/logister/api/internal/db"
	"reflect"
	"testing"
)

type fakeIngestStats struct {
	ids   []string
	calls int
	err   error
}

func (f *fakeIngestStats) Stats(_ context.Context, _ string, ids []string) (db.IngestStats, error) {
	f.ids = ids
	f.calls++
	return db.IngestStats{Streams: []db.StreamIngestStats{}}, f.err
}
func TestIngestStatisticsAccess(t *testing.T) {
	s, _, d := authFixture(t)
	f := &fakeIngestStats{}
	s.metrics = f
	h := s.Handler()
	if w := authRequest(h, "GET", "/api/ingest/stats", "", nil); w.Code != 401 {
		t.Fatal(w.Code)
	}
	c := loginCookie(t, h, "operator", "ldap", "correct")
	if w := authRequest(h, "GET", "/api/ingest/stats", "", c); w.Code != 200 || !reflect.DeepEqual(f.ids, []string{"pay-id"}) {
		t.Fatal(w.Code, f.ids, w.Body.String())
	}
	d.groups = nil
	calls := f.calls
	if w := authRequest(h, "GET", "/api/ingest/stats", "", c); w.Code != 200 || f.calls != calls {
		t.Fatal(w.Code, f.calls)
	}
	admin := loginCookie(t, h, "local-admin", "local", "local-test-password")
	if w := authRequest(h, "GET", "/api/ingest/stats", "", admin); w.Code != 200 || f.ids != nil {
		t.Fatal(w.Code, f.ids)
	}
	f.err = errors.New("private database details")
	if w := authRequest(h, "GET", "/api/ingest/stats", "", admin); w.Code != 503 || w.Body.String() == f.err.Error() {
		t.Fatal(w.Code, w.Body.String())
	}
}
