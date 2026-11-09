package httpapi

import (
	"context"
	"encoding/json"
	"github.com/logister/logister/api/internal/authz"
	"github.com/logister/logister/api/internal/config"
	"github.com/logister/logister/api/internal/db"
	"github.com/logister/logister/api/internal/telemetry"
	"strings"
	"testing"
	"time"
)

func TestMonitoringRequiresAdministratorIncludingDirectRequestsAndRevocation(t *testing.T) {
	s, store, _ := authFixture(t)
	h := s.Handler()
	s.monitor = &monitoringStore{instance: "test", started: time.Now(), config: map[string]any{"bulk_workers": 2}, history: []monitoringPoint{}}
	if w := authRequest(h, "GET", "/api/monitoring", "", nil); w.Code != 401 {
		t.Fatal(w.Code)
	}
	operator := loginCookie(t, h, "operator", "ldap", "correct")
	if w := authRequest(h, "GET", "/api/monitoring", "", operator); w.Code != 403 {
		t.Fatal(w.Code, w.Body.String())
	}
	store.catalog.Grants = append(store.catalog.Grants, authz.Grant{PrincipalKind: authz.PrincipalUser, PrincipalID: "uuid-service", Role: authz.RoleService, Scope: authz.ScopeSystem})
	service := loginCookie(t, h, "service", "ldap", "correct")
	if w := authRequest(h, "GET", "/api/monitoring", "", service); w.Code != 403 {
		t.Fatal(w.Code)
	}
	admin := loginCookie(t, h, "local-admin", "local", "local-test-password")
	if w := authRequest(h, "GET", "/api/monitoring", "", admin); w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal(w.Code, w.Body.String())
	}
	store.catalog.Grants = nil
	if w := authRequest(h, "GET", "/api/monitoring", "", admin); w.Code != 403 {
		t.Fatal("revoked session retained monitoring access", w.Code)
	}
	disabled := NewServer(config.Config{}, nil, nil)
	if w := authRequest(disabled.Handler(), "GET", "/api/monitoring", "", nil); w.Code != 403 {
		t.Fatal("unauthenticated setup exposed monitoring", w.Code)
	}
}
func TestMonitoringHasBoundedHistoryMissingMetricsAndNoSecrets(t *testing.T) {
	s := NewServer(config.Config{LDAPBindPassword: "secret-marker", AuthSettingsKey: "secret-marker", AuthLocalPassword: "secret-marker"}, nil, nil)
	s.telemetry = telemetry.New()
	s.monitor = &monitoringStore{started: time.Now(), config: map[string]any{}, history: []monitoringPoint{}}
	for i := 0; i < monitoringHistory+5; i++ {
		s.telemetry.Accepted(1, 500)
		s.collectMonitoring(context.Background())
	}
	if len(s.monitor.history) != monitoringHistory {
		t.Fatal(len(s.monitor.history))
	}
	last := s.monitor.latest
	if _, exists := last.Components["fluentbit"]; exists {
		t.Fatal("external shipper included in monitoring")
	}
	if last.Components["opensearch"].Status != "down" || len(last.Components["opensearch"].Metrics) != 0 {
		t.Fatal("missing component invented zero metrics", last.Components)
	}
	if last.Rates.Accepted == nil || *last.Rates.Accepted < 0 {
		t.Fatal(last.Rates)
	}
	if rate(10, 1, 5) != nil || rate(1, 10, 0) != nil {
		t.Fatal("invalid rates must be absent")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.StartMonitoring(ctx)
	raw, _ := json.Marshal(s.monitor.config)
	if strings.Contains(string(raw), "secret-marker") {
		t.Fatal("credentials leaked")
	}
}

func TestQueueGrowthIgnoresBurstsAndClearsAfterRecovery(t *testing.T) {
	start := time.Now()
	var g queueGrowth
	check := func(seconds int, value float64, want bool) {
		t.Helper()
		if got := g.update(start.Add(time.Duration(seconds)*time.Second), &value); got != want {
			t.Fatalf("at %ds backlog %v: got %v, want %v", seconds, value, got, want)
		}
	}
	check(0, 1000, false)
	check(1, 5000, false) // A single batch is not a sustained backlog.
	check(10, 1000, false)
	check(20, 3000, true)
	check(21, 2000, true) // Do not flicker on each sample.
	check(30, 1000, false)
	check(40, 1100, true)
	check(41, 0, false)
	check(42, 5000, false)
	if g.update(start.Add(43*time.Second), nil) {
		t.Fatal("unavailable/paused queue retained growth warning")
	}
	check(44, 5000, false)
	check(54, 5010, false) // Ignore insignificant fluctuations.
}

func TestMonitoringNodeViewsTrackActiveReplicasAndPreferLocalSnapshot(t *testing.T) {
	now := time.Now()
	m := monitoringStore{
		instance: "node-2",
		latest: &monitoringSample{At: now, Components: map[string]telemetry.Component{
			"redis": {Status: "warning"},
		}},
		nodes: []db.MonitoringNode{{ID: "old-container", SeenAt: now.Add(-time.Minute)}},
	}
	check := func(at time.Time, want ...string) {
		t.Helper()
		nodes := m.nodeViews(at)
		if len(nodes) != len(want) {
			t.Fatalf("got %+v, want %v", nodes, want)
		}
		for i, id := range want {
			if nodes[i].ID != id || !nodes[i].Online {
				t.Fatalf("got %+v, want %v", nodes, want)
			}
			if id == m.instance && nodes[i].Components["redis"] != "warning" {
				t.Fatal("remote copy replaced local status")
			}
		}
	}

	check(now, "node-2")
	m.nodes = append(m.nodes, db.MonitoringNode{ID: "node-1", SeenAt: now.Add(-time.Second), Components: map[string]string{"kafka": "ok", "fluentbit": "ok"}})
	check(now, "node-1", "node-2")
	peer := m.nodeViews(now)[0]
	if len(peer.Components) != 1 || peer.Components["kafka"] != "ok" || m.nodes[1].Components["fluentbit"] != "ok" {
		t.Fatal("unexpected component filtering on older peer", peer.Components)
	}
	m.nodes = append(m.nodes, db.MonitoringNode{ID: "node-3", SeenAt: now}, db.MonitoringNode{ID: "node-2", SeenAt: now})
	check(now, "node-1", "node-2", "node-3")
	at := now.Add(monitoringNodeTimeout)
	m.latest.At = at
	check(at, "node-2", "node-3")
	check(at.Add(time.Second), "node-2")
}
