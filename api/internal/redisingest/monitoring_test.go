package redisingest

import (
	"context"
	"github.com/logister/logister/api/internal/db"
	"github.com/logister/logister/api/internal/telemetry"
	"testing"
)

func TestTelemetryDoesNotCountDuplicatesAndClearsDeliveryMetadata(t *testing.T) {
	ctx := context.Background()
	s, _ := testStore(t)
	m := telemetry.New()
	s.SetTelemetry(m)
	e := event("a", "payments", "private-payload")
	e.InputBytes = 500
	if _, err := s.Accept(ctx, "logs", "", "", []db.Event{e}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Accept(ctx, "logs", "", "", []db.Event{e}); err != nil {
		t.Fatal(err)
	}
	if s.redis.HGet(ctx, s.eventKey("logs", "a"), "accepted_at_ns").Val() == "" {
		t.Fatal("no system acceptance time")
	}
	if _, err := s.Publish(ctx, "logs", func([]db.Event) error { return nil }); err != nil {
		t.Fatal(err)
	}
	state := m.Snapshot()
	if state.Accepted != 1 || state.Published != 1 || state.Duplicates != 1 || state.InputBytes != 500 || state.Timings["delivery"].Calls != 1 {
		t.Fatal(state)
	}
	if n, _ := s.redis.HExists(ctx, s.eventKey("logs", "a"), "accepted_at_ns").Result(); n {
		t.Fatal("delivery metadata retained after ACK")
	}
}

func TestBufferTelemetryIsReleasedOnPublicationFailure(t *testing.T) {
	s, ctx, cancel := bufferedFixture(t)
	defer cancel()
	m := telemetry.New()
	s.SetTelemetry(m)
	calls := 0
	_, err := s.Publish(ctx, "logs", func([]db.Event) error {
		calls++
		if m.Snapshot().BufferWindows < 1 {
			t.Fatal("inflight window not visible")
		}
		if calls == 2 {
			return context.DeadlineExceeded
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected failure")
	}
	state := m.Snapshot()
	if state.BufferBytes != 0 || state.BufferWindows != 0 || state.Published != 1 || state.Timings["publish"].Errors != 1 {
		t.Fatal(state)
	}
}
