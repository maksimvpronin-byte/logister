package telemetry

import (
	"sync"
	"testing"
	"time"
)

func TestConcurrentCountersAndBoundedPercentile(t *testing.T) {
	m := New()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				m.Accepted(1, 500)
				m.Published(1)
				m.Bulk(600, time.Millisecond, false, false)
			}
		}()
	}
	wg.Wait()
	for i := 0; i < Window; i++ {
		m.Observe("journal", 2*time.Millisecond, false)
	}
	s := m.Snapshot()
	if s.Accepted != 800 || s.InputBytes != 400000 || s.Published != 800 || s.BulkRequests != 800 {
		t.Fatal(s)
	}
	if s.Timings["bulk"].P95MS != 1 || s.Timings["journal"].P95MS != 2 {
		t.Fatal(s.Timings)
	}
	s.Timings["journal"] = Timing{}
	if m.Snapshot().Timings["journal"].Calls != Window {
		t.Fatal("snapshot modifies counters")
	}
	for i := 0; i < Window; i++ {
		m.Observe("journal", time.Millisecond, false)
	}
	if m.Snapshot().Timings["journal"].P95MS != 1 {
		t.Fatal("old samples were not evicted")
	}
}
