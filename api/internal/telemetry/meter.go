// Package telemetry contains bounded, payload-free process metrics.
package telemetry

import (
	"sort"
	"sync"
	"time"
)

const Window = 128

type Timing struct {
	Calls   uint64  `json:"calls"`
	Errors  uint64  `json:"errors"`
	LastMS  float64 `json:"last_ms"`
	P95MS   float64 `json:"p95_ms"`
	samples [Window]float64
	count   int
}
type State struct {
	Accepted      uint64            `json:"accepted"`
	InputBytes    uint64            `json:"input_bytes"`
	Published     uint64            `json:"published"`
	Duplicates    uint64            `json:"duplicates"`
	BulkBytes     uint64            `json:"bulk_bytes"`
	BulkRequests  uint64            `json:"bulk_requests"`
	BulkErrors    uint64            `json:"bulk_errors"`
	BulkRejected  uint64            `json:"bulk_rejected"`
	BufferWindows int64             `json:"buffer_windows"`
	BufferBytes   int64             `json:"buffer_bytes"`
	Timings       map[string]Timing `json:"timings"`
}
type Meter struct {
	mu    sync.Mutex
	state State
}

func New() *Meter { return &Meter{state: State{Timings: map[string]Timing{}}} }
func (m *Meter) Accepted(documents, bytes int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.Accepted += uint64(documents)
	m.state.InputBytes += uint64(bytes)
}
func (m *Meter) Published(documents int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.Published += uint64(documents)
}
func (m *Meter) Duplicates(n int) { m.mu.Lock(); defer m.mu.Unlock(); m.state.Duplicates += uint64(n) }
func (m *Meter) Buffer(windows, bytes int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.BufferWindows += windows
	m.state.BufferBytes += bytes
}
func (m *Meter) Observe(stage string, duration time.Duration, failed bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.observe(stage, duration, failed)
}
func (m *Meter) observe(stage string, duration time.Duration, failed bool) {
	// Stage names are supplied by code, never by incoming records.
	t := m.state.Timings[stage]
	t.Calls++
	if failed {
		t.Errors++
	}
	t.LastMS = float64(duration) / float64(time.Millisecond)
	t.samples[(t.Calls-1)%Window] = t.LastMS
	if t.count < Window {
		t.count++
	}
	m.state.Timings[stage] = t
}
func (m *Meter) Bulk(bytes int, duration time.Duration, failed, rejected bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.BulkRequests++
	m.state.BulkBytes += uint64(bytes)
	if failed {
		m.state.BulkErrors++
	}
	if rejected {
		m.state.BulkRejected++
	}
	m.observe("bulk", duration, failed)
}
func (m *Meter) Snapshot() State {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.state
	s.Timings = make(map[string]Timing, len(m.state.Timings))
	for key, t := range m.state.Timings {
		values := append([]float64(nil), t.samples[:t.count]...)
		sort.Float64s(values)
		if len(values) > 0 {
			t.P95MS = values[(95*len(values)+99)/100-1]
		}
		s.Timings[key] = t
	}
	return s
}
