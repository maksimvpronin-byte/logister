package httpapi

import (
	"context"
	"maps"
	"net/http"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/logister/logister/api/internal/authz"
	"github.com/logister/logister/api/internal/db"
	"github.com/logister/logister/api/internal/telemetry"
)

const monitoringInterval = time.Second
const monitoringHistory = 900
const monitoringClusterInterval = 5 * time.Second
const monitoringNodeTimeout = 20 * time.Second

type monitoringRates struct {
	Accepted  *float64 `json:"accepted"`
	Published *float64 `json:"published"`
	InputMiB  *float64 `json:"input_mib"`
	BulkMiB   *float64 `json:"bulk_mib"`
}
type monitoringSample struct {
	At         time.Time                      `json:"at"`
	DurationMS float64                        `json:"duration_ms"`
	Process    telemetry.State                `json:"process"`
	Rates      monitoringRates                `json:"rates"`
	Components map[string]telemetry.Component `json:"components"`
	Warnings   []string                       `json:"warnings"`
}
type monitoringPoint struct {
	At        time.Time `json:"at"`
	Accepted  *float64  `json:"accepted"`
	Published *float64  `json:"published"`
	InputMiB  *float64  `json:"input_mib"`
	Pending   *float64  `json:"pending"`
	KafkaLag  *float64  `json:"kafka_lag"`
}
type monitoringView struct {
	Instance        string               `json:"instance"`
	StartedAt       time.Time            `json:"started_at"`
	IntervalSeconds int                  `json:"interval_seconds"`
	HistorySeconds  int                  `json:"history_seconds"`
	Config          map[string]any       `json:"config"`
	Latest          *monitoringSample    `json:"latest"`
	History         []monitoringPoint    `json:"history"`
	Nodes           []monitoringNodeView `json:"nodes"`
	ClusterStale    bool                 `json:"cluster_stale"`
}
type monitoringNodeView struct {
	db.MonitoringNode
	Online bool `json:"online"`
}

type queueGrowth struct {
	started  time.Time
	baseline float64
	active   bool
}

// Evaluate net growth over ten seconds instead of reacting to individual batches.
func (g *queueGrowth) update(at time.Time, value *float64) bool {
	if value == nil || *value <= 0 {
		*g = queueGrowth{}
		return false
	}
	if g.started.IsZero() {
		g.started, g.baseline = at, *value
		return false
	}
	if at.Sub(g.started) >= 10*time.Second {
		threshold := g.baseline * .1
		if threshold < 100 {
			threshold = 100
		}
		g.active = *value-g.baseline >= threshold
		g.started, g.baseline = at, *value
	}
	return g.active
}

type monitoringStore struct {
	kafkaGrowth  queueGrowth
	redisGrowth  queueGrowth
	mu           sync.RWMutex
	instance     string
	started      time.Time
	latest       *monitoringSample
	history      []monitoringPoint
	config       map[string]any
	nodes        []db.MonitoringNode
	clusterStale bool
}

func (s *Server) StartMonitoring(ctx context.Context) {
	if s.telemetry == nil {
		s.telemetry = telemetry.New()
	}
	host := strings.TrimSpace(s.cfg.MonitoringNodeID)
	if host == "" {
		host, _ = os.Hostname()
	}
	shareMonitoring := s.database != nil
	s.monitor = &monitoringStore{instance: host, started: time.Now().UTC(), history: []monitoringPoint{}, clusterStale: shareMonitoring, config: map[string]any{
		"ingest_workers": s.cfg.IngestWorkers, "bulk_workers": s.cfg.BulkWorkers, "bulk_bytes": s.cfg.BulkBytes, "output_buffer_bytes": s.cfg.OutputBufferBytes, "publish_batch_bytes": s.cfg.PublishBatchBytes, "queue_max_bytes": s.cfg.QueueMaxBytes, "kafka_batch_records": s.cfg.KafkaBatchRecords, "kafka_batch_bytes": s.cfg.KafkaBatchBytes, "index_shards": s.cfg.IndexShards, "codec": s.cfg.IndexCodec, "codec_level": s.cfg.IndexCodecLevel,
	}}
	if s.cfg.KafkaOnly() {
		s.monitor.config["ingest_mode"] = "kafka-only"
		delete(s.monitor.config, "output_buffer_bytes")
		delete(s.monitor.config, "publish_batch_bytes")
		delete(s.monitor.config, "queue_max_bytes")
	}
	go func() {
		ticker := time.NewTicker(monitoringInterval)
		defer ticker.Stop()
		for {
			if ctx.Err() != nil {
				return
			}
			s.collectMonitoring(ctx)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	if shareMonitoring {
		go s.refreshMonitoringCluster(ctx)
	}
}

// Cluster exchange runs independently of local sampling and HTTP requests.
func (s *Server) refreshMonitoringCluster(parent context.Context) {
	m := s.monitor
	ticker := time.NewTicker(monitoringClusterInterval)
	defer ticker.Stop()
	for {
		m.mu.RLock()
		local := m.localMonitoringNode()
		m.mu.RUnlock()
		ctx, cancel := context.WithTimeout(parent, 2*time.Second)
		var nodes []db.MonitoringNode
		var err error
		if !local.SeenAt.IsZero() {
			err = s.database.SaveMonitoringNode(ctx, local)
		}
		if err == nil {
			nodes, err = s.database.MonitoringNodes(ctx, time.Now().Add(-monitoringNodeTimeout))
		}
		cancel()
		m.mu.Lock()
		if err == nil {
			m.nodes = nodes
		}
		m.clusterStale = err != nil
		m.mu.Unlock()
		select {
		case <-parent.Done():
			return
		case <-ticker.C:
		}
	}
}

// The caller holds the monitoring store lock.
func (m *monitoringStore) localMonitoringNode() db.MonitoringNode {
	node := db.MonitoringNode{ID: m.instance, Components: map[string]string{}}
	if m.latest != nil {
		node.SeenAt = m.latest.At
		for name, component := range m.latest.Components {
			node.Components[name] = component.Status
		}
	}
	return node
}

func (m *monitoringStore) nodeViews(now time.Time) []monitoringNodeView {
	local := m.localMonitoringNode()
	nodes := make([]monitoringNodeView, 0, len(m.nodes)+1)
	appendNode := func(node db.MonitoringNode) {
		// A peer on an older release can still publish this key during an upgrade.
		if _, found := node.Components["fluentbit"]; found {
			node.Components = maps.Clone(node.Components)
			delete(node.Components, "fluentbit")
		}
		nodes = append(nodes, monitoringNodeView{MonitoringNode: node, Online: now.Sub(node.SeenAt) <= monitoringNodeTimeout})
	}
	for _, node := range m.nodes {
		if node.ID != local.ID && now.Sub(node.SeenAt) <= monitoringNodeTimeout {
			appendNode(node)
		}
	}
	appendNode(local)
	slices.SortFunc(nodes, func(a, b monitoringNodeView) int { return strings.Compare(a.ID, b.ID) })
	return nodes
}

func (s *Server) handleMonitoring(w http.ResponseWriter, r *http.Request) {
	identity, ok := r.Context().Value(authContextKey{}).(requestIdentity)
	if !s.cfg.AuthEnabled || !ok || !identity.access.Allowed(authz.SystemGrants, "") {
		writeError(w, 403, "Monitoring is available to administrators only.")
		return
	}
	if s.monitor == nil {
		writeError(w, 503, "Monitoring is starting.")
		return
	}
	s.monitor.mu.RLock()
	view := monitoringView{Instance: s.monitor.instance, StartedAt: s.monitor.started, IntervalSeconds: int(monitoringInterval.Seconds()), HistorySeconds: int((monitoringInterval * monitoringHistory).Seconds()), Config: s.monitor.config, Latest: s.monitor.latest, History: append([]monitoringPoint{}, s.monitor.history...), Nodes: s.monitor.nodeViews(time.Now()), ClusterStale: s.monitor.clusterStale}
	s.monitor.mu.RUnlock()
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, 200, view)
}
func downComponent() telemetry.Component {
	return telemetry.Component{Status: "down", Metrics: map[string]float64{}, Note: "Не удалось получить показатели"}
}
func metric(c telemetry.Component, key string) *float64 {
	if c.Status == "down" || c.Status == "disabled" {
		return nil
	}
	v, ok := c.Metrics[key]
	if !ok {
		return nil
	}
	return &v
}
func rate(previous, current uint64, seconds float64) *float64 {
	if seconds <= 0 || current < previous {
		return nil
	}
	v := float64(current-previous) / seconds
	return &v
}
func (s *Server) collectMonitoring(parent context.Context) {
	started := time.Now()
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	sample := monitoringSample{At: started.UTC(), Process: s.telemetry.Snapshot(), Components: map[string]telemetry.Component{}, Warnings: []string{}}
	type collected struct {
		key string
		c   telemetry.Component
	}
	readers := map[string]func(context.Context) (telemetry.Component, error){
		"api": func(ctx context.Context) (telemetry.Component, error) { return apiResources(), nil },
		"redis": func(ctx context.Context) (telemetry.Component, error) {
			if reader, ok := s.intake.(interface {
				Monitoring(context.Context, string) (telemetry.Component, error)
			}); ok {
				return reader.Monitoring(ctx, s.cfg.OpenSearchIndex)
			}
			return telemetry.Component{Status: "disabled", Metrics: map[string]float64{}, Note: "Redis-журнал не настроен"}, nil
		},
		"opensearch": func(ctx context.Context) (telemetry.Component, error) {
			if s.search == nil {
				return downComponent(), nil
			}
			return s.search.Monitoring(ctx)
		},
		"mongo": s.mongoMonitoring,
		"kafka": func(ctx context.Context) (telemetry.Component, error) {
			if s.kafka == nil {
				return telemetry.Component{Status: "disabled", Metrics: map[string]float64{}, Note: "Kafka не настроена"}, nil
			}
			status, err := s.kafka.Status(ctx)
			if err != nil {
				return telemetry.Component{}, err
			}
			state, note := "ok", "Чтение включено"
			if status.PauseRequested {
				state, note = "warning", "Чтение на паузе"
			} else if !status.Running {
				state, note = "warning", "Подключение"
			}
			if status.Error != "" || status.RetentionLoss {
				state, note = "down", "Ошибка чтения или потеря диапазона Kafka"
			}
			paused := 0.0
			if status.PauseRequested {
				paused = 1
			}
			var maxPartitionLag int64
			for _, partition := range status.Partitions {
				maxPartitionLag = max(maxPartitionLag, partition.Lag)
			}
			return telemetry.Component{Status: state, Note: note, Metrics: map[string]float64{"lag": float64(status.Lag), "max_partition_lag": float64(maxPartitionLag), "available": float64(status.Available), "committed": float64(status.Committed), "paused": paused}}, nil
		},
	}
	if s.cfg.KafkaOnly() {
		delete(readers, "redis")
	}
	results := make(chan collected, len(readers))
	for key, read := range readers {
		go func(key string, read func(context.Context) (telemetry.Component, error)) {
			c, err := read(ctx)
			if err != nil {
				c = downComponent()
			}
			results <- collected{key, c}
		}(key, read)
	}
	for range readers {
		r := <-results
		sample.Components[r.key] = r.c
	}
	m := s.monitor
	m.mu.RLock()
	previous := m.latest
	m.mu.RUnlock()
	if previous != nil {
		dt := sample.At.Sub(previous.At).Seconds()
		sample.Rates = monitoringRates{Accepted: rate(previous.Process.Accepted, sample.Process.Accepted, dt), Published: rate(previous.Process.Published, sample.Process.Published, dt), InputMiB: rate(previous.Process.InputBytes, sample.Process.InputBytes, dt), BulkMiB: rate(previous.Process.BulkBytes, sample.Process.BulkBytes, dt)}
		if sample.Rates.InputMiB != nil {
			*sample.Rates.InputMiB /= 1 << 20
		}
		if sample.Rates.BulkMiB != nil {
			*sample.Rates.BulkMiB /= 1 << 20
		}
		for key, c := range sample.Components {
			before := metric(previous.Components[key], "cpu_seconds")
			now := metric(c, "cpu_seconds")
			if before != nil && now != nil && *now >= *before && dt > 0 {
				c.Metrics["cpu_cores"] = (*now - *before) / dt
				sample.Components[key] = c
			}
		}
		if sample.Process.BulkErrors > previous.Process.BulkErrors {
			warning := "Есть ошибки bulk-записи; сообщения остаются в журнале для повтора"
			if s.cfg.KafkaOnly() {
				warning = "Есть ошибки bulk-записи. Kafka-сообщения будут обработаны повторно"
			}
			sample.Warnings = append(sample.Warnings, warning)
		}

	}
	lag := metric(sample.Components["kafka"], "lag")
	if sample.Components["kafka"].Metrics["paused"] != 0 {
		lag = nil
	}
	if m.kafkaGrowth.update(sample.At, lag) {
		sample.Warnings = append(sample.Warnings, "Очередь Kafka растёт за последние 10 с: входящий поток опережает приём Logister")
	}
	if m.redisGrowth.update(sample.At, metric(sample.Components["redis"], "pending")) {
		sample.Warnings = append(sample.Warnings, "Очередь Redis растёт за последние 10 с: проверьте скорость публикации и OpenSearch")
	}
	if bytes := metric(sample.Components["redis"], "pending_bytes"); bytes != nil && s.cfg.QueueMaxBytes > 0 && *bytes >= float64(s.cfg.QueueMaxBytes)*.9 {
		sample.Warnings = append(sample.Warnings, "Журнал заполнен более чем на 90%: приём может ожидать освобождения места")
	}
	osMetrics := sample.Components["opensearch"]
	if free, total := metric(osMetrics, "disk_free_bytes"), metric(osMetrics, "disk_total_bytes"); free != nil && total != nil && *total > 0 && *free / *total < .15 {
		sample.Warnings = append(sample.Warnings, "На дисках OpenSearch осталось меньше 15% места")
	}
	sample.DurationMS = time.Since(started).Seconds() * 1000
	point := monitoringPoint{At: sample.At, Accepted: sample.Rates.Accepted, Published: sample.Rates.Published, InputMiB: sample.Rates.InputMiB, Pending: metric(sample.Components["redis"], "pending"), KafkaLag: metric(sample.Components["kafka"], "lag")}
	m.mu.Lock()
	m.latest = &sample
	m.history = append(m.history, point)
	if len(m.history) > monitoringHistory {
		m.history = append([]monitoringPoint(nil), m.history[len(m.history)-monitoringHistory:]...)
	}
	m.mu.Unlock()
}
func apiResources() telemetry.Component {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	metrics := map[string]float64{"heap_bytes": float64(mem.HeapAlloc), "goroutines": float64(runtime.NumGoroutine())}
	if raw, e := os.ReadFile("/sys/fs/cgroup/memory.current"); e == nil {
		if n, e := strconv.ParseFloat(strings.TrimSpace(string(raw)), 64); e == nil {
			metrics["memory_bytes"] = n
		}
	}
	if raw, e := os.ReadFile("/sys/fs/cgroup/memory.max"); e == nil {
		if n, e := strconv.ParseFloat(strings.TrimSpace(string(raw)), 64); e == nil {
			metrics["memory_limit_bytes"] = n
		}
	}
	if raw, e := os.ReadFile("/sys/fs/cgroup/cpu.stat"); e == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			f := strings.Fields(line)
			if len(f) == 2 && f[0] == "usage_usec" {
				if n, e := strconv.ParseFloat(f[1], 64); e == nil {
					metrics["cpu_seconds"] = n / 1e6
				}
			}
		}
	}
	return telemetry.Component{Status: "ok", Metrics: metrics, Note: "Текущий экземпляр API"}
}
func (s *Server) mongoMonitoring(ctx context.Context) (telemetry.Component, error) {
	if s.database == nil {
		return telemetry.Component{Status: "disabled", Metrics: map[string]float64{}, Note: "MongoDB не настроена"}, nil
	}
	status, err := s.database.ReplicaStatus(ctx)
	if err != nil {
		return telemetry.Component{}, err
	}
	note := "Одиночный экземпляр конфигурации"
	if status.SetName != "" {
		note = "Набор реплик " + status.SetName
		if status.Primary != "" {
			note += ", primary " + status.Primary
		}
	}
	members := float64(status.Members)
	if status.SetName == "" {
		members = 1
	}
	return telemetry.Component{Status: "ok", Note: note, Metrics: map[string]float64{"database_bytes": status.DataBytes, "connections": status.Connections, "members": members}}, nil
}
