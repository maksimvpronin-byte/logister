package search

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/logister/logister/api/internal/telemetry"
)

func numberAt(v any, path ...string) (float64, bool) {
	for _, key := range path {
		m, ok := v.(map[string]any)
		if !ok {
			return 0, false
		}
		v = m[key]
	}
	n, ok := v.(float64)
	return n, ok
}
func (c *Client) Monitoring(ctx context.Context) (telemetry.Component, error) {
	status, raw, err := c.jsonRequest(ctx, "GET", "/_cluster/health", nil)
	if err != nil || status != 200 {
		return telemetry.Component{}, fmt.Errorf("cluster status unavailable")
	}
	var health map[string]any
	if err = json.Unmarshal(raw, &health); err != nil {
		return telemetry.Component{}, err
	}
	state, _ := health["status"].(string)
	metrics := map[string]float64{}
	for key, path := range map[string]string{"nodes": "number_of_nodes", "shards": "active_shards", "primary_shards": "active_primary_shards", "unassigned_shards": "unassigned_shards"} {
		if n, ok := numberAt(health, path); ok {
			metrics[key] = n
		}
	}
	path := "/_nodes/stats/indices,jvm,process,fs,os,thread_pool?filter_path=nodes.*.process.cpu.total_in_millis,nodes.*.jvm.mem,nodes.*.jvm.gc.collectors,nodes.*.fs.total,nodes.*.os,nodes.*.indices.indexing,nodes.*.indices.refresh,nodes.*.thread_pool.write"
	status, raw, err = c.jsonRequest(ctx, "GET", path, nil)
	if err != nil || status != 200 {
		return telemetry.Component{}, fmt.Errorf("node statistics unavailable")
	}
	var nodes struct {
		Nodes map[string]any `json:"nodes"`
	}
	if err = json.Unmarshal(raw, &nodes); err != nil {
		return telemetry.Component{}, err
	}
	for _, node := range nodes.Nodes {
		for key, p := range map[string][]string{"cpu_seconds": {"process", "cpu", "total_in_millis"}, "heap_bytes": {"jvm", "mem", "heap_used_in_bytes"}, "heap_limit_bytes": {"jvm", "mem", "heap_max_in_bytes"}, "disk_free_bytes": {"fs", "total", "available_in_bytes"}, "disk_total_bytes": {"fs", "total", "total_in_bytes"}, "write_queue": {"thread_pool", "write", "queue"}, "write_rejected": {"thread_pool", "write", "rejected"}, "indexed": {"indices", "indexing", "index_total"}, "index_failed": {"indices", "indexing", "index_failed"}, "young_gc_ms": {"jvm", "gc", "collectors", "young", "collection_time_in_millis"}, "old_gc_ms": {"jvm", "gc", "collectors", "old", "collection_time_in_millis"}} {
			if n, ok := numberAt(node, p...); ok {
				metrics[key] += n
			}
		}
	}
	if n, ok := metrics["cpu_seconds"]; ok {
		metrics["cpu_seconds"] = n / 1000
	}
	// Host metrics refer to the first OpenSearch node, not the user's workstation.
	for _, node := range nodes.Nodes {
		for key, path := range map[string][]string{"host_cpu_percent": {"os", "cpu", "percent"}, "host_memory_total_bytes": {"os", "mem", "total_in_bytes"}, "host_memory_free_bytes": {"os", "mem", "free_in_bytes"}} {
			if n, ok := numberAt(node, path...); ok {
				metrics[key] = n
			}
		}
		break
	}
	if len(nodes.Nodes) == 0 {
		return telemetry.Component{}, fmt.Errorf("no node statistics")
	}
	statusLabel := map[string]string{"green": "ok", "yellow": "warning", "red": "down"}[state]
	if statusLabel == "" {
		statusLabel = "warning"
	}
	return telemetry.Component{Status: statusLabel, Metrics: metrics, Note: state}, nil
}
