package redisingest

import (
	"context"
	"github.com/logister/logister/api/internal/telemetry"
	"strconv"
	"strings"
	"time"
)

func (s *Store) Monitoring(ctx context.Context, index string) (telemetry.Component, error) {
	raw, err := s.redis.Info(ctx, "memory", "cpu", "persistence", "stats", "keyspace").Result()
	if err != nil {
		return telemetry.Component{}, err
	}
	values := map[string]string{}
	for _, line := range strings.Split(raw, "\n") {
		if k, v, ok := strings.Cut(strings.TrimSpace(line), ":"); ok {
			values[k] = v
		}
	}
	metrics := map[string]float64{}
	names := map[string]string{"used_memory": "memory_bytes", "used_memory_rss": "rss_bytes", "maxmemory": "memory_limit_bytes", "used_cpu_user": "cpu_seconds", "aof_enabled": "aof_enabled", "aof_delayed_fsync": "aof_delayed_fsync", "aof_current_size": "aof_bytes", "evicted_keys": "evicted_keys", "aof_rewrite_in_progress": "aof_rewrite", "total_error_replies": "error_replies"}
	for from, to := range names {
		if n, e := strconv.ParseFloat(values[from], 64); e == nil {
			metrics[to] = n
		}
	}
	if n, e := strconv.ParseFloat(values["used_cpu_sys"], 64); e == nil {
		metrics["cpu_seconds"] += n
	}
	for _, entry := range strings.Split(values["db0"], ",") {
		if key, value, ok := strings.Cut(entry, "="); ok && key == "keys" {
			if n, e := strconv.ParseFloat(value, 64); e == nil {
				metrics["keys"] = n
			}
		}
	}
	pending, err := s.Pending(ctx, index)
	if err != nil {
		return telemetry.Component{}, err
	}
	bytes, err := s.PendingBytes(ctx, index)
	if err != nil {
		return telemetry.Component{}, err
	}
	metrics["pending"] = float64(pending)
	metrics["pending_bytes"] = float64(bytes)
	if pending > 0 {
		head, e := s.redis.LRange(ctx, s.pendingKey(index), 0, 0).Result()
		if e != nil {
			return telemetry.Component{}, e
		}
		if len(head) > 0 {
			value, e := s.redis.HGet(ctx, s.eventKey(index, head[0]), "accepted_at_ns").Result()
			if e == nil {
				if n, e := strconv.ParseInt(value, 10, 64); e == nil {
					metrics["oldest_seconds"] = max(0, time.Since(time.Unix(0, n)).Seconds())
				}
			}
		}
	} else {
		metrics["oldest_seconds"] = 0
	}
	return telemetry.Component{Status: "ok", Metrics: metrics}, nil
}
