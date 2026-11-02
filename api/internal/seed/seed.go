package seed

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/logister/logister/api/internal/search"
)

func SampleLogs(ctx context.Context, c *search.Client, index string) error {
	count, err := c.Count(ctx, index)
	if err != nil {
		return err
	}
	if count > 0 {
		log.Printf("seed skipped: index %q already has %d docs", index, count)
		return nil
	}

	now := time.Now().UTC()
	samples := []map[string]any{
		{
			"@timestamp": now.Add(-2 * time.Minute).Format(time.RFC3339Nano),
			"message":    `192.168.1.10 - - [02/Nov/2026:18:01:12 +0000] "GET /api/health HTTP/1.1" 200 42`,
			"source":     "nginx",
			"level":      "info",
			"host":       "web-01",
			"stream":     "web-access",
			"fields":     map[string]any{"method": "GET", "status": 200, "path": "/api/health"},
		},
		{
			"@timestamp": now.Add(-5 * time.Minute).Format(time.RFC3339Nano),
			"message":    `POST /login failed for user alice from 10.0.0.55`,
			"source":     "auth-service",
			"level":      "warn",
			"host":       "app-02",
			"stream":     "all",
			"fields":     map[string]any{"user": "alice", "event": "login_failed"},
		},
		{
			"@timestamp": now.Add(-8 * time.Minute).Format(time.RFC3339Nano),
			"message":    `<34>1 2026-11-02T18:00:01Z bastion sshd 2211 - - Failed password for root from 203.0.113.8 port 52214 ssh2`,
			"source":     "syslog",
			"level":      "error",
			"host":       "bastion",
			"stream":     "system",
			"facility":   "auth",
			"fields":     map[string]any{"process": "sshd"},
		},
		{
			"@timestamp": now.Add(-12 * time.Minute).Format(time.RFC3339Nano),
			"message":    `payment processed order=ORD-1042 amount=49.90 currency=USD`,
			"source":     "billing",
			"level":      "info",
			"host":       "app-01",
			"stream":     "all",
			"fields":     map[string]any{"order": "ORD-1042", "amount": 49.90},
		},
		{
			"@timestamp": now.Add(-20 * time.Minute).Format(time.RFC3339Nano),
			"message":    `disk usage on /var/log is 87%`,
			"source":     "node-exporter",
			"level":      "warn",
			"host":       "db-01",
			"stream":     "system",
			"fields":     map[string]any{"mount": "/var/log", "usage_pct": 87},
		},
		{
			"@timestamp": now.Add(-30 * time.Minute).Format(time.RFC3339Nano),
			"message":    `{"event":"deploy","service":"api","version":"0.1.0","status":"success"}`,
			"source":     "ci",
			"level":      "info",
			"host":       "runner-3",
			"stream":     "all",
			"fields":     map[string]any{"service": "api", "version": "0.1.0"},
		},
		{
			"@timestamp": now.Add(-45 * time.Minute).Format(time.RFC3339Nano),
			"message":    `upstream timeout while reading response header from upstream, client: 10.0.0.12`,
			"source":     "nginx",
			"level":      "error",
			"host":       "web-01",
			"stream":     "web-access",
			"fields":     map[string]any{"client": "10.0.0.12", "error": "upstream_timeout"},
		},
		{
			"@timestamp": now.Add(-60 * time.Minute).Format(time.RFC3339Nano),
			"message":    `cron job backup-postgres finished in 42s`,
			"source":     "cron",
			"level":      "info",
			"host":       "db-01",
			"stream":     "system",
			"fields":     map[string]any{"job": "backup-postgres", "duration_s": 42},
		},
	}

	for i, doc := range samples {
		id := fmt.Sprintf("seed-%03d", i+1)
		if err := c.IndexMessage(ctx, index, id, doc); err != nil {
			return err
		}
	}
	log.Printf("seeded %d sample messages into %q", len(samples), index)
	return nil
}
