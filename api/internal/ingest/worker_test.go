package ingest

import (
	"testing"
	"time"
)

func TestPublicationRetryBackoffAndReporting(t *testing.T) {
	var retry publicationRetry
	now := time.Now()
	for i, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second} {
		delay, report := retry.failure("Redis journal", now.Add(time.Duration(i)*time.Second))
		if delay != want || report != (i == 0) {
			t.Fatalf("retry %d: delay %v, report %v", i, delay, report)
		}
	}
	if delay, report := retry.failure("Redis journal", now.Add(time.Minute)); delay != 5*time.Second || !report {
		t.Fatalf("continuing outage: delay %v, report %v", delay, report)
	}
	if delay, report := retry.failure("OpenSearch", now.Add(time.Minute)); delay != time.Second || !report {
		t.Fatalf("different dependency: delay %v, report %v", delay, report)
	}
}
