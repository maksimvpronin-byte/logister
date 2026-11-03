package search

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRetentionRequiresAcknowledgedSearchDeletion(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
		ok     bool
	}{
		{200, `{"acknowledged":true}`, true}, {404, `{}`, true}, {200, `{"acknowledged":false}`, false}, {200, `{}`, false}, {503, `{"error":"offline"}`, false},
	} {
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(tc.status); fmt.Fprint(w, tc.body) }))
		client, err := NewClient(backend.URL)
		if err != nil {
			t.Fatal(err)
		}
		err = client.DeleteStreamIndex(context.Background(), "owned-stream-index")
		backend.Close()
		if (err == nil) != tc.ok {
			t.Fatalf("status=%d body=%s error=%v", tc.status, tc.body, err)
		}
	}
}
func TestSizeRotationRejectsIncompleteStatistics(t *testing.T) {
	for _, tc := range []struct {
		body string
		ok   bool
	}{
		{`{"_all":{"primaries":{"docs":{"count":2},"store":{"size_in_bytes":1048576}}}}`, true},
		{`{}`, false}, {`{"_all":{"primaries":{"docs":{"count":2}}}}`, false},
	} {
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, tc.body) }))
		client, err := NewClient(backend.URL)
		if err != nil {
			t.Fatal(err)
		}
		stats, err := client.IndexStats(context.Background(), "owned-stream-index")
		backend.Close()
		if (err == nil) != tc.ok {
			t.Fatal(tc.body, stats, err)
		}
		if tc.ok && (stats.Documents != 2 || stats.SizeBytes != 1048576) {
			t.Fatal(stats)
		}
	}
}
