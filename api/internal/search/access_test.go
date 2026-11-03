package search

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCursorsAndSnapshotCloseAreBoundToOwnerAndCurrentScope(t *testing.T) {
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.Write([]byte(`{}`)) }))
	defer upstream.Close()
	client, _ := NewClient(upstream.URL)
	owner := WithAccess(context.Background(), "operator-uuid", []string{"payments"})
	token := client.sealCursor(pageCursor{PIT: "private-pit", Size: 1, Access: accessFingerprint(owner)})
	for _, ctx := range []context.Context{context.Background(), WithAccess(context.Background(), "other-uuid", []string{"payments"}), WithAccess(context.Background(), "operator-uuid", []string{"billing"}), WithAccess(context.Background(), "operator-uuid", []string{}), WithAccess(context.Background(), "operator-uuid", nil)} {
		if _, err := client.SearchPage(ctx, "logs", SearchParams{}, token); !errors.Is(err, ErrExpiredCursor) {
			t.Fatal(err)
		}
		if accessFingerprint(ctx) != "" {
			if err := client.CloseSearch(ctx, token); !errors.Is(err, ErrExpiredCursor) {
				t.Fatal(err)
			}
		}
	}
	if calls != 0 {
		t.Fatal("unauthorized cursor reached OpenSearch")
	}
	if err := client.CloseSearch(owner, token); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal(calls)
	}
}

func TestDeletingStreamInvalidatesOldCursor(t *testing.T) {
	client, _ := NewClient("http://127.0.0.1:1")
	original := WithAccess(context.Background(), "admin", nil)
	hidden := WithExcludedStreams(original, []string{"payments"})
	token := client.sealCursor(pageCursor{PIT: "old-snapshot", Size: 1, Access: accessFingerprint(original)})
	if _, err := client.SearchPage(hidden, "logs", SearchParams{}, token); !errors.Is(err, ErrExpiredCursor) {
		t.Fatal(err)
	}
	if accessFingerprint(hidden) == accessFingerprint(original) {
		t.Fatal("deletion did not alter cursor scope")
	}
}
