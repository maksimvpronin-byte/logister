package db

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"
)

func testDB(t *testing.T) (context.Context, *DB) {
	t.Helper()
	raw := os.Getenv("LOGISTER_TEST_MONGO_URL")
	if raw == "" {
		t.Skip("set LOGISTER_TEST_MONGO_URL for MongoDB integration tests")
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = fmt.Sprintf("/logister_test_%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	database, err := Connect(ctx, parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = database.database.Drop(context.Background())
		database.Close()
	})
	if err := database.ensureIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	return ctx, database
}
