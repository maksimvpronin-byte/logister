package db

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestUpgradeLegacyInputPath(t *testing.T) {
	databaseURL := os.Getenv("LOGISTER_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set LOGISTER_TEST_DATABASE_URL to run the PostgreSQL integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	connection, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(context.Background())
	for _, tc := range []struct{ name, inputType, path, expected string }{
		{"http-json", "http", "/ingest/json", "/api/ingest/json"},
		{"http-json", "http", "/custom/path", "/custom/path"},
		{"http-json", "http", "/api/ingest/json", "/api/ingest/json"},
		{"http-json", "syslog", "/ingest/json", "/ingest/json"},
		{"custom-input", "http", "/ingest/json", "/ingest/json"},
	} {
		t.Run(tc.name+tc.inputType+tc.path, func(t *testing.T) {
			tx, err := connection.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(context.Background())
			// A transaction-local table shadows the real inputs table. The real
			// application metadata is never modified by this integration test.
			schema := pgx.Identifier{fmt.Sprintf("test_upgrade_%d", time.Now().UnixNano())}.Sanitize()
			_, err = tx.Exec(ctx, "CREATE SCHEMA "+schema+"; SET LOCAL search_path TO "+schema)
			if err != nil {
				t.Fatal(err)
			}
			_, err = tx.Exec(ctx, `CREATE TABLE inputs (name text, type text, config jsonb)`)
			if err != nil {
				t.Fatal(err)
			}
			_, err = tx.Exec(ctx, `INSERT INTO inputs VALUES ($1,$2,jsonb_build_object('path',$3::text,'keep','yes'))`, tc.name, tc.inputType, tc.path)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				if err := UpgradeMetadata(ctx, tx); err != nil {
					t.Fatal(err)
				}
			}
			var path, keep string
			if err := tx.QueryRow(ctx, `SELECT config->>'path', config->>'keep' FROM inputs`).Scan(&path, &keep); err != nil {
				t.Fatal(err)
			}
			if path != tc.expected || keep != "yes" {
				t.Fatalf("path=%q keep=%q expected=%q", path, keep, tc.expected)
			}
		})
	}
}
