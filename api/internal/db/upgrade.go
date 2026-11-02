package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

type metadataExecutor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

// UpgradeMetadata repairs the legacy built-in input without overwriting custom
// input paths or unrelated settings. It is safe to run on every API startup.
func UpgradeMetadata(ctx context.Context, executor metadataExecutor) error {
	if _, err := executor.Exec(ctx, ingestSchema); err != nil {
		return fmt.Errorf("create durable ingestion tables: %w", err)
	}
	_, err := executor.Exec(ctx, `
		UPDATE inputs
		SET config = jsonb_set(config, '{path}', '"/api/ingest/json"'::jsonb)
		WHERE name = 'http-json' AND type = 'http'
		  AND config->>'path' = '/ingest/json'
	`)
	if err != nil {
		return fmt.Errorf("upgrade HTTP input path: %w", err)
	}
	return nil
}
