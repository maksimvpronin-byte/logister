package db

import (
	"context"
	"fmt"
)

// Install defaults once, and only where the original routing configuration has
// never been customized. Later API starts must preserve deliberate user changes.
func UpgradeRoutingDefaults(ctx context.Context, executor metadataExecutor) error {
	_, err := executor.Exec(ctx, `
 CREATE TABLE IF NOT EXISTS metadata_migrations(name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now());
 DO $$ BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('logister.routing-defaults-v1'));
  IF EXISTS(SELECT 1 FROM metadata_migrations WHERE name='routing-defaults-v1') THEN RETURN; END IF;
  UPDATE streams SET routing='{"enabled":true,"priority":100,"match":"all","rules":[{"field":"source","operator":"equals","value":"nginx"}]}'
   WHERE name='web-access' AND routing='{"enabled":false,"priority":100,"match":"all","rules":[]}'::jsonb;
  UPDATE streams SET routing='{"enabled":true,"priority":100,"match":"all","rules":[{"field":"source","operator":"equals","value":"cron"}]}'
   WHERE name='system' AND routing='{"enabled":false,"priority":100,"match":"all","rules":[]}'::jsonb;
  UPDATE streams SET title='Unmatched messages',description='Default destination when no routing rule matches'
   WHERE name='all' AND title='All messages' AND description='Default stream for every message';
  INSERT INTO metadata_migrations(name) VALUES('routing-defaults-v1');
 END $$;
 `)
	if err != nil {
		return fmt.Errorf("upgrade default routing rules: %w", err)
	}
	return nil
}
