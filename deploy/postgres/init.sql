CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS inputs (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL UNIQUE,
    type        TEXT NOT NULL,
    config      JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS streams (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL UNIQUE,
    title       TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO streams (name, title, description)
VALUES
    ('all', 'All messages', 'Default stream for every message'),
    ('web-access', 'Web access', 'HTTP / nginx access logs'),
    ('system', 'System', 'Syslog and host messages')
ON CONFLICT (name) DO NOTHING;

INSERT INTO inputs (name, type, config)
VALUES
    ('http-json', 'http', '{"path":"/api/ingest/json"}'),
    ('sample-seed', 'internal', '{"description":"Seeds demo messages on startup"}')
ON CONFLICT (name) DO NOTHING;
