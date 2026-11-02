# Logister

Open-source centralized log management platform (Apache 2.0). Inspired by Graylog, implemented independently.

## Quick start (MVP)

### Full stack (Docker)

From WSL (recommended; Docker lives there on this machine):

```bash
cd /mnt/c/Users/Fidel/Documents/logister
# optional, if OpenSearch fails to start:
# sudo sysctl -w vm.max_map_count=262144
docker compose up --build -d
```

Then open http://localhost:3000

### Dev mode (faster on WSL)

```bash
# infra
cd /mnt/c/Users/Fidel/Documents/logister && docker compose up -d postgres opensearch

# API
export PATH=$HOME/sdk/go/bin:$PATH
cd api && go build -o /tmp/logister-api ./cmd/api
DATABASE_URL='postgres://logister:logister@127.0.0.1:5432/logister?sslmode=disable' \
OPENSEARCH_URL='http://127.0.0.1:9200' CORS_ORIGIN='http://localhost:3000' \
SEED_SAMPLE_LOGS=true /tmp/logister-api

# UI (Windows or WSL with Node)
cd ui && npm install && npm run dev -- --host 127.0.0.1 --port 3000
```

- **UI** — http://127.0.0.1:3000
- **API** — http://127.0.0.1:8080
- **OpenSearch** — http://127.0.0.1:9200
- **Postgres** — `127.0.0.1:5432` (`logister` / `logister` / `logister`)

Sample messages are seeded on first API start (optional).

### Live log pipeline (Fluent Bit)

Synthetic app logs → file → supervised Fluent Bit → durable PostgreSQL queue → OpenSearch:

```bash
# build the custom generator and supervised sender first
docker compose build loggen fluent-bit
# sync the development pipeline definition into WSL
python3 /mnt/c/Users/Fidel/Documents/logister/deploy/sync-runtime.py
cd ~/logister-runtime
INGEST_HOST=host.docker.internal docker compose -f docker-compose.pipeline.yml up -d
```

`loggen` writes JSON lines with stable event IDs every ~700ms and retains rotated
files; Fluent Bit ships them to `/api/ingest/fluent-bit`. HTTP 202 confirms an
atomic PostgreSQL commit. OpenSearch indexing follows asynchronously.

Fluent Bit keeps its read offsets and pending deliveries in the
`fluent_bit_state` Docker volume. Keep this volume when recreating the shipper.
See [ingestion behavior and upgrade instructions](docs/ingestion.md) before
upgrading an existing pipeline. The versioned search index is `logs-v2`;
the original `logs` index remains as a backup after migration.

### Regression checks (WSL)

```bash
cd /mnt/c/Users/Fidel/Documents/logister/api
export PATH=$HOME/sdk/go/bin:$PATH
LOGISTER_TEST_DATABASE_URL='postgres://logister:logister@127.0.0.1:5432/logister?sslmode=disable' go test ./...
go vet ./...
cd ..
docker compose build
python3 deploy/tests/acceptance.py
```

The acceptance suite creates its own full Compose stack with private ports and
volumes, then removes it. It checks deep snapshot pagination, retry deduplication,
legacy migration, load, crashes, rotation and actual ENOSPC on quota filesystems.
Use `--keep` to retain a test stack for browser checks. Compose v2 is required.

For the UI, run `npm ci`, `npm test`, and `npm run build` in `ui`.
The optional Windows browser check is `py ui/tests/browser_smoke.py` from the
project root (Python Playwright and Edge required).
See [search behavior and checks](docs/search.md) for snapshot lifetime and cursor
navigation beyond 10,000 matches, and the [audit closure report](docs/audits/2026-11-03-reliability.md).

## Local workspace

Project sources live in `C:\Users\Fidel\Documents\logister` (WSL:
`/mnt/c/Users/Fidel/Documents/logister`). Run future builds from this directory.
Install UI dependencies with `npm ci` in `ui` before starting development.

Copying the sources does not switch already running processes to this directory.
The existing WSL services and Docker volumes were preserved. Before starting a
new API or UI instance, stop the existing instance using the same port.

## MVP scope

Vertical slice only:

- ingest sample / HTTP JSON through a durable PostgreSQL queue into OpenSearch
- Postgres for metadata, atomic intake and retry identities
- React UI with time-range search and Lucene-like query

## Stack

| Component | Tech |
|-----------|------|
| API | Go |
| UI | React + TypeScript + Vite |
| Logs | OpenSearch |
| Metadata | PostgreSQL |

## License

Apache 2.0
