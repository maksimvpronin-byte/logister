# Logister

Open-source centralized log management platform (Apache 2.0). Inspired by Graylog, implemented independently.

## Quick start (MVP)

### Full stack (Docker)

From WSL (recommended; Docker lives there on this machine):

```bash
cd /mnt/c/Users/Fidel/Documents/logister
# optional, if OpenSearch fails to start:
# sudo sysctl -w vm.max_map_count=262144
docker-compose up --build -d
```

Then open http://localhost:3000

### Dev mode (faster on WSL)

```bash
# infra
cd /mnt/c/Users/Fidel/Documents/logister && docker-compose up -d postgres opensearch

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

Synthetic app logs → file → Fluent Bit → `POST /api/ingest/json`:

```bash
# sync LF configs into WSL filesystem (avoids CRLF on /mnt/c)
python3 /mnt/c/Users/Fidel/Documents/logister/deploy/sync-runtime.py
cd ~/logister-runtime
INGEST_HOST=host.docker.internal docker-compose -f docker-compose.pipeline.yml up -d
```

`loggen` writes JSON lines every ~700ms; Fluent Bit ships them into the API.

## Local workspace

Project sources live in `C:\Users\Fidel\Documents\logister` (WSL:
`/mnt/c/Users/Fidel/Documents/logister`). Run future builds from this directory.
Install UI dependencies with `npm ci` in `ui` before starting development.

Copying the sources does not switch already running processes to this directory.
The existing WSL services and Docker volumes were preserved. Before starting a
new API or UI instance, stop the existing instance using the same port.

## MVP scope

Vertical slice only:

- ingest sample / HTTP JSON into OpenSearch
- Postgres for metadata (inputs, streams)
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
