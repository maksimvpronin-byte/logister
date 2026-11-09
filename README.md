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

Use **Streams & storage** to create destinations, set routing rules, rotate
per-stream indices and configure retention. Automatic deletion defaults to off.
See [stream settings and delivery examples](docs/streams.md).

Administrators can inspect the live pipeline, queues, rates and component resources
in **Мониторинг**. See [monitoring and metric limits](docs/monitoring.md).

For a production deployment with one or more application nodes, use the
[Compose and Ansible deployment guide](deploy/cluster/README.md). This setup
uses shared Kafka, MongoDB and OpenSearch, with Kafka Direct ingestion.

### Dev mode (faster on WSL)

```bash
# infra
cd /mnt/c/Users/Fidel/Documents/logister && docker compose up -d mongo opensearch

# API
export PATH=$HOME/sdk/go/bin:$PATH
cd api && go build -o /tmp/logister-api ./cmd/api
DATABASE_URL='mongodb://127.0.0.1:27017/logister' \
OPENSEARCH_URL='http://127.0.0.1:9200' CORS_ORIGIN='http://localhost:3000' \
SEED_SAMPLE_LOGS=true /tmp/logister-api

# UI (Windows or WSL with Node)
cd ui && npm install && npm run dev -- --host 127.0.0.1 --port 3000
```

- **UI** — http://127.0.0.1:3000
- **API** — http://127.0.0.1:8080
- **OpenSearch** — http://127.0.0.1:9200
- **MongoDB** — `127.0.0.1:27017` (database `logister`, one instance)
- **Redis** — `127.0.0.1:6379` (intake journal, AOF every second)

Sample messages are seeded on first API start (optional).

### Live log pipeline (Fluent Bit)

Synthetic app logs → file → supervised Fluent Bit → Kafka → Redis journal → OpenSearch:

```bash
# build the custom generator and supervised sender first
docker compose build loggen fluent-bit
# sync the development pipeline definition into WSL
python3 /mnt/c/Users/Fidel/Documents/logister/deploy/sync-runtime.py
cd ~/logister-runtime
INGEST_HOST=host.docker.internal docker compose -f docker-compose.pipeline.yml up -d
```

`loggen` writes JSON lines with stable event IDs every ~700ms and retains rotated
files; Fluent Bit ships them through Kafka. HTTP 202 confirms the Redis journal
accepted the batch. OpenSearch indexing follows asynchronously. MongoDB keeps
stream settings and index generations.

Fluent Bit keeps its read offsets and pending deliveries in the
`fluent_bit_state` Docker volume. Keep this volume when recreating the shipper.
See [ingestion behavior and upgrade instructions](docs/ingestion.md) before
upgrading an existing pipeline. Search uses `logs-v2-read` across the preserved
`logs-v2` index and new per-stream generations. The original `logs` index remains
as a backup after migration.

### Regression checks (WSL)

```bash
cd /mnt/c/Users/Fidel/Documents/logister/api
export PATH=$HOME/sdk/go/bin:$PATH
LOGISTER_TEST_MONGO_URL='mongodb://127.0.0.1:27017' go test ./...
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
Arbitrary JSON routing and its verification are documented in the
[JSON routing report](docs/audits/2026-11-03-arbitrary-json.md).

## Local workspace

Project sources live in `C:\Users\Fidel\Documents\logister` (WSL:
`/mnt/c/Users/Fidel/Documents/logister`). Run future builds from this directory.
Install UI dependencies with `npm ci` in `ui` before starting development.

Copying the sources does not switch already running processes to this directory.
The existing WSL services and Docker volumes were preserved. Before starting a
new API or UI instance, stop the existing instance using the same port.

## MVP scope

Vertical slice only:

- ingest sample / HTTP JSON through the Redis journal into OpenSearch
- Postgres for stream settings, access and index generations; Redis for intake and retry identities
- React UI with time-range search and Lucene-like query

## Stack

| Component | Tech |
|-----------|------|
| API | Go |
| UI | React + TypeScript + Vite |
| Logs | OpenSearch |
| Intake journal | Redis |
| Settings | PostgreSQL |

## Access model

LDAP sign-in, users/groups, stream scopes and role mappings are documented in
[docs/roles.md](docs/roles.md). Run `sh deploy/start-ldap-auth.sh` to use the local
LDAP lab with API authorization and the Users & roles screen. The root Compose
file alone keeps development mode without sign-in.

## License

Apache 2.0

Настройка буфера и нагрузочное тестирование: [Kafka между Fluent Bit и Logister](docs/kafka.md).
