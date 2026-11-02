# Search and service status

## Search API

Start a search with `GET /api/search`:

| Parameter | Behavior |
| --- | --- |
| `q` | Lucene-style query; blank means `*` |
| `stream` | Exact stream filter; `all` means all streams |
| `relative` | Positive duration, e.g. `15m`, `1h`, `7d`; overflow rejected |
| `from`, `to` | RFC 3339 timestamps; from must precede to |
| `size` | Integer 1–200, default 50 |
| `offset` | Compatibility option for the first request only; offset + size <= 10,000 |

Use relative or absolute time bounds. The default is the last 24 hours; `to`
without `from` covers the 24 hours ending at `to`. Invalid parameters return 400
before querying OpenSearch.

Responses include `total`, `total_relation`, `messages`, `took_ms`, resolved `from`
and `to`, `offset`, `session_id`, and available `next_cursor`/`previous_cursor`.
`track_total_hits` requests exact counts. Lower-bound counts remain marked `gte`.

Follow a cursor with `GET /api/search?cursor=...`, with no other query parameters.
The signed cursor carries the applied query, bounds, page size, position and PIT
identity. The API uses an OpenSearch point-in-time snapshot and `search_after`,
sorting by timestamp and a unique event ID. New and late-arriving logs do not
shift an existing snapshot. Equal timestamps cannot skip or repeat records.
Cursor navigation has no 10,000-result browsing limit.

Each page refreshes the snapshot's two-minute idle lifetime. An invalid/expired
cursor returns 410: run a new Search. API restarts invalidate existing signed
cursors because the signing key is generated at startup. To release a snapshot,
send `DELETE /api/search/session` with `{"session_id":"..."}`. Abandoned snapshots
expire automatically. Initial searches that fail or return no matches close
their snapshots immediately.

Invalid expressions return a short actionable 400 response. Unavailable storage,
incomplete shard results, timeouts or malformed storage responses return 503.
Technical OpenSearch bodies remain in server logs.

## Interface

The UI shows 100 messages per page with Previous/Next controls using cursor-only
requests. Draft query edits take effect on Search; a new search starts a fresh
snapshot. Old rows/counters are cleared, superseded responses are ignored, and
unused sessions are closed. Errors and successful empty results are distinct.

Health and stream metadata load independently and refresh every 30 seconds.
A valid HTTP 503 health body preserves each database's status; missing status
information appears as unknown. An unavailable health endpoint does not discard
streams. Timers and requests are cancelled when the component unmounts.

Health also exposes `index` (normally `logs-v2`) and `pending_messages` (accepted
events not yet projected into search). Search can lag intake during recovery.

## Metadata upgrade

Fresh metadata uses `/api/ingest/json` for the built-in HTTP source. Startup also
repairs the legacy `/ingest/json` path for that source only, preserving custom
paths, names, input types and unrelated settings.

## Checks

Go tests check parameter bounds, cursor signature validation, forward/backward
navigation, cleanup, errors and metadata migration. Real PostgreSQL tests are
enabled by `LOGISTER_TEST_DATABASE_URL`. UI tests cover metadata failures,
pagination, stale responses, search resets and polling cleanup.

`python3 deploy/tests/acceptance.py` verifies real OpenSearch snapshots over
10,050 records, all with the same timestamp, including the last page after 10,000
and exclusion of a late arrival. The same suite verifies the storage upgrade.

`py ui/tests/browser_smoke.py --url http://localhost:3000` checks the real UI in
Edge. On the retained isolated acceptance stack, add `--deep-query source:paging`
to traverse 101 pages in the browser. The only simulated response is health for
the browser's partial-outage scenario. See the [closure report](audits/2026-11-03-reliability.md).
