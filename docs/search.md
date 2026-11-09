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
Both bounds are inclusive. Timestamp indices have millisecond resolution;
queries send explicit epoch-millisecond bounds so OpenSearch does not expand an
upper bound with an omitted fractional second to the end of that second.

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

Choose **Custom range** in **Time range**, enter **From** and **To** (date and time
including seconds), then click **Search**. Inputs use the browser's local time
zone, shown below them, and are converted to UTC for the API. **Applied period**
shows the actual server-resolved interval and time-zone offsets of the results.
Dates use plain text fields in `DD.MM.YYYY HH:mm:ss` format, with optional seconds;
local ISO timestamps can also be pasted. Wheel scrolling and arrow keys do not
increment the date/time. Custom drafts survive switching to a relative preset
and back.
Editing dates does not change an existing snapshot until Search is submitted;
Next/Previous keep the applied interval. Missing, invalid, equal or reversed
bounds are rejected locally while previous results remain available. Quick
relative presets continue to work.

Click a message row or its arrow to open a full-width field viewer. The default
**Fields** tab separates field names, JSON types and values; objects and arrays
can be expanded and collapsed. Filter by a field path or value to find nested
properties quickly. The **JSON** tab shows formatted JSON, and **Copy JSON**
copies its original text. Numeric literals are preserved without JavaScript
rounding. Old records without original JSON use the available saved metadata
and fields. Large groups show 200 entries at a time with a **Show more fields**
control; exceptionally large/deep documents remain available in the JSON tab.

Each scalar value has a small arrow menu with **Добавить в поиск** (include) and
**Исключить из поиска** (exclude). The actions update the query draft; click
**Search** to apply. Existing expressions are grouped before adding `AND` or
`AND NOT`, preserving OR precedence. Literal values are quoted and escaped.
The menu supports keyboard navigation, Escape and outside-click dismissal and
is positioned outside the scrolling field container so it remains visible.

Actions are available for indexed metadata and supported `fields.*` paths.
The original `timestamp` is mapped to `@timestamp`. Raw-only properties such as
`delivery_id`, nulls, ambiguous field paths and values that cannot be represented
safely show disabled actions with an explanation. No index mappings or stored
events are changed by these menus. `fields.*` retains its existing string query
semantics, and `message` uses phrase search. See OpenSearch's
[query-string syntax](https://docs.opensearch.org/latest/query-dsl/full-text/query-string/).

Health and stream metadata load independently and refresh every 30 seconds.
A valid HTTP 503 health body preserves each database's status; missing status
information appears as unknown. An unavailable health endpoint does not discard
streams. Timers and requests are cancelled when the component unmounts.

Health also exposes `index` (normally `logs-v2`), `search_index` (`logs-v2-read`)
and `pending_messages` (accepted
events not yet projected into search). Search can lag intake during recovery.

The read alias spans preserved legacy data and managed per-stream generations.
A new snapshot includes the currently available generations. Stream rotation
and retention are configured under **Streams & storage**; see [stream settings](streams.md).

## Metadata upgrade

Fresh metadata uses `/api/ingest/json` for the built-in HTTP source. Startup also
repairs the legacy `/ingest/json` path for that source only, preserving custom
paths, names, input types and unrelated settings.

## Checks

Go tests check parameter bounds, cursor signature validation, forward/backward
navigation, cleanup, errors and metadata migration. Real MongoDB tests are
enabled by `LOGISTER_TEST_MONGO_URL`. UI tests cover metadata failures,
pagination, stale responses, search resets and polling cleanup.
Field-viewer tests check nested paths, old records, null/boolean values and exact
large numbers. `py ui/tests/message_fields_smoke.py` checks the real `new-stream`
demo records, filtering, arrays, formatted JSON, closing and mobile page width.
`py ui/tests/time_range_smoke.py` verifies real absolute bounds in a Moscow
browser context, exact fixture membership, both inclusive endpoints, cursor-only
paging after draft changes, local validation and relative/custom switching.
`py ui/tests/field_actions_smoke.py` verifies real include/exclude searches for
nested fields and metadata, OR grouping, escaped URL paths/hyphens, unsupported
raw fields, keyboard control and menu positioning in a mobile viewport.

`python3 deploy/tests/acceptance.py` verifies real OpenSearch snapshots over
10,050 records, all with the same timestamp, including the last page after 10,000
and exclusion of a late arrival. The same suite verifies the storage upgrade.

`py ui/tests/browser_smoke.py --url http://localhost:3000` checks the real UI in
Edge. On the retained isolated acceptance stack, add `--deep-query source:paging`
to traverse 101 pages in the browser. The only simulated response is health for
the browser's partial-outage scenario. See the [closure report](audits/2026-11-03-reliability.md).
