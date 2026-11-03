# Stream routing and storage settings

Open **Streams & storage** in Logister to create a stream, edit its routing and
storage policy, inspect managed indices, rotate now, or send a message to it.
Settings live in PostgreSQL and survive application/container restarts.

The saved-rule overview shows each condition and the fallback destination. Use
**Test routing** to paste one JSON message, evaluate the unsaved draft together
with the other saved streams, and see the selected destination and each rule's
actual field value. Preview does not save settings or ingest the sample.

The initial demo rules are `source = nginx` → `web-access` and `source = cron`
→ `system`. The generator sends event fields without a `stream`, so the same
editable rules used for external messages determine its destination. Defaults
are installed once for untouched configurations; user edits survive restarts.

## Destinations and routing

Stream names are immutable identifiers of 1–63 lowercase letters, digits, hyphens
or underscores, starting with a letter/digit. Titles and descriptions can change.
Create a stream before using its name as a destination.
The form suggests the technical name from the title's Latin characters until
you edit it manually. Typing a name converts uppercase letters to lowercase and
spaces to hyphens (`New stream` becomes `new-stream`). Titles allow any language
and spaces. Invalid technical names are blocked before saving or testing rules.

Messages choose one stream in this order:

1. `POST /api/streams/{name}/ingest` writes to the named stream. A conflicting
   `stream` in the body rejects the whole request with 400.
2. On `/api/ingest/json` or `/api/ingest/fluent-bit`, an explicit `stream` field
   selects that stream. An unknown stream rejects the whole batch with 400.
3. Without an explicit stream, enabled routing rules are evaluated by ascending
   priority, then stream name. The first matching stream wins.
4. Unmatched messages go to the default `all` stream.

Rules support arbitrary JSON fields, including `tenant`, `http.status`,
`items[0].code`, `items[].code`, `["key.with.dots"]` and Unicode names.
JSON Pointer paths such as `/random ~1 key/a.b` select literal keys: `~1`
represents slash and `~0` represents tilde. Literal dotted keys take precedence
with dot paths; quoted keys or JSON Pointer remove ambiguity.

Operators are case-sensitive `equals`, `contains`, `starts_with`, `exists` and
`not_exists`. Null counts as an existing field. Array conditions can match any
element. Scalar values use their text form; objects/arrays can use JSON text.
Choose **All rules** (AND) or **Any rule** (OR). Priority is 0–1000; at most 20
rules per stream. Paths are limited to 1,024 bytes, values to 4,096 bytes.

Paste a real JSON object into the sample to discover fields and add conditions
with one click. The sample need not contain `message`. Preview passes the raw
sample text to avoid rounding large numbers in the browser.

An explicit destination bypasses automatic rules. Automatic routing for `all`
is disabled: it is the fallback destination. In the search selector, **All
messages** continues to search across every stream.
The separate **Unmatched messages** option selects only the default destination
(`stream=@default` in the search API); its stored stream value remains `all`.

Example: route messages from `billing-api` into a `payments` stream by enabling
a rule with field `source`, operator `equals`, value `billing-api`. Clients can
also select it directly:

```http
POST /api/streams/payments/ingest
Content-Type: application/json

{"id":"payment-event-123","message":"Payment accepted","source":"billing-api"}
```

The scoped endpoint has the same 1 MiB/1,000-message limit and atomic PostgreSQL
acceptance contract as `/api/ingest/json`. Stable IDs and `Idempotency-Key` remain
necessary for safe retries. Reusing one batch key for a different scoped endpoint
is a conflict. The UI keeps the same event identity when retrying a failed send.

## Rotation

Each stream receives its own physical indices. Rotation thresholds are ORed:

| Setting | Default | Behavior |
| --- | --- | --- |
| Maximum age | 24 hours | Age since index allocation; checked on intake and by the background worker |
| Maximum size | 1,024 MiB | Primary-shard store bytes reported by OpenSearch; checked approximately every 10 seconds |
| Maximum messages | 1,000,000 | Count of newly accepted events; enforced during assignment, including within a batch |
| Retention after rotation | 0 days | 0 disables automatic deletion |

Zero disables an individual rotation threshold; at least one must be positive.
A positive retention requires age rotation so an idle stream can eventually
expire. Size checks can overshoot the configured threshold between checks;
pending accepted events stay assigned to their original index.

Rotation seals the current generation. **Rotate now** does the same immediately.
The next new message allocates its successor; no empty successor is created.
An already accepted event keeps its physical destination even when its first
publication or a retry happens after rotation. Duplicate retries do not count
toward the message threshold or allocate another generation.

Writing targets are durable PostgreSQL metadata, rather than a moving write
alias. This prevents a lost acknowledgement followed by rollover from putting
the same event in two physical indices.

## Retention and PostgreSQL cleanup

Set a positive retention value to enable permanent deletion of **sealed managed
indices** after that many days since sealing. The active index is never deleted.
Indices with unpublished events are protected even if their retention age has
passed. A temporary outage delays publication and deletion; it does not discard
the durable queue.

After OpenSearch acknowledges deletion (or confirms the index is already absent),
the same lifecycle operation clears published document payloads for that physical
index in PostgreSQL. Stable event IDs, hashes and batch receipts remain as small
deduplication tombstones. An old source replay or HTTP retry cannot resurrect an
expired event. Tombstone expiry and automatic source-file removal are not part
of this policy.

Changing retention applies to all managed indices of that stream, including
previously sealed generations. Lowering it can make older sealed indices eligible
on the next background pass. The form explains deletion before saving.

These policies apply to indices created by the stream-storage feature. The
pre-existing `logs` backup and `logs-v2` index are preserved, searchable, and
excluded from automatic deletion. Old events are not reassigned when rules change.

## Search and lifecycle ownership

The default logical namespace remains `logs-v2`. Managed index names have the
form `logs-v2-s-<stream UUID>-000001`. The `logs-v2-read` alias includes both
the legacy `logs-v2` index and the new managed indices. Search snapshots cover
the alias, with the existing stream filter and cursor navigation.

A snapshot already open at rotation keeps its original view and does not include
the new generation. Retention can leave an existing snapshot readable until its
short PIT lifetime expires. Starting a new search reflects the current indices.

Logister's background worker owns rotation and deletion. It does not install an
OpenSearch ISM policy. Do not assign a second rollover/deletion policy to these
managed indices: their durable event destinations must remain coordinated with
the PostgreSQL publication and retention state.

## API and verification

- `GET /api/streams`: saved configurations.
- `POST /api/streams`: create; duplicate name returns 409.
- `PUT /api/streams/{name}`: replace settings; names cannot change.
- `POST /api/streams/{name}/ingest`: write directly to a configured stream.
- `GET /api/streams/{name}/indices`: latest 100 generations with status/stats.
- `POST /api/streams/{name}/rotate`: seal the active generation, if any.
- `POST /api/streams/preview-routing`: explain routing for a sample and an
  optional unsaved `draft`, without writing messages or settings.

Run `python3 deploy/tests/acceptance.py --phase streams` after building the images,
or run the full default suite. Checks use private Docker volumes and include
actual age/size/count rotation, explicit/rule routing, snapshot stability,
retention, pending protection during an OpenSearch outage, retry tombstones and
settings persistence. Test fixtures adjust their own metadata timestamps to
exercise day/hour boundaries without waiting in real time.

`py ui/tests/streams_smoke.py --url http://localhost:3000` verifies creation,
editing, sending, rotation, search and a mobile viewport in Edge. It creates a
uniquely named fixture stream; use a retained isolated stack for regression runs.
