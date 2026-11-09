# Durable log ingestion

## HTTP contract

`POST /api/ingest/json` accepts an object, an array, or a Fluent Bit wrapper
containing a JSON line in `log`. The complete request is validated before writing.

Any JSON object can be an event; `message` is optional. Null/scalar elements in a
batch are rejected before writing. Without a text message, the complete JSON representation
is used as searchable text. Arbitrary properties, nested shapes and original types
are preserved as `payload_json`, returned alongside the decoded `payload`.
Recognized string ID/delivery_id/stream retain their existing protocol meanings.

The complete payload is stored as an unindexed JSON string, avoiding dynamic
mapping conflicts and numeric/NUL limitations. Routing reads
the full input before projection. The searchable `message` contains the complete message text. Only the UI table
preview is shortened to 500 characters; expanded details show the full message.
`payload_json` remains unindexed, while OpenSearch compresses stored source using
the configured codec (currently `zstd_no_dict`, level 1 for new indices).
Explicit projections `service`, `request_id`, and `trace_id` are keywords; `status`
is a signed integer and `ip` is an IP address. They are read from top-level
properties, falling back to `fields` when absent. Invalid types remain in the
original payload without blocking ingestion or generating dynamic mappings.
Legacy searchable `fields` are bounded for large values; response fields are
restored from the original where available.
Expanded UI rows display structured fields by default, with the original
JSON available in a separate tab and through the copy button.

New events hash the complete payload, so an additional-field change under the
same ID returns 409. Existing accepted identities retain their old comparison
version for compatible retries. Fields discarded by older API versions cannot
be recovered retroactively.

| Endpoint | Body limit | Batch limit |
| --- | --- | --- |
| `/api/ingest/json` | 1 MiB | 1,000 messages |
| `/api/ingest/fluent-bit` | 16 MiB | 10,000 messages |

The second endpoint accommodates Fluent Bit chunks; both use the same validation
and durable acceptance path. Oversized bodies return 413, including bodies with
unknown Content-Length. Invalid JSON/messages or empty batches return 400.

HTTP 202 with `{"status":"accepted","count":N,"duplicates":D}` means Redis has
accepted the whole batch. Indexing in OpenSearch is asynchronous. A stream
index refreshes every 30 seconds, so a new message can take that long to
appear in search. Its translog is flushed every 5 seconds rather than on
every batch. PostgreSQL
stores stream settings and the index-generation catalog; it does not store the
log body. Redis appends with `appendfsync everysec`, so this is not a
synchronous commit on every batch: a crash can lose about the last second.
An unavailable Redis or PostgreSQL returns 503 with `count: 0`. A connection
lost during the write has an unknown outcome: retry with the same identities.

## Retry identities

Supply a stable `id` for each event (at most 256 bytes), or an `Idempotency-Key`
header (at most 200 bytes) for a request. Repeating the same identity and content
returns a duplicate receipt without creating another logical record. Reusing an
identity with different content returns 409 and rejects the entire batch.

Batch keys identify the exact request bytes, including formatting and order.
Keep the same body on a batch retry. Event identity comparisons use canonical
JSON before generated timestamps and remote-address defaults are added. This
allows an event without a timestamp to be safely retried. Explicit event IDs
take precedence over the shipper's fallback `delivery_id`.

Without stable event IDs or a batch key, two identical requests are two new
events. Fluent Bit's fallback UUID survives queued HTTP retries, but rereading a
source file generates a new UUID. Sources that must survive checkpoint resets
must therefore include stable IDs in the file itself. The bundled generator does.

Redis keeps the document body until OpenSearch acknowledges it, then deletes
the body and keeps the identity hash. A retry of the same identity is a
duplicate; a different payload is a conflict while that hash remains. Hashes
accepted before this journal existed are not consulted. The publisher uses
fixed OpenSearch document IDs and deletes the body only after the whole
projection batch succeeds. Retrying after a crash or a lost acknowledgement
overwrites the same IDs. See [stream retention](streams.md).

The journal compresses larger bodies with Zstandard when compression saves
space. Queue and publication byte limits still count the original document
size. The API can publish older, uncompressed journal entries. For an upgrade
on a shared journal, stop the old API publishers before enabling the new
instances. Drain the queue with the new version before rolling back.

## Index schema and upgrade

`OPENSEARCH_INDEX` names the logical base (`logs` by default); the legacy storage
index is `logs-v2`. New messages use per-stream generations and search uses the
`logs-v2-read` alias. The schema stores a bounded compatibility projection of
`fields` as `flat_object`. Searches over these values use string semantics;
typed numeric/date analytics need dedicated mapped fields. The full original
JSON is stored separately in unindexed `payload_json`; arbitrary top-level
properties support routing, but are not automatically searchable Lucene fields.

On first startup, the API copies an existing base index into the versioned index,
preserves document IDs, verifies counts, and marks the new mapping ready. The
original index remains intact as a backup. An interrupted migration is retried.
Search health remains degraded until preparation completes; intake can continue
when Redis and PostgreSQL are available. The queue is published after preparation.

Stop legacy writers before upgrading a running installation. For the bundled
legacy pipeline:

1. Pause the old generator at a complete newline. Confirm its last record is
   searchable and Fluent Bit input/output counters agree.
2. Stop the old sender and generator without restarting the old generator,
   whose previous implementation truncates the source file on startup.
3. Preserve the already delivered legacy file under `/logs/pre-v2/`, outside
   `/logs/app.log*`. This prevents later recovery from replaying old ID-less lines.
4. Back up the old API binary, install the checked version, and wait until
   health reports `index: logs-v2` and `search_index: logs-v2-read`, with both services available. Verify the old
   count is preserved in both indexes while producers are stopped.
5. Build the custom sender/generator images and sync the dev pipeline definition.
   Recreate those two services with the same named volumes. Verify new delivery
   and a drained `pending_messages` counter.

## Files, checkpoints and disk exhaustion

The sender keeps `tail.db` and `buffer/` in the `fluent_bit_state` named volume.
Ordinary restarts resume saved offsets; failed HTTP deliveries retry without a
five-attempt discard limit. Keep this volume and the source volume. Use Compose
v2 (`docker compose`); `down -v` intentionally destroys persisted data.

The custom image runs a supervisor around Fluent Bit. Disk persistence failures
or unclean exits invalidate the tail checkpoint; persisted queued chunks remain.
Retained source files are replayed when storage becomes writable. Stable source
IDs make this replay idempotent. A volume lock prevents two supervisors sharing
one state directory. The supervisor also drains final error output on shutdown.

The generator appends, uses a per-run UUID plus sequence number, and rotates by
rename-and-create without deleting archives. If a write fails, it truncates back
to the last complete line and retries the same event after space is freed.
Archives deliberately remain available for recovery; provision disk space and
remove only files whose delivery/retention requirements have been met.

Recovery cannot reconstruct deleted source bytes. Concurrent `copytruncate` can
lose bytes between copy and truncation; prefer rename-and-create. The integration
suite checks `copytruncate` with a paused writer, not arbitrary concurrent writers.
Source lines must fit Fluent Bit's input buffers and endpoint batch/body limits;
arbitrarily large lines are outside this contract.

## Verification

Stream creation, rule routing, explicit destinations, rotation and storage
settings are described in [stream settings](streams.md). Unknown explicit stream
names are rejected before any part of the batch is committed.

Run `docker compose build`, then `python3 deploy/tests/acceptance.py` from WSL.
The suite creates a unique `logister-check-*` project with private ports/volumes,
uses quota tmpfs filesystems for ENOSPC, and cleans up its own resources. It checks
lost acknowledgements, concurrent retries, migration, mixed fields, load, process
SIGKILL, recovery, rotation, and disk exhaustion at each pipeline stage. It finishes
by comparing all durable records against the search count with no pending events.

Go regression tests cover body/batch validation, whole-batch rollback, conflicts,
identity retries and publication acknowledgement loss in the Redis journal. Set
`LOGISTER_TEST_MONGO_URL` to enable the MongoDB catalog tests; each test uses
its own database and drops it.

These checks simulate process crashes, not a physical host power cut, failed
hardware, or arbitrary filesystem corruption. See the [closure report](audits/2026-11-03-reliability.md)
for the measured scenarios and boundaries.

## Показатели обработки

`GET /api/ingest/stats` возвращает накопительные счётчики уникальных сообщений:
`accepted` — принято в журнал Redis и назначен поток, `published` — запись
в OpenSearch подтверждена и тело удалено из журнала, `pending` — ожидает
подтверждения отправки. Повторная доставка с тем же идентификатором не увеличивает
счётчики. Невалидные/отклонённые запросы в принятые не входят.

Учёт начинается с включения управляемых индексов потоков; старые сообщения без
назначенного индекса в эти показатели не входят. Удаление логов по сроку хранения
не обнуляет накопительные значения. Это показатели доставки, а не текущее число
документов в поиске. Ответ ограничен потоками с правом чтения текущего пользователя.

Интерфейс обновляет значения каждую секунду. Скорость вычисляется по разнице
двух последовательных измерений; после открытия страницы или ошибки нужно
дождаться следующего измерения. При ошибке сохраняются последние значения
с явным предупреждением об их актуальности.

## Restoring full-text search for existing messages

`python3 deploy/tests/reindex_full_messages.py` previews the number of documents
whose searchable projection differs from the preserved payload. `--apply` updates
those existing documents in place. It preserves IDs, original payload strings,
timestamps, streams and index generations. Legacy records without `payload_json`
are left unchanged. Run after deploying the updated API so its mappings include
the typed metadata fields. The migration is idempotent and saves counts, without
message bodies, under `graphify-out/fulltext-migration/`.

Earlier performance reports with a 500-byte indexed excerpt are historical.
New load tests verify that sampled indexed messages have the full generated
length, report `message_indexing: full`, and remove synthetic data after success.

## Intake transaction ownership

The acceptance lock is fenced with Redis WATCH: its owner is checked before
the write, and expiry or replacement before EXEC aborts the transaction. Event
bodies, queue entries, counters and an optional batch receipt commit together.
Rotation also fences removal of the active-generation pointer. Legacy Kafka hash
normalization uses compare-and-set. These safeguards do not change the existing
Redis AOF or OpenSearch translog persistence windows. Identity hashes currently
have no automatic expiry; their memory use is separate from the pending-body budget.
