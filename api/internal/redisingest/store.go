package redisingest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/logister/logister/api/internal/db"
	"github.com/logister/logister/api/internal/telemetry"
	"github.com/redis/go-redis/v9"
)

// PublishBatch caps the records in one publication window. The separate byte
// budget lets small records fill multiple bulk requests while bounding bodies.
const PublishBatch = 4000

// Store is the ingest journal. MongoDB keeps stream settings and the index
// catalog; message bodies and the queue waiting for OpenSearch live here.
// After a successful publication the body is deleted and the identity hash stays.
type Store struct {
	telemetry         *telemetry.Meter
	redis             *redis.Client
	catalog           Catalog
	maxPublishBytes   int
	maxPendingBytes   int64
	profile           bool
	outputBufferBytes int
	requireRestore    bool
	restored          atomic.Bool
}

func New(client *redis.Client, catalog Catalog) *Store {
	return &Store{redis: client, catalog: catalog}
}

func (s *Store) Ping(ctx context.Context) error { return s.redis.Ping(ctx).Err() }

func (s *Store) RequireRestore() { s.requireRestore = true }

var errJournalRestoring = errors.New("HTTP ingest journal is restoring")

func (s *Store) ready() bool { return !s.requireRestore || s.restored.Load() }

// RestoreCounters copies historical PostgreSQL totals into Redis once.
// A later restart keeps the Redis counter, which already includes new events.
func (s *Store) RestoreCounters(ctx context.Context, namespace string) error {
	totals, err := s.catalog.AcceptedTotals(ctx, namespace)
	if err != nil {
		return err
	}
	for name, total := range totals {
		if err := s.redis.SetNX(ctx, s.acceptedKey(namespace, name), total, 0).Err(); err != nil {
			return err
		}
	}
	_, err = s.redis.Eval(ctx, bodyLengthLua+`if redis.call('exists',KEYS[1])==0 then local size=0;for _,id in ipairs(redis.call('lrange',KEYS[2],0,-1)) do size=size+bodyLength(ARGV[1]..id) end;redis.call('set',KEYS[1],size) end;return 1`, []string{s.pendingBytesKey(namespace), s.pendingKey(namespace)}, s.eventKey(namespace, "")).Result()
	if err == nil {
		s.restored.Store(true)
	}
	return err
}

var errQueueFull = errors.New("ingest queue is at its byte budget")

func (s *Store) SetQueueMaxBytes(value int) { s.maxPendingBytes = int64(value) }
func (s *Store) PendingBytes(ctx context.Context, index string) (int64, error) {
	return s.counter(ctx, s.pendingBytesKey(index))
}
func (s *Store) pendingBytesKey(index string) string { return "logister:pending-bytes:" + index }
func (s *Store) Accept(ctx context.Context, index, key, hash string, events []db.Event) (receipt db.Receipt, err error) {
	if !s.ready() {
		return db.Receipt{}, errJournalRestoring
	}
	if s.telemetry != nil {
		started := time.Now()
		defer func() { s.telemetry.Observe("journal", time.Since(started), err != nil) }()
	}
	started := time.Now()
	retries := 0
	if s.profile {
		defer func() {
			s.trace("accept_total", map[string]any{"documents": len(events), "ms": time.Since(started).Seconds() * 1000, "budget_retries": retries})
		}()
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		receipt, err := s.acceptOnce(ctx, index, key, hash, events)
		if !errors.Is(err, errQueueFull) {
			if err == nil && s.telemetry != nil {
				s.telemetry.Duplicates(receipt.Duplicates)
			}
			return receipt, err
		}
		select {
		case <-ctx.Done():
			return db.Receipt{}, ctx.Err()
		case <-time.After(10 * time.Millisecond):
			retries++
		}
	}
}
func (s *Store) acceptOnce(ctx context.Context, index, key, hash string, events []db.Event) (db.Receipt, error) {
	started := time.Now()
	lockMS, dedupeMS, storeMS := 0.0, 0.0, 0.0
	if s.profile {
		defer func() {
			s.trace("accept_once", map[string]any{"documents": len(events), "ms": time.Since(started).Seconds() * 1000, "lock_ms": lockMS, "dedupe_ms": dedupeMS, "store_ms": storeMS})
		}()
	}
	receipt := db.Receipt{Count: len(events)}
	if len(events) == 0 && key == "" {
		return receipt, nil
	}
	owner, unlock, err := s.lock(ctx, index)
	if err != nil {
		return db.Receipt{}, err
	}
	defer unlock()
	lockMS = time.Since(started).Seconds() * 1000
	if key != "" {
		previous, count, ok, err := s.batch(ctx, index, key)
		if err != nil {
			return db.Receipt{}, err
		}
		if ok {
			if previous != hash {
				return db.Receipt{}, db.ErrIdentityConflict
			}
			return db.Receipt{Count: count, Duplicates: count}, nil
		}
	}
	dedupeStarted := time.Now()
	fresh, duplicates, err := s.classify(ctx, index, events)
	dedupeMS = time.Since(dedupeStarted).Seconds() * 1000
	if err != nil {
		return db.Receipt{}, err
	}
	if s.maxPendingBytes > 0 && len(fresh) > 0 {
		pending, err := s.PendingBytes(ctx, index)
		if err != nil {
			return db.Receipt{}, err
		}
		size := int64(0)
		for _, event := range fresh {
			size += int64(len(event.Document))
		}
		// Permit one byte-bounded batch when empty so a single large event progresses.
		if pending > 0 && pending+size > s.maxPendingBytes {
			return db.Receipt{}, errQueueFull
		}
	}
	receipt.Duplicates = duplicates
	storeStarted := time.Now()
	err = s.storeFresh(ctx, index, fresh, owner, key, hash, receipt.Count)
	storeMS = time.Since(storeStarted).Seconds() * 1000
	if err != nil {
		return db.Receipt{}, err
	}
	return receipt, nil
}

func (s *Store) Pending(ctx context.Context, index string) (int64, error) {
	return s.redis.LLen(ctx, s.pendingKey(index)).Result()
}

func (s *Store) TargetPending(ctx context.Context, index, target string) (int64, error) {
	return s.counter(ctx, s.targetPendingKey(index, target))
}

func (s *Store) ActiveAccepted(ctx context.Context, index, stream string) (string, int64, bool) {
	gen, err := s.loadActive(ctx, index, stream)
	if err != nil || gen.Index == "" {
		return "", 0, false
	}
	return gen.Index, gen.Accepted, true
}

// NoteRotation seals the open generation recorded in Redis and forgets it.
// expected limits the seal to that index; an empty expected seals the active one.
// A different active index is left untouched so an older catalog row can close on its own.
func (s *Store) NoteRotation(ctx context.Context, index, stream, expected string) (bool, error) {
	owner, unlock, err := s.lock(ctx, index)
	if err != nil {
		return false, err
	}
	defer unlock()
	gen, err := s.loadActive(ctx, index, stream)
	if err != nil {
		return false, err
	}
	if gen.Index == "" || (expected != "" && gen.Index != expected) {
		return false, nil
	}
	if err := s.catalog.SealGeneration(ctx, gen.Index, gen.Accepted); err != nil {
		return false, err
	}
	if err := s.withIntakeLease(ctx, index, owner, func(pipe redis.Pipeliner) error { pipe.Del(ctx, s.activeKey(index, stream)); return nil }); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) Stats(ctx context.Context, namespace string, ids []string) (db.IngestStats, error) {
	result := db.IngestStats{CapturedAt: time.Now().UTC(), Streams: []db.StreamIngestStats{}}
	streams, err := s.catalog.ListStreams(ctx)
	if err != nil {
		return result, err
	}
	allowed := map[string]bool{}
	if ids != nil {
		for _, id := range ids {
			allowed[id] = true
		}
	}
	type streamCounters struct {
		stream            db.Stream
		accepted, pending *redis.StringCmd
	}
	counters := make([]streamCounters, 0, len(streams))
	// Ingestion updates both counters atomically. Read them in one Redis
	// transaction so a concurrent intake cannot split the snapshot.
	pipe := s.redis.TxPipeline()
	for _, stream := range streams {
		if ids != nil && !allowed[stream.ID] {
			continue
		}
		counters = append(counters, streamCounters{
			stream:   stream,
			accepted: pipe.Get(ctx, s.acceptedKey(namespace, stream.Name)),
			pending:  pipe.Get(ctx, s.pendingCountKey(namespace, stream.Name)),
		})
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return result, err
	}
	for _, values := range counters {
		accepted, err := readCounter(values.accepted)
		if err != nil {
			return result, err
		}
		pending, err := readCounter(values.pending)
		if err != nil {
			return result, err
		}
		if pending < 0 {
			pending = 0
		}
		if pending > accepted {
			return result, errors.New("pending stream messages exceed the durable accepted counter")
		}
		row := db.StreamIngestStats{ID: values.stream.ID, Name: values.stream.Name, Title: values.stream.Title}
		row.Accepted = accepted
		row.Pending = pending
		row.Published = accepted - pending
		result.Totals.Accepted += accepted
		result.Totals.Published += row.Published
		result.Totals.Pending += pending
		result.Streams = append(result.Streams, row)
	}
	sort.Slice(result.Streams, func(i, j int) bool { return result.Streams[i].Name < result.Streams[j].Name })
	return result, nil
}

type stored struct {
	hash, body, stream string
	version            int
}

func (s *Store) classify(ctx context.Context, index string, events []db.Event) ([]db.Event, int, error) {
	seen := map[string]db.Event{}
	fresh := make([]db.Event, 0, len(events))
	replays := make([]db.Event, 0)
	for _, event := range events {
		if event.HashVersion == 0 {
			event.HashVersion = 1
		}
		if _, ok := seen[event.ID]; ok {
			replays = append(replays, event)
			continue
		}
		seen[event.ID] = event
		fresh = append(fresh, event)
	}
	duplicates := len(replays)
	// One round trip for the whole batch. The caller commits Kafka offsets
	// only after Accept returns, so a failed lookup leaves the batch unread.
	found, err := s.loadEvents(ctx, index, fresh)
	if err != nil {
		return nil, 0, err
	}
	accepted := make([]db.Event, 0, len(fresh))
	for i, event := range fresh {
		if found[i] == nil {
			accepted = append(accepted, event)
			continue
		}
		match, err := s.sameIdentity(ctx, index, event, *found[i])
		if err != nil {
			return nil, 0, err
		}
		if !match {
			return nil, 0, db.ErrIdentityConflict
		}
		duplicates++
	}
	for _, event := range replays {
		current, ok := seen[event.ID]
		if !ok {
			return nil, 0, fmt.Errorf("accepted event %s was not stored", event.ID)
		}
		storedEvent := stored{hash: current.Hash, version: current.HashVersion, body: string(current.Document)}
		match, err := s.sameIdentity(ctx, index, event, storedEvent)
		if err != nil {
			return nil, 0, err
		}
		if !match {
			return nil, 0, db.ErrIdentityConflict
		}
	}
	return accepted, duplicates, nil
}

func (s *Store) sameIdentity(ctx context.Context, index string, event db.Event, current stored) (bool, error) {
	if current.hash == event.Hash {
		return true, nil
	}
	if current.version == 1 && event.LegacyIdentityHash() != "" && current.hash == event.LegacyIdentityHash() {
		return true, nil
	}
	if current.version == 2 && event.KafkaTransport {
		body := current.body
		if body == "" {
			values, err := s.redis.HMGet(ctx, s.eventKey(index, event.ID), "body", "body_size").Result()
			if err != nil {
				return false, err
			}
			value, _ := values[0].(string)
			size, _ := values[1].(string)
			raw, err := decodeBody(value, size)
			if err != nil {
				return false, err
			}
			body = string(raw)
		}
		if body != "" && db.KafkaReplayCompatible(payloadJSON(body), current.hash, event.Hash) {
			value, err := s.redis.Eval(ctx, `local hash=redis.call('hget',KEYS[1],'hash');if hash==ARGV[2] then return 1 end;if hash==ARGV[1] then redis.call('hset',KEYS[1],'hash',ARGV[2]);return 1 end;return 0`, []string{s.eventKey(index, event.ID)}, current.hash, event.Hash).Int()
			return value == 1, err
		}
	}
	return false, nil
}

func payloadJSON(document string) string {
	var doc struct {
		Payload string `json:"payload_json"`
	}
	_ = json.Unmarshal([]byte(document), &doc)
	return doc.Payload
}

func (s *Store) storeFresh(ctx context.Context, index string, events []db.Event, owner, key, hash string, count int) error {
	if len(events) == 0 && key == "" {
		return nil
	}
	grouped := map[string][]db.Event{}
	order := make([]string, 0)
	plain := make([]db.Event, 0)
	for _, event := range events {
		if event.StreamName == "" {
			plain = append(plain, event)
			continue
		}
		if _, ok := grouped[event.StreamName]; !ok {
			order = append(order, event.StreamName)
		}
		grouped[event.StreamName] = append(grouped[event.StreamName], event)
	}
	type placed struct {
		event  db.Event
		target string
	}
	rows := make([]placed, 0, len(events))
	active := map[string]Generation{}
	for _, name := range order {
		batch := grouped[name]
		targets, gen, err := s.place(ctx, index, name, len(batch))
		if err != nil {
			return err
		}
		active[name] = gen
		for i, event := range batch {
			event.TargetIndex = targets[i]
			rows = append(rows, placed{event, targets[i]})
		}
	}
	for _, event := range plain {
		rows = append(rows, placed{event, ""})
	}
	err := s.withIntakeLease(ctx, index, owner, func(pipe redis.Pipeliner) error {
		streamCounts := map[string]int64{}
		targetCounts := map[string]int64{}
		ids := make([]any, 0, len(rows))
		bodyBytes := int64(0)
		for _, row := range rows {
			bodyBytes += int64(len(row.event.Document))
			body, originalSize := encodeBody(row.event.Document)
			fields := []any{
				"hash", row.event.Hash,
				"ver", row.event.HashVersion,
				"body", body,
				"accepted_at_ns", time.Now().UnixNano(),
				"target", row.target,
				"stream", row.event.StreamName,
			}
			if originalSize != 0 {
				fields = append(fields, "body_size", originalSize)
			}
			pipe.HSet(ctx, s.eventKey(index, row.event.ID), fields...)
			ids = append(ids, row.event.ID)
			if row.event.StreamName != "" {
				streamCounts[row.event.StreamName]++
			}
			if row.target != "" {
				targetCounts[row.target]++
			}
		}
		if len(ids) > 0 {
			pipe.RPush(ctx, s.pendingKey(index), ids...)
			pipe.IncrBy(ctx, s.pendingBytesKey(index), bodyBytes)
		}
		for name, count := range streamCounts {
			pipe.IncrBy(ctx, s.acceptedKey(index, name), count)
			pipe.IncrBy(ctx, s.pendingCountKey(index, name), count)
		}
		for target, count := range targetCounts {
			pipe.IncrBy(ctx, s.targetPendingKey(index, target), count)
		}
		for name, gen := range active {
			pipe.HSet(ctx, s.activeKey(index, name),
				"index", gen.Index,
				"stream_id", gen.StreamID,
				"accepted", gen.Accepted,
				"generation", gen.Generation,
				"created", gen.Created.UnixNano(),
			)
		}
		if key != "" {
			pipe.HSet(ctx, s.batchKey(index, key), "hash", hash, "count", count)
		}
		return nil
	})
	if err == nil && s.telemetry != nil {
		bytes := 0
		for _, event := range events {
			bytes += event.InputBytes
		}
		s.telemetry.Accepted(len(events), bytes)
	}
	return err
}

func (s *Store) place(ctx context.Context, index, name string, count int) ([]string, Generation, error) {
	info, err := s.catalog.Stream(ctx, name)
	if err != nil {
		return nil, Generation{}, err
	}
	gen, err := s.loadActive(ctx, index, name)
	if err != nil {
		return nil, Generation{}, err
	}
	if gen.Index == "" {
		adopted, ok, err := s.catalog.ActiveGeneration(ctx, index, info.ID)
		if err != nil {
			return nil, Generation{}, err
		}
		if ok {
			gen = adopted
		} else {
			gen, err = s.open(ctx, index, info)
			if err != nil {
				return nil, Generation{}, err
			}
		}
	}
	targets := make([]string, 0, count)
	for count > 0 {
		if due(gen, info.Storage) {
			if err := s.rotate(ctx, index, info, &gen); err != nil {
				return nil, Generation{}, err
			}
		}
		take := count
		if info.Storage.MaxDocuments > 0 {
			room := info.Storage.MaxDocuments - gen.Accepted
			if room < 1 {
				if err := s.rotate(ctx, index, info, &gen); err != nil {
					return nil, Generation{}, err
				}
				continue
			}
			if int64(take) > room {
				take = int(room)
			}
		}
		gen.Accepted += int64(take)
		for i := 0; i < take; i++ {
			targets = append(targets, gen.Index)
		}
		count -= take
	}
	return targets, gen, nil
}

func due(gen Generation, storage db.StorageConfig) bool {
	if gen.Index == "" {
		return false
	}
	if storage.MaxDocuments > 0 && gen.Accepted >= storage.MaxDocuments {
		return true
	}
	return storage.MaxAgeHours > 0 && time.Since(gen.Created) >= time.Duration(storage.MaxAgeHours)*time.Hour
}

func (s *Store) rotate(ctx context.Context, index string, info StreamInfo, gen *Generation) error {
	if err := s.catalog.SealGeneration(ctx, gen.Index, gen.Accepted); err != nil {
		return err
	}
	next, err := s.open(ctx, index, info)
	if err != nil {
		return err
	}
	*gen = next
	return nil
}

func (s *Store) open(ctx context.Context, index string, info StreamInfo) (Generation, error) {
	generation, err := s.catalog.NextGeneration(ctx, index, info.ID)
	if err != nil {
		return Generation{}, err
	}
	gen := Generation{
		Index:      fmt.Sprintf("%s-s-%s-%06d", index, strings.ReplaceAll(info.ID, "-", ""), generation),
		StreamID:   info.ID,
		Generation: generation,
		Created:    time.Now().UTC(),
	}
	if err := s.catalog.OpenGeneration(ctx, index, info.ID, gen.Index, generation); err != nil {
		return Generation{}, err
	}
	return gen, nil
}

func (s *Store) loadEvents(ctx context.Context, index string, events []db.Event) ([]*stored, error) {
	found := make([]*stored, len(events))
	if len(events) == 0 {
		return found, nil
	}
	read := s.redis.Pipeline()
	commands := make([]*redis.SliceCmd, len(events))
	for i, event := range events {
		commands[i] = read.HMGet(ctx, s.eventKey(index, event.ID), "hash", "ver", "stream")
	}
	if _, err := read.Exec(ctx); err != nil {
		return nil, err
	}
	for i, cmd := range commands {
		values, err := cmd.Result()
		if err != nil {
			return nil, err
		}
		if values[0] == nil {
			continue
		}
		hash, _ := values[0].(string)
		version, _ := strconv.Atoi(fmt.Sprint(values[1]))
		stream, _ := values[2].(string)
		found[i] = &stored{hash: hash, version: version, stream: stream}
	}
	return found, nil
}

func (s *Store) loadActive(ctx context.Context, index, stream string) (Generation, error) {
	record, err := s.redis.HGetAll(ctx, s.activeKey(index, stream)).Result()
	if err != nil || record["index"] == "" {
		return Generation{}, err
	}
	accepted, _ := strconv.ParseInt(record["accepted"], 10, 64)
	generation, _ := strconv.ParseInt(record["generation"], 10, 64)
	created, _ := strconv.ParseInt(record["created"], 10, 64)
	return Generation{Index: record["index"], StreamID: record["stream_id"], Accepted: accepted, Generation: generation, Created: time.Unix(0, created).UTC()}, nil
}

func (s *Store) batch(ctx context.Context, index, key string) (string, int, bool, error) {
	record, err := s.redis.HGetAll(ctx, s.batchKey(index, key)).Result()
	if err != nil || record["hash"] == "" {
		return "", 0, false, err
	}
	count, _ := strconv.Atoi(record["count"])
	return record["hash"], count, true, nil
}

func (s *Store) counter(ctx context.Context, key string) (int64, error) {
	return readCounter(s.redis.Get(ctx, key))
}

func readCounter(cmd *redis.StringCmd) (int64, error) {
	value, err := cmd.Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(value, 10, 64)
}

func (s *Store) lock(ctx context.Context, index string) (string, func(), error) {
	token := make([]byte, 8)
	if _, err := rand.Read(token); err != nil {
		return "", nil, err
	}
	key := "logister:lock:" + index
	owner := hex.EncodeToString(token)
	for {
		ok, err := s.redis.SetNX(ctx, key, owner, 30*time.Second).Result()
		if err != nil {
			return "", nil, err
		}
		if ok {
			return owner, func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				s.redis.Eval(cleanup, `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) end return 0`, []string{key}, owner)
			}, nil
		}
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *Store) eventKey(index, id string) string { return "logister:evt:" + index + ":" + id }
func (s *Store) pendingKey(index string) string   { return "logister:pending:" + index }
func (s *Store) activeKey(index, stream string) string {
	return "logister:active:" + index + ":" + stream
}
func (s *Store) acceptedKey(index, stream string) string {
	return "logister:accepted:" + index + ":" + stream
}
func (s *Store) pendingCountKey(index, stream string) string {
	return "logister:pending-count:" + index + ":" + stream
}
func (s *Store) targetPendingKey(index, target string) string {
	return "logister:target-pending:" + index + ":" + target
}
func (s *Store) batchKey(index, key string) string { return "logister:batch:" + index + ":" + key }

var errIntakeLeaseLost = errors.New("intake lease lost")

// WATCH fences the whole transaction against lease expiry or a replacement
// owner between classification and EXEC. Body, counters and batch receipt commit
// together; a stale owner cannot overwrite a newer intake's records.
func (s *Store) withIntakeLease(ctx context.Context, index, owner string, write func(redis.Pipeliner) error) error {
	key := "logister:lock:" + index
	err := s.redis.Watch(ctx, func(tx *redis.Tx) error {
		current, err := tx.Get(ctx, key).Result()
		if errors.Is(err, redis.Nil) || (err == nil && current != owner) {
			return errIntakeLeaseLost
		}
		if err != nil {
			return err
		}
		_, err = tx.TxPipelined(ctx, write)
		return err
	}, key)
	if errors.Is(err, redis.TxFailedErr) {
		return fmt.Errorf("%w: transaction aborted", errIntakeLeaseLost)
	}
	return err
}

func (s *Store) SetTelemetry(m *telemetry.Meter) { s.telemetry = m }
