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
	"time"

	"github.com/logister/logister/api/internal/db"
	"github.com/redis/go-redis/v9"
)

// PublishBatch is how many pending messages one publication reads and sends.
const PublishBatch = 1000

// Store is the ingest journal. PostgreSQL keeps stream settings and the index
// catalog; message bodies and the queue waiting for OpenSearch live here.
// After a successful publication the body is deleted and the identity hash stays.
type Store struct {
	redis   *redis.Client
	catalog Catalog
}

func New(client *redis.Client, catalog Catalog) *Store {
	return &Store{redis: client, catalog: catalog}
}

func (s *Store) Ping(ctx context.Context) error { return s.redis.Ping(ctx).Err() }

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
	return nil
}

func (s *Store) Accept(ctx context.Context, index, key, hash string, events []db.Event) (db.Receipt, error) {
	receipt := db.Receipt{Count: len(events)}
	if len(events) == 0 && key == "" {
		return receipt, nil
	}
	unlock, err := s.lock(ctx, index)
	if err != nil {
		return db.Receipt{}, err
	}
	defer unlock()
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
	fresh, duplicates, err := s.classify(ctx, index, events)
	if err != nil {
		return db.Receipt{}, err
	}
	receipt.Duplicates = duplicates
	if err := s.storeFresh(ctx, index, fresh); err != nil {
		return db.Receipt{}, err
	}
	if key != "" {
		if err := s.redis.HSet(ctx, s.batchKey(index, key), "hash", hash, "count", receipt.Count).Err(); err != nil {
			return db.Receipt{}, err
		}
	}
	return receipt, nil
}

func (s *Store) Publish(ctx context.Context, index string, write func([]db.Event) error) (int, error) {
	ids, err := s.redis.LRange(ctx, s.pendingKey(index), 0, int64(PublishBatch-1)).Result()
	if err != nil || len(ids) == 0 {
		return 0, err
	}
	read := s.redis.Pipeline()
	commands := make([]*redis.MapStringStringCmd, len(ids))
	for i, id := range ids {
		commands[i] = read.HGetAll(ctx, s.eventKey(index, id))
	}
	if _, err := read.Exec(ctx); err != nil {
		return 0, err
	}
	events := make([]db.Event, 0, len(ids))
	kept := make([]string, 0, len(ids))
	for i, id := range ids {
		record, err := commands[i].Result()
		if err != nil {
			return 0, err
		}
		if record["body"] == "" {
			kept = append(kept, id)
			continue
		}
		events = append(events, db.Event{ID: id, Document: json.RawMessage(record["body"]), TargetIndex: record["target"], StreamName: record["stream"]})
		kept = append(kept, id)
	}
	if len(events) > 0 {
		if err := write(events); err != nil {
			return 0, err
		}
	}
	pipe := s.redis.TxPipeline()
	for _, event := range events {
		pipe.HDel(ctx, s.eventKey(index, event.ID), "body")
		if event.StreamName != "" {
			pipe.Decr(ctx, s.pendingCountKey(index, event.StreamName))
		}
		if event.TargetIndex != "" {
			pipe.Decr(ctx, s.targetPendingKey(index, event.TargetIndex))
		}
	}
	for _, id := range kept {
		pipe.LRem(ctx, s.pendingKey(index), 1, id)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return len(events), nil
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
	unlock, err := s.lock(ctx, index)
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
	if err := s.redis.Del(ctx, s.activeKey(index, stream)).Err(); err != nil {
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
	for _, stream := range streams {
		if ids != nil && !allowed[stream.ID] {
			continue
		}
		accepted, err := s.counter(ctx, s.acceptedKey(namespace, stream.Name))
		if err != nil {
			return result, err
		}
		pending, err := s.counter(ctx, s.pendingCountKey(namespace, stream.Name))
		if err != nil {
			return result, err
		}
		if pending < 0 {
			pending = 0
		}
		if pending > accepted {
			return result, errors.New("pending stream messages exceed the durable accepted counter")
		}
		row := db.StreamIngestStats{ID: stream.ID, Name: stream.Name, Title: stream.Title}
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
	accepted := make([]db.Event, 0, len(fresh))
	for _, event := range fresh {
		current, ok, err := s.loadEvent(ctx, index, event.ID)
		if err != nil {
			return nil, 0, err
		}
		if !ok {
			accepted = append(accepted, event)
			continue
		}
		match, err := s.sameIdentity(ctx, index, event, current)
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
	if current.version == 1 && event.LegacyHash != "" && current.hash == event.LegacyHash {
		return true, nil
	}
	if current.version == 2 && event.KafkaTransport && current.body != "" && db.KafkaReplayCompatible(payloadJSON(current.body), current.hash, event.Hash) {
		return true, s.redis.HSet(ctx, s.eventKey(index, event.ID), "hash", event.Hash).Err()
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

func (s *Store) storeFresh(ctx context.Context, index string, events []db.Event) error {
	if len(events) == 0 {
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
	pipe := s.redis.TxPipeline()
	streamCounts := map[string]int64{}
	targetCounts := map[string]int64{}
	ids := make([]any, 0, len(rows))
	for _, row := range rows {
		pipe.HSet(ctx, s.eventKey(index, row.event.ID),
			"hash", row.event.Hash,
			"ver", row.event.HashVersion,
			"body", string(row.event.Document),
			"target", row.target,
			"stream", row.event.StreamName,
		)
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
	_, err := pipe.Exec(ctx)
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

func (s *Store) loadEvent(ctx context.Context, index, id string) (stored, bool, error) {
	record, err := s.redis.HGetAll(ctx, s.eventKey(index, id)).Result()
	if err != nil || len(record) == 0 {
		return stored{}, false, err
	}
	version, _ := strconv.Atoi(record["ver"])
	return stored{hash: record["hash"], body: record["body"], stream: record["stream"], version: version}, true, nil
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
	value, err := s.redis.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(value, 10, 64)
}

func (s *Store) lock(ctx context.Context, index string) (func(), error) {
	token := make([]byte, 8)
	if _, err := rand.Read(token); err != nil {
		return nil, err
	}
	key := "logister:lock:" + index
	owner := hex.EncodeToString(token)
	for {
		ok, err := s.redis.SetNX(ctx, key, owner, 30*time.Second).Result()
		if err != nil {
			return nil, err
		}
		if ok {
			return func() {
				s.redis.Eval(context.Background(), `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) end return 0`, []string{key}, owner)
			}, nil
		}
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
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
