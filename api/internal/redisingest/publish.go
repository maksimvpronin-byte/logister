package redisingest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/logister/logister/api/internal/db"
	"github.com/redis/go-redis/v9"
	"strconv"
	"time"
)

func (s *Store) SetPublishMaxBytes(value int) { s.maxPublishBytes = value }

// A fenced lease owns the queue prefix. Concurrent publishers cannot read and
// acknowledge the same prefix; a late expired owner can never remove new work.
func (s *Store) publishLease(ctx context.Context, index string) (context.Context, string, func(), error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return nil, "", nil, err
	}
	owner := hex.EncodeToString(raw)
	key := "logister:publish-lock:" + index
	ok, err := s.redis.SetNX(ctx, key, owner, 30*time.Second).Result()
	if err != nil || !ok {
		// A healthy peer already owns publication. The worker retries on its
		// idle interval, without polling Redis or reporting a storage error.
		return nil, "", nil, err
	}
	work, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		timer := time.NewTicker(10 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-work.Done():
				return
			case <-timer.C:
				value, err := s.redis.Eval(work, `if redis.call('get',KEYS[1])==ARGV[1] then return redis.call('pexpire',KEYS[1],30000) end return 0`, []string{key}, owner).Int()
				if err != nil || value != 1 {
					cancel()
					return
				}
			}
		}
	}()
	release := func() {
		cancel()
		<-done
		cleanup, finish := context.WithTimeout(context.Background(), 2*time.Second)
		defer finish()
		s.redis.Eval(cleanup, `if redis.call('get',KEYS[1])==ARGV[1] then return redis.call('del',KEYS[1]) end return 0`, []string{key}, owner)
	}
	return work, owner, release, nil
}

const bodyLengthLua = `
local function bodyLength(key)
 local size=redis.call('hget',key,'body_size')
 if size then return tonumber(size) end
 return redis.call('hstrlen',key,'body')
end
`

// Stop at the byte limit before fetching bodies or probing the rest of the window.
const selectPublicationPrefix = bodyLengthLua + `
local offset=tonumber(ARGV[2])
local ids=redis.call('lrange',KEYS[1],offset,offset+tonumber(ARGV[3])-1)
local limit=tonumber(ARGV[4])
local selected={}
local total=0
for _,id in ipairs(ids) do
 local size=bodyLength(ARGV[1]..id)
 if size>0 and total>0 and total+size>limit then break end
 total=total+size
 selected[#selected+1]=id
end
return selected
`

const acknowledgePrefix = `
if redis.call('get',KEYS[1])~=ARGV[1] then return redis.error_reply('publication lease lost') end
local n=tonumber(ARGV[2]);local counters=tonumber(ARGV[3])
local head=redis.call('lrange',KEYS[2],0,n-1)
for i=1,n do
 if head[i]~=ARGV[3+i] then return redis.error_reply('publication prefix changed') end
 local length=redis.call('hstrlen',KEYS[2+i],'body')
 if (length>0 and '1' or '0')~=ARGV[3+n+i] then return redis.error_reply('publication body changed') end
end
for i=1,counters do
 local current=tonumber(redis.call('get',KEYS[2+n+i]) or '0')
 if not current or current<tonumber(ARGV[3+2*n+i]) then return redis.error_reply('publication counter mismatch') end
end
for i=1,n do if ARGV[3+n+i]=='1' then redis.call('hdel',KEYS[2+i],'body','body_size','accepted_at_ns') end end
for i=1,counters do redis.call('decrby',KEYS[2+n+i],ARGV[3+2*n+i]) end
redis.call('ltrim',KEYS[2],n,-1)
return n
`

type publicationBatch struct {
	ids    []string
	flags  []string
	counts map[string]int
	events []db.Event
	readMS float64
}

func (s *Store) readPublication(ctx context.Context, index string, offset int64) (publicationBatch, error) {
	started := time.Now()
	limit := s.publicationLimit()
	ids, err := s.redis.Eval(ctx, selectPublicationPrefix, []string{s.pendingKey(index)}, s.eventKey(index, ""), offset, PublishBatch, limit).StringSlice()
	if err != nil || len(ids) == 0 {
		return publicationBatch{}, err
	}
	pipe := s.redis.Pipeline()
	commands := make([]*redis.MapStringStringCmd, len(ids))
	for i, id := range ids {
		commands[i] = pipe.HGetAll(ctx, s.eventKey(index, id))
	}
	if _, err = pipe.Exec(ctx); err != nil {
		return publicationBatch{}, err
	}

	events := make([]db.Event, 0, len(ids))
	kept := make([]string, 0, len(ids))
	flags := make([]string, 0, len(ids))
	counts := map[string]int{}
	total := 0
	for i, id := range ids {
		record, err := commands[i].Result()
		if err != nil {
			return publicationBatch{}, err
		}
		body, err := decodeBody(record["body"], record["body_size"])
		if err != nil {
			return publicationBatch{}, err
		}
		if len(body) != 0 && len(events) > 0 && total+len(body) > limit {
			break
		}

		kept = append(kept, id)
		flag := "0"
		if len(body) != 0 {
			flag = "1"
			total += len(body)
			acceptedAt, _ := strconv.ParseInt(record["accepted_at_ns"], 10, 64)
			event := db.Event{AcceptedAtNS: acceptedAt, ID: id, Document: body, TargetIndex: record["target"], StreamName: record["stream"]}
			events = append(events, event)
			counts[s.pendingBytesKey(index)] += len(body)
			if event.StreamName != "" {
				counts[s.pendingCountKey(index, event.StreamName)]++
			}
			if event.TargetIndex != "" {
				counts[s.targetPendingKey(index, event.TargetIndex)]++
			}
		}
		flags = append(flags, flag)
	}
	if len(kept) == 0 {
		return publicationBatch{}, errors.New("publication made no progress")
	}
	return publicationBatch{ids: kept, flags: flags, counts: counts, events: events, readMS: time.Since(started).Seconds() * 1000}, nil
}
func (s *Store) acknowledgePublication(ctx context.Context, index, owner string, batch publicationBatch) error {
	keys := []string{"logister:publish-lock:" + index, s.pendingKey(index)}
	for _, id := range batch.ids {
		keys = append(keys, s.eventKey(index, id))
	}
	args := []any{owner, len(batch.ids), len(batch.counts)}
	for _, id := range batch.ids {
		args = append(args, id)
	}
	for _, flag := range batch.flags {
		args = append(args, flag)
	}
	for key, count := range batch.counts {
		keys = append(keys, key)
		args = append(args, strconv.Itoa(count))
	}
	if _, err := s.redis.Eval(ctx, acknowledgePrefix, keys, args...).Result(); err != nil {
		return fmt.Errorf("publication acknowledgement: %w", err)
	}
	if s.telemetry != nil {
		var oldest int64
		for _, event := range batch.events {
			if event.AcceptedAtNS > 0 && (oldest == 0 || event.AcceptedAtNS < oldest) {
				oldest = event.AcceptedAtNS
			}
		}
		if oldest > 0 {
			s.telemetry.Observe("delivery", max(0, time.Since(time.Unix(0, oldest))), false)
		}
	}
	return nil
}
func (s *Store) SetOutputBufferBytes(bytes int) { s.outputBufferBytes = bytes }
func (s *Store) publicationLimit() int {
	if s.maxPublishBytes > 0 {
		return s.maxPublishBytes
	}
	return 10 << 20
}
func (s *Store) Publish(ctx context.Context, index string, write func([]db.Event) error) (n int, err error) {
	if !s.ready() {
		return 0, nil
	}
	if s.telemetry != nil {
		started := time.Now()
		defer func() {
			if n > 0 || err != nil {
				s.telemetry.Observe("publish", time.Since(started), err != nil)
			}
			if n > 0 {
				s.telemetry.Published(n)
			}
		}()
	}
	slots := min(s.outputBufferBytes/s.publicationLimit(), 8)
	if slots >= 2 {
		return s.publishBuffered(ctx, index, slots, write)
	}
	return s.publishSerial(ctx, index, write)
}
func (s *Store) publishSerial(ctx context.Context, index string, write func([]db.Event) error) (int, error) {
	started := time.Now()
	leaseStarted := time.Now()
	work, owner, release, err := s.publishLease(ctx, index)
	leaseMS := time.Since(leaseStarted).Seconds() * 1000
	if err != nil || work == nil {
		return 0, err
	}
	defer release()
	batch, err := s.readPublication(work, index, 0)
	if err != nil || len(batch.ids) == 0 {
		return 0, err
	}
	writeStarted := time.Now()
	if len(batch.events) > 0 {
		if err = write(batch.events); err != nil {
			return 0, err
		}
	}
	writeMS := time.Since(writeStarted).Seconds() * 1000
	ackStarted := time.Now()
	if err = s.acknowledgePublication(work, index, owner, batch); err != nil {
		return 0, err
	}
	s.trace("publish", map[string]any{"documents": len(batch.events), "ms": time.Since(started).Seconds() * 1000, "lease_ms": leaseMS, "read_ms": batch.readMS, "write_ms": writeMS, "ack_ms": time.Since(ackStarted).Seconds() * 1000, "buffer_slots": 1})
	return len(batch.events), nil
}
