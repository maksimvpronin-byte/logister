// Package kafkainput connects Kafka to Logister's durable ingestion pipeline.
package kafkainput

import (
	"context"
	"fmt"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"sync"
	"time"
)

type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Value     []byte
	Key       []byte
}
type Accept func(context.Context, []Message) error
type StateStore interface {
	LoadPause(context.Context) (bool, error)
	SavePause(context.Context, bool) error
}
type Config struct {
	Brokers      []string
	Topic, Group string
	BatchSize    int
}
type Consumer struct {
	cfg             Config
	admin           *kadm.Client
	client          *kgo.Client
	accept          Accept
	state           StateStore
	mu              sync.Mutex
	paused, running bool
	lastError       string
}
type Status struct {
	Enabled        bool      `json:"enabled"`
	Topic          string    `json:"topic"`
	Group          string    `json:"group"`
	PauseRequested bool      `json:"pause_requested"`
	Running        bool      `json:"running"`
	Error          string    `json:"error,omitempty"`
	Available      int64     `json:"available"`
	Committed      int64     `json:"committed"`
	Lag            int64     `json:"lag"`
	RetentionLoss  bool      `json:"retention_loss"`
	CapturedAt     time.Time `json:"captured_at"`
}

func New(ctx context.Context, cfg Config, state StateStore, accept Accept) (*Consumer, error) {
	if len(cfg.Brokers) == 0 || cfg.Topic == "" || cfg.Group == "" || cfg.BatchSize < 1 || cfg.BatchSize > 1000 {
		return nil, fmt.Errorf("invalid Kafka input configuration")
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.ClientID("logister-admin"))
	if err != nil {
		return nil, err
	}
	paused, err := state.LoadPause(ctx)
	if err != nil {
		client.Close()
		return nil, err
	}
	return &Consumer{cfg: cfg, client: client, admin: kadm.NewClient(client), state: state, accept: accept, paused: paused}, nil
}
func (c *Consumer) Close()       { c.client.Close() }
func (c *Consumer) Paused() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.paused }
func (c *Consumer) SetPaused(ctx context.Context, value bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.state.SavePause(ctx, value); err != nil {
		return err
	}
	c.paused = value
	return nil
}
func (c *Consumer) setError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err == nil {
		c.lastError = ""
	} else {
		c.lastError = err.Error()
	}
}
func (c *Consumer) setRunning(value bool) { c.mu.Lock(); defer c.mu.Unlock(); c.running = value }
func wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
func (c *Consumer) Run(ctx context.Context) {
	defer c.setRunning(false)
	for ctx.Err() == nil {
		if c.Paused() {
			if !wait(ctx, 100*time.Millisecond) {
				return
			}
			continue
		}
		err := c.consume(ctx)
		c.setRunning(false)
		if err != nil && ctx.Err() == nil {
			log.Printf("Kafka input retry: %v", err)
			c.setError(err)
			if !wait(ctx, time.Second) {
				return
			}
		}
	}
}
func (c *Consumer) consume(ctx context.Context) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.cfg.Brokers...), kgo.ClientID("logister-input"), kgo.ConsumerGroup(c.cfg.Group), kgo.ConsumeTopics(c.cfg.Topic), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.ConsumeResetOffset(kgo.NoResetOffset().AtStart()), kgo.FetchMaxBytes(8<<20), kgo.FetchMaxWait(100*time.Millisecond))
	if err != nil {
		return err
	}
	defer cl.Close()
	c.setRunning(true)
	for ctx.Err() == nil && !c.Paused() {
		pollCtx, cancel := context.WithTimeout(ctx, time.Second)
		fetches := cl.PollRecords(pollCtx, c.cfg.BatchSize)
		cancel()
		if ctx.Err() != nil {
			cl.AllowRebalance()
			return ctx.Err()
		}
		records := fetches.Records()
		for _, e := range fetches.Errors() {
			if e.Err != context.DeadlineExceeded && e.Err != context.Canceled {
				cl.AllowRebalance()
				return fmt.Errorf("Kafka fetch %s/%d: %w", e.Topic, e.Partition, e.Err)
			}
		}
		if len(records) == 0 {
			cl.AllowRebalance()
			continue
		}
		messages := make([]Message, len(records))
		for i, r := range records {
			messages[i] = Message{r.Topic, r.Partition, r.Offset, r.Value, r.Key}
		}
		task, done := context.WithTimeout(ctx, 30*time.Second)
		err = c.accept(task, messages)
		done()
		if err == nil {
			commitCtx, finish := context.WithTimeout(ctx, 10*time.Second)
			err = cl.CommitRecords(commitCtx, records...)
			finish()
		}
		cl.AllowRebalance()
		// On any failure, close and rejoin from committed offsets. Never advance
		// past a rejected batch. A PG commit followed by a failed offset commit is
		// replayed with stable IDs and therefore cannot create a second document.
		if err != nil {
			return err
		}
		c.setError(nil)
	}
	return nil
}
func (c *Consumer) Status(ctx context.Context) (Status, error) {
	c.mu.Lock()
	out := Status{Enabled: true, Topic: c.cfg.Topic, Group: c.cfg.Group, PauseRequested: c.paused, Running: c.running, Error: c.lastError, CapturedAt: time.Now().UTC()}
	c.mu.Unlock()
	starts, err := c.admin.ListStartOffsets(ctx, c.cfg.Topic)
	if err != nil {
		return out, err
	}
	if err = starts.Error(); err != nil {
		return out, err
	}
	ends, err := c.admin.ListEndOffsets(ctx, c.cfg.Topic)
	if err != nil {
		return out, err
	}
	if err = ends.Error(); err != nil {
		return out, err
	}
	committed, err := c.admin.FetchOffsets(ctx, c.cfg.Group)
	if err != nil {
		return out, err
	}
	if err = committed.Error(); err != nil {
		return out, err
	}
	for partition, end := range ends[c.cfg.Topic] {
		start, ok := starts.Lookup(c.cfg.Topic, partition)
		if !ok {
			return out, fmt.Errorf("Kafka start offset missing")
		}
		at := start.Offset
		if value, ok := committed.Lookup(c.cfg.Topic, partition); ok && value.At >= 0 {
			at = value.At
			if at < start.Offset {
				out.RetentionLoss = true
				at = start.Offset
			}
		}
		if at > end.Offset {
			out.RetentionLoss = true
			at = end.Offset
		}
		out.Available += end.Offset - start.Offset
		out.Committed += at - start.Offset
		out.Lag += end.Offset - at
	}
	return out, nil
}
