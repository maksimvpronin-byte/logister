package search

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/logister/logister/api/internal/db"
	"golang.org/x/sync/errgroup"
	"log"
	"net/url"
	"sort"
	"time"
)

func (c *Client) ConfigureCodec(codec string, level int) error {
	switch codec {
	case "", "default":
		c.indexCodec = "default"
		c.codecLevel = 0
		return nil
	case "zstd", "zstd_no_dict":
		if level < 1 || level > 6 {
			return fmt.Errorf("codec compression level must be between 1 and 6")
		}
		c.indexCodec = codec
		c.codecLevel = level
		return nil
	default:
		return fmt.Errorf("unsupported index codec %q", codec)
	}
}

func (c *Client) ConfigureShards(shards int) { c.indexShards = max(1, min(shards, 16)) }

func (c *Client) EnableBulkProfile(enabled bool) { c.bulkProfile = enabled }

func (c *Client) ConfigureBulk(workers, maxBytes int) {
	if workers < 1 {
		workers = 1
	}
	if maxBytes < 1 {
		maxBytes = 5 << 20
	}
	c.bulkWorkers = min(workers, 16)
	c.bulkBytes = maxBytes
}
func (c *Client) prepareOnce(ctx context.Context, key string, prepare func() error) error {
	c.preparedMu.Lock()
	until := c.prepared[key]
	c.preparedMu.Unlock()
	if time.Now().Before(until) {
		return nil
	}
	_, err, _ := c.prepareGroup.Do(key, func() (any, error) {
		c.preparedMu.Lock()
		until := c.prepared[key]
		c.preparedMu.Unlock()
		if time.Now().Before(until) {
			return nil, nil
		}
		if err := prepare(); err != nil {
			return nil, err
		}
		c.preparedMu.Lock()
		if c.prepared == nil || len(c.prepared) >= 512 {
			c.prepared = map[string]time.Time{}
		}
		c.prepared[key] = time.Now().Add(5 * time.Minute)
		c.preparedMu.Unlock()
		return nil, nil
	})
	return err
}
func (c *Client) forgetIndex(index string) {
	c.preparedMu.Lock()
	delete(c.prepared, index)
	delete(c.prepared, index+":durable")
	c.preparedMu.Unlock()
}
func (c *Client) prepareDurableIndex(ctx context.Context, namespace, index string) error {
	return c.prepareOnce(ctx, index+":durable", func() error {
		var err error
		if index == namespace {
			err = c.setRequestDurability(ctx, index)
		} else {
			err = c.ensureStreamIndex(ctx, namespace, index, true)
		}
		if err != nil {
			return err
		}
		// Templates can assign replicas after index creation. Wait until the
		// index is green and shard movement has settled before sending bulks.
		status, raw, err := c.jsonRequest(ctx, "GET", "/_cluster/health/"+url.PathEscape(index)+"?wait_for_status=green&wait_for_no_relocating_shards=true&timeout=20s", nil)
		if err != nil {
			return err
		}
		if status != 200 {
			return fmt.Errorf("wait for durable index %s: %d %s", index, status, raw)
		}
		var response struct {
			Status   string `json:"status"`
			TimedOut bool   `json:"timed_out"`
		}
		if err := json.Unmarshal(raw, &response); err != nil {
			return err
		}
		if response.Status != "green" || response.TimedOut {
			return fmt.Errorf("durable index %s is not ready: %s", index, response.Status)
		}
		return nil
	})
}

func (c *Client) setRequestDurability(ctx context.Context, index string) error {
	status, raw, err := c.jsonRequest(ctx, "PUT", "/"+url.PathEscape(index)+"/_settings", map[string]any{"index.translog.durability": "request"})
	if err != nil {
		return err
	}
	if status != 200 {
		return fmt.Errorf("prepare durable index: %d %s", status, raw)
	}
	var response struct {
		Acknowledged bool `json:"acknowledged"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	if !response.Acknowledged {
		return fmt.Errorf("durable index settings were not acknowledged")
	}
	return nil
}

type bulkJob struct {
	index  string
	events []db.Event
}

// Replay this job with the same IDs while the Kafka consumer still owns its
// partitions. The caller's batch deadline also applies to all attempts.
func (c *Client) writeBulkJob(ctx context.Context, job bulkJob, durable bool) error {
	const maxRetries = 5
	delay := 100 * time.Millisecond
	for attempt := 0; ; attempt++ {
		err := c.indexEvents(ctx, job.index, job.events, durable)
		if attempt == maxRetries || !errors.Is(err, errIncompleteShardAcknowledgment) {
			if c.bulkProfile && attempt > 0 && err == nil {
				log.Printf("durable bulk shard acknowledgments recovered: index=%s retries=%d", job.index, attempt)
			}
			return err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		delay *= 2
	}
}

func (c *Client) publishEvents(ctx context.Context, namespace string, events []db.Event, durable bool) error {
	grouped := map[string][]db.Event{}
	for _, event := range events {
		target := event.TargetIndex
		if target == "" {
			target = namespace
		}
		grouped[target] = append(grouped[target], event)
	}
	names := make([]string, 0, len(grouped))
	for name := range grouped {
		names = append(names, name)
	}
	sort.Strings(names)
	maxBytes := c.bulkBytes
	if maxBytes < 1 {
		maxBytes = 5 << 20
	}
	jobs := []bulkJob{}
	for _, name := range names {
		items := grouped[name]
		for start := 0; start < len(items); {
			end, total := start, 0
			for end < len(items) {
				size := len(items[end].Document) + len(items[end].ID) + len(name) + 80
				if end > start && total+size > maxBytes {
					break
				}
				total += size
				end++
			}
			jobs = append(jobs, bulkJob{name, items[start:end]})
			start = end
		}
	}
	workers := c.bulkWorkers
	if workers < 1 {
		workers = 1
	}
	group, work := errgroup.WithContext(ctx)
	group.SetLimit(workers)
	for _, job := range jobs {
		job := job
		group.Go(func() error {
			if durable {
				if err := c.prepareDurableIndex(work, namespace, job.index); err != nil {
					return err
				}
			} else if job.index != namespace {
				if err := c.EnsureStreamIndex(work, namespace, job.index); err != nil {
					return err
				}
			}
			return c.writeBulkJob(work, job, durable)
		})
	}
	return group.Wait()
}
