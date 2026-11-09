// kafka-direct-bench is an isolated transport experiment, not the production
// journal. It reads a dedicated topic, uses the application normalizer and
// writes one fixed test index with request-durable ACKs before Kafka commits.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/logister/logister/api/internal/config"
	"github.com/logister/logister/api/internal/db"
	"github.com/logister/logister/api/internal/httpapi"
	"github.com/logister/logister/api/internal/kafkainput"
	"github.com/logister/logister/api/internal/search"
	"github.com/twmb/franz-go/pkg/kgo"
	"net/http"
	"os"
	"strings"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	topic := flag.String("topic", "", "dedicated directbench-* topic")
	index := flag.String("index", "", "dedicated directbench-* index")
	count := flag.Int("count", 100000, "expected records")
	workers := flag.Int("workers", 2, "parallel bulk requests")
	bytes := flag.Int("bulk-bytes", 5<<20, "bulk bytes")
	flag.Parse()
	if !strings.HasPrefix(*topic, "directbench-") || !strings.HasPrefix(*index, "directbench-") || *count < 1 {
		return fmt.Errorf("dedicated directbench-* topic/index and positive count required")
	}
	cfg := config.FromEnv()
	cfg.OpenSearchIndex = search.StorageIndex(cfg.OpenSearchIndex)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	database, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer database.Close()
	client, err := search.NewClient(cfg.OpenSearchURL)
	if err != nil {
		return err
	}
	client.ConfigureBulk(*workers, *bytes)
	normalizer := httpapi.NewServer(cfg, database, client)
	reader, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(cfg.KafkaBrokers, ",")...), kgo.ConsumerGroup(*topic), kgo.ConsumeTopics(*topic), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.ConsumeResetOffset(kgo.NoResetOffset().AtStart()), kgo.FetchMaxBytes(32<<20), kgo.FetchMaxPartitionBytes(16<<20))
	if err != nil {
		return err
	}
	defer reader.Close()
	started := time.Now()
	processed := 0
	for processed < *count {
		fetch := reader.PollRecords(ctx, 1000)
		if ctx.Err() != nil {
			reader.AllowRebalance()
			return ctx.Err()
		}
		if errs := fetch.Errors(); len(errs) > 0 {
			reader.AllowRebalance()
			return errs[0].Err
		}
		records := fetch.Records()
		if len(records) == 0 {
			reader.AllowRebalance()
			continue
		}
		messages := make([]kafkainput.Message, len(records))
		for i, r := range records {
			messages[i] = kafkainput.Message{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: r.Key, Value: r.Value}
		}
		events, err := normalizer.PrepareKafka(ctx, messages)
		if err == nil {
			for i := range events {
				events[i].TargetIndex = *index
			}
			err = client.PublishEventsDurable(ctx, *index+"-namespace", events)
		}
		if err == nil {
			err = reader.CommitRecords(ctx, records...)
		}
		reader.AllowRebalance()
		if err != nil {
			return err
		}
		processed += len(records)
	}
	elapsed := time.Since(started).Seconds()
	total := int64(-1)
	// Wait for natural refresh; report delivery and search visibility separately.
	for i := 0; i < 65; i++ {
		req, _ := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(cfg.OpenSearchURL, "/")+"/"+*index+"/_count", nil)
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		var parsed struct {
			Count int64 `json:"count"`
		}
		err = json.NewDecoder(response.Body).Decode(&parsed)
		response.Body.Close()
		if err != nil {
			return err
		}
		total = parsed.Count
		if total == int64(*count) {
			break
		}
		time.Sleep(time.Second)
	}
	if total != int64(*count) || processed != *count {
		return fmt.Errorf("count mismatch: processed=%d search=%d expected=%d", processed, total, *count)
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"mode": "direct-experiment", "topic": *topic, "index": *index, "processed": processed, "search_count": total, "delivery_seconds": elapsed, "delivery_eps": float64(processed) / elapsed, "visible_seconds": time.Since(started).Seconds(), "translog_durability": "request", "bulk_workers": *workers, "bulk_max_bytes": *bytes})
}
