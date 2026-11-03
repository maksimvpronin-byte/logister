// kafka-load fills Kafka with unique messages; no scheduled events are skipped.
// Pause Logister first, produce the backlog, then resume to measure consumption.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

func main() {
	brokers := flag.String("brokers", "127.0.0.1:19092", "comma-separated Kafka brokers")
	topic := flag.String("topic", "logister-ingest", "topic")
	stream := flag.String("stream", "", "required existing Logister stream")
	count := flag.Int("count", 100000, "messages to produce")
	size := flag.Int("bytes", 512, "approximate message text bytes, excluding JSON metadata")
	run := flag.String("run", "", "unique run ID (default random)")
	flag.Parse()
	if *stream == "" || *count < 1 || *count > 10000000 || *size < 64 || *size > 16384 {
		fmt.Fprintln(os.Stderr, "require -stream, count 1..10000000, bytes 64..16384")
		os.Exit(2)
	}
	if *run == "" {
		var id [12]byte
		if _, err := rand.Read(id[:]); err != nil {
			panic(err)
		}
		*run = hex.EncodeToString(id[:])
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(*brokers, ",")...), kgo.ClientID("logister-kafka-load"), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.MaxBufferedRecords(5000), kgo.ProducerLinger(10*time.Millisecond), kgo.ProducerBatchMaxBytes(1<<20))
	if err != nil {
		panic(err)
	}
	defer cl.Close()
	var acknowledged, failed, bytes atomic.Int64
	start := time.Now()
	for i := 0; i < *count; i++ {
		if ctx.Err() != nil {
			break
		}
		entropy := make([]byte, (*size+1)/2)
		if _, err := rand.Read(entropy); err != nil {
			panic(err)
		}
		text := hex.EncodeToString(entropy)[:*size]
		raw, _ := json.Marshal(map[string]any{"id": fmt.Sprintf("%s-%d", *run, i), "message": text, "source": "kafka-load", "stream": *stream, "fields": map[string]any{"run": *run, "sequence": i}})
		record := &kgo.Record{Topic: *topic, Key: []byte(fmt.Sprint(i)), Value: raw}
		cl.Produce(ctx, record, func(record *kgo.Record, err error) {
			if err != nil {
				if failed.Add(1) == 1 {
					fmt.Fprintln(os.Stderr, "Kafka delivery error:", err)
				}
			} else {
				acknowledged.Add(1)
				bytes.Add(int64(len(record.Value)))
			}
		})
	}
	if err := cl.Flush(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "Kafka flush:", err)
	}
	result := map[string]any{"run": *run, "topic": *topic, "stream": *stream, "requested": *count, "acknowledged": acknowledged.Load(), "failed": failed.Load(), "bytes": bytes.Load(), "seconds": time.Since(start).Seconds(), "producer_eps": float64(acknowledged.Load()) / time.Since(start).Seconds()}
	raw, _ := json.Marshal(result)
	fmt.Println(string(raw))
	if acknowledged.Load() != int64(*count) || failed.Load() != 0 {
		os.Exit(1)
	}
}
