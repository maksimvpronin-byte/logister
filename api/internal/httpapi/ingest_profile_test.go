package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/logister/logister/api/internal/config"
	"github.com/logister/logister/api/internal/db"
	"github.com/logister/logister/api/internal/kafkainput"
)

type profileAcceptor struct{}

func (profileAcceptor) Accept(_ context.Context, _ string, _ string, _ string, events []db.Event) (db.Receipt, error) {
	return db.Receipt{Count: len(events)}, nil
}

// Isolates JSON parsing, canonical hashing, normalization and document creation.
// Excludes Kafka/Redis/OpenSearch I/O and is not an end-to-end throughput claim.
func BenchmarkKafkaNormalize10KiB(b *testing.B) {
	const records = 1000
	messages := make([]kafkainput.Message, records)
	total := 0
	for i := range messages {
		raw, _ := json.Marshal(map[string]any{"id": fmt.Sprint(i), "message": strings.Repeat("0123456789abcdef", 640), "stream": "profile", "source": "kafka-load", "fields": map[string]any{"sequence": i}})
		messages[i] = kafkainput.Message{Topic: "input", Offset: int64(i), Value: raw}
		total += len(raw)
	}
	s := NewServer(config.Config{OpenSearchIndex: "profile"}, nil, nil)
	s.store = profileAcceptor{}
	s.catalog = &fakeCatalog{streams: []db.Stream{{ID: "profile", Name: "profile", Title: "Profile"}}}
	b.SetBytes(int64(total))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.acceptKafka(context.Background(), messages); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N*records)/b.Elapsed().Seconds(), "logs/s")
}
