package config

import "testing"

func TestKafkaDirectWithOptionalRedis(t *testing.T) {
	t.Setenv("REDIS_URL", "")
	t.Setenv("KAFKA_DIRECT", "false")
	if cfg := FromEnv(); cfg.KafkaOnly() || cfg.RedisURL == "" {
		t.Fatal("legacy intake lost its Redis default")
	}
	t.Setenv("KAFKA_DIRECT", "true")
	if cfg := FromEnv(); !cfg.KafkaOnly() {
		t.Fatal("Kafka Direct requires Redis when no Redis URL was configured")
	}
	t.Setenv("REDIS_URL", "redis://journal:6379")
	if cfg := FromEnv(); cfg.KafkaOnly() {
		t.Fatal("Kafka Direct disabled the explicitly configured HTTP journal")
	}
}

func TestOutputBufferCanBeDisabledWithoutDisablingOtherBudgets(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  int
	}{{"", 32 << 20}, {"0", 0}, {"1", 1}, {"33554432", 32 << 20}, {"-1", 32 << 20}, {"invalid", 32 << 20}} {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv("OUTPUT_BUFFER_BYTES", tc.value)
			t.Setenv("INGEST_QUEUE_MAX_BYTES", "0")
			cfg := FromEnv()
			if cfg.OutputBufferBytes != tc.want {
				t.Fatalf("output buffer=%d, want=%d", cfg.OutputBufferBytes, tc.want)
			}
			if cfg.QueueMaxBytes != 256<<20 {
				t.Fatal("mandatory queue budget was disabled")
			}
		})
	}
}
