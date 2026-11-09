package config

import (
	"os"
	"strconv"
)

type Config struct {
	MonitoringNodeID  string
	IngestWorkers     int
	KafkaBatchRecords int
	KafkaBatchBytes   int
	PublishBatchBytes int
	OutputBufferBytes int
	QueueMaxBytes     int
	BulkBytes         int
	BulkWorkers       int
	IndexShards       int
	IndexCodec        string
	IndexCodecLevel   int
	BulkProfile       bool
	StageProfile      bool
	KafkaBrokers      string
	KafkaDirect       bool
	KafkaTopic        string
	KafkaGroup        string
	HTTPAddr          string
	DatabaseURL       string
	RedisURL          string
	OpenSearchURL     string
	OpenSearchIndex   string
	SearchReadIndex   string
	CORSOrigin        string
	SeedSampleLogs    bool
	AuthEnabled       bool
	AuthLocalPassword string
	AuthSettingsKey   string
	AuthCookieSecure  bool
	LDAPURL           string
	LDAPBindDN        string
	LDAPBindPassword  string
	LDAPUsersDN       string
	LDAPGroupsDN      string
}

func FromEnv() Config {
	kafkaDirect := getenv("KAFKA_DIRECT", "false") == "true"
	redisURL := os.Getenv("REDIS_URL")
	if !kafkaDirect && redisURL == "" {
		redisURL = "redis://127.0.0.1:6379"
	}
	return Config{
		MonitoringNodeID:  os.Getenv("MONITORING_NODE_ID"),
		IngestWorkers:     envInt("INGEST_WORKERS", 4),
		KafkaBatchRecords: envInt("KAFKA_BATCH_RECORDS", 1000),
		KafkaBatchBytes:   envInt("KAFKA_BATCH_BYTES", 10<<20),
		PublishBatchBytes: envInt("PUBLISH_BATCH_BYTES", 10<<20),
		OutputBufferBytes: envNonNegativeInt("OUTPUT_BUFFER_BYTES", 32<<20),
		QueueMaxBytes:     envInt("INGEST_QUEUE_MAX_BYTES", 256<<20),
		BulkBytes:         envInt("BULK_MAX_BYTES", 5<<20),
		BulkWorkers:       envInt("BULK_WORKERS", 2),
		IndexShards:       max(1, min(envInt("OPENSEARCH_SHARDS", 2), 16)),
		IndexCodec:        getenv("OPENSEARCH_CODEC", "zstd_no_dict"),
		IndexCodecLevel:   max(1, min(envInt("OPENSEARCH_CODEC_LEVEL", 1), 6)),
		BulkProfile:       getenv("INGEST_PROFILE_BULK", "false") == "true",
		StageProfile:      getenv("INGEST_PROFILE_STAGES", "false") == "true",
		KafkaBrokers:      os.Getenv("KAFKA_BROKERS"),
		KafkaDirect:       kafkaDirect,
		KafkaTopic:        getenv("KAFKA_TOPIC", "logister-ingest"),
		KafkaGroup:        getenv("KAFKA_GROUP", "logister"),
		HTTPAddr:          getenv("HTTP_ADDR", ":8080"),
		DatabaseURL:       getenv("DATABASE_URL", "mongodb://127.0.0.1:27017/logister"),
		RedisURL:          redisURL,
		OpenSearchURL:     getenv("OPENSEARCH_URL", "http://localhost:9200"),
		OpenSearchIndex:   getenv("OPENSEARCH_INDEX", "logs"),
		CORSOrigin:        getenv("CORS_ORIGIN", "http://localhost:3000"),
		SeedSampleLogs:    getenv("SEED_SAMPLE_LOGS", "true") == "true",
		AuthEnabled:       getenv("AUTH_ENABLED", "false") == "true",
		AuthLocalPassword: os.Getenv("AUTH_LOCAL_PASSWORD"),
		AuthSettingsKey:   os.Getenv("AUTH_SETTINGS_KEY"),
		AuthCookieSecure:  getenv("AUTH_COOKIE_SECURE", "true") == "true",
		LDAPURL:           os.Getenv("LDAP_URL"),
		LDAPBindDN:        os.Getenv("LDAP_BIND_DN"),
		LDAPBindPassword:  os.Getenv("LDAP_BIND_PASSWORD"),
		LDAPUsersDN:       os.Getenv("LDAP_USERS_DN"),
		LDAPGroupsDN:      os.Getenv("LDAP_GROUPS_DN"),
	}
}

func (c Config) KafkaOnly() bool { return c.KafkaDirect && c.RedisURL == "" }

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil || value < 1 {
		return fallback
	}
	return value
}

// Zero explicitly disables the optional output read-ahead buffer.
func envNonNegativeInt(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil || value < 0 {
		return fallback
	}
	return value
}
