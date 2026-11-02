package config

import "os"

type Config struct {
	HTTPAddr         string
	DatabaseURL      string
	OpenSearchURL    string
	OpenSearchIndex  string
	CORSOrigin       string
	SeedSampleLogs   bool
}

func FromEnv() Config {
	return Config{
		HTTPAddr:        getenv("HTTP_ADDR", ":8080"),
		DatabaseURL:     getenv("DATABASE_URL", "postgres://logister:logister@localhost:5432/logister?sslmode=disable"),
		OpenSearchURL:   getenv("OPENSEARCH_URL", "http://localhost:9200"),
		OpenSearchIndex: getenv("OPENSEARCH_INDEX", "logs"),
		CORSOrigin:      getenv("CORS_ORIGIN", "http://localhost:3000"),
		SeedSampleLogs:  getenv("SEED_SAMPLE_LOGS", "true") == "true",
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
