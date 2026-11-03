package config

import "os"

type Config struct {
	KafkaBrokers      string
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
	return Config{
		KafkaBrokers:      os.Getenv("KAFKA_BROKERS"),
		KafkaTopic:        getenv("KAFKA_TOPIC", "logister-ingest"),
		KafkaGroup:        getenv("KAFKA_GROUP", "logister"),
		HTTPAddr:          getenv("HTTP_ADDR", ":8080"),
		DatabaseURL:       getenv("DATABASE_URL", "postgres://logister:logister@localhost:5432/logister?sslmode=disable"),
		RedisURL:          getenv("REDIS_URL", "redis://127.0.0.1:6379"),
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

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
