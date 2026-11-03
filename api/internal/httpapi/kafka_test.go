package httpapi

import (
	"context"
	"encoding/json"
	"github.com/logister/logister/api/internal/config"
	"github.com/logister/logister/api/internal/db"
	"github.com/logister/logister/api/internal/kafkainput"
	"testing"
)

func TestKafkaReplayUsesStableIDsAndCommonValidation(t *testing.T) {
	store := &ingestStore{hashes: map[string]string{}, keys: map[string]string{}}
	s := NewServer(config.Config{OpenSearchIndex: "logs-v2"}, nil, nil)
	s.store = store
	s.catalog = &fakeCatalog{streams: []db.Stream{{Name: "payments", Title: "Payments"}}}
	records := []kafkainput.Message{{Topic: "input", Partition: 2, Offset: 9, Value: []byte(`{"message":"one","stream":"payments"}`)}}
	if err := s.acceptKafka(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	if err := s.acceptKafka(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	if len(store.docs) != 1 {
		t.Fatal("replay duplicated the document", store.docs)
	}
	records[0].Offset++
	if err := s.acceptKafka(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	if len(store.docs) != 2 {
		t.Fatal(store.docs)
	}
	records[0].Value = []byte(`{"message":"x","stream":"unknown"}`)
	if err := s.acceptKafka(context.Background(), records); err == nil {
		t.Fatal("invalid destination accepted")
	}
	records[0].Value = []byte(`[]`)
	if err := s.acceptKafka(context.Background(), records); err == nil {
		t.Fatal("non-object accepted")
	}
	store.failAt = 1
	records[0].Value = []byte(`{"message":"x","stream":"payments"}`)
	if err := s.acceptKafka(context.Background(), records); err == nil {
		t.Fatal("storage failure hidden")
	}
}
func TestKafkaAndDeletionAdministrativeRoutes(t *testing.T) {
	s, _, _ := authFixture(t)
	h := s.Handler()
	operator := loginCookie(t, h, "operator", "ldap", "correct")
	for _, path := range []string{"/api/ingest/kafka", "/api/streams/deleted"} {
		if w := authRequest(h, "GET", path, "", operator); w.Code != 403 {
			t.Fatal(path, w.Code)
		}
	}
	for _, path := range []string{"/api/streams/payments", "/api/streams/all"} {
		if w := authRequest(h, "DELETE", path, "", operator); w.Code != 403 {
			t.Fatal(path, w.Code)
		}
	}
	if w := authRequest(h, "PUT", "/api/ingest/kafka", `{"paused":true}`, operator); w.Code != 403 {
		t.Fatal(w.Code)
	}
	if w := authRequest(h, "POST", "/api/streams/payments/restore", `{}`, operator); w.Code != 403 {
		t.Fatal(w.Code)
	}
	admin := loginCookie(t, h, "local-admin", "local", "local-test-password")
	if w := authRequest(h, "GET", "/api/ingest/kafka", "", admin); w.Code != 200 {
		t.Fatal(w.Code)
	}
}
func TestFluentBitTransportTimeDoesNotChangeApplicationIdentity(t *testing.T) {
	store := &ingestStore{hashes: map[string]string{}, keys: map[string]string{}}
	s := NewServer(config.Config{OpenSearchIndex: "logs-v2"}, nil, nil)
	s.store = store
	records := []kafkainput.Message{{Topic: "input", Offset: 1, Key: []byte("uuid"), Value: []byte(`{"delivery_id":"uuid","message":"one","__logister_kafka_timestamp":123}`)}}
	if err := s.acceptKafka(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	records[0].Offset++
	records[0].Value = []byte(`{"delivery_id":"uuid","message":"one","__logister_kafka_timestamp":456}`)
	if err := s.acceptKafka(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	records[0].Offset++
	records[0].Value = []byte(`{"delivery_id":"uuid","message":"one","@timestamp":789}`)
	if err := s.acceptKafka(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	if len(store.docs) != 1 {
		t.Fatal("transport timestamp changed identity", store.docs)
	}
	records[0].Value = []byte(`{"delivery_id":"uuid","message":"changed","__logister_kafka_timestamp":999}`)
	if err := s.acceptKafka(context.Background(), records); err == nil {
		t.Fatal("changed application data was accepted")
	}
}
func TestWrappedFluentBitPreservesTransportCompatibility(t *testing.T) {
	var req ingestRequest
	if err := json.Unmarshal([]byte(`{"delivery_id":"uuid","log":"{\"message\":\"one\"}"}`), &req); err != nil {
		t.Fatal(err)
	}
	req.KafkaTransport = true
	got := unwrapIngest(req)
	if !got.KafkaTransport || got.Message != "one" {
		t.Fatal("lost transport metadata on unwrap", got)
	}
}
func TestApplicationTimestampWithoutFluentBitKeyIsPreserved(t *testing.T) {
	store := &ingestStore{hashes: map[string]string{}, keys: map[string]string{}}
	s := NewServer(config.Config{OpenSearchIndex: "logs-v2"}, nil, nil)
	s.store = store
	records := []kafkainput.Message{{Topic: "input", Value: []byte(`{"delivery_id":"application-id","message":"one","@timestamp":123}`)}}
	if err := s.acceptKafka(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	records[0].Value = []byte(`{"delivery_id":"application-id","message":"one","@timestamp":456}`)
	if err := s.acceptKafka(context.Background(), records); err == nil {
		t.Fatal("application timestamp was removed from identity")
	}
}
