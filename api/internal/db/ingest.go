package db

import (
	"encoding/json"
	"errors"
)

var ErrIdentityConflict = errors.New("idempotency key or event id was reused with different data")

type Event struct {
	AcceptedAtNS   int64
	DocumentTime   string
	InputBytes     int
	ID, Hash       string
	Document       json.RawMessage
	StreamName     string
	TargetIndex    string
	HashVersion    int
	LegacyHash     string
	LegacyHashLazy func() string `json:"-"`
	KafkaTransport bool
}

func (e Event) LegacyIdentityHash() string {
	if e.LegacyHash != "" {
		return e.LegacyHash
	}
	if e.LegacyHashLazy != nil {
		return e.LegacyHashLazy()
	}
	return ""
}

type Receipt struct {
	Count      int `json:"count"`
	Duplicates int `json:"duplicates"`
}
