package redisingest

import (
	"context"
	"time"

	"github.com/logister/logister/api/internal/db"
)

// Catalog reads stream settings from MongoDB and records index generations.
// The hot log payload does not pass through it.
type Catalog interface {
	Stream(context.Context, string) (StreamInfo, error)
	NextGeneration(context.Context, string, string) (int64, error)
	ActiveGeneration(context.Context, string, string) (Generation, bool, error)
	OpenGeneration(context.Context, string, string, string, int64) error
	SealGeneration(context.Context, string, int64) error
	ListStreams(context.Context) ([]db.Stream, error)
	AcceptedTotals(context.Context, string) (map[string]int64, error)
}

type StreamInfo struct {
	ID      string
	Storage db.StorageConfig
}

type MongoCatalog struct{ db *db.DB }

func NewCatalog(database *db.DB) *MongoCatalog { return &MongoCatalog{db: database} }

func (c *MongoCatalog) Stream(ctx context.Context, name string) (StreamInfo, error) {
	id, storage, err := c.db.StreamInfo(ctx, name)
	if err != nil {
		return StreamInfo{}, err
	}
	return StreamInfo{ID: id, Storage: storage}, nil
}

func (c *MongoCatalog) NextGeneration(ctx context.Context, namespace, streamID string) (int64, error) {
	return c.db.NextGeneration(ctx, namespace, streamID)
}

func (c *MongoCatalog) ActiveGeneration(ctx context.Context, namespace, streamID string) (Generation, bool, error) {
	doc, ok, err := c.db.ActiveGeneration(ctx, namespace, streamID)
	if err != nil || !ok {
		return Generation{}, ok, err
	}
	return Generation{Index: doc.Index, StreamID: doc.StreamID, Accepted: doc.Accepted, Generation: doc.Generation, Created: doc.Created}, true, nil
}

func (c *MongoCatalog) OpenGeneration(ctx context.Context, namespace, streamID, indexName string, generation int64) error {
	return c.db.OpenGeneration(ctx, namespace, streamID, indexName, generation)
}

func (c *MongoCatalog) SealGeneration(ctx context.Context, indexName string, accepted int64) error {
	return c.db.SealGeneration(ctx, indexName, accepted)
}

func (c *MongoCatalog) ListStreams(ctx context.Context) ([]db.Stream, error) {
	return db.NewStreamStore(c.db).List(ctx)
}

func (c *MongoCatalog) AcceptedTotals(ctx context.Context, namespace string) (map[string]int64, error) {
	return c.db.AcceptedTotals(ctx, namespace)
}

// Created is the moment a generation became active. Stored in Redis so age
// rotation survives a process restart without a catalog write per batch.
type Generation struct {
	Index      string
	StreamID   string
	Accepted   int64
	Generation int64
	Created    time.Time
}
