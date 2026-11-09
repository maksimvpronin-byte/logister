package db

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var ErrDefaultStream = errors.New("the default stream cannot be deleted")

// Deletion is reversible. The document remains, so a removed name cannot be
// recreated and a delayed delivery cannot resurrect the stream by accident.
func (s *StreamStore) SetDeleted(ctx context.Context, name string, deleted bool) error {
	if name == "all" && deleted {
		return ErrDefaultStream
	}
	var doc streamDoc
	err := s.db.streams().FindOne(ctx, bson.M{"name": name}).Decode(&doc)
	if missing(err) {
		return ErrStreamNotFound
	}
	if err != nil {
		return err
	}
	var value any
	if deleted {
		when := time.Now().UTC()
		if doc.DeletedAt != nil {
			when = *doc.DeletedAt
		}
		value = when
	}
	_, err = s.db.streams().UpdateOne(ctx, bson.M{"_id": doc.ID}, bson.M{"$set": bson.M{"deleted_at": value}})
	return err
}

func (s *StreamStore) ListArchived(ctx context.Context) ([]Stream, error) {
	cursor, err := s.db.streams().Find(ctx, bson.M{"deleted_at": bson.M{"$ne": nil}}, options.Find().SetSort(bson.D{{Key: "deleted_at", Value: -1}}))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	out := []Stream{}
	for cursor.Next(ctx) {
		var doc streamDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}
		out = append(out, doc.stream())
	}
	return out, cursor.Err()
}
