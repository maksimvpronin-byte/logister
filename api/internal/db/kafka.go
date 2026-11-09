package db

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
)

type KafkaState struct{ db *DB }

func NewKafkaState(ctx context.Context, database *DB) (*KafkaState, error) {
	_, err := database.coll("kafka_input_state").UpdateOne(ctx, bson.M{"_id": "singleton"}, bson.M{"$setOnInsert": bson.M{"paused": false}}, mongoOptionsUpsert())
	return &KafkaState{db: database}, err
}

func (s *KafkaState) LoadPause(ctx context.Context) (bool, error) {
	var doc struct {
		Paused bool `bson:"paused"`
	}
	err := s.db.coll("kafka_input_state").FindOne(ctx, bson.M{"_id": "singleton"}).Decode(&doc)
	return doc.Paused, err
}

func (s *KafkaState) SavePause(ctx context.Context, value bool) error {
	_, err := s.db.coll("kafka_input_state").UpdateOne(ctx, bson.M{"_id": "singleton"}, bson.M{"$set": bson.M{"paused": value}})
	return err
}
