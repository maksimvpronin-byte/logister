package db

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MonitoringNode stores only the last dependency status of an API instance.
type MonitoringNode struct {
	ID         string            `bson:"_id" json:"id"`
	SeenAt     time.Time         `bson:"seen_at" json:"seen_at"`
	Components map[string]string `bson:"components" json:"components"`
}

func (d *DB) SaveMonitoringNode(ctx context.Context, node MonitoringNode) error {
	_, err := d.coll("monitoring_nodes").ReplaceOne(ctx, bson.M{"_id": node.ID}, node, options.Replace().SetUpsert(true))
	return err
}

func (d *DB) MonitoringNodes(ctx context.Context, since time.Time) ([]MonitoringNode, error) {
	cursor, err := d.coll("monitoring_nodes").Find(ctx, bson.M{"seen_at": bson.M{"$gte": since}}, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return nil, err
	}
	nodes := []MonitoringNode{}
	err = cursor.All(ctx, &nodes)
	return nodes, err
}
