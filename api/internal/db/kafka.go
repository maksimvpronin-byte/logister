package db

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
)

type KafkaState struct{ pool *pgxpool.Pool }

func NewKafkaState(ctx context.Context, pool *pgxpool.Pool) (*KafkaState, error) {
	_, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS kafka_input_state(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),paused boolean NOT NULL DEFAULT false); INSERT INTO kafka_input_state(singleton)VALUES(true)ON CONFLICT DO NOTHING;`)
	return &KafkaState{pool}, err
}
func (s *KafkaState) LoadPause(ctx context.Context) (bool, error) {
	var paused bool
	err := s.pool.QueryRow(ctx, `SELECT paused FROM kafka_input_state WHERE singleton`).Scan(&paused)
	return paused, err
}
func (s *KafkaState) SavePause(ctx context.Context, value bool) error {
	_, err := s.pool.Exec(ctx, `UPDATE kafka_input_state SET paused=$1 WHERE singleton`, value)
	return err
}
