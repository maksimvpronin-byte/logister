package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Connect(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	cfg.MaxConns = 8
	cfg.MinConns = 1

	var pool *pgxpool.Pool
	deadline := time.Now().Add(60 * time.Second)
	for {
		pool, err = pgxpool.NewWithConfig(ctx, cfg)
		if err == nil {
			if pingErr := pool.Ping(ctx); pingErr == nil {
				return pool, nil
			} else {
				err = pingErr
				pool.Close()
			}
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("connect postgres: %w", err)
		}
		time.Sleep(2 * time.Second)
	}
}

type Stream struct {
	ID          string        `json:"id"`
	Name        string        `json:"name"`
	Title       string        `json:"title"`
	Description string        `json:"description"`
	Routing     RoutingConfig `json:"routing"`
	Storage     StorageConfig `json:"storage"`
}

type Input struct {
	ID     string         `json:"id"`
	Name   string         `json:"name"`
	Type   string         `json:"type"`
	Config map[string]any `json:"config"`
}

func ListStreams(ctx context.Context, pool *pgxpool.Pool) ([]Stream, error) {
	return NewStreamStore(pool).List(ctx)
}

func ListInputs(ctx context.Context, pool *pgxpool.Pool) ([]Input, error) {
	rows, err := pool.Query(ctx, `SELECT id::text, name, type, config FROM inputs ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Input
	for rows.Next() {
		var in Input
		if err := rows.Scan(&in.ID, &in.Name, &in.Type, &in.Config); err != nil {
			return nil, err
		}
		out = append(out, in)
	}
	return out, rows.Err()
}

func Ping(ctx context.Context, pool *pgxpool.Pool) error {
	return pool.Ping(ctx)
}
