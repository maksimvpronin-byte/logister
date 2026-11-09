package db

import "time"

type IngestCounts struct {
	Accepted  int64 `json:"accepted"`
	Published int64 `json:"published"`
	Pending   int64 `json:"pending"`
}
type StreamIngestStats struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Title string `json:"title"`
	IngestCounts
}
type IngestStats struct {
	CapturedAt time.Time           `json:"captured_at"`
	Totals     IngestCounts        `json:"totals"`
	Streams    []StreamIngestStats `json:"streams"`
}
