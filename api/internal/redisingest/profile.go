package redisingest

import (
	"encoding/json"
	"log"
)

func (s *Store) EnableProfile(enabled bool) { s.profile = enabled }
func (s *Store) trace(stage string, fields map[string]any) {
	if !s.profile {
		return
	}
	fields["stage"] = stage
	raw, _ := json.Marshal(fields)
	log.Printf("ingest_stage_profile %s", raw)
}
