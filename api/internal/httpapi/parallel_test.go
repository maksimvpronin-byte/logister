package httpapi

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestParallelPreparationKeepsFirstInputError(t *testing.T) {
	first, second := errors.New("first"), errors.New("second")
	seen := make([]int, 100)
	err := parallelEvents(100, 4, func(i int) error {
		seen[i] = i
		if i == 3 {
			return first
		}
		if i == 20 {
			return second
		}
		return nil
	})
	if err != first {
		t.Fatal(err)
	}
	for i, v := range seen {
		if v != i {
			t.Fatal(i, v)
		}
	}
}
func TestDirectJSONDecoderRejectsTrailingData(t *testing.T) {
	var req ingestRequest
	for _, raw := range []string{`{"message":"x"} {}`, `[]`, `null`} {
		if err := req.UnmarshalJSON([]byte(raw)); err == nil {
			t.Fatal(raw)
		}
	}
	raw := []byte(` {"message":"x","large":9007199254740993} `)
	if err := req.UnmarshalJSON(raw); err != nil {
		t.Fatal(err)
	}
	document := ingestDocument(req, "id")
	var decoded map[string]any
	if err := json.Unmarshal(document, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["payload_json"] != string(raw) {
		t.Fatal("original JSON was not retained", decoded)
	}
}
