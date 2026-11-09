package db

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestUpgradeLegacyInputPath(t *testing.T) {
	ctx, database := testDB(t)
	for _, tc := range []struct{ name, inputType, path, expected string }{
		{"http-json", "http", "/ingest/json", "/api/ingest/json"},
		{"http-json", "http", "/custom/path", "/custom/path"},
		{"http-json", "http", "/api/ingest/json", "/api/ingest/json"},
		{"http-json", "syslog", "/ingest/json", "/ingest/json"},
		{"custom-input", "http", "/ingest/json", "/ingest/json"},
	} {
		t.Run(tc.name+tc.inputType+tc.path, func(t *testing.T) {
			if err := database.database.Drop(ctx); err != nil {
				t.Fatal(err)
			}
			if err := database.ensureIndexes(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := database.coll("inputs").InsertOne(ctx, inputDoc{ID: newID(), Name: tc.name, Type: tc.inputType, Config: bson.M{"path": tc.path, "keep": "yes"}}); err != nil {
				t.Fatal(err)
			}
			if err := database.upgradeInputPath(ctx); err != nil {
				t.Fatal(err)
			}
			inputs, err := ListInputs(ctx, database)
			if err != nil || len(inputs) != 1 || inputs[0].Config["path"] != tc.expected || inputs[0].Config["keep"] != "yes" {
				t.Fatal(inputs, err)
			}
		})
	}
}
