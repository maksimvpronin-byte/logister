package db

import (
	"errors"
	"testing"
)

func TestStreamDeleteRestoreKeepsTheName(t *testing.T) {
	ctx, database := testDB(t)
	catalog := NewStreamStore(database)
	if _, err := catalog.Save(ctx, validStream("payments"), true); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SetDeleted(ctx, "all", true); !errors.Is(err, ErrDefaultStream) {
		t.Fatal(err)
	}
	if err := catalog.SetDeleted(ctx, "missing", true); !errors.Is(err, ErrStreamNotFound) {
		t.Fatal(err)
	}
	if err := catalog.SetDeleted(ctx, "payments", true); err != nil {
		t.Fatal(err)
	}
	if rows, err := catalog.List(ctx); err != nil || len(rows) != 0 {
		t.Fatal(rows, err)
	}
	if rows, err := catalog.ListArchived(ctx); err != nil || len(rows) != 1 {
		t.Fatal(rows, err)
	}
	if _, err := catalog.Save(ctx, validStream("payments"), false); !errors.Is(err, ErrStreamNotFound) {
		t.Fatal(err)
	}
	if _, err := catalog.Save(ctx, validStream("payments"), true); !errors.Is(err, ErrStreamExists) {
		t.Fatal(err)
	}
	if err := catalog.SetDeleted(ctx, "payments", false); err != nil {
		t.Fatal(err)
	}
	if rows, err := catalog.List(ctx); err != nil || len(rows) != 1 || rows[0].Name != "payments" {
		t.Fatal(rows, err)
	}
}
