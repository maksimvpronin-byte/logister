package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRecoveryPreservesPendingChunks(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "buffer/tail.0"), 0755); err != nil {
		t.Fatal(err)
	}
	pending := filepath.Join(dir, "buffer/tail.0/unacknowledged.flb")
	for _, path := range []string{filepath.Join(dir, "tail.db"), filepath.Join(dir, "tail.db-wal"), pending} {
		if err := os.WriteFile(path, []byte("durable data"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := mark(dir); err != nil {
		t.Fatal(err)
	}
	if err := rewind(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "tail.db")); !os.IsNotExist(err) {
		t.Fatal("unsafe offset was retained")
	}
	if data, err := os.ReadFile(pending); err != nil || string(data) != "durable data" {
		t.Fatal("pending delivery was removed")
	}
	if _, err := os.Stat(filepath.Join(dir, ".running")); err != nil {
		t.Fatal("unclean-exit marker was removed")
	}
}

func TestShutdownDrainsFinalStorageError(t *testing.T) {
	dir := t.TempDir()
	checkpoint := filepath.Join(dir, "tail.db")
	if err := os.WriteFile(checkpoint, []byte("unsafe offset"), 0600); err != nil {
		t.Fatal(err)
	}
	ready := filepath.Join(dir, "ready")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, dir, []string{"sh", "-c", `trap 'echo "No space left on device"; exit 0' TERM; touch "$1"; while :; do sleep .05; done`, "test-child", ready})
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("test child did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("supervisor did not stop")
	}
	if _, err := os.Stat(checkpoint); !os.IsNotExist(err) {
		t.Fatal("final storage error left an unsafe checkpoint")
	}
	if _, err := os.Stat(filepath.Join(dir, ".running")); err != nil {
		t.Fatal("replay marker must survive unsafe shutdown:", err)
	}
}

func TestOnlyStorageFailuresTriggerReplay(t *testing.T) {
	for _, line := range []string{"No space left on device", "database or disk is full", "[input chunk] no available chunk"} {
		if !storageFailure(line) {
			t.Fatal(line)
		}
	}
	for _, line := range []string{"HTTP status=503", "failed to flush chunk: retry in 5 seconds", "HTTP status=202"} {
		if storageFailure(line) {
			t.Fatal("network retry must not discard an offset:", line)
		}
	}
}
