// The supervisor treats failed disk persistence as an unsafe tail checkpoint.
// Retained source files + stable event IDs let recovery replay safely, while the
// filesystem backlog is preserved. An unclean supervisor exit leaves a marker.
package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func storageFailure(line string) bool {
	lower := strings.ToLower(line)
	return strings.Contains(lower, "no space left on device") || strings.Contains(lower, "database or disk is full") ||
		strings.Contains(lower, "could not create chunk file") || strings.Contains(lower, "no available chunk")
}

func rewind(dir string) error {
	for _, name := range []string{"tail.db", "tail.db-wal", "tail.db-shm"} {
		err := os.Remove(filepath.Join(dir, name))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func mark(dir string) error {
	file, err := os.OpenFile(filepath.Join(dir, ".running"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	if _, err = file.WriteString("unclean until graceful shutdown\n"); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func run(ctx context.Context, dir string, command []string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	lock, err := os.OpenFile(filepath.Join(dir, ".supervisor.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("state volume is already in use: %w", err)
	}
	if _, err := os.Stat(filepath.Join(dir, ".running")); err == nil {
		if err := rewind(dir); err != nil {
			return err
		}
		fmt.Fprintln(os.Stderr, "[logister] unclean exit: replaying retained source files")
	}
	for ctx.Err() == nil {
		if err := mark(dir); err != nil {
			fmt.Fprintln(os.Stderr, "[logister] state storage not writable:", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
				continue
			}
		}
		child := exec.Command(command[0], command[1:]...)
		output, err := child.StdoutPipe()
		if err != nil {
			return err
		}
		child.Stderr = child.Stdout
		if err := child.Start(); err != nil {
			return err
		}
		diskFailure := make(chan struct{}, 1)
		done := make(chan error, 1)
		go func() {
			scanner := bufio.NewScanner(output)
			scanner.Buffer(make([]byte, 4096), 1<<20)
			for scanner.Scan() {
				line := scanner.Text()
				fmt.Fprintln(os.Stdout, line)
				if storageFailure(line) {
					select {
					case diskFailure <- struct{}{}:
					default:
					}
				}
			}
			// Drain output before Wait closes the pipe: a final storage error
			// during shutdown must still invalidate the saved offset.
			if err := scanner.Err(); err != nil {
				select {
				case diskFailure <- struct{}{}:
				default:
				}
			}
			done <- child.Wait()
		}()
		unsafe := false
		stopping := false
		select {
		case <-ctx.Done():
			stopping = true
		case <-diskFailure:
			unsafe = true
			// Let already persisted chunks flush before stopping; failed new input is
			// recovered from retained files on the next pass.
			select {
			case <-ctx.Done():
				stopping = true
			case <-time.After(time.Second):
			}
		case err := <-done:
			unsafe = true
			fmt.Fprintln(os.Stderr, "[logister] shipper exited unexpectedly:", err)
			if err := rewind(dir); err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
				continue
			}
		}
		_ = child.Process.Signal(syscall.SIGTERM)
		select {
		case err := <-done:
			if err != nil {
				unsafe = true
			}
		case <-time.After(5 * time.Second):
			unsafe = true
			_ = child.Process.Kill()
			<-done
		}
		select {
		case <-diskFailure:
			unsafe = true
		default:
		}
		if unsafe {
			if err := rewind(dir); err != nil {
				return err
			}
			fmt.Fprintln(os.Stderr, "[logister] unsafe storage checkpoint discarded; queued chunks retained")
		} else {
			if err := os.Remove(filepath.Join(dir, ".running")); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		if stopping {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Second):
		}
	}
	return nil
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	args := os.Args[1:]
	if len(args) == 0 {
		args = []string{"/fluent-bit/bin/fluent-bit", "-c", "/fluent-bit/etc/fluent-bit.conf"}
	}
	if err := run(ctx, "/var/lib/fluent-bit", args); err != nil {
		fmt.Fprintln(os.Stderr, "[logister] supervisor:", err)
		os.Exit(1)
	}
}
