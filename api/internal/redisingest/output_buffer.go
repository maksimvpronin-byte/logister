package redisingest

import (
	"context"
	"sync"
	"time"

	"github.com/logister/logister/api/internal/db"
)

type preparedPublication struct {
	batch publicationBatch
	err   error
}

// The Redis journal remains authoritative until OpenSearch ACK. A single reader
// prepares a bounded number of windows while one writer publishes the previous
// window. A mutex coordinates offsets with prefix trimming; it is never held
// during OpenSearch I/O. The lease excludes other sessions.
func (s *Store) publishBuffered(ctx context.Context, index string, slots int, write func([]db.Event) error) (int, error) {
	work, owner, release, err := s.publishLease(ctx, index)
	if err != nil || work == nil {
		return 0, err
	}
	defer release()
	pipeline, cancel := context.WithCancel(work)
	var position sync.Mutex
	reserved := int64(0)
	bufferedWindows, bufferedBytes := int64(0), int64(0)
	windows := make(chan preparedPublication, slots)
	credits := make(chan struct{}, slots)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(windows)
		// Finite sessions keep shutdown/retry boundaries and renew the lease normally.
		for loaded := 0; loaded < 8; {
			select {
			case credits <- struct{}{}:
			case <-pipeline.Done():
				return
			}
			position.Lock()
			batch, readErr := s.readPublication(pipeline, index, reserved)
			if readErr == nil {
				reserved += int64(len(batch.ids))
				if len(batch.ids) > 0 && s.telemetry != nil {
					n := int64(batch.counts[s.pendingBytesKey(index)])
					bufferedWindows++
					bufferedBytes += n
					s.telemetry.Buffer(1, n)
				}
			}
			inflight := reserved > 0
			position.Unlock()
			if readErr != nil || len(batch.ids) == 0 {
				<-credits

				if readErr == nil && inflight {
					timer := time.NewTimer(10 * time.Millisecond)
					select {
					case <-timer.C:
					case <-pipeline.Done():
						timer.Stop()
						return
					}
					continue
				}
				if readErr != nil {
					select {
					case windows <- preparedPublication{err: readErr}:
					case <-pipeline.Done():
					}
				}
				return
			}
			select {
			case windows <- preparedPublication{batch: batch}:
				loaded++
			case <-pipeline.Done():
				<-credits
				return
			}
		}
	}()
	// Join before releasing ownership, including failure and cancellation paths.
	defer func() {
		cancel()
		<-done
		if s.telemetry != nil {
			s.telemetry.Buffer(-bufferedWindows, -bufferedBytes)
		}
	}()
	confirmed := 0
	for {
		waiting := time.Now()
		var result preparedPublication
		var ok bool
		select {
		case <-pipeline.Done():
			return confirmed, pipeline.Err()
		case result, ok = <-windows:
		}
		if !ok {
			return confirmed, nil
		}
		if result.err != nil {
			return confirmed, result.err
		}
		batch := result.batch
		waitMS := time.Since(waiting).Seconds() * 1000
		started := time.Now()
		if len(batch.events) > 0 {
			if err = write(batch.events); err != nil {
				return confirmed, err
			}
		}
		writeMS := time.Since(started).Seconds() * 1000
		ackStarted := time.Now()
		position.Lock()
		err = s.acknowledgePublication(pipeline, index, owner, batch)
		if err == nil {
			reserved -= int64(len(batch.ids))
			if s.telemetry != nil {
				n := int64(batch.counts[s.pendingBytesKey(index)])
				bufferedWindows--
				bufferedBytes -= n
				s.telemetry.Buffer(-1, -n)
			}
		}
		position.Unlock()
		if err != nil {
			return confirmed, err
		}
		confirmed += len(batch.events)
		<-credits
		s.trace("publish", map[string]any{"documents": len(batch.events), "ms": waitMS + time.Since(started).Seconds()*1000, "read_ms": batch.readMS, "buffer_wait_ms": waitMS, "write_ms": writeMS, "ack_ms": time.Since(ackStarted).Seconds() * 1000, "buffer_slots": slots})
	}
}
