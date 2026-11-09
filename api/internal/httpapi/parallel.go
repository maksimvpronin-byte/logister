package httpapi

import "sync"

// Work is stateless; results remain in input order and storage sees one batch.
// The first input error is deterministic even if workers finish out of order.
func parallelEvents(count, workers int, fn func(int) error) error {
	if workers < 2 || count < 32 {
		for i := 0; i < count; i++ {
			if err := fn(i); err != nil {
				return err
			}
		}
		return nil
	}
	workers = min(workers, count, 16)
	errs := make([]error, count)
	var group sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			for i := worker; i < count; i += workers {
				errs[i] = fn(i)
			}
		}(worker)
	}
	group.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
