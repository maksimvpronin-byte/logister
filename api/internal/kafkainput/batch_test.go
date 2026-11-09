package kafkainput

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestByteBoundedAcceptancePreservesOrderAndStopsOnFailure(t *testing.T) {
	records := []Message{{Offset: 0, Value: make([]byte, 6)}, {Offset: 1, Value: make([]byte, 6)}, {Offset: 2, Value: make([]byte, 20)}, {Offset: 3, Value: make([]byte, 2)}}
	var batches [][]int64
	err := acceptBounded(context.Background(), records, 10, func(_ context.Context, records []Message) error {
		ids := []int64{}
		for _, r := range records {
			ids = append(ids, r.Offset)
		}
		batches = append(batches, ids)
		return nil
	})
	if err != nil || !reflect.DeepEqual(batches, [][]int64{{0}, {1}, {2}, {3}}) {
		t.Fatal(batches, err)
	}
	calls := 0
	err = acceptBounded(context.Background(), records, 10, func(context.Context, []Message) error { calls++; return errors.New("storage unavailable") })
	if err == nil || calls != 1 {
		t.Fatal(calls, err)
	}
}
