package redisingest

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const maxCompressedBody = 32 << 20

var bodyEncoder = sync.OnceValues(func() (*zstd.Encoder, error) {
	return zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(64<<10))
})

var bodyDecoder = sync.OnceValues(func() (*zstd.Decoder, error) {
	return zstd.NewReader(nil, zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderMaxMemory(maxCompressedBody), zstd.WithDecodeAllCapLimit(true))
})

func encodeBody(document []byte) (body []byte, originalSize int) {
	if len(document) >= 1024 && len(document) <= maxCompressedBody {
		if encoder, err := bodyEncoder(); err == nil {
			compressed := encoder.EncodeAll(document, nil)
			if len(compressed)+32 < len(document) {
				return compressed, len(document)
			}
		}
	}
	return document, 0
}

func decodeBody(body, size string) ([]byte, error) {
	if size == "" {
		return []byte(body), nil
	}
	want, err := strconv.Atoi(size)
	if err != nil || want <= 0 || want > maxCompressedBody {
		return nil, fmt.Errorf("invalid compressed journal body size %q", size)
	}
	decoder, err := bodyDecoder()
	if err != nil {
		return nil, err
	}
	document, err := decoder.DecodeAll([]byte(body), make([]byte, 0, want))
	if err != nil {
		return nil, fmt.Errorf("decode compressed journal body: %w", err)
	}
	if len(document) != want {
		return nil, fmt.Errorf("compressed journal body length mismatch: want %d, got %d", want, len(document))
	}
	return document, nil
}
