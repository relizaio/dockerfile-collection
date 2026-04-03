package pipeline

import (
	"io"
	"sync/atomic"
)

type byteCounter struct {
	io.Reader
	bytesRead atomic.Int64
}

func (bc *byteCounter) Read(p []byte) (int, error) {
	n, err := bc.Reader.Read(p)
	bc.bytesRead.Add(int64(n))
	return n, err
}
