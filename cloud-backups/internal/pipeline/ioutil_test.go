package pipeline

import (
	"bytes"
	"io"
	"testing"
)

func TestByteCounter_CountsBytes(t *testing.T) {
	data := []byte("hello world")
	bc := &byteCounter{Reader: bytes.NewReader(data)}

	buf := make([]byte, len(data))
	n, err := bc.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("read error: %v", err)
	}
	if n != len(data) {
		t.Errorf("read %d bytes, want %d", n, len(data))
	}
	if got := bc.bytesRead.Load(); got != int64(len(data)) {
		t.Errorf("bytesRead: got %d want %d", got, len(data))
	}
}

func TestByteCounter_MultipleReads(t *testing.T) {
	data := []byte("abcdefghij") // 10 bytes
	bc := &byteCounter{Reader: bytes.NewReader(data)}

	buf := make([]byte, 3)
	total := 0
	for {
		n, err := bc.Read(buf)
		total += n
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read error: %v", err)
		}
	}
	if total != len(data) {
		t.Errorf("total bytes read: got %d want %d", total, len(data))
	}
	if got := bc.bytesRead.Load(); got != int64(len(data)) {
		t.Errorf("bytesRead counter: got %d want %d", got, len(data))
	}
}

func TestByteCounter_EmptyReader(t *testing.T) {
	bc := &byteCounter{Reader: bytes.NewReader(nil)}
	buf := make([]byte, 8)
	n, err := bc.Read(buf)
	if n != 0 {
		t.Errorf("expected 0 bytes, got %d", n)
	}
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
	if got := bc.bytesRead.Load(); got != 0 {
		t.Errorf("bytesRead should be 0, got %d", got)
	}
}
