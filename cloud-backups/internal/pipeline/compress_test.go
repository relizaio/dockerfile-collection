package pipeline

import (
	"bytes"
	"compress/gzip"
	"io"
	"strings"
	"testing"
)

func TestGzipRoundTrip(t *testing.T) {
	original := []byte("hello gzip world - some repeated data data data data")

	var compressed bytes.Buffer
	pw := &pipeWriteCloser{Writer: &compressed}
	gw := wrapGzipWriter(pw)
	if _, err := gw.Write(original); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("close gzip writer: %v", err)
	}

	gr, err := wrapGzipReader(&compressed)
	if err != nil {
		t.Fatalf("create gzip reader: %v", err)
	}
	got, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("read gzip: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Errorf("round-trip mismatch: got %q want %q", got, original)
	}
}

func TestWithGzip_WriterModifier(t *testing.T) {
	original := []byte("modifier gzip test payload")

	var buf bytes.Buffer
	base := &pipeWriteCloser{Writer: &buf}
	mod := WithGzip()
	wc, err := mod(base)
	if err != nil {
		t.Fatalf("WithGzip modifier: %v", err)
	}
	if _, err := wc.Write(original); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := wc.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	gr, err := gzip.NewReader(&buf)
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}
	got, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Errorf("round-trip via modifier: got %q want %q", got, original)
	}
}

func TestWithGunzip_ReaderModifier(t *testing.T) {
	original := []byte("gunzip modifier test")

	var compressed bytes.Buffer
	gw := gzip.NewWriter(&compressed)
	gw.Write(original)
	gw.Close()

	mod := WithGunzip()
	r, err := mod(&compressed)
	if err != nil {
		t.Fatalf("WithGunzip modifier: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Errorf("got %q want %q", got, original)
	}
}

func TestWrapGzipReader_InvalidInput(t *testing.T) {
	_, err := wrapGzipReader(strings.NewReader("not gzip data"))
	if err == nil {
		t.Fatal("expected error for invalid gzip input, got nil")
	}
}

// pipeWriteCloser wraps an io.Writer with a no-op Close so it satisfies io.WriteCloser.
type pipeWriteCloser struct {
	io.Writer
}

func (p *pipeWriteCloser) Close() error { return nil }
