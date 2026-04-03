package pipeline

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// countingWriteCloser records how many times it was wrapped via Write calls.
type countingWriteCloser struct {
	buf    *bytes.Buffer
	layer  int
	closed bool
}

func (c *countingWriteCloser) Write(p []byte) (int, error) { return c.buf.Write(p) }
func (c *countingWriteCloser) Close() error                { c.closed = true; return nil }

func TestApplyWriterModifiers_NoMods(t *testing.T) {
	buf := &bytes.Buffer{}
	base := &countingWriteCloser{buf: buf}
	got, closers, err := applyWriterModifiers(base, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(closers) != 0 {
		t.Errorf("expected 0 closers, got %d", len(closers))
	}
	if got != base {
		t.Error("expected base writer to be returned unchanged")
	}
}

func TestApplyWriterModifiers_TwoMods(t *testing.T) {
	buf := &bytes.Buffer{}
	base := &pipeWriteCloser{Writer: buf}

	calls := 0
	makePassthrough := func() WriterModifier {
		return func(w io.WriteCloser) (io.WriteCloser, error) {
			calls++
			return w, nil
		}
	}

	mods := []WriterModifier{makePassthrough(), makePassthrough()}
	_, closers, err := applyWriterModifiers(base, mods)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 2 {
		t.Errorf("expected 2 modifier calls, got %d", calls)
	}
	if len(closers) != 2 {
		t.Errorf("expected 2 closers, got %d", len(closers))
	}
}

func TestApplyWriterModifiers_ErrorBubbles(t *testing.T) {
	base := &pipeWriteCloser{Writer: &bytes.Buffer{}}
	wantErr := errors.New("modifier failed")
	mods := []WriterModifier{
		func(w io.WriteCloser) (io.WriteCloser, error) { return nil, wantErr },
	}
	_, _, err := applyWriterModifiers(base, mods)
	if !errors.Is(err, wantErr) {
		t.Fatalf("got %v, want %v", err, wantErr)
	}
}

func TestApplyReaderModifiers_NoMods(t *testing.T) {
	src := bytes.NewReader([]byte("hello"))
	got, err := ApplyReaderModifiers(src, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != src {
		t.Error("expected src reader to be returned unchanged")
	}
}

func TestApplyReaderModifiers_TwoMods(t *testing.T) {
	src := bytes.NewReader([]byte("hello"))
	calls := 0
	makePassthrough := func() ReaderModifier {
		return func(r io.Reader) (io.Reader, error) {
			calls++
			return r, nil
		}
	}
	mods := []ReaderModifier{makePassthrough(), makePassthrough()}
	_, err := ApplyReaderModifiers(src, mods)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 2 {
		t.Errorf("expected 2 modifier calls, got %d", calls)
	}
}

func TestApplyReaderModifiers_ErrorBubbles(t *testing.T) {
	src := bytes.NewReader([]byte("hello"))
	wantErr := errors.New("reader modifier failed")
	mods := []ReaderModifier{
		func(r io.Reader) (io.Reader, error) { return nil, wantErr },
	}
	_, err := ApplyReaderModifiers(src, mods)
	if !errors.Is(err, wantErr) {
		t.Fatalf("got %v, want %v", err, wantErr)
	}
}
