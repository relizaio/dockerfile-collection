package registry

import (
	"bytes"
	"strings"
	"sync"
	"testing"
)

func TestTailBuffer_WriteWithinMax(t *testing.T) {
	tb := &tailBuffer{max: 20}
	data := []byte("hello world")
	n, err := tb.Write(data)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != len(data) {
		t.Errorf("Write returned %d, want %d", n, len(data))
	}
	if got := tb.String(); got != string(data) {
		t.Errorf("String: got %q want %q", got, string(data))
	}
}

func TestTailBuffer_WriteTruncatesOldData(t *testing.T) {
	tb := &tailBuffer{max: 10}
	// Write 15 bytes; only last 10 should be retained
	data := []byte("abcdefghijklmno") // 15 bytes
	tb.Write(data)
	got := tb.String()
	if len(got) != 10 {
		t.Errorf("expected 10 bytes after truncation, got %d: %q", len(got), got)
	}
	if !strings.HasSuffix(string(data), got) {
		t.Errorf("expected tail of original data, got %q", got)
	}
}

func TestTailBuffer_MultipleWritesTruncation(t *testing.T) {
	tb := &tailBuffer{max: 5}
	tb.Write([]byte("12345"))
	tb.Write([]byte("678"))
	got := tb.String()
	if len(got) != 5 {
		t.Errorf("expected 5 bytes, got %d: %q", len(got), got)
	}
	// After writing "12345" then "678", buf is "12345678" → truncated to last 5 = "45678"
	if got != "45678" {
		t.Errorf("got %q want %q", got, "45678")
	}
}

func TestTailBuffer_EmptyString(t *testing.T) {
	tb := &tailBuffer{max: 100}
	if got := tb.String(); got != "" {
		t.Errorf("empty tailBuffer.String(): got %q want %q", got, "")
	}
}

func TestTailBuffer_WriteReturnsFullLength(t *testing.T) {
	tb := &tailBuffer{max: 3}
	data := []byte("this is longer than max")
	n, err := tb.Write(data)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != len(data) {
		t.Errorf("Write returned %d, want %d (full length)", n, len(data))
	}
}

func TestTailBuffer_ConcurrentWrites(t *testing.T) {
	tb := &tailBuffer{max: 1024}
	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			tb.Write([]byte("concurrent write payload"))
		}()
	}
	wg.Wait()
	// Just verify no race and result is non-empty
	s := tb.String()
	if s == "" {
		t.Error("expected non-empty String() after concurrent writes")
	}
}

func TestTailBuffer_ExactMax(t *testing.T) {
	tb := &tailBuffer{max: 5}
	tb.Write([]byte("12345"))
	if got := tb.String(); got != "12345" {
		t.Errorf("got %q want %q", got, "12345")
	}
}

func TestTailBuffer_ZeroWrite(t *testing.T) {
	tb := &tailBuffer{max: 10}
	n, err := tb.Write([]byte{})
	if err != nil {
		t.Fatalf("Write empty: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0, got %d", n)
	}
}

// Verify tailBuffer.String() content matches bytes.Buffer behaviour for small inputs.
func TestTailBuffer_MatchesBytesBuffer(t *testing.T) {
	input := "log line one\nlog line two\n"
	tb := &tailBuffer{max: 1000}
	tb.Write([]byte(input))

	var bb bytes.Buffer
	bb.WriteString(input)

	if tb.String() != bb.String() {
		t.Errorf("tailBuffer %q != bytes.Buffer %q", tb.String(), bb.String())
	}
}
