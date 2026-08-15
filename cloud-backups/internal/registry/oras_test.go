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
	// After writing "12345" then "678", buf is "12345678" -> truncated to last 5 = "45678"
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

// This predicate decides SKIP (no backup, no alert, exit 0) versus FAIL, so a
// false positive silently loses a backup. Both directions are pinned.
func TestRepositoryAbsent(t *testing.T) {
	tests := []struct {
		name string
		logs string
		want bool
	}{
		// Genuine absences must still skip, or the previous-month rolling
		// target would fail every month.
		{"canonical distribution error", `Error response from registry: name unknown: repository name not known to registry`, true},
		{"wrapped repository message", "repository name not known to registry: reg/repo", true},
		{"lowercase not found", "Error: repo not found", true},
		{"capitalised status text", "Error: unexpected status 404 Not Found", true},

		// A transport failure is NOT an absence. A content digest containing
		// "404" must never turn a refused dial into a silent skip -- this is the
		// case that produced a successful-looking run with no backup.
		{
			name: "refused dial with 404 inside a digest",
			logs: `Uploading sha256:a481e31691d2b86354c9ebbe3db446dd4041b514 100.00%
Error: failed to find tags: Get "https://reg/v2/org/repo/tags/list?last=x&n=100&orderby=": dial tcp 10.0.5.5:443: connect: connection refused`,
			want: false,
		},
		{"connection reset with 404 digest", "sha256:404aa1 ... read: connection reset by peer", false},
		{"i/o timeout with not found text", "Get \"https://reg/v2/\": i/o timeout, repo not found in cache", false},

		// Neither absence nor transport failure.
		{"generic server error", "Error: unexpected status code 500 Internal Server Error", false},
		{"empty", "", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := repositoryAbsent(tc.logs); got != tc.want {
				t.Errorf("repositoryAbsent: got %v want %v\nlogs: %s", got, tc.want, tc.logs)
			}
		})
	}
}
