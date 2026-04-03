package stats

import (
	"sync"
	"testing"
)

func TestRecordJob(t *testing.T) {
	tr := New()
	tr.RecordJob()
	tr.RecordJob()
	if got := tr.GetTotal(); got != 2 {
		t.Fatalf("GetTotal: got %d want 2", got)
	}
}

func TestRecordSuccess(t *testing.T) {
	tr := New()
	tr.RecordSuccess()
	tr.RecordSuccess()
	tr.mu.Lock()
	got := tr.Success
	tr.mu.Unlock()
	if got != 2 {
		t.Fatalf("Success: got %d want 2", got)
	}
}

func TestAddBytes(t *testing.T) {
	tr := New()
	tr.AddBytes(100)
	tr.AddBytes(256)
	tr.mu.Lock()
	got := tr.TotalBytes
	tr.mu.Unlock()
	if got != 356 {
		t.Fatalf("TotalBytes: got %d want 356", got)
	}
}

func TestRecordFailure(t *testing.T) {
	tr := New()
	tr.RecordFailure("path/a")
	tr.RecordFailure("path/b")
	if got := tr.GetFailedCount(); got != 2 {
		t.Fatalf("GetFailedCount: got %d want 2", got)
	}
	tr.mu.Lock()
	paths := tr.Failed
	tr.mu.Unlock()
	if len(paths) != 2 || paths[0] != "path/a" || paths[1] != "path/b" {
		t.Fatalf("Failed paths: got %v", paths)
	}
}

func TestRecordSkipped(t *testing.T) {
	tr := New()
	tr.RecordSkipped("path/x")
	if got := tr.GetSkippedCount(); got != 1 {
		t.Fatalf("GetSkippedCount: got %d want 1", got)
	}
	tr.mu.Lock()
	paths := tr.Skipped
	tr.mu.Unlock()
	if len(paths) != 1 || paths[0] != "path/x" {
		t.Fatalf("Skipped paths: got %v", paths)
	}
}

func TestRecordFailure_Cap(t *testing.T) {
	tr := New()
	for i := 0; i < MaxPathsTracked+10; i++ {
		tr.RecordFailure("p")
	}
	if got := tr.GetFailedCount(); got != MaxPathsTracked+10 {
		t.Fatalf("FailureCount: got %d want %d", got, MaxPathsTracked+10)
	}
	tr.mu.Lock()
	tracked := len(tr.Failed)
	tr.mu.Unlock()
	if tracked != MaxPathsTracked {
		t.Fatalf("tracked paths: got %d want %d", tracked, MaxPathsTracked)
	}
}

func TestRecordSkipped_Cap(t *testing.T) {
	tr := New()
	for i := 0; i < MaxPathsTracked+5; i++ {
		tr.RecordSkipped("p")
	}
	if got := tr.GetSkippedCount(); got != MaxPathsTracked+5 {
		t.Fatalf("SkippedCount: got %d want %d", got, MaxPathsTracked+5)
	}
	tr.mu.Lock()
	tracked := len(tr.Skipped)
	tr.mu.Unlock()
	if tracked != MaxPathsTracked {
		t.Fatalf("tracked skipped: got %d want %d", tracked, MaxPathsTracked)
	}
}

func TestConcurrentAccess(t *testing.T) {
	tr := New()
	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			tr.RecordJob()
			tr.RecordSuccess()
			tr.AddBytes(1)
		}()
	}
	wg.Wait()

	if got := tr.GetTotal(); got != goroutines {
		t.Errorf("Total: got %d want %d", got, goroutines)
	}
	tr.mu.Lock()
	success := tr.Success
	bytes := tr.TotalBytes
	tr.mu.Unlock()
	if success != goroutines {
		t.Errorf("Success: got %d want %d", success, goroutines)
	}
	if bytes != goroutines {
		t.Errorf("TotalBytes: got %d want %d", bytes, goroutines)
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		bytes int64
		want  string
	}{
		{0, "0 B"},
		{1, "1 B"},
		{1023, "1023 B"},
		{1024, "1.00 KB"},
		{1536, "1.50 KB"},
		{1024 * 1024, "1.00 MB"},
		{1024 * 1024 * 1024, "1.00 GB"},
		{1024 * 1024 * 1024 * 1024, "1.00 TB"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			got := FormatBytes(tc.bytes)
			if got != tc.want {
				t.Errorf("FormatBytes(%d): got %q want %q", tc.bytes, got, tc.want)
			}
		})
	}
}
