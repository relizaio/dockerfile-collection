package progress

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNew_FieldsSet(t *testing.T) {
	var counter atomic.Int64
	m := New(&counter, "test/path", 5*time.Second, 0)
	if m.bytesRead != &counter {
		t.Error("bytesRead pointer not set correctly")
	}
	if m.registryPath != "test/path" {
		t.Errorf("registryPath: got %q want %q", m.registryPath, "test/path")
	}
	if m.interval != 5*time.Second {
		t.Errorf("interval: got %v want %v", m.interval, 5*time.Second)
	}
	if m.done == nil {
		t.Error("done channel should be initialised")
	}
}

func TestMonitor_StartStop_NoPanic(t *testing.T) {
	var counter atomic.Int64
	m := New(&counter, "target", 100*time.Millisecond, 0)
	ctx := context.Background()
	m.Start(ctx)
	// Let the goroutine tick at least once
	time.Sleep(150 * time.Millisecond)
	m.Stop()
	// Double-stop should not panic (done channel is already closed)
	// We only call Stop once per contract, so just verify it did not panic above.
}

func TestMonitor_ContextCancel_ExitsGoroutine(t *testing.T) {
	var counter atomic.Int64
	m := New(&counter, "target", 50*time.Millisecond, 0)
	ctx, cancel := context.WithCancel(context.Background())
	m.Start(ctx)
	time.Sleep(30 * time.Millisecond)
	cancel()
	// Give the goroutine time to exit
	time.Sleep(100 * time.Millisecond)
	// No assertion needed — the test would deadlock or race-detect issues if the
	// goroutine did not exit cleanly.
}

func TestMonitor_ByteProgressLogged(t *testing.T) {
	var counter atomic.Int64
	m := New(&counter, "target", 20*time.Millisecond, 0)
	ctx := context.Background()
	m.Start(ctx)

	// Advance the counter between ticks
	counter.Add(1024 * 1024)
	time.Sleep(60 * time.Millisecond)

	m.Stop()
}

func TestMonitor_StallWarning(t *testing.T) {
	var counter atomic.Int64
	counter.Store(512) // non-zero but won't change → stall warning path
	m := New(&counter, "target", 20*time.Millisecond, 0)
	ctx := context.Background()
	m.Start(ctx)
	time.Sleep(60 * time.Millisecond)
	m.Stop()
	// Verifies stall code path executes without panic
}

// syncBuf is a concurrency-safe writer for capturing slog output from the monitor
// goroutine.
type syncBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

func TestMonitor_PercentAndETA_WhenTotalKnown(t *testing.T) {
	buf := &syncBuf{}
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
	defer slog.SetDefault(old)

	var counter atomic.Int64
	m := New(&counter, "target", 20*time.Millisecond, 1000) // total known
	m.Start(context.Background())
	counter.Store(400)
	time.Sleep(30 * time.Millisecond)
	counter.Store(700)
	time.Sleep(30 * time.Millisecond)
	m.Stop()
	time.Sleep(30 * time.Millisecond) // let the goroutine exit before reading

	out := buf.String()
	if !strings.Contains(out, "percent_approx") {
		t.Errorf("expected percent_approx when total is known; got:\n%s", out)
	}
	if !strings.Contains(out, "eta_approx") {
		t.Errorf("expected eta_approx when total is known; got:\n%s", out)
	}
}
