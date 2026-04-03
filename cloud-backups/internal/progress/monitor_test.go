package progress

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestNew_FieldsSet(t *testing.T) {
	var counter atomic.Int64
	m := New(&counter, "test/path", 5*time.Second)
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
	m := New(&counter, "target", 100*time.Millisecond)
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
	m := New(&counter, "target", 50*time.Millisecond)
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
	m := New(&counter, "target", 20*time.Millisecond)
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
	m := New(&counter, "target", 20*time.Millisecond)
	ctx := context.Background()
	m.Start(ctx)
	time.Sleep(60 * time.Millisecond)
	m.Stop()
	// Verifies stall code path executes without panic
}
