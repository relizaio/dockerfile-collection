package progress

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/relizaio/cloud-backup/internal/stats"
)

// Monitor periodically logs upload speed and stall warnings for a byte counter.
type Monitor struct {
	bytesRead    *atomic.Int64
	registryPath string
	interval     time.Duration
	done         chan struct{}
	stopOnce     sync.Once
}

// New creates a Monitor. bytesRead must be the same atomic counter advanced by the reader.
func New(bytesRead *atomic.Int64, registryPath string, interval time.Duration) *Monitor {
	return &Monitor{
		bytesRead:    bytesRead,
		registryPath: registryPath,
		interval:     interval,
		done:         make(chan struct{}),
	}
}

// Start spawns the background goroutine. Call Stop or cancel ctx to end it.
func (m *Monitor) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(m.interval)
		defer ticker.Stop()
		var lastBytes int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-m.done:
				return
			case <-ticker.C:
				current := m.bytesRead.Load()
				if current > lastBytes {
					delta := current - lastBytes
					speedMBps := float64(delta) / 1024 / 1024 / m.interval.Seconds()
					slog.Info("upload_in_progress",
						"registry_path", m.registryPath,
						"streamed_so_far", stats.FormatBytes(current),
						"speed", fmt.Sprintf("%.2f MB/s", speedMBps),
					)
					lastBytes = current
				} else if current > 0 {
					slog.Warn("upload_stalled_or_waiting",
						"registry_path", m.registryPath,
						"stuck_at", stats.FormatBytes(current),
					)
				}
			}
		}
	}()
}

// Stop signals the background goroutine to exit. Safe to call multiple times.
func (m *Monitor) Stop() { m.stopOnce.Do(func() { close(m.done) }) }
