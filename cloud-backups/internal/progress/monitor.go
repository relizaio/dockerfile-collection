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
	total        int64 // approximate expected size; 0 = unknown (no percent/ETA)
	done         chan struct{}
	stopOnce     sync.Once
}

// New creates a Monitor. bytesRead must be the same atomic counter advanced by the
// reader. total is an APPROXIMATE expected byte count (0 if unknown): when > 0, each
// progress line also reports an approximate percent-done and ETA. The estimate can be
// off (compression), so those are labelled "_approx" and percent is capped below 100.
func New(bytesRead *atomic.Int64, registryPath string, interval time.Duration, total int64) *Monitor {
	return &Monitor{
		bytesRead:    bytesRead,
		registryPath: registryPath,
		interval:     interval,
		total:        total,
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
					attrs := []any{
						"registry_path", m.registryPath,
						"streamed_so_far", stats.FormatBytes(current),
						"speed", fmt.Sprintf("%.2f MB/s", speedMBps),
					}
					if m.total > 0 {
						pct := float64(current) / float64(m.total) * 100
						if pct > 99.9 { // estimate; the final success line marks 100%
							pct = 99.9
						}
						attrs = append(attrs, "percent_approx", fmt.Sprintf("%.1f%%", pct))
						if bps := float64(delta) / m.interval.Seconds(); bps > 0 && current < m.total {
							eta := time.Duration(float64(m.total-current)/bps) * time.Second
							attrs = append(attrs, "eta_approx", eta.Round(time.Second).String())
						}
					}
					slog.Info("upload_in_progress", attrs...)
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
