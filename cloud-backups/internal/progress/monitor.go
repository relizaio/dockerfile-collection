package progress

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/relizaio/cloud-backup/internal/stats"
)

// maxETASeconds is the largest remaining-seconds estimate we'll turn into a
// time.Duration. Above it, `time.Duration(secs) * time.Second` overflows int64
// nanoseconds and would log a garbage/negative ETA, so we omit the ETA instead.
const maxETASeconds = float64(math.MaxInt64 / int64(time.Second))

// Monitor periodically logs transfer speed and stall warnings for a byte counter.
type Monitor struct {
	bytesRead    *atomic.Int64
	registryPath string
	interval     time.Duration
	total        int64  // expected size; 0 = unknown (no percent/ETA)
	event        string // slog msg for progress lines (default "upload_in_progress")
	stallEvent   string // slog msg for stall warnings (default "upload_stalled_or_waiting")
	precise      bool   // total is exact (not an estimate): report percent/eta, uncapped
	done         chan struct{}
	stopOnce     sync.Once
}

// SetEvent overrides the slog messages for progress lines and stall warnings (e.g.
// for a download instead of an upload). Must be called BEFORE Start() -- the fields
// are read by the monitor goroutine without synchronization. Returns m for chaining.
func (m *Monitor) SetEvent(progressEvent, stallEvent string) *Monitor {
	m.event = progressEvent
	m.stallEvent = stallEvent
	return m
}

// SetPrecise marks total as an EXACT byte count (e.g. a HeadObject size for a
// re-download) rather than an estimate. Progress lines then report "percent"/"eta"
// (not "_approx") and percent is not capped below 100. Must be called BEFORE Start().
// Returns m for chaining.
func (m *Monitor) SetPrecise() *Monitor { m.precise = true; return m }

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
		event:        "upload_in_progress",
		stallEvent:   "upload_stalled_or_waiting",
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
						pctKey, etaKey := "percent_approx", "eta_approx"
						if m.precise {
							pctKey, etaKey = "percent", "eta"
						}
						pct := float64(current) / float64(m.total) * 100
						if pct > 99.9 && !m.precise { // estimate; the final success line marks 100%
							pct = 99.9
						}
						attrs = append(attrs, pctKey, fmt.Sprintf("%.1f%%", pct))
						// bps > 0 always holds here (delta >= 1); guard the ETA against int64
						// nanosecond overflow at absurdly low speeds (a near-stall) so we never
						// log a wrapped/negative Duration -- omit it instead.
						if bps := float64(delta) / m.interval.Seconds(); current < m.total {
							if etaSecs := float64(m.total-current) / bps; etaSecs < maxETASeconds {
								eta := time.Duration(etaSecs * float64(time.Second))
								attrs = append(attrs, etaKey, eta.Round(time.Second).String())
							}
						}
					}
					slog.Info(m.event, attrs...)
					lastBytes = current
				} else if current > 0 {
					slog.Warn(m.stallEvent,
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
