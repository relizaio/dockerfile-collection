package stats

import (
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"
)

const MaxPathsTracked = 100

type Tracker struct {
	mu           sync.Mutex
	Total        int64
	Success      int64
	FailureCount int64
	SkippedCount int64
	TotalBytes   int64
	Failed       []string
	Skipped      []string
}

func New() *Tracker {
	return &Tracker{}
}

func (t *Tracker) RecordJob()             { t.mu.Lock(); defer t.mu.Unlock(); t.Total++ }
func (t *Tracker) RecordSuccess()         { t.mu.Lock(); defer t.mu.Unlock(); t.Success++ }
func (t *Tracker) AddBytes(b int64)       { t.mu.Lock(); defer t.mu.Unlock(); t.TotalBytes += b }
func (t *Tracker) GetTotal() int64        { t.mu.Lock(); defer t.mu.Unlock(); return t.Total }
func (t *Tracker) GetSuccess() int64      { t.mu.Lock(); defer t.mu.Unlock(); return t.Success }
func (t *Tracker) GetFailedCount() int64  { t.mu.Lock(); defer t.mu.Unlock(); return t.FailureCount }
func (t *Tracker) GetSkippedCount() int64 { t.mu.Lock(); defer t.mu.Unlock(); return t.SkippedCount }

// GetSkipped returns the skipped paths. A skipped target produced no backup, so
// callers that know a skip is illegitimate need the names, not just the count.
func (t *Tracker) GetSkipped() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return slices.Clone(t.Skipped)
}

func (t *Tracker) RecordSkipped(path string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.SkippedCount++
	if len(t.Skipped) < MaxPathsTracked {
		t.Skipped = append(t.Skipped, path)
	}
}

func (t *Tracker) RecordFailure(path string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.FailureCount++
	if len(t.Failed) < MaxPathsTracked {
		t.Failed = append(t.Failed, path)
	}
}

func FormatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func PrintSummary(eventName string, t *Tracker, storageType string, duration time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()

	summary := map[string]any{
		"event":               eventName,
		"total_targeted":      t.Total,
		"success_count":       t.Success,
		"failed_count":        t.FailureCount,
		"skipped_count":       t.SkippedCount,
		"total_duration_secs": duration.Seconds(),
		"total_bytes_human":   FormatBytes(t.TotalBytes),
		"storage_destination": storageType,
	}

	if t.SkippedCount > 0 {
		summary["skipped_paths"] = slices.Clone(t.Skipped)
	}
	if t.FailureCount > 0 {
		summary["failed_paths"] = slices.Clone(t.Failed)
	}

	allSkipped := t.Total > 0 && t.SkippedCount == t.Total

	// The failing paths are already inside summary, but alerting reads flat
	// fields -- a nested map renders as an opaque blob, so the one ERROR that
	// says the run lost data would arrive without saying which target.
	if allSkipped {
		// "detail", not "msg": slog's JSON handler emits the event name as
		// "msg", so a "msg" attr becomes a duplicate key and every JSON parser
		// keeps the last one -- the event name would never survive parsing.
		slog.Error("pipeline_failed_all_repos_missing", "summary", summary, "detail", "CRITICAL: No repositories found.",
			"error", fmt.Sprintf("no repositories found: all %d target(s) missing from the registry: %s",
				t.Total, strings.Join(t.Skipped, ", ")))
	} else if t.FailureCount > 0 {
		slog.Error("pipeline_completed_with_failures", "summary", summary,
			"error", fmt.Sprintf("%d of %d target(s) failed: %s",
				t.FailureCount, t.Total, strings.Join(t.Failed, ", ")))
	} else {
		slog.Info("pipeline_completed_successfully", "summary", summary)
	}
}
