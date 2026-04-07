package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/relizaio/cloud-backup/internal/config"
	"github.com/relizaio/cloud-backup/internal/datasource"
	"github.com/relizaio/cloud-backup/internal/pipeline"
	"github.com/relizaio/cloud-backup/internal/stats"
	"github.com/relizaio/cloud-backup/internal/storage"
)

// restoreJob describes a single file-to-target restore unit.
type restoreJob struct {
	remoteFile string
	restoreTo  string // full registry path e.g. namespace/repo-YYYY-MM
	monthLabel string // YYYY-MM, for logging
	repo       string
}

// RunRollingRestore resolves the target month × repo matrix and fans out concurrent restore workers.
// With deterministic backup naming, each repo×month maps to exactly one known filename — no storage
// listing or sorting required.
func RunRollingRestore(ctx context.Context, regClient datasource.Source, storeProvider storage.Provider, cfg *config.AppConfig, tracker *stats.Tracker) {
	months := resolveMonths(cfg)
	if len(months) == 0 {
		slog.Warn("rolling_restore_no_months_resolved")
		return
	}

	restoreNS := cfg.RestoreNamespace

	nameSuffix := ".tar.gz"
	if cfg.EncryptionPassword != "" {
		nameSuffix = ".tar.gz.age"
	}

	jobs := buildRestoreJobs(restoreNS, cfg.RestoreRepos, months, nameSuffix)

	concurrency := cfg.MaxConcurrentJobs
	if concurrency < 1 {
		concurrency = 3
	}

	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for _, job := range jobs {
		wg.Add(1)
		tracker.RecordJob()

		go func(j restoreJob) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				tracker.RecordFailure(j.restoreTo)
				return
			}
			defer func() { <-sem }()

			readerMods := buildReaderModifiers(j.remoteFile, cfg.EncryptionPassword)

			slog.Info("rolling_restore_started",
				"backup_file", j.remoteFile,
				"restore_to", j.restoreTo,
				"month", j.monthLabel,
				"repo", j.repo,
			)

			start := time.Now()
			if err := pipeline.RunRestore(ctx, regClient, storeProvider, j.remoteFile, j.restoreTo, readerMods, cfg.Timeout); err != nil {
				slog.Error("rolling_restore_failed",
					"backup_file", j.remoteFile,
					"restore_to", j.restoreTo,
					"error", err.Error(),
				)
				tracker.RecordFailure(j.restoreTo)
				return
			}

			slog.Info("rolling_restore_succeeded",
				"backup_file", j.remoteFile,
				"restore_to", j.restoreTo,
				"duration", time.Since(start).Round(time.Second).String(),
			)
			tracker.RecordSuccess()
		}(job)
	}

	wg.Wait()
}

// buildRestoreJobs constructs the restore job list from repos × months.
// With deterministic backup naming the filename is always {repo}-{month}{nameSuffix},
// so no cloud storage listing is needed.
func buildRestoreJobs(restoreNS string, repos, months []string, nameSuffix string) []restoreJob {
	var jobs []restoreJob
	for _, repo := range repos {
		for _, month := range months {
			jobs = append(jobs, restoreJob{
				remoteFile: fmt.Sprintf("%s-%s%s", repo, month, nameSuffix),
				restoreTo:  fmt.Sprintf("%s/%s-%s", restoreNS, repo, month),
				monthLabel: month,
				repo:       repo,
			})
		}
	}
	return jobs
}

// buildReaderModifiers constructs the ordered ReaderModifier chain from the backup filename.
// Decryption (age) is applied first, then decompression (gunzip).
func buildReaderModifiers(filename, encPassword string) []pipeline.ReaderModifier {
	var mods []pipeline.ReaderModifier
	base := filename
	if strings.HasSuffix(base, ".age") {
		mods = append(mods, pipeline.WithAgeDecryption(encPassword))
		base = strings.TrimSuffix(base, ".age")
	}
	if strings.HasSuffix(base, ".tar.gz") {
		mods = append(mods, pipeline.WithGunzip())
	}
	return mods
}

// resolveMonths returns the ordered list of YYYY-MM strings to restore.
// Mode A: last cfg.Months months ending at CutoffDate (default today).
// Mode B: every month between FromDate and ToDate inclusive.
func resolveMonths(cfg *config.AppConfig) []string {
	rangeMode := !cfg.FromDate.IsZero() && !cfg.ToDate.IsZero()

	if rangeMode {
		return monthsBetween(cfg.FromDate, cfg.ToDate)
	}

	n := cfg.Months
	if n < 1 {
		n = 2
	}
	anchor := cfg.CutoffDate
	if anchor.IsZero() {
		anchor = time.Now().UTC()
	}
	// Normalize to the 1st to avoid day-overflow (e.g. March 31 - 1 month = March 3, not February).
	anchor = time.Date(anchor.Year(), anchor.Month(), 1, 0, 0, 0, 0, anchor.Location())

	months := make([]string, n)
	for i := 0; i < n; i++ {
		months[i] = anchor.AddDate(0, -(n - 1 - i), 0).Format("2006-01")
	}
	return months
}

// monthsBetween returns all YYYY-MM labels from start to end (inclusive), oldest first.
func monthsBetween(from, to time.Time) []string {
	cur := time.Date(from.Year(), from.Month(), 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(to.Year(), to.Month(), 1, 0, 0, 0, 0, time.UTC)

	var months []string
	for !cur.After(end) {
		months = append(months, cur.Format("2006-01"))
		cur = cur.AddDate(0, 1, 0)
	}
	return months
}
