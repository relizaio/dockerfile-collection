package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/relizaio/cloud-backup/internal/datasource"
	"github.com/relizaio/cloud-backup/internal/pipeline"
	"github.com/relizaio/cloud-backup/internal/stats"
	"github.com/relizaio/cloud-backup/internal/storage"
)

// buildWriterModifiers constructs the ordered WriterModifier chain from config.
// Order: Age (outermost) then Gzip (innermost) so applyWriterModifiers wraps Age around Gzip.
func buildWriterModifiers(encPassword string) (nameSuffix string, mods []pipeline.WriterModifier) {
	nameSuffix = ".tar.gz"
	// 1. Age (outermost layer applied first in ApplyWriterModifiers)
	if encPassword != "" {
		mods = append(mods, pipeline.WithAgeEncryption(encPassword))
		nameSuffix += ".age"
	}
	// 2. Gzip (innermost layer applied second)
	mods = append(mods, pipeline.WithGzip())
	return nameSuffix, mods
}

// BackupManager orchestrates concurrent backup jobs.
type BackupManager struct {
	Storage           storage.Provider
	StorageType       string
	Tracker           *stats.Tracker
	Concurrency       int
	DataSource        datasource.Source
	EncPassword       string // used only to build the modifier chain
	DumpPrefix        string
	Timeout           time.Duration
	DeterministicName bool // when true, use last path segment as filename (no timestamp/random) - overwrites on re-run
}

// RunBackups resolves the final target list and fans out concurrent backup workers.
func (m *BackupManager) RunBackups(ctx context.Context, basePaths []string, rollingMonths bool) {
	finalTargets := m.resolveTargets(basePaths, rollingMonths)
	if len(finalTargets) == 0 {
		return
	}

	concurrency := m.Concurrency
	if concurrency < 1 {
		concurrency = 3
	}

	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for _, path := range finalTargets {
		var backupName string
		if m.DeterministicName {
			parts := strings.Split(path, "/")
			backupName = parts[len(parts)-1]
		} else {
			safeName := strings.ReplaceAll(path, "/", "-")
			backupName = fmt.Sprintf("%s-%s", m.DumpPrefix, safeName)
		}

		wg.Add(1)
		nameSuffix, writerMods := buildWriterModifiers(m.EncPassword)

		go func(targetPath, fileName string) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				m.Tracker.RecordJob()
				m.Tracker.RecordFailure(targetPath)
				return
			}
			defer func() { <-sem }()

			pipeline.RunWithRetry(ctx, m.DataSource, m.Storage, targetPath, fileName, nameSuffix, writerMods, m.Tracker, m.Timeout, m.DeterministicName, 0)
		}(path, backupName)
	}

	wg.Wait()
}

func (m *BackupManager) resolveTargets(basePaths []string, rollingMonths bool) []string {
	if !rollingMonths {
		slog.Info("explicit_path_strategy_enabled")
		return basePaths
	}

	slog.Info("rolling_months_strategy_enabled", "base_paths", basePaths)
	now := time.Now().UTC()

	var targets []string
	for _, p := range basePaths {
		targets = append(targets, TargetsForBasePath(p, rollingMonths, now)...)
	}
	return targets
}

// TargetsForBasePath expands one configured base path into the repositories a
// run will attempt. Under rolling months these names are FABRICATED from the
// calendar -- nothing checks that they exist, because the registry creates a
// monthly repository lazily on the first push into it. A month with no
// artifacts therefore has no repository at all, which is normal, not a fault.
func TargetsForBasePath(basePath string, rollingMonths bool, now time.Time) []string {
	if !rollingMonths {
		return []string{basePath}
	}
	return []string{
		fmt.Sprintf("%s-%s", basePath, now.Format("2006-01")),
		fmt.Sprintf("%s-%s", basePath, PreviousMonthSuffix(now)),
	}
}

// PreviousMonthSuffix is the YYYY-MM appended to build the previous-month
// target. Shared so no caller re-derives the date arithmetic independently.
func PreviousMonthSuffix(now time.Time) string {
	return now.AddDate(0, 0, -now.Day()).Format("2006-01")
}

// UncoveredBasePaths returns the configured base paths for which NO target
// produced a backup, given the set of targets the run skipped as absent.
//
// This is the strongest coverage question answerable from the tool's own
// credentials. Whether an individual month SHOULD exist is not knowable here:
// the registry cannot distinguish "never created" from "deleted" without
// catalogue access, and the destination cannot be read back at all under a
// write-only credential. But "this base path produced nothing" is unambiguous,
// and it is what a renamed, deleted or mistyped path looks like. Under explicit
// paths a base path expands to exactly one target, so this keeps the strict
// behaviour there: any skip means that path yielded nothing.
func UncoveredBasePaths(basePaths []string, rollingMonths bool, now time.Time, skipped []string) []string {
	absent := make(map[string]struct{}, len(skipped))
	for _, s := range skipped {
		absent[s] = struct{}{}
	}

	var uncovered []string
	for _, base := range basePaths {
		targets := TargetsForBasePath(base, rollingMonths, now)
		missing := 0
		for _, t := range targets {
			if _, ok := absent[t]; ok {
				missing++
			}
		}
		if len(targets) > 0 && missing == len(targets) {
			uncovered = append(uncovered, base)
		}
	}
	return uncovered
}
