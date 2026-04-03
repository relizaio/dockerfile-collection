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
	Storage     storage.Provider
	StorageType string
	Tracker     *stats.Tracker
	Concurrency int
	DataSource  datasource.Source
	EncPassword string // used only to build the modifier chain
	DumpPrefix  string
	Timeout     time.Duration
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
		safeName := strings.ReplaceAll(path, "/", "-")
		backupName := fmt.Sprintf("%s-%s", m.DumpPrefix, safeName)

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

			pipeline.RunWithRetry(ctx, m.DataSource, m.Storage, targetPath, fileName, nameSuffix, writerMods, m.Tracker, m.Timeout)
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
	currentMonth := now.Format("2006-01")
	previousMonth := now.AddDate(0, 0, -now.Day()).Format("2006-01")

	var targets []string
	for _, p := range basePaths {
		targets = append(targets, fmt.Sprintf("%s-%s", p, currentMonth))
		targets = append(targets, fmt.Sprintf("%s-%s", p, previousMonth))
	}
	return targets
}
