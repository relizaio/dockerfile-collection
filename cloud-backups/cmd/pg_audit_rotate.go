package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/relizaio/cloud-backup/internal/config"
	"github.com/relizaio/cloud-backup/internal/pg"
	"github.com/relizaio/cloud-backup/internal/pipeline"
	"github.com/relizaio/cloud-backup/internal/stats"
	"github.com/relizaio/cloud-backup/internal/storage"
)

var pgAuditRotateCmd = &cobra.Command{
	Use:   "audit-rotate",
	Short: "Rotate a write-only audit table: back up + drop old rows, reclaim disk, keep readers hot",
	Long: `audit-rotate frees disk held by a large, append-only audit table without a
partitioning migration or any application change.

Each run: renames the live audit table aside, stands up a fresh identical table
that immediately receives new writes (carrying forward only the rows anything
reads -- INSTANCES plus an optional recent tail), streams the rotated-out archive
to permanent-retention cloud storage, and only then DROPs it (instant reclaim).
A failed run is safe: the rename rolls back on lock contention, and a leftover
archive from an interrupted run is finished (backed up + dropped) on the next run.

Point --dump-prefix / the storage secret at a SEPARATE permanent-retention bucket,
distinct from the regular DB backup bucket.`,
	Run: func(cmd *cobra.Command, args []string) {
		if err := runPGAuditRotate(); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	},
}

func runPGAuditRotate() error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	pgHost := viper.GetString("pg-host")
	pgPort := viper.GetString("pg-port")
	if host, port, err := net.SplitHostPort(pgHost); err == nil {
		pgHost = host
		pgPort = port
	}

	cfg := &config.AppConfig{
		PGHost:              pgHost,
		PGPort:              pgPort,
		PGDatabase:          viper.GetString("pg-database"),
		PGUser:              viper.GetString("pg-user"),
		PGSchema:            viper.GetString("pg-schema"),
		AuditTable:          viper.GetString("audit-table"),
		KeepTailDays:        viper.GetInt("keep-tail-days"),
		LockTimeout:         viper.GetString("lock-timeout"),
		StorageType:         viper.GetString("backup-storage-type"),
		EncryptionPassword:  viper.GetString("encryption-password"),
		DumpPrefix:          viper.GetString("dump-prefix"),
		Timeout:             viper.GetDuration("timeout"),
		AWSBucket:           viper.GetString("aws-bucket"),
		AWSRegion:           viper.GetString("aws-region"),
		AWSAccessKeyID:      viper.GetString("aws-access-key-id"),
		AWSSecretAccessKey:  viper.GetString("aws-secret-access-key"),
		AzureStorageAccount: viper.GetString("azure-storage-account"),
		AzureTenantID:       viper.GetString("azure-tenant-id"),
		AzureClientID:       viper.GetString("azure-client-id"),
		AzureClientSecret:   viper.GetString("azure-client-secret"),
		AzureContainer:      viper.GetString("azure-container"),
	}
	if err := cfg.ValidatePGAuditRotate(); err != nil {
		slog.Error("validation_error", "error", err.Error())
		return err
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(fmt.Errorf("runPGAuditRotate exited"))

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	go func() {
		select {
		case sig := <-sigCh:
			slog.Error("received_termination_signal", "signal", sig.String())
			cancel(fmt.Errorf("received OS signal: %v", sig))
		case <-ctx.Done():
		}
	}()

	storeProvider, err := storage.New(ctx, cfg.StorageConfig())
	if err != nil {
		slog.Error("storage_initialization_failed", "error", err.Error())
		return err
	}

	pgClient := &pg.Client{Host: cfg.PGHost, Port: cfg.PGPort, Database: cfg.PGDatabase, User: cfg.PGUser}
	slog.Info("running_preflight_check", "host", cfg.PGHost, "port", cfg.PGPort)
	if err := pgClient.PreflightCheck(ctx, cfg.PGDatabase); err != nil {
		slog.Error("preflight_check_failed", "error", err.Error())
		return err
	}

	start := time.Now()
	tracker := stats.New()

	// 1. Recover any archives left behind by an interrupted prior run: finish them
	//    (back up + drop) BEFORE creating a new one, so archives never pile up.
	leftovers, err := pgClient.QueryRows(ctx, listArchivesSQL(cfg.PGSchema, cfg.AuditTable))
	if err != nil {
		slog.Error("list_pending_archives_failed", "error", err.Error())
		return err
	}
	for _, archive := range leftovers {
		slog.Info("recovering_pending_archive", "archive", archive)
		if err := backupAndDropArchive(ctx, pgClient, storeProvider, cfg, archive, tracker); err != nil {
			slog.Error("recover_pending_archive_failed", "archive", archive, "error", err.Error())
			return err
		}
	}

	// 2. Rotate: rename the live table aside and stand up a fresh one (fail-safe on
	//    lock contention). The read set is carried forward inside backupAndDropArchive
	//    (step 3), so it also runs on the recovery path above.
	archive := fmt.Sprintf("%s_archive_%s", cfg.AuditTable, time.Now().UTC().Format("20060102t150405z"))
	slog.Info("rotating_audit_table", "schema", cfg.PGSchema, "table", cfg.AuditTable, "archive", archive)
	if err := pgClient.Exec(ctx, rotateSQL(cfg.PGSchema, cfg.AuditTable, archive, cfg.LockTimeout)); err != nil {
		slog.Error("rotate_failed_will_retry_next_run", "error", err.Error())
		return err
	}

	// 3. Carry forward the read set, back up the archive, then drop it.
	if err := backupAndDropArchive(ctx, pgClient, storeProvider, cfg, archive, tracker); err != nil {
		slog.Error("backup_and_drop_failed", "archive", archive, "error", err.Error())
		return err
	}

	stats.PrintSummary("pg_audit_rotate_completed", tracker, cfg.StorageType, time.Since(start))
	if tracker.GetFailedCount() > 0 {
		return fmt.Errorf("pg audit-rotate completed with failures")
	}
	return nil
}

// backupAndDropArchive streams the archive table to permanent storage and, only
// on a verified upload, DROPs it. If the upload fails the archive is left in
// place for the next run's recovery step -- the drop is the sole irreversible
// step and is always gated on a clean upload.
func backupAndDropArchive(ctx context.Context, pgClient *pg.Client, store storage.Provider, cfg *config.AppConfig, archive string, tracker *stats.Tracker) error {
	fqTable := fmt.Sprintf("%s.%s", cfg.PGSchema, archive)

	// Carry the read set (INSTANCES + optional tail) forward from this sealed
	// archive into the live table before we drop it. Idempotent (ON CONFLICT DO
	// NOTHING) and safe to run on both the normal and recovery paths, so a
	// keep-copy that failed on a prior run is repaired here rather than stranding
	// readable rows in an about-to-be-dropped archive.
	if err := pgClient.Exec(ctx, keepCopySQL(cfg.PGSchema, cfg.AuditTable, archive, cfg.KeepTailDays)); err != nil {
		return fmt.Errorf("keep-copy from %s failed: %w", fqTable, err)
	}

	nameSuffix := ".dump"
	var writerMods []pipeline.WriterModifier
	if cfg.EncryptionPassword != "" {
		nameSuffix += ".age"
		writerMods = append(writerMods, pipeline.WithAgeEncryption(cfg.EncryptionPassword))
	}

	// Dump only this table. The object name is DETERMINISTIC per archive: the
	// archive name already carries a unique UTC timestamp, so each rotation gets a
	// distinct, never-overwritten key across runs -- while a retry or a recovery
	// re-upload of the SAME archive reuses that key instead of littering the
	// permanent (no-expiry) bucket with truncated/duplicate multi-GB objects.
	dumpClient := &pg.Client{Host: cfg.PGHost, Port: cfg.PGPort, Database: cfg.PGDatabase, User: cfg.PGUser, Table: fqTable}
	backupName := fmt.Sprintf("%s-%s", cfg.DumpPrefix, archive)

	failedBefore := tracker.GetFailedCount()
	skippedBefore := tracker.GetSkippedCount()
	pipeline.RunWithRetry(ctx, dumpClient, store, cfg.PGDatabase, backupName, nameSuffix, writerMods, tracker, cfg.Timeout, true)
	// Drop only on a positively verified upload: neither a failure nor a skip.
	if tracker.GetFailedCount() > failedBefore || tracker.GetSkippedCount() > skippedBefore {
		return fmt.Errorf("archive %s not uploaded; leaving it in place for next-run recovery", fqTable)
	}

	slog.Info("archive_uploaded_dropping", "archive", fqTable)
	if err := pgClient.Exec(ctx, fmt.Sprintf("DROP TABLE %s.%s;", cfg.PGSchema, archive)); err != nil {
		return fmt.Errorf("archive uploaded but drop failed for %s: %w", fqTable, err)
	}
	return nil
}

// listArchivesSQL lists archive tables left by prior runs (audit_archive_<stamp>).
// Underscores in the audit name are escaped so they are matched literally rather
// than as LIKE single-char wildcards.
func listArchivesSQL(schema, audit string) string {
	escAudit := strings.ReplaceAll(audit, "_", `\_`)
	return fmt.Sprintf(
		"SELECT tablename FROM pg_tables WHERE schemaname = '%s' AND tablename LIKE '%s\\_archive\\_%%' ESCAPE '\\' ORDER BY tablename;",
		schema, escAudit,
	)
}

// rotateSQL renames the live table aside and creates a fresh identical one in a
// single transaction. lock_timeout keeps it fail-safe: on contention the whole
// statement rolls back (table untouched) and the run retries next cycle. The
// archive's constraints/indexes are renamed aside first so the fresh CREATE ...
// LIKE gets stable, non-drifting canonical names.
func rotateSQL(schema, audit, archive, lockTimeout string) string {
	return fmt.Sprintf(`BEGIN;
SET LOCAL lock_timeout = '%[4]s';
ALTER TABLE %[1]s.%[2]s RENAME TO %[3]s;
DO $ROT$
DECLARE r record;
BEGIN
  FOR r IN SELECT conname FROM pg_constraint WHERE conrelid = '%[1]s.%[3]s'::regclass LOOP
    EXECUTE format('ALTER TABLE %[1]s.%[3]s RENAME CONSTRAINT %%I TO %%I', r.conname, left(r.conname || '_arch', 63));
  END LOOP;
  FOR r IN SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
           WHERE i.indrelid = '%[1]s.%[3]s'::regclass
             AND NOT EXISTS (SELECT 1 FROM pg_constraint con WHERE con.conindid = i.indexrelid) LOOP
    EXECUTE format('ALTER INDEX %[1]s.%%I RENAME TO %%I', r.relname, left(r.relname || '_arch', 63));
  END LOOP;
END
$ROT$;
CREATE TABLE %[1]s.%[2]s (LIKE %[1]s.%[3]s INCLUDING ALL);
COMMIT;
`, schema, audit, archive, lockTimeout)
}

// keepCopySQL carries the read set forward from the now-sealed archive into the
// fresh table, out of the rotation lock. INSTANCES rows (the only ones read) are
// always kept; keepTailDays optionally keeps a recent tail of all entities.
// ON CONFLICT DO NOTHING guards against a rare concurrent re-audit.
func keepCopySQL(schema, audit, archive string, keepTailDays int) string {
	return fmt.Sprintf(`INSERT INTO %[1]s.%[2]s
SELECT * FROM %[1]s.%[3]s
WHERE entity_name = 'instances'
   OR revision_created_date >= now() - make_interval(days => %[4]d)
ON CONFLICT DO NOTHING;
`, schema, audit, archive, keepTailDays)
}

func init() {
	pgCmd.AddCommand(pgAuditRotateCmd)
}
