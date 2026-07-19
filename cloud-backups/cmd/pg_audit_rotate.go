package cmd

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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
		AllowUnencrypted:    viper.GetBool("allow-unencrypted"),
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

	// This mode assumes a client-generated PK (e.g. UUID). A serial/identity column
	// would have an owned sequence that CREATE ... LIKE shares and DROP later kills,
	// wedging app inserts -- so refuse rather than risk it.
	if seqCols, err := pgClient.QueryRows(ctx, assertNoOwnedSequenceSQL(cfg.PGSchema, cfg.AuditTable)); err != nil {
		slog.Error("owned_sequence_precheck_failed", "error", err.Error())
		return err
	} else if len(seqCols) > 0 {
		err := fmt.Errorf("audit table %s.%s has a serial/identity column (%s); audit-rotate requires a client-generated (e.g. UUID) primary key", cfg.PGSchema, cfg.AuditTable, strings.Join(seqCols, ","))
		slog.Error("unsupported_audit_table", "error", err.Error())
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
	archive, err := newArchiveName(cfg.AuditTable, time.Now())
	if err != nil {
		return err
	}
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
	// readable rows in an about-to-be-dropped archive. Columns are enumerated by
	// NAME (not SELECT *) so a schema change to the live table between a failed run
	// and its recovery copies the shared columns instead of wedging or corrupting.
	cols, err := pgClient.QueryRows(ctx, sharedColumnsSQL(cfg.PGSchema, cfg.AuditTable, archive))
	if err != nil {
		return fmt.Errorf("resolving shared columns for %s: %w", fqTable, err)
	}
	if len(cols) == 0 {
		return fmt.Errorf("no shared columns between %s.%s and %s -- refusing keep-copy", cfg.PGSchema, cfg.AuditTable, fqTable)
	}
	if err := pgClient.Exec(ctx, keepCopySQL(cfg.PGSchema, cfg.AuditTable, archive, cfg.KeepTailDays, cols)); err != nil {
		return fmt.Errorf("keep-copy from %s failed: %w", fqTable, err)
	}

	nameSuffix := ".dump"
	var writerMods []pipeline.WriterModifier
	if cfg.EncryptionPassword != "" {
		nameSuffix += ".age"
		writerMods = append(writerMods, pipeline.WithAgeEncryption(cfg.EncryptionPassword))
	}

	// Dump only this table. The object name is DETERMINISTIC per archive: the
	// archive name carries a unique UTC timestamp + random suffix, so each rotation
	// gets a distinct, never-overwritten key across runs -- while a retry or a
	// recovery re-upload of the SAME archive reuses that key instead of littering
	// the permanent (no-expiry) bucket with truncated/duplicate multi-GB objects.
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
	if err := pgClient.Exec(ctx, dropArchiveSQL(cfg.PGSchema, archive, cfg.LockTimeout)); err != nil {
		// Upload succeeded; a drop that lost the lock race just leaves the archive
		// for next-run recovery (which re-uploads to the same key and retries).
		return fmt.Errorf("archive uploaded but drop failed for %s: %w", fqTable, err)
	}
	return nil
}

// newArchiveName builds a per-rotation archive table name with a second-resolution
// UTC timestamp plus a random suffix, so two distinct rotations landing in the same
// wall-second (rapid manual re-run, or a backward clock step) never collide on the
// table name or -- since the object key is derived from it -- on the storage object.
func newArchiveName(audit string, now time.Time) (string, error) {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generating archive suffix: %w", err)
	}
	return fmt.Sprintf("%s_archive_%s_%s", audit, now.UTC().Format("20060102t150405z"), hex.EncodeToString(b)), nil
}

// listArchivesSQL lists archive tables left by prior runs. The match is ANCHORED to
// the exact generated name shape (<audit>_archive_<utc>[_<hex>]) via a regex, so it
// cannot pick up an operator's unrelated `<audit>_archive_manual` table (which
// recovery would otherwise back up and DROP) and cannot be steered by a maliciously
// named table (the recovered name is interpolated into DDL downstream).
func listArchivesSQL(schema, audit string) string {
	return fmt.Sprintf(
		"SELECT tablename FROM pg_tables WHERE schemaname = '%s' AND tablename ~ '^%s_archive_[0-9]{8}t[0-9]{6}z(_[0-9a-f]+)?$' ORDER BY tablename;",
		schema, audit,
	)
}

// assertNoOwnedSequenceSQL detects a serial/identity surrogate key on the audit
// table. CREATE TABLE ... LIKE INCLUDING ALL would share/duplicate the owned
// sequence, and the later DROP of the archive would take it down -- wedging app
// inserts. This mode assumes a client-generated (e.g. UUID) primary key.
func assertNoOwnedSequenceSQL(schema, audit string) string {
	return fmt.Sprintf(
		"SELECT column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' AND (is_identity = 'YES' OR column_default LIKE 'nextval(%%');",
		schema, audit,
	)
}

// sharedColumnsSQL returns the columns present in BOTH the live audit table and the
// archive, ordered by the live table's column order, so the keep-copy can name them
// explicitly and survive a schema change across a failed run + recovery.
func sharedColumnsSQL(schema, audit, archive string) string {
	return fmt.Sprintf(`SELECT a.column_name FROM information_schema.columns a
JOIN information_schema.columns b
  ON b.table_schema = a.table_schema AND b.table_name = '%[3]s' AND b.column_name = a.column_name
WHERE a.table_schema = '%[1]s' AND a.table_name = '%[2]s'
ORDER BY a.ordinal_position;`, schema, audit, archive)
}

// dropArchiveSQL drops the archive inside a txn bounded by lock_timeout (so a
// concurrent ACCESS SHARE holder -- e.g. the full-DB backup's pg_dump -- makes the
// drop fail fast and defer to next-run recovery, not hang) with no statement_timeout.
func dropArchiveSQL(schema, archive, lockTimeout string) string {
	return fmt.Sprintf(`BEGIN;
SET LOCAL lock_timeout = '%[3]s';
SET LOCAL statement_timeout = 0;
DROP TABLE %[1]s.%[2]s;
COMMIT;
`, schema, archive, lockTimeout)
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
// always kept; keepTailDays>0 additionally keeps a recent tail of all entities.
// Columns are named explicitly (survives schema drift; see backupAndDropArchive).
// With keepTailDays==0 the WHERE is INSTANCES-only so it uses the leading-column
// index instead of a full seq-scan on the unindexed revision_created_date. A
// generous statement_timeout=0 prevents a role-level statement_timeout from
// aborting the copy. ON CONFLICT DO NOTHING guards against a rare concurrent re-audit.
func keepCopySQL(schema, audit, archive string, keepTailDays int, cols []string) string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = `"` + strings.ReplaceAll(c, `"`, `""`) + `"`
	}
	colList := strings.Join(quoted, ", ")
	where := "entity_name = 'instances'"
	if keepTailDays > 0 {
		where += fmt.Sprintf("\n   OR revision_created_date >= now() - make_interval(days => %d)", keepTailDays)
	}
	return fmt.Sprintf(`SET statement_timeout = 0;
INSERT INTO %[1]s.%[2]s (%[4]s)
SELECT %[4]s FROM %[1]s.%[3]s
WHERE %[5]s
ON CONFLICT DO NOTHING;
`, schema, audit, archive, colList, where)
}

func init() {
	pgCmd.AddCommand(pgAuditRotateCmd)
}
