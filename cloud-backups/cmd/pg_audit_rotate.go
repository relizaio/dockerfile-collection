package cmd

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
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
		NoDrop:              viper.GetBool("no-drop"),
		VerifyRestore:       viper.GetBool("verify-restore"),
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

	// The keep-copy's ON CONFLICT DO NOTHING is a silent no-op (and would duplicate
	// INSTANCES rows on a recovery re-copy) unless a PK/UNIQUE constraint exists.
	if uniq, err := pgClient.QueryRows(ctx, assertHasUniqueSQL(cfg.PGSchema, cfg.AuditTable)); err != nil {
		slog.Error("unique_constraint_precheck_failed", "error", err.Error())
		return err
	} else if len(uniq) == 0 {
		err := fmt.Errorf("audit table %s.%s has no PRIMARY KEY / UNIQUE constraint; audit-rotate needs one for idempotent keep-copy", cfg.PGSchema, cfg.AuditTable)
		slog.Error("unsupported_audit_table", "error", err.Error())
		return err
	}

	start := time.Now()
	tracker := stats.New()
	backend := &pgArchiveBackend{Client: pgClient, store: storeProvider, cfg: cfg}

	// 1. Recover any archives left behind by an interrupted prior run: finish them
	//    (back up + drop) BEFORE creating a new one, so archives never pile up. A
	//    single archive that can't be recovered (e.g. a live-table schema change it
	//    can't be keep-copied into) is QUARANTINED (logged + surfaced at the end) but
	//    must NOT halt the rotation -- otherwise one poison archive wedges the whole
	//    disk-relief mechanism forever.
	leftovers, err := pgClient.QueryRows(ctx, listArchivesSQL(cfg.PGSchema, cfg.AuditTable))
	if err != nil {
		slog.Error("list_pending_archives_failed", "error", err.Error())
		return err
	}
	quarantined := 0
	for _, archive := range leftovers {
		slog.Info("recovering_pending_archive", "archive", archive)
		if err := backupAndDropArchive(ctx, backend, cfg, archive, tracker); err != nil {
			slog.Error("recover_pending_archive_quarantined", "archive", archive, "error", err.Error())
			quarantined++
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
	if err := backupAndDropArchive(ctx, backend, cfg, archive, tracker); err != nil {
		slog.Error("backup_and_drop_failed", "archive", archive, "error", err.Error())
		return err
	}

	stats.PrintSummary("pg_audit_rotate_completed", tracker, cfg.StorageType, time.Since(start))
	if tracker.GetFailedCount() > 0 {
		return fmt.Errorf("pg audit-rotate completed with failures")
	}
	if quarantined > 0 {
		// Rotation succeeded (disk reclaimed for this cycle) but a leftover needs
		// attention; return non-zero so the CronJob surfaces it.
		return fmt.Errorf("%d leftover archive(s) could not be recovered and were quarantined; rotation proceeded", quarantined)
	}
	return nil
}

// archiveBackend is the seam backupAndDropArchive depends on. Isolating the DB
// (QueryRows/Exec) and the backup+verify step behind an interface lets the
// drop-gate -- the sole irreversible step -- be unit-tested with a fake, without
// a real Postgres or object store.
type archiveBackend interface {
	QueryRows(ctx context.Context, sql string) ([]string, error)
	Exec(ctx context.Context, sql string) error
	// BackupAndVerify streams the archive table to storage and verifies it landed
	// intact (upload success + HeadObject size match + sidecar SHA-256, and -- when
	// verify-restore is set -- a full re-download SHA-256 match). Returns nil ONLY
	// on a fully verified upload; the caller drops only on nil.
	BackupAndVerify(ctx context.Context, archive string, tracker *stats.Tracker) error
}

// pgArchiveBackend is the production archiveBackend: real psql/pg_dump + storage.
type pgArchiveBackend struct {
	*pg.Client
	store storage.Provider
	cfg   *config.AppConfig
}

// countingWriter counts bytes written (io.Writer) for the size check.
type countingWriter struct{ n int64 }

func (c *countingWriter) Write(p []byte) (int, error) { c.n += int64(len(p)); return len(p), nil }

// hashingProvider wraps a storage.Provider and, on UploadStream, computes a SHA-256
// and byte count of EXACTLY the bytes streamed to storage (the final stored object,
// post-encryption). Used single-threaded per archive, so no locking is needed.
type hashingProvider struct {
	storage.Provider
	sha256Hex string
	bytes     int64
}

func (h *hashingProvider) UploadStream(ctx context.Context, remotePath string, reader io.Reader) error {
	hasher := sha256.New()
	counter := &countingWriter{}
	tee := io.TeeReader(reader, io.MultiWriter(hasher, counter))
	if err := h.Provider.UploadStream(ctx, remotePath, tee); err != nil {
		return err
	}
	h.sha256Hex = hex.EncodeToString(hasher.Sum(nil))
	h.bytes = counter.n
	return nil
}

// objectKey returns the deterministic storage key for an archive's dump.
func (b *pgArchiveBackend) objectKey(archive string) string {
	suffix := ".dump"
	if b.cfg.EncryptionPassword != "" {
		suffix += ".age"
	}
	return fmt.Sprintf("%s-%s%s", b.cfg.DumpPrefix, archive, suffix)
}

// BackupAndVerify dumps the archive to a DETERMINISTIC per-archive key, then verifies
// it landed intact before the caller drops it: (1) upload success (S3 verifies every
// part's SHA-256 server-side and refuses on mismatch), (2) a HeadObject size match,
// (3) a whole-object SHA-256 recorded as a sidecar, and (4) -- when verify-restore is
// set -- a full re-download SHA-256 match against that digest.
func (b *pgArchiveBackend) BackupAndVerify(ctx context.Context, archive string, tracker *stats.Tracker) error {
	nameSuffix := ".dump"
	var writerMods []pipeline.WriterModifier
	if b.cfg.EncryptionPassword != "" {
		nameSuffix += ".age"
		writerMods = append(writerMods, pipeline.WithAgeEncryption(b.cfg.EncryptionPassword))
	}
	dumpClient := &pg.Client{Host: b.cfg.PGHost, Port: b.cfg.PGPort, Database: b.cfg.PGDatabase, User: b.cfg.PGUser, Table: fmt.Sprintf("%s.%s", b.cfg.PGSchema, archive)}
	key := b.objectKey(archive)
	backupName := fmt.Sprintf("%s-%s", b.cfg.DumpPrefix, archive)

	hp := &hashingProvider{Provider: b.store}
	successBefore := tracker.GetSuccess()
	pipeline.RunWithRetry(ctx, dumpClient, hp, b.cfg.PGDatabase, backupName, nameSuffix, writerMods, tracker, b.cfg.Timeout, true)
	if tracker.GetSuccess() <= successBefore {
		return fmt.Errorf("upload did not complete for %s", key)
	}

	// Cheap: confirm the object exists with the exact size we streamed.
	info, err := b.store.Head(ctx, key)
	if err != nil {
		return fmt.Errorf("post-upload HeadObject failed for %s: %w", key, err)
	}
	if info.Size != hp.bytes {
		return fmt.Errorf("uploaded size mismatch for %s: streamed %d, stored %d", key, hp.bytes, info.Size)
	}

	// Record the whole-object SHA-256 as a sidecar for independent re-verification.
	if err := b.store.UploadStream(ctx, key+".sha256", strings.NewReader(hp.sha256Hex+"\n")); err != nil {
		return fmt.Errorf("writing sha256 sidecar for %s: %w", key, err)
	}

	if b.cfg.VerifyRestore {
		hasher := sha256.New()
		if err := b.store.DownloadStream(ctx, key, hasher); err != nil {
			return fmt.Errorf("verify-restore re-download failed for %s: %w", key, err)
		}
		if got := hex.EncodeToString(hasher.Sum(nil)); got != hp.sha256Hex {
			return fmt.Errorf("verify-restore SHA-256 mismatch for %s: recorded %s, re-downloaded %s", key, hp.sha256Hex, got)
		}
		slog.Info("archive_verify_restore_passed", "key", key, "sha256", hp.sha256Hex)
	}
	slog.Info("archive_backup_verified", "key", key, "size", hp.bytes, "sha256", hp.sha256Hex, "verify_restore", b.cfg.VerifyRestore)
	return nil
}

// backupAndDropArchive carries the read set forward, backs up + verifies the archive,
// and -- only on a fully VERIFIED upload (and unless --no-drop) -- DROPs it. Anything
// short of verified leaves the archive for the next run's recovery: the drop is the
// sole irreversible step.
func backupAndDropArchive(ctx context.Context, b archiveBackend, cfg *config.AppConfig, archive string, tracker *stats.Tracker) error {
	fqTable := fmt.Sprintf("%s.%s", cfg.PGSchema, archive)

	// Carry the read set (INSTANCES + optional tail) forward from this sealed
	// archive into the live table before we drop it. Idempotent (ON CONFLICT DO
	// NOTHING) and safe to run on both the normal and recovery paths, so a
	// keep-copy that failed on a prior run is repaired here rather than stranding
	// readable rows in an about-to-be-dropped archive. Columns are enumerated by
	// NAME (not SELECT *) so a schema change to the live table between a failed run
	// and its recovery copies the shared columns instead of wedging or corrupting.
	cols, err := b.QueryRows(ctx, sharedColumnsSQL(cfg.PGSchema, cfg.AuditTable, archive))
	if err != nil {
		return fmt.Errorf("resolving shared columns for %s: %w", fqTable, err)
	}
	if len(cols) == 0 {
		return fmt.Errorf("no shared columns between %s.%s and %s -- refusing keep-copy", cfg.PGSchema, cfg.AuditTable, fqTable)
	}
	if err := b.Exec(ctx, keepCopySQL(cfg.PGSchema, cfg.AuditTable, archive, cfg.KeepTailDays, cfg.LockTimeout, cols)); err != nil {
		return fmt.Errorf("keep-copy from %s failed: %w", fqTable, err)
	}

	// Back up + verify. Drop ONLY on nil (fully verified); any error leaves the
	// archive in place for next-run recovery.
	if err := b.BackupAndVerify(ctx, archive, tracker); err != nil {
		return fmt.Errorf("archive %s not verified; leaving it in place for next-run recovery: %w", fqTable, err)
	}

	if cfg.NoDrop {
		slog.Info("drop_deferred_no_drop", "archive", fqTable, "note", "backup verified; drop deferred -- run again without --no-drop, or DROP manually, after confirming")
		return nil
	}

	slog.Info("archive_verified_dropping", "archive", fqTable)
	if err := b.Exec(ctx, dropArchiveSQL(cfg.PGSchema, archive, cfg.LockTimeout)); err != nil {
		// Backup verified; a drop that lost the lock race just leaves the archive
		// for next-run recovery (which re-uploads to the same key and retries).
		return fmt.Errorf("archive verified but drop failed for %s: %w", fqTable, err)
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

// assertHasUniqueSQL returns the PRIMARY KEY / UNIQUE constraints on the audit
// table. The keep-copy's ON CONFLICT DO NOTHING idempotency (safe re-copy on the
// recovery path) is a no-op unless such a constraint exists; refuse if there is none.
func assertHasUniqueSQL(schema, audit string) string {
	return fmt.Sprintf(
		"SELECT constraint_name FROM information_schema.table_constraints WHERE table_schema = '%s' AND table_name = '%s' AND constraint_type IN ('PRIMARY KEY', 'UNIQUE');",
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
func keepCopySQL(schema, audit, archive string, keepTailDays int, lockTimeout string, cols []string) string {
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
SET lock_timeout = '%[6]s';
INSERT INTO %[1]s.%[2]s (%[4]s)
SELECT %[4]s FROM %[1]s.%[3]s
WHERE %[5]s
ON CONFLICT DO NOTHING;
`, schema, audit, archive, colList, where, lockTimeout)
}

func init() {
	pgCmd.AddCommand(pgAuditRotateCmd)
}
