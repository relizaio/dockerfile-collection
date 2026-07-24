package cmd

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/relizaio/cloud-backup/internal/config"
	"github.com/relizaio/cloud-backup/internal/pg"
	"github.com/relizaio/cloud-backup/internal/pipeline"
	"github.com/relizaio/cloud-backup/internal/progress"
	"github.com/relizaio/cloud-backup/internal/stats"
	"github.com/relizaio/cloud-backup/internal/storage"
)

var pgAuditRotateCmd = &cobra.Command{
	Use:   "audit-rotate",
	Short: "Rotate a write-only audit table: back up, retain by age, drop, reclaim disk",
	Long: `audit-rotate frees disk held by a large, append-only audit table without a
partitioning migration or any application change.

Each run has two passes. Pass 1 reconciles the archives left by prior runs: it
finishes any interrupted backup (recovery, without re-dumping an already-uploaded
one) and DROPs whole any archive older than the retention window (instant reclaim).
Pass 2 rotates -- renames the live table aside as an immutable, timestamp-named
archive and stands up a fresh EMPTY table for new writes -- then backs the archive
up to permanent-retention cloud storage and RETAINS it (queryable by name for ops
inspection) until a later run ages it out. --drain-backlog drops the new archive
immediately, for the one-off cutover run that reclaims the historical backlog.

A failed run is safe: the rename rolls back on lock contention; an archive is
dropped only after its backup is verified present; and cross-run state lives only
in Postgres (the archive tables) and the bucket (dump + .sha256 sidecar), never on
the pod. Point --dump-prefix / the storage secret at a SEPARATE permanent-retention
bucket, distinct from the regular DB backup bucket.`,
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
		RetentionDays:       viper.GetInt("audit-retention-days"),
		RotationInterval:    viper.GetInt("rotation-interval-days"),
		LockTimeout:         viper.GetString("lock-timeout"),
		AllowUnencrypted:    viper.GetBool("allow-unencrypted"),
		VerifyRestore:       viper.GetBool("verify-restore"),
		DrainBacklog:        viper.GetBool("drain-backlog"),
		DropInstanceRows:    viper.GetBool("drop-instance-rows"),
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

	// Preflight: CREATE TABLE ... LIKE ... INCLUDING ALL does NOT reproduce table
	// ownership or GRANTs, so any non-owner privilege on the audit table would be lost
	// on the fresh table after the first rotation -- silently breaking any writer/reader
	// that relies on that GRANT (a split-role deployment). Refuse rather than break
	// audit writes in production. No-op when the rotate role owns the table and there
	// are no extra grants (the common single-role case: relacl is owner-only/NULL).
	if grantees, err := pgClient.QueryRows(ctx, nonOwnerGrantsSQL(cfg.PGSchema, cfg.AuditTable)); err != nil {
		slog.Error("grants_precheck_failed", "error", err.Error())
		return err
	} else if len(grantees) > 0 {
		err := fmt.Errorf("audit table %s.%s has GRANTs to non-owner role(s) [%s] that rotation (CREATE TABLE LIKE) will NOT reproduce; audit writes/reads via those roles would break after the first rotation. Make the rotate role the table owner (or remove those direct grants) before enabling audit-rotate", cfg.PGSchema, cfg.AuditTable, strings.Join(grantees, ", "))
		slog.Error("non_owner_grants_refusing", "error", err.Error())
		return err
	}

	// Preflight: this mode does NOT carry the frozen entity_name='instances' rows
	// forward, but InstanceService still READS them (old-revision instance deep-links
	// + 30-day analytics). Dropping is a deliberate, documented decision valid only
	// where none exist. Enforce that precondition rather than silently destroying
	// live-read data on a deployment that does have them; --drop-instance-rows is the
	// conscious override. Only meaningful pre-rotation (the fresh table has none), so
	// on the steady-state cron this is a cheap count returning 0.
	//
	// This guard is SPECIFIC to the generic audit table's frozen-instances semantics:
	// it only applies to a table that HAS an entity_name column. Another rotated table
	// (e.g. metrics_audit) has no such column and no such rows, so the guard is skipped
	// there -- and running the count unconditionally would error on the missing column.
	if !cfg.DropInstanceRows {
		hasEntityName, err := pgClient.QueryRows(ctx, columnExistsSQL(cfg.PGSchema, cfg.AuditTable, "entity_name"))
		if err != nil {
			slog.Error("entity_name_column_check_failed", "error", err.Error())
			return err
		}
		if len(hasEntityName) == 1 && hasEntityName[0] == "t" {
			rows, err := pgClient.QueryRows(ctx, countInstancesSQL(cfg.PGSchema, cfg.AuditTable))
			if err != nil {
				slog.Error("instances_precount_failed", "error", err.Error())
				return err
			}
			// Proceed only on a definitive count of exactly 0; refuse on any rows OR an
			// unexpected result shape (never assume zero from an ambiguous answer).
			if len(rows) != 1 || rows[0] != "0" {
				got := "an unexpected count result"
				if len(rows) == 1 {
					got = rows[0] + " entity_name='instances' row(s)"
				}
				err := fmt.Errorf("%s.%s returned %s still read by the app (InstanceService); audit-rotate does not carry them forward, so the app's instance-revision reads would return empty once they age out of the DB (the rows are still backed up to the permanent bucket). Set --drop-instance-rows to proceed as a conscious cutover", cfg.PGSchema, cfg.AuditTable, got)
				slog.Error("instances_rows_present_refusing", "error", err.Error())
				return err
			}
		}
	}

	now := time.Now()
	tracker := stats.New()
	backend := &pgArchiveBackend{Client: pgClient, store: storeProvider, cfg: cfg}

	// Pass 1: reconcile the archives left by prior runs BEFORE creating a new one.
	// For each, independently: (a) if its backup is not yet durable in the bucket,
	// finish it (recovery -- an interrupted upload is re-dumped; an already-present
	// one is NOT re-dumped); (b) if it has aged past the retention window, DROP it
	// whole (O(1) reclaim); otherwise it is retained (a forensic safety buffer /
	// inspectable-by-name for the retention window). A single archive that can't be
	// reconciled is QUARANTINED (logged + surfaced at the end) but must NOT halt the
	// run -- one poison archive can't be allowed to wedge disk relief forever.
	leftovers, err := pgClient.QueryRows(ctx, listArchivesSQL(cfg.PGSchema, cfg.AuditTable))
	if err != nil {
		slog.Error("list_pending_archives_failed", "error", err.Error())
		return err
	}
	quarantined, dropped, recovered, retained := 0, 0, 0, 0
	for _, leftover := range leftovers {
		rec, drp, rerr := reconcileArchive(ctx, backend, cfg, leftover, now, tracker)
		if rerr != nil {
			slog.Error("reconcile_archive_quarantined", "archive", leftover, "error", rerr.Error())
			quarantined++
			continue
		}
		if rec {
			recovered++
		}
		if drp {
			dropped++
		} else {
			retained++
		}
	}

	// Rotation gate: decide whether to cut a new archive THIS run. With
	// rotation-interval-days == 0 (default) we rotate every run (unchanged behavior).
	// Otherwise rotation is decoupled from the cron cadence -- we rotate only when the
	// newest existing archive is >= the interval old (or none exists), so a fast cron
	// reconciles (Pass 1) every run but cuts archives only every interval. --drain-backlog
	// always rotates (the one-off cutover). Re-query the archive set AFTER Pass 1's drops
	// so the decision reflects reality. newestSeen feeds the rotate guard below.
	current, err := pgClient.QueryRows(ctx, listArchivesSQL(cfg.PGSchema, cfg.AuditTable))
	if err != nil {
		slog.Error("list_current_archives_failed", "error", err.Error())
		return err
	}
	newestSeen, newestRot, haveNewest := newestArchive(current, cfg.AuditTable)
	newestAgeDays := 0
	if haveNewest {
		newestAgeDays = int(now.UTC().Sub(newestRot).Hours() / 24)
	}
	rotate, skipReason := rotationDecision(cfg, now, newestRot, haveNewest)

	rotated := false
	if rotate {
		// Pass 2: rotate -- rename the live table aside and stand up a fresh EMPTY one
		// (fail-safe on lock contention). The new archive is backed up + verified and then
		// RETAINED for the retention window; a later run drops it once aged. Exception:
		// --drain-backlog drops it THIS run, to reclaim the historical backlog immediately
		// on the one-off cutover run (the recurring cron never sets it).
		archive, err := newArchiveName(cfg.AuditTable, now)
		if err != nil {
			return err
		}
		slog.Info("rotating_audit_table", "schema", cfg.PGSchema, "table", cfg.AuditTable, "archive", archive)
		// The rotate transaction is self-guarding against a concurrent rotation (a manual
		// job racing the cron): it takes a transaction advisory lock and aborts if a newer
		// archive already exists, so two overlapping runs can't both cut an archive (the
		// second would otherwise rotate the fresh EMPTY table into a stray archive that
		// squats for a whole retention window). A guarded abort is a benign skip, not a
		// failure -- another run already did the rotation.
		if err := pgClient.Exec(ctx, rotateSQL(cfg.PGSchema, cfg.AuditTable, archive, cfg.LockTimeout, advisoryLockKey(cfg.PGSchema, cfg.AuditTable), newestSeen)); err != nil {
			if isRotateSkip(err) {
				slog.Info("rotation_skipped_concurrent", "archive", archive, "reason", "another run rotated concurrently (advisory-lock/supersession guard)")
				skipReason = "concurrent rotation by another run"
			} else {
				slog.Error("rotate_failed_will_retry_next_run", "error", err.Error())
				return err
			}
		} else {
			rotated = true
			if err := backend.BackupAndVerify(ctx, archive, tracker); err != nil {
				slog.Error("backup_and_verify_failed", "archive", archive, "error", err.Error())
				return err
			}
			if cfg.DrainBacklog {
				slog.Info("drain_backlog_dropping_new_archive", "schema", cfg.PGSchema, "archive", archive)
				// deepVerify=false: BackupAndVerify above already did the restore-verify this
				// run (when --verify-restore), so skip a redundant full re-download.
				if err := backend.verifyAndDrop(ctx, archive, false); err != nil {
					slog.Error("drain_backlog_drop_failed", "archive", archive, "error", err.Error())
					return err
				}
				dropped++
			} else {
				retained++
			}
		}
	} else {
		slog.Info("rotation_skipped_not_due", "reason", skipReason, "newest_archive_age_days", newestAgeDays, "rotation_interval_days", cfg.RotationInterval)
	}

	// A single at-a-glance signal for alerting. rotated_this_run + newest_archive_age_days
	// let a monitor page on "was due but did not rotate" (rotated_this_run=false AND
	// newest_archive_age_days >= rotation_interval_days + grace) -- the failure that, under
	// interval rotation, would otherwise read as healthy (a stalled rotation leaves a young
	// or zero archive set, so the old oldest>retention signal can't see it). A reconcile-
	// only run reads clearly as "skipped: not due yet". oldest_archive_age_days is retained
	// for the retention-health signal (climbing past retention_days = drops not keeping up).
	rotationSkippedReason := ""
	if !rotated {
		rotationSkippedReason = skipReason
	}
	slog.Info("audit_rotate_summary",
		"archives_found", len(leftovers),
		"archives_recovered", recovered,
		"archives_dropped", dropped,
		"archives_retained", retained,
		"archives_quarantined", quarantined,
		"rotated_this_run", rotated,
		"rotation_interval_days", cfg.RotationInterval,
		"rotation_skipped_reason", rotationSkippedReason,
		"newest_archive_age_days", newestAgeDays,
		"oldest_archive_age_days", oldestArchiveAgeDays(leftovers, cfg.AuditTable, now),
		"retention_days", cfg.RetentionDays)

	stats.PrintSummary("pg_audit_rotate_completed", tracker, cfg.StorageType, time.Since(now))
	if tracker.GetFailedCount() > 0 {
		return fmt.Errorf("pg audit-rotate completed with failures")
	}
	if quarantined > 0 {
		// Rotation succeeded (this cycle's archive rotated + backed up) but a leftover
		// needs attention; return non-zero so the CronJob surfaces it.
		return fmt.Errorf("%d archive(s) could not be reconciled and were quarantined; rotation proceeded", quarantined)
	}
	return nil
}

// reconcileArchive brings one leftover archive to its correct state WITHOUT ever
// dropping an un-backed-up one, and reports what it did. First it ensures a durable
// backup exists in the bucket: an already-backed-up archive is left as-is, while one
// with a missing/incomplete backup is re-dumped (recovered=true). Then, if the archive
// has aged past the retention window, it is DROPped whole (dropped=true); otherwise it
// is left in place.
// A name whose rotation time can't be parsed is a quarantine (error), never dropped
// -- fail safe.
func reconcileArchive(ctx context.Context, b *pgArchiveBackend, cfg *config.AppConfig, archive string, now time.Time, tracker *stats.Tracker) (recovered, dropped bool, err error) {
	backedUp, err := b.hasBackup(ctx, archive)
	if err != nil {
		return false, false, fmt.Errorf("checking backup state of %s: %w", archive, err)
	}
	if !backedUp {
		slog.Info("recovering_pending_archive", "archive", archive)
		if err := b.BackupAndVerify(ctx, archive, tracker); err != nil {
			return false, false, fmt.Errorf("recovering %s: %w", archive, err)
		}
		recovered = true
	}
	aged, err := agedOut(archive, cfg.AuditTable, now, cfg.RetentionDays)
	if err != nil {
		return recovered, false, fmt.Errorf("cannot determine rotation time for %s (will not drop): %w", archive, err)
	}
	if !aged {
		slog.Info("archive_retained_in_window", "archive", archive, "retention_days", cfg.RetentionDays)
		return recovered, false, nil
	}
	slog.Info("archive_aged_out_dropping", "archive", archive, "retention_days", cfg.RetentionDays)
	if err := b.verifyAndDrop(ctx, archive, cfg.VerifyRestore); err != nil {
		return recovered, false, err
	}
	return recovered, true, nil
}

// pgArchiveBackend wires the real psql/pg_dump client to the object store. The
// backup+verify step and the pre-drop gate are isolated as methods that touch ONLY
// the store (backupIsDroppable) so the sole irreversible step -- the drop -- can be
// unit-tested against a fake store without a real Postgres.
type pgArchiveBackend struct {
	*pg.Client
	store storage.Provider
	cfg   *config.AppConfig
}

// countingWriter counts bytes written (io.Writer) for the size check.
type countingWriter struct{ n int64 }

func (c *countingWriter) Write(p []byte) (int, error) { c.n += int64(len(p)); return len(p), nil }

// atomicCountWriter counts bytes written into an atomic counter so a progress.Monitor
// on another goroutine can read it concurrently. Used to surface verify-restore
// re-download progress (the download goroutine writes; the monitor reads).
type atomicCountWriter struct{ n *atomic.Int64 }

func (a *atomicCountWriter) Write(p []byte) (int, error) { a.n.Add(int64(len(p))); return len(p), nil }

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
// keyAndSuffix is the SINGLE source of the object key + name suffix for an archive
// (both the RunWithRetry upload target and the Head/sidecar/verify lookups derive
// from here, so they can never drift).
func (b *pgArchiveBackend) keyAndSuffix(archive string) (key, suffix string) {
	suffix = ".dump"
	if b.cfg.EncryptionPassword != "" {
		suffix += ".age"
	}
	return fmt.Sprintf("%s-%s%s", b.cfg.DumpPrefix, archive, suffix), suffix
}

// verifyTimeout bounds the post-upload verification calls (Head/sidecar/re-download),
// which run outside pipeline.RunWithRetry's per-job timeout.
func (b *pgArchiveBackend) verifyCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if b.cfg.Timeout > 0 {
		return context.WithTimeout(ctx, b.cfg.Timeout)
	}
	return ctx, func() {}
}

// BackupAndVerify dumps the archive to a DETERMINISTIC per-archive key, then verifies
// it landed intact before the caller drops it: upload success (on real AWS, S3
// verifies every part's SHA-256 server-side), a HeadObject size match, and a
// whole-object SHA-256 recorded as a <key>.sha256 sidecar. With --verify-restore it
// additionally re-downloads, decrypts, runs pg_restore -l (proves the archive is a
// RESTORABLE dump, not just intact bytes), and matches the SHA-256. NOTE: without
// --verify-restore this is a BYTE-INTEGRITY gate, not a proof of restorability.
func (b *pgArchiveBackend) BackupAndVerify(ctx context.Context, archive string, tracker *stats.Tracker) error {
	key, nameSuffix := b.keyAndSuffix(archive)
	var writerMods []pipeline.WriterModifier
	if b.cfg.EncryptionPassword != "" {
		writerMods = append(writerMods, pipeline.WithAgeEncryption(b.cfg.EncryptionPassword))
	}
	dumpClient := &pg.Client{Host: b.cfg.PGHost, Port: b.cfg.PGPort, Database: b.cfg.PGDatabase, User: b.cfg.PGUser, Table: fmt.Sprintf("%s.%s", b.cfg.PGSchema, archive)}
	backupName := fmt.Sprintf("%s-%s", b.cfg.DumpPrefix, archive)

	// Estimate the dump size from the table's on-disk size so the upload progress can
	// report an approximate percent + ETA and the watcher has a size expectation up
	// front. pg_table_size = heap + TOAST (no indexes); the compressed dump is usually
	// somewhat smaller, so this is an approximate upper bound.
	var totalHint int64
	if rows, err := b.QueryRows(ctx, fmt.Sprintf("SELECT pg_table_size('%s.%s')", b.cfg.PGSchema, archive)); err == nil && len(rows) == 1 {
		if n, perr := strconv.ParseInt(rows[0], 10, 64); perr == nil {
			totalHint = n
			slog.Info("archive_backup_starting", "archive", archive, "estimated_size", stats.FormatBytes(n), "note", "estimate from table size; the compressed/encrypted object may differ")
		}
	}

	hp := &hashingProvider{Provider: b.store}
	successBefore := tracker.GetSuccess()
	pipeline.RunWithRetry(ctx, dumpClient, hp, b.cfg.PGDatabase, backupName, nameSuffix, writerMods, tracker, b.cfg.Timeout, true, totalHint)
	if tracker.GetSuccess() <= successBefore {
		return fmt.Errorf("upload did not complete for %s", key)
	}

	vctx, cancel := b.verifyCtx(ctx)
	defer cancel()
	if err := b.verifyUploadedObject(vctx, key, hp.bytes, hp.sha256Hex); err != nil {
		return err
	}
	slog.Info("archive_backup_verified", "key", key, "size", hp.bytes, "sha256", hp.sha256Hex, "verify_restore", b.cfg.VerifyRestore)
	return nil
}

// verifyUploadedObject is the post-upload gate: confirm the object exists with the
// exact streamed size, record the SHA-256 sidecar, and -- when verify-restore is set
// -- prove it's a restorable dump (re-download + decrypt + pg_restore -l + SHA-256
// match). Returns nil only when all checks pass; the caller drops only on nil.
func (b *pgArchiveBackend) verifyUploadedObject(ctx context.Context, key string, wantBytes int64, wantSHA256 string) error {
	info, err := b.store.Head(ctx, key)
	if err != nil {
		return fmt.Errorf("post-upload HeadObject failed for %s: %w", key, err)
	}
	if info.Size != wantBytes {
		return fmt.Errorf("uploaded size mismatch for %s: streamed %d, stored %d", key, wantBytes, info.Size)
	}
	if err := b.store.UploadStream(ctx, key+".sha256", strings.NewReader(wantSHA256+"\n")); err != nil {
		return fmt.Errorf("writing sha256 sidecar for %s: %w", key, err)
	}
	if b.cfg.VerifyRestore {
		if err := b.verifyRestorable(ctx, key, wantSHA256, info.Size); err != nil {
			return err
		}
		slog.Info("archive_verify_restore_passed", "key", key, "sha256", wantSHA256)
	}
	return nil
}

// verifyRestorable re-downloads the object and confirms BOTH that its raw bytes hash
// to wantSHA256 (independent, end-to-end byte integrity) AND -- decrypting first if
// needed -- that `pg_restore -l` accepts it (a structurally valid, restorable dump).
// The download is teed into the hasher while pg_restore consumes the decrypted stream.
// total is the exact object size (from HeadObject), so re-download progress can report
// a real (not estimated) percent-done and ETA; the long re-download would otherwise be
// silent and look like a hang.
func (b *pgArchiveBackend) verifyRestorable(ctx context.Context, key, wantSHA256 string, total int64) error {
	pr, pw := io.Pipe()
	hasher := sha256.New()
	dlErrCh := make(chan error, 1)
	var downloaded atomic.Int64
	slog.Info("archive_verify_restore_starting", "key", key, "download_size", stats.FormatBytes(total))
	mon := progress.New(&downloaded, "verify-restore:"+key, 10*time.Second, total).
		SetEvent("verify_download_in_progress", "verify_download_stalled_or_waiting").SetPrecise()
	mon.Start(ctx)
	go func() {
		var err error
		// Guarantee the monitor is stopped, the pipe is closed, and dlErrCh receives
		// exactly once on EVERY exit -- including a panic in DownloadStream/MultiWriter
		// (mirrors executeStream's download goroutine) so the reader never hangs on
		// <-dlErrCh and the monitor goroutine never leaks.
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic in verify-restore download: %v", r)
			}
			mon.Stop()
			pw.CloseWithError(err)
			dlErrCh <- err
		}()
		err = b.store.DownloadStream(ctx, key, io.MultiWriter(hasher, &atomicCountWriter{&downloaded}, pw))
	}()

	var reader io.Reader = pr
	if b.cfg.EncryptionPassword != "" {
		dec, err := pipeline.ApplyReaderModifiers(pr, []pipeline.ReaderModifier{pipeline.WithAgeDecryption(b.cfg.EncryptionPassword)})
		if err != nil {
			pr.CloseWithError(err)
			<-dlErrCh
			return fmt.Errorf("verify-restore decrypt setup failed for %s: %w", key, err)
		}
		reader = dec
	}
	restoreErr := b.RestoreList(ctx, reader)
	if restoreErr != nil {
		// Invalid/corrupt archive: abort the download, we already have our answer.
		pr.CloseWithError(restoreErr)
		<-dlErrCh
		return fmt.Errorf("verify-restore pg_restore -l failed for %s: %w", key, restoreErr)
	}
	// pg_restore -l reads only the header + TOC, not the whole archive. Drain the
	// remainder so the download completes and the SHA-256 covers the ENTIRE object.
	if _, err := io.Copy(io.Discard, pr); err != nil {
		<-dlErrCh
		return fmt.Errorf("verify-restore drain failed for %s: %w", key, err)
	}
	if dlErr := <-dlErrCh; dlErr != nil {
		return fmt.Errorf("verify-restore re-download failed for %s: %w", key, dlErr)
	}
	if got := hex.EncodeToString(hasher.Sum(nil)); got != wantSHA256 {
		return fmt.Errorf("verify-restore SHA-256 mismatch for %s: recorded %s, re-downloaded %s", key, wantSHA256, got)
	}
	return nil
}

// readSidecar downloads the <key>.sha256 sidecar and returns the recorded digest.
func (b *pgArchiveBackend) readSidecar(ctx context.Context, key string) (string, error) {
	var buf strings.Builder
	if err := b.store.DownloadStream(ctx, key+".sha256", &buf); err != nil {
		return "", fmt.Errorf("reading sidecar %s.sha256: %w", key, err)
	}
	return strings.TrimSpace(buf.String()), nil
}

// hasBackup reports whether the archive's backup is durably present in the bucket.
// It requires BOTH the dump object AND its .sha256 sidecar (the sidecar is written
// LAST, after the dump upload + a HeadObject size match, so in normal operation its
// presence already implies a complete upload; requiring the dump too makes recovery
// self-heal the anomaly where the dump was later deleted out-of-band -- either
// missing => not-backed-up => re-dump, which overwrites both keys). This state lives
// in the bucket, NOT on the (ephemeral) pod. A definitive ErrNotFound on either
// object means not-backed-up; any other (transient/credential) error is propagated
// so the caller neither re-dumps blindly nor treats an ambiguous state as absence.
func (b *pgArchiveBackend) hasBackup(ctx context.Context, archive string) (bool, error) {
	key, _ := b.keyAndSuffix(archive)
	vctx, cancel := b.verifyCtx(ctx)
	defer cancel()
	for _, obj := range []string{key, key + ".sha256"} {
		if _, err := b.store.Head(vctx, obj); err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return false, nil
			}
			return false, fmt.Errorf("checking backup object %s: %w", obj, err)
		}
	}
	return true, nil
}

// verifyAndDrop is the guarded drop, shared by the retention aged-out drop and the
// --drain-backlog path. It confirms the archive's backup is durably present in
// the bucket, then DROPs the archive table (the sole irreversible step, so it runs
// only after the gate passes). The cheap default gate is EXISTENCE-only -- the dump
// object AND its .sha256 sidecar must both be present (Head, no download): the size
// was verified at write time before the sidecar was written, and object stores don't
// truncate in place, so both-present is a sound pre-drop gate. With --verify-restore
// it additionally re-downloads, decrypts, runs pg_restore -l, and matches the
// whole-object SHA-256 against the sidecar (proves restorability, at the cost of a
// full re-download) -- reserve that for the on-demand/cutover job.
func (b *pgArchiveBackend) verifyAndDrop(ctx context.Context, archive string, deepVerify bool) error {
	if err := b.backupIsDroppable(ctx, archive, deepVerify); err != nil {
		return err
	}
	slog.Info("archive_verified_dropping", "schema", b.cfg.PGSchema, "archive", archive, "deep_verify", deepVerify)
	return b.Exec(ctx, dropArchiveSQL(b.cfg.PGSchema, archive, b.cfg.LockTimeout))
}

// backupIsDroppable is the pre-drop gate: it returns nil only when the archive's
// backup is safe to drop. It touches ONLY the object store (no DB), so it -- the
// decision guarding the sole irreversible step -- is unit-testable against a fake
// store. The cheap default gate is existence-only: the dump object AND its .sha256
// sidecar must both be present (Head, no download). With --verify-restore it also
// re-downloads, decrypts, runs pg_restore -l, and matches the whole-object SHA-256
// against the sidecar (proves restorability, at the cost of a full re-download) --
// callers pass deepVerify=false to skip that when the archive was already
// verify-restored earlier in the SAME run (e.g. --drain-backlog right after
// BackupAndVerify), avoiding a redundant full re-download. A definitive ErrNotFound
// -- or any other error -- returns non-nil so the caller does NOT drop.
func (b *pgArchiveBackend) backupIsDroppable(ctx context.Context, archive string, deepVerify bool) error {
	key, _ := b.keyAndSuffix(archive)
	vctx, cancel := b.verifyCtx(ctx)
	defer cancel()

	dumpInfo, err := b.store.Head(vctx, key)
	if err != nil {
		return fmt.Errorf("pre-drop check: backup object missing/unreadable for %s (NOT dropping): %w", key, err)
	}
	if _, err := b.store.Head(vctx, key+".sha256"); err != nil {
		return fmt.Errorf("pre-drop check: sidecar missing/unreadable for %s (NOT dropping): %w", key, err)
	}
	if deepVerify {
		want, err := b.readSidecar(vctx, key)
		if err != nil {
			return err
		}
		if err := b.verifyRestorable(vctx, key, want, dumpInfo.Size); err != nil {
			return err
		}
		slog.Info("archive_verify_restore_passed", "key", key, "sha256", want)
	}
	return nil
}

// archiveInfix separates the audit base name from the rotation timestamp in an
// archive table name (<audit><archiveInfix><ts>_<hex>). archiveTSLayout is the UTC
// timestamp layout used to BOTH format (newArchiveName) and parse (archiveRotationTime)
// that timestamp -- a single source so the two can never drift. Drift would silently
// break archiveRotationTime, which by its fail-safe design would then retain every
// archive forever (never drop) -- the exact disk-relief failure this feature prevents.
const (
	archiveInfix    = "_archive_"
	archiveTSLayout = "20060102t150405z"
)

// newArchiveName builds a per-rotation archive table name with a second-resolution
// UTC timestamp plus a random suffix, so two distinct rotations landing in the same
// wall-second (rapid manual re-run, or a backward clock step) never collide on the
// table name or -- since the object key is derived from it -- on the storage object.
func newArchiveName(audit string, now time.Time) (string, error) {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generating archive suffix: %w", err)
	}
	return fmt.Sprintf("%s%s%s_%s", audit, archiveInfix, now.UTC().Format(archiveTSLayout), hex.EncodeToString(b)), nil
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

// countInstancesSQL counts the FROZEN legacy entity_name='instances' rows in the
// live audit table. These are still READ by InstanceService (old-revision instance
// deep-links + 30-day analytics) but no longer written; this mode does NOT carry
// them forward, so a first rotation against a table that has them would eventually
// destroy them. The preflight uses this to refuse (unless --drop-instance-rows)
// rather than silently lose live-read data.
func countInstancesSQL(schema, audit string) string {
	return fmt.Sprintf("SELECT count(*) FROM %s.%s WHERE entity_name = 'instances';", schema, audit)
}

// columnExistsSQL returns 't'/'f' for whether the table has the named column. Used to
// scope the entity_name-specific instances preflight to tables that actually have that
// column (the generic audit table); a table without it -- e.g. metrics_audit -- skips the
// guard rather than erroring on the missing column.
func columnExistsSQL(schema, table, column string) string {
	return fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' AND column_name = '%s');", schema, table, column)
}

// nonOwnerGrantsSQL lists the roles (or PUBLIC) that hold a GRANT on the audit table
// other than its owner. CREATE TABLE ... LIKE ... INCLUDING ALL copies neither table
// ownership nor ACLs, so any such grant silently vanishes on the fresh table after a
// rotation. aclexplode(relacl) yields one row per (grantee, privilege); relacl is NULL
// (=> no rows) for the common owner-only table, so this returns nothing there.
func nonOwnerGrantsSQL(schema, audit string) string {
	return fmt.Sprintf(`SELECT DISTINCT CASE WHEN acl.grantee = 0 THEN 'PUBLIC' ELSE acl.grantee::regrole::text END
FROM pg_class c, aclexplode(c.relacl) AS acl
WHERE c.oid = '%s.%s'::regclass AND acl.grantee <> c.relowner;`, schema, audit)
}

// oldestArchiveAgeDays returns the age in whole days of the oldest parseable archive
// (0 if none) -- an at-a-glance signal for alerting: climbing past retentionDays means
// aged archives aren't being dropped (retention isn't keeping up). Unparseable names
// are skipped (they're quarantined elsewhere).
func oldestArchiveAgeDays(archives []string, audit string, now time.Time) int {
	oldest := 0
	for _, a := range archives {
		if rot, err := archiveRotationTime(a, audit); err == nil {
			if d := int(now.UTC().Sub(rot).Hours() / 24); d > oldest {
				oldest = d
			}
		}
	}
	return oldest
}

// archiveRotationTime extracts the rotation instant encoded in an archive table name
// (<audit>_archive_<YYYYMMDDtHHMMSSz>_<hex>). This is the drop-gate oracle --
// deliberately NOT revision_created_date, a content column that can be backdated. The
// timestamp is UTC (the trailing 'z' is a literal in the layout, not a zone). Returns
// an error on any malformed name so the caller can fail SAFE (retain, never drop) --
// never the time.Parse zero-value (year 1), which would read as "ancient -> drop".
func archiveRotationTime(archive, audit string) (time.Time, error) {
	rest := strings.TrimPrefix(archive, audit+archiveInfix)
	if rest == archive || len(rest) < len(archiveTSLayout) {
		return time.Time{}, fmt.Errorf("archive name %q does not carry a rotation timestamp", archive)
	}
	return time.Parse(archiveTSLayout, rest[:len(archiveTSLayout)])
}

// agedOut reports whether an archive has passed the retention window, measured from
// its rotation time (from the name) -- never from backdatable row content. It returns
// an error (caller must NOT drop) rather than a boolean on a name it can't parse.
// retentionDays==0 makes any prior-run archive eligible for drop on the next run.
func agedOut(archive, audit string, now time.Time, retentionDays int) (bool, error) {
	rot, err := archiveRotationTime(archive, audit)
	if err != nil {
		return false, err
	}
	cutoff := now.UTC().Add(-time.Duration(retentionDays) * 24 * time.Hour)
	return rot.Before(cutoff), nil
}

// dropArchiveSQL drops the archive inside a txn bounded by lock_timeout (so a
// concurrent ACCESS SHARE holder -- e.g. the full-DB backup's pg_dump -- makes the
// drop fail fast and defer to next-run recovery, not hang) with no statement_timeout.
// DROP TABLE IF EXISTS makes a concurrent double-drop benign: two overlapping runs can
// both list the same aged archive in Pass 1 and both try to drop it; the loser's drop of
// the already-gone table is a no-op success, not a spurious quarantine + non-zero exit.
func dropArchiveSQL(schema, archive, lockTimeout string) string {
	return fmt.Sprintf(`BEGIN;
SET LOCAL lock_timeout = '%[3]s';
SET LOCAL statement_timeout = 0;
DROP TABLE IF EXISTS %[1]s.%[2]s;
COMMIT;
`, schema, archive, lockTimeout)
}

// advisoryLockKey derives the transaction advisory-lock key that serializes rotation for
// a given audit table (distinct tables don't block each other). Stable across runs and
// hosts (a pure hash of schema.table), 64-bit to match pg_try_advisory_xact_lock(bigint).
func advisoryLockKey(schema, audit string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("cloud-backup:audit-rotate:" + schema + "." + audit))
	return int64(h.Sum64())
}

// rotateSkipToken marks a rotate transaction that aborted because another run already
// rotated (advisory-lock contention or a superseding newer archive). It's a benign skip,
// not a failure -- the caller detects it via isRotateSkip and continues without error.
const rotateSkipToken = "AUDIT_ROTATE_SKIP"

func isRotateSkip(err error) bool {
	return err != nil && strings.Contains(err.Error(), rotateSkipToken)
}

// newestArchive returns the newest (latest rotation time) archive in the set and whether
// any exists. Names from listArchivesSQL all carry a well-formed timestamp (the SQL regex
// matches exactly the generated shape), so each parses; the skip-on-parse-error is
// defensive. Used to decide whether the interval has elapsed since the last rotation.
func newestArchive(archives []string, audit string) (name string, rot time.Time, ok bool) {
	for _, a := range archives {
		t, err := archiveRotationTime(a, audit)
		if err != nil {
			continue
		}
		if !ok || t.After(rot) {
			name, rot, ok = a, t, true
		}
	}
	return name, rot, ok
}

// rotationDecision decides whether Pass 2 cuts a new archive this run. --drain-backlog and
// the OFF setting (rotation-interval-days == 0) always rotate (today's every-run behavior).
// Otherwise rotate only when no archive exists (bootstrap / all aged out) OR the newest one
// is older than the interval -- using the SAME precise cutoff as the retention drop
// (agedOut). Sharing the threshold is what makes interval == retention hold EXACTLY one
// archive: on the run the lone archive crosses the line, Pass 1 drops it, so this re-queried
// set is empty and we rotate a fresh one -- never a transient second archive.
func rotationDecision(cfg *config.AppConfig, now, newestRot time.Time, haveNewest bool) (rotate bool, skipReason string) {
	if cfg.DrainBacklog || cfg.RotationInterval == 0 || !haveNewest {
		return true, ""
	}
	cutoff := now.UTC().Add(-time.Duration(cfg.RotationInterval) * 24 * time.Hour)
	if newestRot.Before(cutoff) {
		return true, ""
	}
	return false, fmt.Sprintf("newest archive rotated %s; rotation interval %dd not yet elapsed", newestRot.UTC().Format(time.RFC3339), cfg.RotationInterval)
}

// rotateSQL renames the live table aside and creates a fresh identical one in a
// single transaction. lock_timeout keeps it fail-safe: on contention the whole
// statement rolls back (table untouched) and the run retries next cycle. The
// archive's constraints/indexes are renamed aside first so the fresh CREATE ...
// LIKE can reclaim the canonical names.
//
// The rename-aside suffix is derived from the (unique) ARCHIVE name, NOT from the
// original constraint/index name: multiple archives coexist under retention (up to
// ~retention/cadence at once, plus any quarantined leftover), and every rotation
// starts from a fresh table whose PK is again named `audit_pkey`. A suffix derived
// from the constant original name would collide schema-wide across archives
// (`relation "audit_pkey_..." already exists`); md5(archive) makes it per-archive
// unique. The renamed names are throwaway (the archive is dropped later); only
// uniqueness matters. left(name,54)+'_'+8 stays within the 63-byte identifier limit.
//
// The transaction is self-guarding against a CONCURRENT rotation (a manual job racing
// the cron, which k8s concurrencyPolicy can't prevent for distinct jobs): it takes a
// transaction advisory lock (lockKey) and aborts if any archive newer than newestSeen
// already exists. Without this, two runs that both passed the rotation gate would each
// rename -- the second renaming the fresh EMPTY table into a stray archive that squats
// for a whole retention window. Both checks run BEFORE the rename, inside the lock, so
// they're atomic w.r.t. another rotation; the abort raises rotateSkipToken, which the
// caller treats as a benign skip. newestSeen is "" when no archive existed at decision
// time (then any archive => superseded); otherwise it's the newest name the gate saw.
// The supersession test is lexical (tablename > newestSeen), which equals chronological
// because the embedded timestamp is fixed-width UTC -- sound unless the wall clock steps
// backward far enough that a concurrent winner's name sorts below newestSeen (needs
// multiple rotations inside one wall-second plus a clock step; not operationally reachable,
// same NTP assumption newArchiveName already notes).
func rotateSQL(schema, audit, archive, lockTimeout string, lockKey int64, newestSeen string) string {
	return fmt.Sprintf(`BEGIN;
SET LOCAL lock_timeout = '%[4]s';
SET LOCAL statement_timeout = 0;
DO $GUARD$
BEGIN
  IF NOT pg_try_advisory_xact_lock(%[5]d) THEN
    RAISE EXCEPTION '%[6]s: another rotation holds the advisory lock';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = '%[1]s'
             AND tablename ~ '^%[2]s_archive_[0-9]{8}t[0-9]{6}z(_[0-9a-f]+)?$'
             AND tablename > '%[7]s') THEN
    RAISE EXCEPTION '%[6]s: a newer archive already exists (superseded by a concurrent run)';
  END IF;
END
$GUARD$;
ALTER TABLE %[1]s.%[2]s RENAME TO %[3]s;
DO $ROT$
DECLARE r record;
DECLARE sfx text := substr(md5('%[3]s'), 1, 8);
BEGIN
  FOR r IN SELECT conname FROM pg_constraint WHERE conrelid = '%[1]s.%[3]s'::regclass LOOP
    EXECUTE format('ALTER TABLE %[1]s.%[3]s RENAME CONSTRAINT %%I TO %%I', r.conname, left(r.conname, 54) || '_' || sfx);
  END LOOP;
  FOR r IN SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
           WHERE i.indrelid = '%[1]s.%[3]s'::regclass
             AND NOT EXISTS (SELECT 1 FROM pg_constraint con WHERE con.conindid = i.indexrelid) LOOP
    EXECUTE format('ALTER INDEX %[1]s.%%I RENAME TO %%I', r.relname, left(r.relname, 54) || '_' || sfx);
  END LOOP;
END
$ROT$;
CREATE TABLE %[1]s.%[2]s (LIKE %[1]s.%[3]s INCLUDING ALL);
COMMIT;
`, schema, audit, archive, lockTimeout, lockKey, rotateSkipToken, newestSeen)
}

func init() {
	pgCmd.AddCommand(pgAuditRotateCmd)
}
