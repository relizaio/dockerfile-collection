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
		DropPending:         viper.GetBool("drop-pending"),
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

	// --drop-pending: do NOT rotate. Verify each already-backed-up leftover archive
	// against its stored sidecar (+ pg_restore -l) and drop the ones that verify.
	// This is the confirm step after a --no-drop run -- it drops the exact object
	// you inspected, without re-dumping.
	if cfg.DropPending {
		return dropPendingArchives(ctx, backend, cfg)
	}

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
	if cfg.NoDrop {
		// Every run under --no-drop leaves the rotated archive backed up but undropped
		// (disk NOT reclaimed). Return non-zero + WARN so this can't be mistaken for a
		// completed rotation and archives don't accumulate silently.
		slog.Warn("no_drop_archives_retained", "note", "archive(s) rotated + backed up + verified but NOT dropped (--no-drop); disk not reclaimed -- finalize with --drop-pending after confirming")
		return fmt.Errorf("--no-drop: archive(s) retained and drop deferred; finalize with --drop-pending")
	}
	return nil
}

// dropPendingArchives implements --drop-pending: verify each already-backed-up
// leftover archive against its sidecar (+ pg_restore -l) and drop the ones that
// verify. A failure quarantines that archive (logged, run exits non-zero) but does
// not stop the others. No rotation, no re-dump.
func dropPendingArchives(ctx context.Context, backend *pgArchiveBackend, cfg *config.AppConfig) error {
	leftovers, err := backend.QueryRows(ctx, listArchivesSQL(cfg.PGSchema, cfg.AuditTable))
	if err != nil {
		return fmt.Errorf("listing pending archives: %w", err)
	}
	if len(leftovers) == 0 {
		slog.Info("drop_pending_no_leftovers")
		return nil
	}
	quarantined := 0
	for _, archive := range leftovers {
		slog.Info("verifying_pending_archive", "archive", archive)
		if err := backend.verifyExistingAndDrop(ctx, archive); err != nil {
			slog.Error("pending_archive_verify_failed_not_dropped", "archive", archive, "error", err.Error())
			quarantined++
		}
	}
	if quarantined > 0 {
		return fmt.Errorf("%d pending archive(s) failed verification and were NOT dropped", quarantined)
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

// verifyExistingAndDrop is the --drop-pending path: verify an ALREADY-backed-up
// leftover archive against its stored sidecar digest (independent re-download +
// pg_restore -l) and, only if it verifies, DROP it -- WITHOUT re-dumping. This is
// the confirm step after a --no-drop run: it drops the exact object you inspected.
func (b *pgArchiveBackend) verifyExistingAndDrop(ctx context.Context, archive string) error {
	key, _ := b.keyAndSuffix(archive)
	vctx, cancel := b.verifyCtx(ctx)
	defer cancel()

	info, err := b.store.Head(vctx, key)
	if err != nil {
		return fmt.Errorf("no backup object for %s: %w", key, err)
	}
	want, err := b.readSidecar(vctx, key)
	if err != nil {
		return err
	}
	if err := b.verifyRestorable(vctx, key, want, info.Size); err != nil {
		return err
	}
	slog.Info("pending_archive_verified_dropping", "schema", b.cfg.PGSchema, "archive", archive, "key", key, "sha256", want)
	return b.Exec(ctx, dropArchiveSQL(b.cfg.PGSchema, archive, b.cfg.LockTimeout))
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
// LIKE can reclaim the canonical names.
//
// The rename-aside suffix is derived from the (unique) ARCHIVE name, NOT from the
// original constraint/index name: multiple archives can coexist (a --no-drop staging
// run, or a leftover that couldn't be dropped/was quarantined), and every rotation
// starts from a fresh table whose PK is again named `audit_pkey`. A suffix derived
// from the constant original name would collide schema-wide across archives
// (`relation "audit_pkey_..." already exists`); md5(archive) makes it per-archive
// unique. The renamed names are throwaway (the archive is dropped later); only
// uniqueness matters. left(name,54)+'_'+8 stays within the 63-byte identifier limit.
func rotateSQL(schema, audit, archive, lockTimeout string) string {
	return fmt.Sprintf(`BEGIN;
SET LOCAL lock_timeout = '%[4]s';
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
