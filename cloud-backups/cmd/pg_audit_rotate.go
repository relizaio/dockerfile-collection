package cmd

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
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
	Short: "Rotate a write-only audit table: retain by age, then back up and drop, reclaiming disk",
	Long: `audit-rotate frees disk held by a large, append-only audit table without a
partitioning migration or any application change.

Each run has two passes. Pass 1 handles the archives left by prior runs: any that
has aged past the retention window is dumped to permanent-retention cloud storage
and then DROPped whole (instant reclaim) -- backup and drop in the SAME run.
Younger archives are left untouched, on disk and queryable by name, as a forensic
buffer. Pass 2 rotates: it renames the live table aside as an immutable,
timestamp-named archive and stands up a fresh EMPTY table for new writes. The new
archive is NOT uploaded yet; the later run that ages it out backs it up and drops
it together. --drain-backlog does both immediately, for the one-off cutover run
that reclaims the historical backlog.

Backing up at drop time rather than at rotation time is what keeps the credential
write-only: the run that decides to drop is the run that just wrote the backup, so
it never asks the bucket whether an older archive is still backed up. The tool
therefore needs no read or list permission on the permanent bucket at all --
--verify-restore is the sole exception, and it is opt-in.

A failed run is safe: the rename rolls back on lock contention; a table is dropped
only after its own upload completed and, on real AWS, was checksum-verified
server-side seconds earlier; an archive that cannot be backed up is left in place and retried next run;
and cross-run state lives only in Postgres (the archive tables), never on the pod.
NOTE the trade this makes: an archive has no permanent copy until it ages out, so
keep it inside the whole-database backup for that window. Point --dump-prefix / the
storage secret at a SEPARATE permanent-retention bucket, distinct from the regular
DB backup bucket.`,
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
		KeepTailDays:        viper.GetInt("keep-tail-days"),
		KeepTailColumn:      viper.GetString("keep-tail-column"),
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

	// Preflight: keep-tail seeding filters on KeepTailColumn, so it must exist on the table.
	// Fail loudly here rather than let the rotate transaction blow up mid-rename on an
	// unknown column (which would roll back and wedge every run silently). No-op when not
	// seeding (KeepTailDays == 0, the write-only-table default).
	if cfg.KeepTailDays > 0 {
		hasKeepTailCol, err := pgClient.QueryRows(ctx, columnExistsSQL(cfg.PGSchema, cfg.AuditTable, cfg.KeepTailColumn))
		if err != nil {
			slog.Error("keep_tail_column_check_failed", "error", err.Error())
			return err
		}
		if len(hasKeepTailCol) != 1 || hasKeepTailCol[0] != "t" {
			err := fmt.Errorf("--keep-tail-days=%d needs column %q on %s.%s to seed the fresh table's recent tail, but that column is absent; a live reader's rolling-window read would lose its lookback across each rotation. Set --keep-tail-column to the table's rolling-window timestamp, or --keep-tail-days=0 for a pure (write-only-table) rotation", cfg.KeepTailDays, cfg.KeepTailColumn, cfg.PGSchema, cfg.AuditTable)
			slog.Error("keep_tail_column_absent_refusing", "error", err.Error())
			return err
		}
	}

	now := time.Now()
	tracker := stats.New()
	backend := newPGArchiveBackend(pgClient, storeProvider, cfg)

	// Pass 1: dump, back up and DROP every archive that has aged past the retention
	// window -- all in THIS run. That single-run coupling is the whole design: the run
	// that decides to drop is the run that just wrote the backup, so it never has to ask
	// the bucket "is this still backed up?" on a later run, and therefore needs no
	// read/list permission on the permanent bucket at all. Archives inside the window are
	// left completely alone (no bucket call whatsoever) as the forensic buffer. One
	// archive that can't be backed up must NOT halt the run: it is logged, counted and
	// retried next run, because a single poison archive can't be allowed to wedge disk
	// relief forever.
	existing, err := pgClient.QueryRows(ctx, listArchivesSQL(cfg.PGSchema, cfg.AuditTable))
	if err != nil {
		slog.Error("list_archives_failed", "error", err.Error())
		return err
	}
	failed, dropped, retained := 0, 0, 0
	for _, archive := range existing {
		aged, aerr := agedOut(archive, cfg.AuditTable, now, cfg.RetentionDays)
		if aerr != nil {
			// A name whose rotation time can't be parsed is never dropped -- fail safe.
			slog.Error("archive_age_unknown_not_dropping", "archive", archive, "error", aerr.Error())
			failed++
			continue
		}
		if !aged {
			slog.Info("archive_retained_in_window", "archive", archive, "retention_days", cfg.RetentionDays)
			retained++
			continue
		}
		slog.Info("archive_aged_out_backing_up_then_dropping", "archive", archive, "retention_days", cfg.RetentionDays)
		if derr := backend.backupAndDrop(ctx, archive, tracker); derr != nil {
			// An overlapping run (manual job racing the cron) may have backed up and
			// dropped this same archive between our listing and our dump, which surfaces
			// as a pg_dump failure on a missing table. That is someone else succeeding,
			// not us failing -- counting it would fire the dead-man alert on a healthy
			// pair of runs. Re-check existence before blaming ourselves.
			if gone, cerr := archiveGone(ctx, pgClient, cfg.PGSchema, archive); cerr == nil && gone {
				slog.Info("archive_dropped_by_concurrent_run", "archive", archive)
				continue
			}
			slog.Error("archive_backup_and_drop_failed", "archive", archive, "error", derr.Error())
			failed++
			continue
		}
		dropped++
	}

	// Rotation gate: decide whether to cut a new archive THIS run. With
	// rotation-interval-days == 0 (default) we rotate every run (unchanged behavior).
	// Otherwise rotation is decoupled from the cron cadence -- we rotate only when the
	// newest existing archive is >= the interval old (or none exists), so a fast cron
	// runs Pass 1 every run but cuts archives only every interval. --drain-backlog
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
		// (fail-safe on lock contention). The new archive is RETAINED for the retention
		// window and is not backed up yet; the later run that ages it out dumps it and
		// drops it together. Exception: --drain-backlog does both now.
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
		if err := pgClient.Exec(ctx, rotateSQL(cfg.PGSchema, cfg.AuditTable, archive, cfg.LockTimeout, advisoryLockKey(cfg.PGSchema, cfg.AuditTable), newestSeen, cfg.KeepTailDays, cfg.KeepTailColumn)); err != nil {
			if isRotateSkip(err) {
				slog.Info("rotation_skipped_concurrent", "archive", archive, "reason", "another run rotated concurrently (advisory-lock/supersession guard)")
				skipReason = "concurrent rotation by another run"
			} else {
				slog.Error("rotate_failed_will_retry_next_run", "error", err.Error())
				return err
			}
		} else {
			rotated = true
			// Deliberately NO upload here. The archive is retained on disk and will be
			// dumped when it ages out, by the very run that drops it. --drain-backlog is
			// the one-off cutover that reclaims the historical backlog immediately, so it
			// does both now; the recurring cron never sets it.
			if cfg.DrainBacklog {
				slog.Info("drain_backlog_backing_up_and_dropping_new_archive", "schema", cfg.PGSchema, "archive", archive)
				if err := backend.backupAndDrop(ctx, archive, tracker); err != nil {
					slog.Error("drain_backlog_backup_and_drop_failed", "archive", archive, "error", err.Error())
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
	// or zero archive set, so the old oldest>retention signal can't see it). A drop-only run
	// reads clearly as "skipped: not due yet". oldest_archive_age_days is the
	// retention-health signal (climbing past retention_days = drops not keeping up) and is
	// measured AFTER Pass 1: measured before, it would report >= retention_days on every
	// healthy run that dropped an aged archive -- an alert that fires on success.
	rotationSkippedReason := ""
	if !rotated {
		rotationSkippedReason = skipReason
	}
	slog.Info("audit_rotate_summary",
		// archives_found counts what existed BEFORE this run; dropped/retained also cover
		// the archive cut by Pass 2, so on a rotating run they sum to found+1.
		"archives_found", len(existing),
		"archives_dropped", dropped,
		"archives_retained", retained,
		"archives_failed", failed,
		"rotated_this_run", rotated,
		"rotation_interval_days", cfg.RotationInterval,
		"rotation_skipped_reason", rotationSkippedReason,
		"newest_archive_age_days", newestAgeDays,
		"oldest_archive_age_days", oldestArchiveAgeDays(current, cfg.AuditTable, now),
		"retention_days", cfg.RetentionDays,
		"keep_tail_days", cfg.KeepTailDays)

	stats.PrintSummary("pg_audit_rotate_completed", tracker, cfg.StorageType, time.Since(now))
	// Report the specific failure first: the generic tracker check below also fires for a
	// failed dump/upload, and would otherwise mask the message that names what is stuck.
	if failed > 0 {
		// Rotation itself succeeded but an aged-out archive could not be backed up and so
		// was NOT dropped; return non-zero so the CronJob surfaces it. Disk is not being
		// reclaimed until this clears, which is exactly what the dead-man alert watches.
		return fmt.Errorf("%d archive(s) could not be backed up and dropped, and were left in place; rotation proceeded", failed)
	}
	if tracker.GetFailedCount() > 0 {
		return fmt.Errorf("pg audit-rotate completed with failures")
	}
	return nil
}

// sqlExecutor is the DDL seam used for the DROP. It exists so the sole irreversible step
// stays unit-testable against a recorder instead of requiring a live Postgres -- the
// property the old pre-drop-gate tests protected, which would otherwise have been lost
// along with them.
type sqlExecutor interface {
	Exec(ctx context.Context, sql string) error
}

// pgArchiveBackend wires the real psql/pg_dump client to the object store. Both halves of
// the drop decision are injectable -- backupArchive (must succeed first) and exec (runs
// the DROP) -- so the ordering that makes the drop safe can be tested directly.
type pgArchiveBackend struct {
	*pg.Client
	store storage.Provider
	cfg   *config.AppConfig
	// exec runs the DROP statement; production wires it to the embedded *pg.Client.
	exec sqlExecutor
	// backupArchive is BackupAndVerify in production. Tests substitute a failing stub to
	// prove that a failed backup can never reach the DROP.
	backupArchive func(ctx context.Context, archive string, tracker *stats.Tracker) error
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
// (both the RunWithRetry upload target and the sidecar/verify keys derive
// from here, so they can never drift).
func (b *pgArchiveBackend) keyAndSuffix(archive string) (key, suffix string) {
	suffix = ".dump"
	if b.cfg.EncryptionPassword != "" {
		suffix += ".age"
	}
	return fmt.Sprintf("%s-%s%s", b.cfg.DumpPrefix, archive, suffix), suffix
}

// verifyCtx bounds the post-upload steps (sidecar write / re-download),
// which run outside pipeline.RunWithRetry's per-job timeout.
func (b *pgArchiveBackend) verifyCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if b.cfg.Timeout > 0 {
		return context.WithTimeout(ctx, b.cfg.Timeout)
	}
	return ctx, func() {}
}

// BackupAndVerify dumps the archive to a DETERMINISTIC per-archive key, then verifies
// it landed intact before the caller drops it: upload success (on real AWS, S3 verifies a
// full-object CRC32C of the ASSEMBLED object server-side) plus a whole-object SHA-256
// recorded as a <key>.sha256 sidecar. With --verify-restore it
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
	// Belt-and-braces on the authorization for an irreversible DROP. The check above is a
	// delta on a tracker shared by the whole run; hp is per-call, and its fields are only
	// assigned once UploadStream returned nil. Requiring both means a miscounted delta
	// alone can never authorize a drop.
	if hp.bytes <= 0 || hp.sha256Hex == "" {
		return fmt.Errorf("upload reported success for %s but streamed %d bytes (digest %q); refusing to treat it as backed up", key, hp.bytes, hp.sha256Hex)
	}

	vctx, cancel := b.verifyCtx(ctx)
	defer cancel()
	if err := b.finalizeUpload(vctx, key, hp.bytes, hp.sha256Hex); err != nil {
		return err
	}
	slog.Info("archive_backup_verified", "key", key, "size", hp.bytes, "sha256", hp.sha256Hex, "verify_restore", b.cfg.VerifyRestore)
	return nil
}

// finalizeUpload records the whole-object SHA-256 as a <key>.sha256 sidecar and, when
// --verify-restore is set, proves the object is a RESTORABLE dump before the caller drops
// the source.
//
// There is deliberately no post-upload existence/size probe. It would require read or list
// permission on the permanent bucket -- the thing this design exists to avoid -- and it did
// not catch what it appears to. A truncated SOURCE shrinks the stored object and our own
// streamed byte count equally, so the comparison passed; that case is caught instead by
// pg_dump's exit status propagating through the pipe, which aborts the upload outright
// (verified: an interrupted dump leaves no object at all and the archive is not dropped).
// What the probe genuinely caught -- an uploader assembling a short object from a partial
// part list -- is now caught server-side by the full-object checksum on S3. On Azure that
// residual is knowingly accepted: Azure validates every block on arrival but does not
// validate that the committed block list names them all, and offers no whole-blob
// equivalent. That is documented service behavior, not an SDK gap.
func (b *pgArchiveBackend) finalizeUpload(ctx context.Context, key string, uploaded int64, sha256Hex string) error {
	if err := b.store.UploadStream(ctx, key+".sha256", strings.NewReader(sha256Hex+"\n")); err != nil {
		return fmt.Errorf("writing sha256 sidecar for %s: %w", key, err)
	}
	if b.cfg.VerifyRestore {
		if err := b.verifyRestorable(ctx, key, sha256Hex, uploaded); err != nil {
			return err
		}
		slog.Info("archive_verify_restore_passed", "key", key, "sha256", sha256Hex)
	}
	return nil
}

// newPGArchiveBackend wires the production seams: the DROP goes through the real pg client
// and the backup step is the real dump+upload.
func newPGArchiveBackend(client *pg.Client, store storage.Provider, cfg *config.AppConfig) *pgArchiveBackend {
	b := &pgArchiveBackend{Client: client, store: store, cfg: cfg, exec: client}
	b.backupArchive = b.BackupAndVerify
	return b
}

// backupAndDrop is an archive's entire exit path, start to finish, in ONE run: dump ->
// upload (checksum-verified server-side) -> sidecar -> optionally prove restorability ->
// DROP. The drop is authorized by an upload that completed seconds earlier rather than by
// a bucket lookup days later -- both a stronger guarantee and the reason the steady-state
// credential needs no read access at all.
func (b *pgArchiveBackend) backupAndDrop(ctx context.Context, archive string, tracker *stats.Tracker) error {
	// Ordering is the safety property: the DROP below is unreachable unless the backup
	// returned nil. Nothing re-checks the bucket afterwards, so this is the only thing
	// standing between a failed upload and an irreversible drop.
	if b.backupArchive == nil || b.exec == nil {
		// Fail closed: an unwired seam must never fall through to the DROP.
		return fmt.Errorf("pgArchiveBackend is not fully wired (use newPGArchiveBackend); refusing to drop %s", archive)
	}
	if err := b.backupArchive(ctx, archive, tracker); err != nil {
		return err
	}
	slog.Info("archive_verified_dropping", "schema", b.cfg.PGSchema, "archive", archive, "verify_restore", b.cfg.VerifyRestore)
	return b.exec.Exec(ctx, dropArchiveSQL(b.cfg.PGSchema, archive, b.cfg.LockTimeout))
}

// verifyRestorable re-downloads the object and confirms BOTH that its raw bytes hash
// to wantSHA256 (independent, end-to-end byte integrity) AND -- decrypting first if
// needed -- that `pg_restore -l` accepts it (a structurally valid, restorable dump).
// The download is teed into the hasher while pg_restore consumes the decrypted stream.
// total is the byte count we streamed to storage, so re-download progress can report
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

// archiveGone reports whether the named archive table no longer exists -- used to tell a
// concurrent run's success apart from our own failure.
func archiveGone(ctx context.Context, c *pg.Client, schema, archive string) (bool, error) {
	rows, err := c.QueryRows(ctx, fmt.Sprintf("SELECT to_regclass('%s.%s') IS NULL;", schema, archive))
	if err != nil || len(rows) != 1 {
		return false, fmt.Errorf("could not determine whether %s.%s still exists", schema, archive)
	}
	return rows[0] == "t", nil
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
	return fmt.Sprintf("%s%s%s_%s", audit, archiveInfix, now.UTC().Format(archiveTSLayout), hex.EncodeToString(b)), nil
}

// listArchivesSQL lists archive tables left by prior runs. The match is ANCHORED to
// the exact generated name shape (<audit>_archive_<utc>[_<hex>]) via a regex, so it
// cannot pick up an operator's unrelated `<audit>_archive_manual` table (which
// Pass 1 would otherwise back up and DROP) and cannot be steered by a maliciously
// named table (the listed name is interpolated into DDL downstream).
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
// are skipped (they are counted as failed and never dropped).
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
// drop fail fast and defer to the next run, not hang) with no statement_timeout.
// DROP TABLE IF EXISTS makes a concurrent double-drop benign: two overlapping runs can
// both list the same aged archive in Pass 1 and both try to drop it; the loser's drop of
// the already-gone table is a no-op success, not a spurious failure + non-zero exit.
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
// ~retention/cadence at once, plus any archive left in place by a failed backup), and every rotation
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
//
// keepTailDays > 0 seeds the fresh table, in the SAME transaction, with the most recent
// keepTailDays of rows from the just-sealed archive (filtered by keepTailColumn). This
// exists for a table with a LIVE reader that queries the live table over a rolling date
// window -- the finding-change repair sweep reads metrics_audit WHERE revision_created_date
// >= now()-lookback. Pure rotation empties the live table, so for up to that lookback after
// each rotation the sweep's window would fall entirely into the archive and it would lose
// its self-heal (a permanent v3 hole). Seeding the same column the reader filters on keeps
// the whole window resident in the live table. The seed is atomic with the rename (the
// fresh table is not yet visible to other sessions, so a plain INSERT cannot conflict, and
// a failure rolls back the whole rotation to retry cleanly next run); the cost is that the
// ACCESS EXCLUSIVE window becomes O(rows copied) rather than catalog-only, so keepTailDays
// must stay small. 0 (default) writes no seed and is byte-for-byte the old behaviour --
// correct for a write-only table (audit).
func rotateSQL(schema, audit, archive, lockTimeout string, lockKey int64, newestSeen string, keepTailDays int, keepTailColumn string) string {
	keepTailSeed := ""
	if keepTailDays > 0 {
		keepTailSeed = fmt.Sprintf("INSERT INTO %[1]s.%[2]s SELECT * FROM %[1]s.%[3]s WHERE %[4]s >= now() - make_interval(days => %[5]d);\n",
			schema, audit, archive, keepTailColumn, keepTailDays)
	}
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
%[8]sCOMMIT;
`, schema, audit, archive, lockTimeout, lockKey, rotateSkipToken, newestSeen, keepTailSeed)
}

func init() {
	pgCmd.AddCommand(pgAuditRotateCmd)
}
