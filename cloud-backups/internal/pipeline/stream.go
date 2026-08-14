package pipeline

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/relizaio/cloud-backup/internal/datasource"
	"github.com/relizaio/cloud-backup/internal/progress"
	"github.com/relizaio/cloud-backup/internal/stats"
	"github.com/relizaio/cloud-backup/internal/storage"
)

const (
	MaxBackupAttempts = 3
	DefaultTimeout    = 2 * time.Hour
	// MaxCauseLength bounds a single attempt's cause inside an aggregated log
	// field, so an alert payload stays ingestible. Generous enough to keep the
	// failing URL and the tool's error line, which is what identifies the fault.
	MaxCauseLength = 2000
)

var (
	RetryBackoffBase   = 10 * time.Second
	MaxBackoffDuration = 5 * time.Minute
)

// RunWithRetry handles the retry logic and graceful degradation for missing source targets.
// nameSuffix is appended to the remote filename (e.g. ".tar.gz" or ".tar.gz.age").
// totalHint is an APPROXIMATE expected byte count for progress percent/ETA (0 = unknown).
// writerModifiers are applied in order to the upload stream (compress, then encrypt).
func RunWithRetry(ctx context.Context, src datasource.Source, storeProvider storage.Provider, target, backupName, nameSuffix string, writerModifiers []WriterModifier, tracker *stats.Tracker, timeout time.Duration, deterministicName bool, totalHint int64) {
	tracker.RecordJob()
	startTimer := time.Now()
	jobHandled := false
	defer func() {
		if !jobHandled {
			tracker.RecordFailure(target)
		}
	}()

	var attemptErrs []string

	for attempt := 1; attempt <= MaxBackupAttempts; attempt++ {
		if ctx.Err() != nil {
			return
		}
		slog.Info("backup_started", "target", target, "attempt", attempt)
		bytesUploaded, err := executeStream(ctx, src, storeProvider, target, backupName, nameSuffix, writerModifiers, timeout, deterministicName, totalHint)
		if err == nil {
			// A target that needed a retry recovered on its own, so it is not
			// operator-actionable and must not alert. It is still worth a line,
			// because a repeatedly flaky registry shows up here first.
			if len(attemptErrs) > 0 {
				slog.Warn("backup_recovered_after_retry", "target", target, "attempts_used", attempt,
					"earlier_failures", strings.Join(attemptErrs, " | "))
			}
			slog.Info("backup_successful", "target", target, "duration", time.Since(startTimer).Round(time.Second).String(), "size_human", stats.FormatBytes(bytesUploaded))
			jobHandled = true
			tracker.RecordSuccess()
			tracker.AddBytes(bytesUploaded)
			return
		}
		// FAST-FAIL ON UNAUTHORIZED
		if isAuthRejection(err) {
			// "detail", not "msg": slog's JSON handler already emits the event
			// name as "msg", and a second "msg" attr is written verbatim, so
			// every JSON parser in the alerting path keeps the LAST one and the
			// event name never survives parsing.
			slog.Error("fatal_authentication_error", "target", target, "detail", "Credentials rejected. Halting retries.",
				"error", TruncateCause(err.Error()))
			jobHandled = true
			tracker.RecordFailure(target)
			return // Exit immediately, do not wait for backoff
		}

		if strings.Contains(err.Error(), "repository name not known to registry") {
			// An absent repository is a legitimate skip, but only if it is the
			// FIRST thing we saw. Reaching here after an attempt already failed
			// for another reason means the classification is suspect (it is a
			// substring match on a log tail), and swallowing it would retire the
			// target with no ERROR at all. Surface those earlier causes.
			if len(attemptErrs) > 0 {
				slog.Error("repository_not_found_after_failed_attempts", "target", target,
					"detail", "classified as absent only after earlier attempts failed; treat the absence as unconfirmed",
					"error", strings.Join(attemptErrs, " | "))
			}
			slog.Warn("repository_not_found_skipping", "target", target)
			jobHandled = true
			tracker.RecordSkipped(target)
			return
		}

		// WARN, not ERROR: this attempt may still be retried, and operator
		// alerting fires on ERROR. A transient registry blip that the next
		// attempt recovers from is not an incident, and paging on it trains
		// operators to ignore the channel. The single ERROR for a target that
		// really did fail is backup_exhausted below.
		attemptErrs = append(attemptErrs, fmt.Sprintf("attempt %d: %s", attempt, TruncateCause(err.Error())))
		slog.Warn("backup_attempt_failed", "target", target, "attempt", attempt, "error", err.Error())
		if attempt < MaxBackupAttempts {
			if !waitBackoff(ctx, attempt) {
				// Abandoned mid-retry. PrintSummary will report the target as
				// failed, but only the causes we collected explain WHY, and they
				// are otherwise WARN-only -- invisible where alerting is
				// ERROR-only. Emit them before giving up.
				slog.Error("backup_abandoned", "target", target, "attempts_used", len(attemptErrs),
					"detail", "run cancelled before the retries were exhausted",
					"error", strings.Join(attemptErrs, " | "))
				return
			}
		}
	}
	// Carry every attempt's cause, because this is the only ERROR the operator
	// sees for a failed target and an empty one is unactionable.
	slog.Error("backup_exhausted", "target", target, "attempts_used", len(attemptErrs),
		"error", strings.Join(attemptErrs, " | "))
}

// isAuthRejection reports whether the registry refused the credentials, in which
// case retrying cannot help. Matching on error text is this module's established
// idiom (see internal/registry/oras.go); the point of the helper is that the
// backup and preflight drivers share ONE definition of the predicate.
func isAuthRejection(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "unauthorized") || strings.Contains(msg, "authentication required")
}

// TruncateCause bounds one attempt's cause before it goes into an aggregated
// log field. A cause carries the tail of the backup tool's own output (an 8KB
// tailBuffer), and backup_exhausted joins one per attempt -- unbounded, that is
// a single ~24KB field, which alerting backends truncate at an arbitrary point
// or reject outright.
//
// Keep the TAIL, not the head. The upstream buffer is already a tail buffer
// precisely because these tools print their diagnostic last, after progress
// chatter: `oras` ends with "Error: failed to ...". Head-truncating a tail
// buffer throws away the only line that names the fault and keeps the progress
// noise, so the surviving ERROR says nothing.
func TruncateCause(cause string) string {
	if len(cause) <= MaxCauseLength {
		return cause
	}
	// ToValidUTF8 because the cut can land mid-rune, and an invalid byte becomes
	// U+FFFD once the record is JSON-encoded -- corruption that reads like a bug
	// in the failure itself.
	tail := strings.ToValidUTF8(cause[len(cause)-MaxCauseLength:], "")
	return fmt.Sprintf("(truncated, %d bytes total, showing last %d) ...", len(cause), MaxCauseLength) + tail
}

// waitBackoff sleeps the exponential backoff for the just-failed attempt.
// It reports false when ctx was cancelled while waiting, meaning the caller
// should give up rather than start another attempt.
func waitBackoff(ctx context.Context, attempt int) bool {
	backoff := RetryBackoffBase * time.Duration(1<<uint(attempt))
	if backoff > MaxBackoffDuration {
		backoff = MaxBackoffDuration
	}
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// RunPreflightWithRetry probes the source with the same bounded retry/backoff a
// backup attempt gets. Preflight gates the WHOLE run, so without this a single
// transient network failure on one probe aborts every target with no retry at
// all -- a strictly worse outcome than the per-target failure it exists to
// prevent. Credential rejections still fail fast, since retrying cannot help.
func RunPreflightWithRetry(ctx context.Context, src datasource.Source, target string) error {
	var lastErr error
	for attempt := 1; attempt <= MaxBackupAttempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		lastErr = src.PreflightCheck(ctx, target)
		if lastErr == nil {
			return nil
		}
		if isAuthRejection(lastErr) {
			return lastErr
		}
		// Raw, not truncated: this is WARN and stays in the pod log, where the
		// full detail is worth having. Truncation belongs on the ERROR that
		// leaves the pod and lands in an alert payload.
		slog.Warn("preflight_attempt_failed", "target", target, "attempt", attempt, "error", lastErr.Error())
		if attempt < MaxBackupAttempts {
			if !waitBackoff(ctx, attempt) {
				return lastErr
			}
		}
	}
	return lastErr
}

func executeStream(parentCtx context.Context, src datasource.Source, storeProvider storage.Provider, target, backupName, nameSuffix string, writerModifiers []WriterModifier, timeout time.Duration, deterministicName bool, totalHint int64) (int64, error) {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	ctx, cancel := context.WithTimeoutCause(parentCtx, timeout, fmt.Errorf("backup timed out"))
	defer cancel()

	var remotePath string
	if deterministicName {
		remotePath = backupName + nameSuffix
	} else {
		timestamp := time.Now().UTC().Format("2006-01-02-15-04-05")
		randBytes := make([]byte, 8)
		if _, err := rand.Read(randBytes); err != nil {
			return 0, fmt.Errorf("failed to generate random bytes: %w", err)
		}
		remotePath = fmt.Sprintf("%s-%s-%s%s", backupName, timestamp, hex.EncodeToString(randBytes), nameSuffix)
	}

	cloudReader, cloudWriter := io.Pipe()
	defer cloudReader.Close()
	counter := &byteCounter{Reader: cloudReader}
	errChan := make(chan error, 1)

	go func() {
		var gErr error
		defer func() {
			if r := recover(); r != nil {
				gErr = fmt.Errorf("panic: %v", r)
			}
			cloudWriter.CloseWithError(gErr)
			errChan <- gErr
		}()

		if ctx.Err() != nil {
			gErr = context.Cause(ctx)
			return
		}

		outWriter, closers, applyErr := applyWriterModifiers(cloudWriter, writerModifiers)
		if applyErr != nil {
			gErr = applyErr
			return
		}

		if backupErr := src.Backup(ctx, target, outWriter); backupErr != nil {
			gErr = fmt.Errorf("backup failed: %w", backupErr)
		}
		// Close in LIFO order so that inner wrappers (e.g. gzip) flush before outer
		// ones (e.g. age). Capture the first error; a closer error means the backup
		// stream is corrupt (e.g. truncated gzip footer) even if Backup returned nil.
		for i := len(closers) - 1; i >= 0; i-- {
			if closeErr := closers[i].Close(); closeErr != nil && gErr == nil {
				gErr = fmt.Errorf("finalization failed: %w", closeErr)
			}
		}
	}()

	mon := progress.New(&counter.bytesRead, target, 10*time.Second, totalHint)
	mon.Start(ctx)

	uploadErr := storeProvider.UploadStream(ctx, remotePath, counter)
	mon.Stop()

	cloudReader.Close()
	if uploadErr != nil {
		cancel()
	}
	goroutineErr := <-errChan

	if uploadErr != nil {
		return 0, fmt.Errorf("upload failed: %w", uploadErr)
	}
	if goroutineErr != nil {
		return 0, fmt.Errorf("stream failed: %w", goroutineErr)
	}
	return counter.bytesRead.Load(), nil
}

// RunRestore downloads a backup from cloud storage and restores it to the target.
// readerModifiers are applied in order to the download stream (decrypt first, then decompress).
func RunRestore(ctx context.Context, src datasource.Source, storeProvider storage.Provider, remoteFile, target string, readerModifiers []ReaderModifier, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	ctx, cancel := context.WithTimeoutCause(ctx, timeout, fmt.Errorf("restore timed out"))
	defer cancel()

	// 1. Pipe between download+transform goroutine and the registry client
	pipeR, pipeW := io.Pipe()
	errChan := make(chan error, 1)

	// 2. Goroutine: download -> apply reader modifiers -> write to pipeW
	go func() {
		var gErr error
		defer func() {
			pipeW.CloseWithError(gErr)
			errChan <- gErr
		}()

		cloudR, cloudW := io.Pipe()
		defer cloudR.Close()
		go func() {
			if dlErr := storeProvider.DownloadStream(ctx, remoteFile, cloudW); dlErr != nil {
				cloudW.CloseWithError(dlErr)
				return
			}
			cloudW.Close()
		}()

		reader, applyErr := ApplyReaderModifiers(cloudR, readerModifiers)
		if applyErr != nil {
			gErr = applyErr
			return
		}

		if _, copyErr := io.Copy(pipeW, reader); copyErr != nil {
			gErr = fmt.Errorf("stream copy failed: %w", copyErr)
		}
	}()

	// 3. Restore blocks until the source client finishes consuming pipeR
	restoreErr := src.Restore(ctx, target, pipeR)
	cancel() // unblock the download goroutine if still running
	pipeR.Close()
	goroutineErr := <-errChan

	if restoreErr != nil && goroutineErr != nil {
		return errors.Join(restoreErr, fmt.Errorf("download/transform stream failed: %w", goroutineErr))
	}
	if restoreErr != nil {
		return restoreErr
	}
	if goroutineErr != nil {
		return fmt.Errorf("download/transform stream failed: %w", goroutineErr)
	}
	return nil
}
