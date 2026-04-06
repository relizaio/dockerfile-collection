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
)

var (
	RetryBackoffBase   = 10 * time.Second
	MaxBackoffDuration = 5 * time.Minute
)

// RunWithRetry handles the retry logic and graceful degradation for missing source targets.
// nameSuffix is appended to the remote filename (e.g. ".tar.gz" or ".tar.gz.age").
// writerModifiers are applied in order to the upload stream (compress, then encrypt).
func RunWithRetry(ctx context.Context, src datasource.Source, storeProvider storage.Provider, target, backupName, nameSuffix string, writerModifiers []WriterModifier, tracker *stats.Tracker, timeout time.Duration, deterministicName bool) {
	tracker.RecordJob()
	startTimer := time.Now()
	jobHandled := false
	defer func() {
		if !jobHandled {
			tracker.RecordFailure(target)
		}
	}()

	for attempt := 1; attempt <= MaxBackupAttempts; attempt++ {
		if ctx.Err() != nil {
			return
		}
		slog.Info("backup_started", "target", target, "attempt", attempt)
		bytesUploaded, err := executeStream(ctx, src, storeProvider, target, backupName, nameSuffix, writerModifiers, timeout, deterministicName)
		if err == nil {
			slog.Info("backup_successful", "target", target, "duration", time.Since(startTimer).Round(time.Second).String(), "size_human", stats.FormatBytes(bytesUploaded))
			jobHandled = true
			tracker.RecordSuccess()
			tracker.AddBytes(bytesUploaded)
			return
		}
		// FAST-FAIL ON UNAUTHORIZED
		if strings.Contains(err.Error(), "unauthorized") || strings.Contains(err.Error(), "authentication required") {
			slog.Error("fatal_authentication_error", "target", target, "msg", "Credentials rejected. Halting retries.")
			jobHandled = true
			tracker.RecordFailure(target)
			return // Exit immediately, do not wait for backoff
		}

		if strings.Contains(err.Error(), "repository name not known to registry") {
			slog.Warn("repository_not_found_skipping", "target", target)
			jobHandled = true
			tracker.RecordSkipped(target)
			return
		}

		slog.Error("backup_attempt_failed", "target", target, "attempt", attempt, "error", err.Error())
		if attempt < MaxBackupAttempts {
			backoff := RetryBackoffBase * time.Duration(1<<uint(attempt))
			if backoff > MaxBackoffDuration {
				backoff = MaxBackoffDuration
			}
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}
	slog.Error("backup_exhausted", "target", target)
}

func executeStream(parentCtx context.Context, src datasource.Source, storeProvider storage.Provider, target, backupName, nameSuffix string, writerModifiers []WriterModifier, timeout time.Duration, deterministicName bool) (int64, error) {
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

	mon := progress.New(&counter.bytesRead, target, 10*time.Second)
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

	// 2. Goroutine: download → apply reader modifiers → write to pipeW
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
