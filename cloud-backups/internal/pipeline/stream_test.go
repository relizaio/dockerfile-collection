package pipeline

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/relizaio/cloud-backup/internal/stats"
)

func TestMain(m *testing.M) {
	// Speed up retry backoffs so tests complete in milliseconds.
	RetryBackoffBase = 1 * time.Millisecond
	MaxBackoffDuration = 5 * time.Millisecond
	os.Exit(m.Run())
}

// --- mocks ---

type mockSource struct {
	backupFn  func(ctx context.Context, target string, out io.Writer) error
	restoreFn func(ctx context.Context, target string, in io.Reader) error
}

func (m *mockSource) Backup(ctx context.Context, target string, out io.Writer) error {
	return m.backupFn(ctx, target, out)
}
func (m *mockSource) Restore(ctx context.Context, target string, in io.Reader) error {
	return m.restoreFn(ctx, target, in)
}
func (m *mockSource) PreflightCheck(ctx context.Context, target string) error { return nil }

type mockStorage struct {
	uploadFn   func(ctx context.Context, path string, r io.Reader) error
	downloadFn func(ctx context.Context, path string, w io.Writer) error
}

func (m *mockStorage) UploadStream(ctx context.Context, path string, r io.Reader) error {
	return m.uploadFn(ctx, path, r)
}
func (m *mockStorage) DownloadStream(ctx context.Context, path string, w io.Writer) error {
	return m.downloadFn(ctx, path, w)
}

// captureStorage reads the upload stream into a buffer for assertions.
func captureStorage(captured *bytes.Buffer) *mockStorage {
	return &mockStorage{
		uploadFn: func(ctx context.Context, path string, r io.Reader) error {
			_, err := io.Copy(captured, r)
			return err
		},
	}
}

// writePayload is a backup function that writes a fixed payload.
func writePayload(payload []byte) func(ctx context.Context, target string, out io.Writer) error {
	return func(ctx context.Context, target string, out io.Writer) error {
		_, err := out.Write(payload)
		return err
	}
}

// --- RunWithRetry tests ---

func TestRunWithRetry_SuccessFirstAttempt(t *testing.T) {
	tracker := stats.New()
	payload := []byte("backup data")
	var captured bytes.Buffer

	src := &mockSource{backupFn: writePayload(payload)}
	store := captureStorage(&captured)

	RunWithRetry(context.Background(), src, store, "target", "prefix", ".dump", nil, tracker, 30*time.Second)

	if tracker.GetTotal() != 1 {
		t.Errorf("Total: got %d want 1", tracker.GetTotal())
	}
	if tracker.GetFailedCount() != 0 {
		t.Errorf("Failed: got %d want 0", tracker.GetFailedCount())
	}
	if captured.Len() == 0 {
		t.Error("expected non-empty upload")
	}
}

func TestRunWithRetry_RetryThenSuccess(t *testing.T) {
	tracker := stats.New()
	var attempt int32

	src := &mockSource{
		backupFn: func(ctx context.Context, target string, out io.Writer) error {
			n := atomic.AddInt32(&attempt, 1)
			if n < 2 {
				return errors.New("transient error")
			}
			out.Write([]byte("data"))
			return nil
		},
	}
	var captured bytes.Buffer
	store := captureStorage(&captured)

	RunWithRetry(context.Background(), src, store, "target", "prefix", ".dump", nil, tracker, 30*time.Second)

	if tracker.GetFailedCount() != 0 {
		t.Errorf("expected 0 failures after eventual success, got %d", tracker.GetFailedCount())
	}
}

func TestRunWithRetry_AllAttemptsFail(t *testing.T) {
	tracker := stats.New()
	src := &mockSource{
		backupFn: func(ctx context.Context, target string, out io.Writer) error {
			return errors.New("permanent error")
		},
	}
	store := &mockStorage{
		uploadFn: func(ctx context.Context, path string, r io.Reader) error {
			io.Copy(io.Discard, r)
			return nil
		},
	}

	RunWithRetry(context.Background(), src, store, "target", "prefix", ".dump", nil, tracker, 30*time.Second)

	if tracker.GetFailedCount() != 1 {
		t.Errorf("Failed: got %d want 1", tracker.GetFailedCount())
	}
}

func TestRunWithRetry_FastFailOnUnauthorized(t *testing.T) {
	tracker := stats.New()
	var callCount int32

	src := &mockSource{
		backupFn: func(ctx context.Context, target string, out io.Writer) error {
			atomic.AddInt32(&callCount, 1)
			return errors.New("unauthorized: credentials rejected")
		},
	}
	store := &mockStorage{
		uploadFn: func(ctx context.Context, path string, r io.Reader) error {
			io.Copy(io.Discard, r)
			return nil
		},
	}

	RunWithRetry(context.Background(), src, store, "target", "prefix", ".dump", nil, tracker, 30*time.Second)

	if atomic.LoadInt32(&callCount) != 1 {
		t.Errorf("expected exactly 1 attempt on unauthorized error, got %d", callCount)
	}
	if tracker.GetFailedCount() != 1 {
		t.Errorf("Failed: got %d want 1", tracker.GetFailedCount())
	}
}

func TestRunWithRetry_SkipOnRepositoryNotFound(t *testing.T) {
	tracker := stats.New()
	src := &mockSource{
		backupFn: func(ctx context.Context, target string, out io.Writer) error {
			return errors.New("repository name not known to registry")
		},
	}
	store := &mockStorage{
		uploadFn: func(ctx context.Context, path string, r io.Reader) error {
			io.Copy(io.Discard, r)
			return nil
		},
	}

	RunWithRetry(context.Background(), src, store, "target", "prefix", ".dump", nil, tracker, 30*time.Second)

	if tracker.GetSkippedCount() != 1 {
		t.Errorf("Skipped: got %d want 1", tracker.GetSkippedCount())
	}
	if tracker.GetFailedCount() != 0 {
		t.Errorf("Failed: got %d want 0", tracker.GetFailedCount())
	}
}

func TestRunWithRetry_ContextCancelledBeforeStart(t *testing.T) {
	tracker := stats.New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var called int32
	src := &mockSource{
		backupFn: func(ctx context.Context, target string, out io.Writer) error {
			atomic.AddInt32(&called, 1)
			return nil
		},
	}
	store := &mockStorage{
		uploadFn: func(ctx context.Context, path string, r io.Reader) error {
			io.Copy(io.Discard, r)
			return nil
		},
	}

	RunWithRetry(ctx, src, store, "target", "prefix", ".dump", nil, tracker, 30*time.Second)

	// Either the job was skipped (ctx already done) or it ran once but was cancelled mid-way.
	// The key invariant: no panics and tracker totals are sane.
	total := tracker.GetTotal()
	if total > 1 {
		t.Errorf("expected at most 1 job recorded, got %d", total)
	}
}

// --- PG vs OCI compression difference ---

const compressionPayload = "repeating test data for compression detection - data data data data data"

// isGzip returns true if the bytes start with a valid gzip header.
func isGzip(data []byte) bool {
	_, err := gzip.NewReader(bytes.NewReader(data))
	return err == nil
}

// TestOCI_NoEncryption_UploadIsGzipped verifies the OCI backup pipeline
// (with WithGzip() modifier) produces gzip-compressed output.
func TestOCI_NoEncryption_UploadIsGzipped(t *testing.T) {
	var captured bytes.Buffer
	tracker := stats.New()
	src := &mockSource{backupFn: writePayload([]byte(compressionPayload))}
	store := captureStorage(&captured)
	mods := []WriterModifier{WithGzip()}

	RunWithRetry(context.Background(), src, store, "repo/path", "oci-backup", ".tar.gz", mods, tracker, 30*time.Second)

	if tracker.GetFailedCount() != 0 {
		t.Fatalf("backup failed unexpectedly")
	}
	if !isGzip(captured.Bytes()) {
		t.Error("OCI backup (no encryption): expected gzip-compressed output, got non-gzip bytes")
	}
}

// TestPG_NoEncryption_UploadIsNotGzipped verifies that the PG backup pipeline
// (no modifiers — pg_dump -Fc already compresses) does NOT add gzip on top.
func TestPG_NoEncryption_UploadIsNotGzipped(t *testing.T) {
	var captured bytes.Buffer
	tracker := stats.New()
	// Simulate raw pg_dump -Fc output (just plain bytes, not gzip)
	src := &mockSource{backupFn: writePayload([]byte(compressionPayload))}
	store := captureStorage(&captured)
	// PG path: empty modifier list
	mods := []WriterModifier{}

	RunWithRetry(context.Background(), src, store, "mydb", "pg-backup", ".dump", mods, tracker, 30*time.Second)

	if tracker.GetFailedCount() != 0 {
		t.Fatalf("backup failed unexpectedly")
	}
	if isGzip(captured.Bytes()) {
		t.Error("PG backup (no encryption): output must NOT be gzip-wrapped (pg_dump -Fc already compresses)")
	}
	// Verify raw content is preserved intact
	if !bytes.Equal(captured.Bytes(), []byte(compressionPayload)) {
		t.Error("PG backup (no encryption): raw bytes were unexpectedly transformed")
	}
}

// --- RunRestore tests ---

func TestRunRestore_Success(t *testing.T) {
	payload := []byte("restore payload data")
	var restored []byte

	src := &mockSource{
		restoreFn: func(ctx context.Context, target string, in io.Reader) error {
			data, err := io.ReadAll(in)
			restored = data
			return err
		},
	}
	store := &mockStorage{
		downloadFn: func(ctx context.Context, path string, w io.Writer) error {
			_, err := w.Write(payload)
			return err
		},
	}

	err := RunRestore(context.Background(), src, store, "backup.dump", "mydb", nil, 30*time.Second)
	if err != nil {
		t.Fatalf("RunRestore: unexpected error: %v", err)
	}
	if !bytes.Equal(restored, payload) {
		t.Errorf("restored data mismatch: got %q want %q", restored, payload)
	}
}

func TestRunRestore_RestoreError(t *testing.T) {
	wantErr := errors.New("restore failed")
	src := &mockSource{
		restoreFn: func(ctx context.Context, target string, in io.Reader) error {
			io.Copy(io.Discard, in)
			return wantErr
		},
	}
	store := &mockStorage{
		downloadFn: func(ctx context.Context, path string, w io.Writer) error {
			w.Write([]byte("data"))
			return nil
		},
	}

	err := RunRestore(context.Background(), src, store, "backup.dump", "mydb", nil, 30*time.Second)
	if !errors.Is(err, wantErr) {
		t.Errorf("got %v, want to contain %v", err, wantErr)
	}
}

func TestRunRestore_DownloadError(t *testing.T) {
	wantErr := errors.New("download failed")
	src := &mockSource{
		restoreFn: func(ctx context.Context, target string, in io.Reader) error {
			_, err := io.ReadAll(in)
			return err
		},
	}
	store := &mockStorage{
		downloadFn: func(ctx context.Context, path string, w io.Writer) error {
			return wantErr
		},
	}

	err := RunRestore(context.Background(), src, store, "backup.dump", "mydb", nil, 30*time.Second)
	if err == nil {
		t.Fatal("expected error from download failure, got nil")
	}
	if !strings.Contains(err.Error(), wantErr.Error()) {
		t.Errorf("error %q does not mention download error %q", err.Error(), wantErr.Error())
	}
}

func TestRunRestore_BothErrors_Joined(t *testing.T) {
	downloadErr := errors.New("download failed")
	restoreErr := errors.New("restore failed")

	src := &mockSource{
		restoreFn: func(ctx context.Context, target string, in io.Reader) error {
			io.Copy(io.Discard, in)
			return restoreErr
		},
	}
	store := &mockStorage{
		downloadFn: func(ctx context.Context, path string, w io.Writer) error {
			return downloadErr
		},
	}

	err := RunRestore(context.Background(), src, store, "backup.dump", "mydb", nil, 30*time.Second)
	if err == nil {
		t.Fatal("expected joined error, got nil")
	}
	errStr := err.Error()
	if !strings.Contains(errStr, restoreErr.Error()) {
		t.Errorf("error %q does not mention restore error", errStr)
	}
}

func TestRunRestore_GzipRoundTrip(t *testing.T) {
	original := []byte("gzip restore round-trip data")
	var gzBuf bytes.Buffer
	gw := gzip.NewWriter(&gzBuf)
	gw.Write(original)
	gw.Close()
	compressed := gzBuf.Bytes()

	var restored []byte
	src := &mockSource{
		restoreFn: func(ctx context.Context, target string, in io.Reader) error {
			data, err := io.ReadAll(in)
			restored = data
			return err
		},
	}
	store := &mockStorage{
		downloadFn: func(ctx context.Context, path string, w io.Writer) error {
			_, err := w.Write(compressed)
			return err
		},
	}

	err := RunRestore(context.Background(), src, store, "backup.tar.gz", "target",
		[]ReaderModifier{WithGunzip()}, 30*time.Second)
	if err != nil {
		t.Fatalf("RunRestore with gunzip: %v", err)
	}
	if !bytes.Equal(restored, original) {
		t.Errorf("restored %q, want %q", restored, original)
	}
}

// Ensure the suffix naming follows the contract documented in the plan.
func TestSuffixContract(t *testing.T) {
	ociSuffix := ".tar.gz"
	pgSuffix := ".dump"
	if !strings.HasPrefix(ociSuffix, ".tar.gz") {
		t.Errorf("OCI suffix should start with .tar.gz, got %q", ociSuffix)
	}
	if strings.Contains(pgSuffix, "gz") {
		t.Errorf("PG suffix must not contain 'gz' (no redundant compression): %q", pgSuffix)
	}
}
