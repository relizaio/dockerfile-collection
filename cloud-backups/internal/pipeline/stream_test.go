package pipeline

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"os"
	"strings"
	"sync"
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
	backupFn    func(ctx context.Context, target string, out io.Writer) error
	restoreFn   func(ctx context.Context, target string, in io.Reader) error
	preflightFn func(ctx context.Context, target string) error
}

func (m *mockSource) Backup(ctx context.Context, target string, out io.Writer) error {
	return m.backupFn(ctx, target, out)
}
func (m *mockSource) Restore(ctx context.Context, target string, in io.Reader) error {
	return m.restoreFn(ctx, target, in)
}
func (m *mockSource) PreflightCheck(ctx context.Context, target string) error {
	if m.preflightFn == nil {
		return nil
	}
	return m.preflightFn(ctx, target)
}

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

	RunWithRetry(context.Background(), src, store, "target", "prefix", ".dump", nil, tracker, 30*time.Second, false, 0)

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

	RunWithRetry(context.Background(), src, store, "target", "prefix", ".dump", nil, tracker, 30*time.Second, false, 0)

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

	RunWithRetry(context.Background(), src, store, "target", "prefix", ".dump", nil, tracker, 30*time.Second, false, 0)

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

	RunWithRetry(context.Background(), src, store, "target", "prefix", ".dump", nil, tracker, 30*time.Second, false, 0)

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

	RunWithRetry(context.Background(), src, store, "target", "prefix", ".dump", nil, tracker, 30*time.Second, false, 0)

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

	RunWithRetry(ctx, src, store, "target", "prefix", ".dump", nil, tracker, 30*time.Second, false, 0)

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

	RunWithRetry(context.Background(), src, store, "repo/path", "oci-backup", ".tar.gz", mods, tracker, 30*time.Second, false, 0)

	if tracker.GetFailedCount() != 0 {
		t.Fatalf("backup failed unexpectedly")
	}
	if !isGzip(captured.Bytes()) {
		t.Error("OCI backup (no encryption): expected gzip-compressed output, got non-gzip bytes")
	}
}

// TestPG_NoEncryption_UploadIsNotGzipped verifies that the PG backup pipeline
// (no modifiers - pg_dump -Fc already compresses) does NOT add gzip on top.
func TestPG_NoEncryption_UploadIsNotGzipped(t *testing.T) {
	var captured bytes.Buffer
	tracker := stats.New()
	// Simulate raw pg_dump -Fc output (just plain bytes, not gzip)
	src := &mockSource{backupFn: writePayload([]byte(compressionPayload))}
	store := captureStorage(&captured)
	// PG path: empty modifier list
	mods := []WriterModifier{}

	RunWithRetry(context.Background(), src, store, "mydb", "pg-backup", ".dump", mods, tracker, 30*time.Second, false, 0)

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

// --- alerting-level tests ---
//
// These pin the log LEVEL of each outcome, not just the text. Operator alerting
// fires on ERROR, so a level regression here silently either pages on a
// self-healing blip or, worse, hides a run that genuinely lost a backup.

type capturedLog struct {
	Level slog.Level
	Msg   string
	Attrs map[string]string
}

type captureHandler struct {
	mu   *sync.Mutex
	recs *[]capturedLog
}

func (h captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h captureHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h captureHandler) WithGroup(string) slog.Handler            { return h }
func (h captureHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := map[string]string{}
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.String()
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	*h.recs = append(*h.recs, capturedLog{Level: r.Level, Msg: r.Message, Attrs: attrs})
	return nil
}

// captureLogs redirects the default slog logger for the duration of the test.
func captureLogs(t *testing.T) *[]capturedLog {
	t.Helper()
	recs := &[]capturedLog{}
	prev := slog.Default()
	slog.SetDefault(slog.New(captureHandler{mu: &sync.Mutex{}, recs: recs}))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return recs
}

func findLog(recs *[]capturedLog, msg string) (capturedLog, bool) {
	for _, r := range *recs {
		if r.Msg == msg {
			return r, true
		}
	}
	return capturedLog{}, false
}

func errorLevelMsgs(recs *[]capturedLog) []string {
	var out []string
	for _, r := range *recs {
		if r.Level >= slog.LevelError {
			out = append(out, r.Msg)
		}
	}
	return out
}

// A blip that the next attempt recovers from must not reach ERROR. This is the
// production false alarm: one refused TCP dial to the registry alerted even
// though the retry uploaded the backup seconds later.
func TestRunWithRetry_RecoveredAttemptDoesNotLogError(t *testing.T) {
	recs := captureLogs(t)
	tracker := stats.New()
	var attempts atomic.Int32
	var captured bytes.Buffer

	src := &mockSource{backupFn: func(ctx context.Context, target string, out io.Writer) error {
		if attempts.Add(1) == 1 {
			return errors.New("dial tcp 10.0.5.5:443: connect: connection refused")
		}
		_, err := out.Write([]byte("ok"))
		return err
	}}

	RunWithRetry(context.Background(), src, captureStorage(&captured), "target", "prefix", ".dump", nil, tracker, 30*time.Second, false, 0)

	if got := tracker.GetFailedCount(); got != 0 {
		t.Fatalf("recovered target must not count as failed, got %d", got)
	}
	if msgs := errorLevelMsgs(recs); len(msgs) != 0 {
		t.Errorf("a recovered backup must emit no ERROR, got %v", msgs)
	}
	rec, ok := findLog(recs, "backup_attempt_failed")
	if !ok {
		t.Fatal("expected backup_attempt_failed to still be logged")
	}
	if rec.Level != slog.LevelWarn {
		t.Errorf("backup_attempt_failed level: got %v want WARN", rec.Level)
	}
	if _, ok := findLog(recs, "backup_recovered_after_retry"); !ok {
		t.Error("expected backup_recovered_after_retry so a flaky registry is still visible")
	}
}

// A target that really failed must emit exactly one ERROR, and that ERROR must
// carry the causes -- it is the only line the operator receives.
func TestRunWithRetry_ExhaustedLogsSingleErrorWithCauses(t *testing.T) {
	recs := captureLogs(t)
	tracker := stats.New()
	var attempts atomic.Int32

	src := &mockSource{backupFn: func(ctx context.Context, target string, out io.Writer) error {
		return fmt.Errorf("boom %d", attempts.Add(1))
	}}
	store := &mockStorage{uploadFn: func(ctx context.Context, path string, r io.Reader) error {
		_, err := io.Copy(io.Discard, r)
		return err
	}}

	RunWithRetry(context.Background(), src, store, "target", "prefix", ".dump", nil, tracker, 30*time.Second, false, 0)

	if msgs := errorLevelMsgs(recs); len(msgs) != 1 || msgs[0] != "backup_exhausted" {
		t.Fatalf("want exactly one ERROR (backup_exhausted), got %v", msgs)
	}
	rec, _ := findLog(recs, "backup_exhausted")
	cause := rec.Attrs["error"]
	if cause == "" {
		t.Fatal("backup_exhausted must carry an error field; an empty alert is unactionable")
	}
	for i := 1; i <= MaxBackupAttempts; i++ {
		if !strings.Contains(cause, fmt.Sprintf("boom %d", i)) {
			t.Errorf("error field must include attempt %d cause, got %q", i, cause)
		}
	}
}

// --- RunPreflightWithRetry tests ---

// Preflight gates the whole run, so a transient failure here used to abort every
// target with no retry at all.
func TestRunPreflightWithRetry_RetriesTransientFailure(t *testing.T) {
	captureLogs(t)
	var calls atomic.Int32
	src := &mockSource{preflightFn: func(ctx context.Context, target string) error {
		if calls.Add(1) < 3 {
			return errors.New("dial tcp 10.0.5.5:443: connect: connection refused")
		}
		return nil
	}}

	if err := RunPreflightWithRetry(context.Background(), src, "target"); err != nil {
		t.Fatalf("preflight should have recovered, got %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("expected 3 preflight attempts, got %d", got)
	}
}

func TestRunPreflightWithRetry_FastFailsOnUnauthorized(t *testing.T) {
	captureLogs(t)
	var calls atomic.Int32
	src := &mockSource{preflightFn: func(ctx context.Context, target string) error {
		calls.Add(1)
		return errors.New("unauthorized to access repo: check token scopes")
	}}

	if err := RunPreflightWithRetry(context.Background(), src, "target"); err == nil {
		t.Fatal("expected an error for rejected credentials")
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("credentials cannot fix themselves; want 1 attempt, got %d", got)
	}
}

func TestRunPreflightWithRetry_ExhaustsAndReturnsLastError(t *testing.T) {
	captureLogs(t)
	var calls atomic.Int32
	src := &mockSource{preflightFn: func(ctx context.Context, target string) error {
		calls.Add(1)
		return errors.New("connection refused")
	}}

	if err := RunPreflightWithRetry(context.Background(), src, "target"); err == nil {
		t.Fatal("expected the last error to be returned")
	}
	if got := calls.Load(); got != int32(MaxBackupAttempts) {
		t.Errorf("want %d preflight attempts, got %d", MaxBackupAttempts, got)
	}
}

// An unbounded cause is not just untidy: backup_exhausted joins one per attempt,
// and the backup tool's own output tail can be kilobytes, so the single ERROR
// the operator receives can exceed what an alerting backend will ingest.
func TestTruncateCause_BoundsAggregatedField(t *testing.T) {
	short := "dial tcp 10.0.5.5:443: connect: connection refused"
	if got := TruncateCause(short); got != short {
		t.Errorf("a short cause must pass through unchanged, got %q", got)
	}

	long := strings.Repeat("x", MaxCauseLength*3)
	got := TruncateCause(long)
	if len(got) >= len(long) {
		t.Fatalf("a long cause must shrink: got %d bytes, original %d", len(got), len(long))
	}
	if !strings.HasPrefix(got, strings.Repeat("x", MaxCauseLength)) {
		t.Error("truncation must keep the head, which names the failure")
	}
	if !strings.Contains(got, "truncated") {
		t.Error("a truncated cause must say so, or it reads as the whole error")
	}
}

// The aggregate that actually reaches the operator must stay bounded across all
// attempts, which is the property the truncation exists to protect.
func TestRunWithRetry_ExhaustedErrorFieldStaysBounded(t *testing.T) {
	recs := captureLogs(t)
	tracker := stats.New()

	src := &mockSource{backupFn: func(ctx context.Context, target string, out io.Writer) error {
		return errors.New(strings.Repeat("y", 8192)) // an 8KB tool log tail
	}}
	store := &mockStorage{uploadFn: func(ctx context.Context, path string, r io.Reader) error {
		_, err := io.Copy(io.Discard, r)
		return err
	}}

	RunWithRetry(context.Background(), src, store, "target", "prefix", ".dump", nil, tracker, 30*time.Second, false, 0)

	rec, ok := findLog(recs, "backup_exhausted")
	if !ok {
		t.Fatal("expected backup_exhausted")
	}
	limit := MaxCauseLength * MaxBackupAttempts * 2
	if got := len(rec.Attrs["error"]); got > limit {
		t.Errorf("aggregated error field is %d bytes, want <= %d", got, limit)
	}
}

// Every terminal outcome, pinned by LEVEL. Alerting fires on ERROR, so a level
// regression on any row either pages on a self-healing blip or hides a run that
// lost a backup. Table-driven so a new outcome cannot be added without deciding
// which side of the alerting line it falls on.
func TestRunWithRetry_OutcomeLevels(t *testing.T) {
	failWith := func(msg string) func(context.Context, string, io.Writer) error {
		return func(context.Context, string, io.Writer) error { return errors.New(msg) }
	}
	recoverOnSecond := func() func(context.Context, string, io.Writer) error {
		var n atomic.Int32
		return func(_ context.Context, _ string, out io.Writer) error {
			if n.Add(1) == 1 {
				return errors.New("connection refused")
			}
			_, err := out.Write([]byte("ok"))
			return err
		}
	}

	tests := []struct {
		name       string
		backupFn   func(context.Context, string, io.Writer) error
		wantErrors []string // ERROR-level events, i.e. what reaches the operator
	}{
		{"success on first attempt", writePayload([]byte("ok")), nil},
		{"recovered after retry", recoverOnSecond(), nil},
		{"all attempts failed", failWith("boom"), []string{"backup_exhausted"}},
		{"credentials rejected", failWith("unauthorized: nope"), []string{"fatal_authentication_error"}},
		{"repository absent", failWith("repository name not known to registry"), nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			recs := captureLogs(t)
			var captured bytes.Buffer
			RunWithRetry(context.Background(), &mockSource{backupFn: tc.backupFn}, captureStorage(&captured),
				"target", "prefix", ".dump", nil, stats.New(), 30*time.Second, false, 0)

			got := errorLevelMsgs(recs)
			if len(got) != len(tc.wantErrors) {
				t.Fatalf("ERROR-level events: got %v want %v", got, tc.wantErrors)
			}
			for i, want := range tc.wantErrors {
				if got[i] != want {
					t.Errorf("ERROR event %d: got %q want %q", i, got[i], want)
				}
			}
		})
	}
}

// slog's JSON handler emits the event name as "msg". An attr also named "msg"
// is written as a SECOND "msg" key, and every JSON parser keeps the last one --
// so the event name silently disappears from the parsed record and alerting can
// no longer route on it. Nothing may reintroduce that.
func TestLogRecords_NeverUseMsgAsAttrKey(t *testing.T) {
	cases := map[string]func(context.Context, string, io.Writer) error{
		"unauthorized": func(context.Context, string, io.Writer) error {
			return errors.New("unauthorized: nope")
		},
		"exhausted": func(context.Context, string, io.Writer) error {
			return errors.New("boom")
		},
	}

	for name, fn := range cases {
		t.Run(name, func(t *testing.T) {
			recs := captureLogs(t)
			var captured bytes.Buffer
			RunWithRetry(context.Background(), &mockSource{backupFn: fn}, captureStorage(&captured),
				"target", "prefix", ".dump", nil, stats.New(), 30*time.Second, false, 0)

			for _, r := range *recs {
				if _, clash := r.Attrs["msg"]; clash {
					t.Errorf("event %q uses \"msg\" as an attr key, which shadows the event name after JSON parsing", r.Msg)
				}
			}
		})
	}
}
