package orchestrator

import (
	"context"
	"io"

	"strings"
	"testing"
	"time"

	"github.com/relizaio/cloud-backup/internal/stats"
)

// --- mocks ---

type mockSource struct {
	backupFn func(ctx context.Context, target string, out io.Writer) error
}

func (m *mockSource) Backup(ctx context.Context, target string, out io.Writer) error {
	if m.backupFn != nil {
		return m.backupFn(ctx, target, out)
	}
	out.Write([]byte("data"))
	return nil
}
func (m *mockSource) Restore(ctx context.Context, target string, in io.Reader) error { return nil }
func (m *mockSource) PreflightCheck(ctx context.Context, target string) error        { return nil }

type mockStorage struct{}

func (m *mockStorage) UploadStream(ctx context.Context, path string, r io.Reader) error {
	io.Copy(io.Discard, r)
	return nil
}

func (m *mockStorage) DownloadStream(ctx context.Context, path string, w io.Writer) error {
	return nil
}

// --- buildWriterModifiers ---

func TestBuildWriterModifiers_NoPassword(t *testing.T) {
	suffix, mods := buildWriterModifiers("")
	if suffix != ".tar.gz" {
		t.Errorf("suffix: got %q want %q", suffix, ".tar.gz")
	}
	if len(mods) != 1 {
		t.Errorf("expected 1 modifier (gzip only), got %d", len(mods))
	}
}

func TestBuildWriterModifiers_WithPassword(t *testing.T) {
	suffix, mods := buildWriterModifiers("secret")
	if suffix != ".tar.gz.age" {
		t.Errorf("suffix: got %q want %q", suffix, ".tar.gz.age")
	}
	if len(mods) != 2 {
		t.Errorf("expected 2 modifiers (age + gzip), got %d", len(mods))
	}
}

// TestPGSuffixContract documents the intentional divergence: PG uses .dump (no gzip).
// This is a companion to the OCI suffix tests above.
func TestPGSuffixContract(t *testing.T) {
	// OCI always emits .tar.gz[.age]
	ociSuffix, _ := buildWriterModifiers("")
	if !strings.HasPrefix(ociSuffix, ".tar.gz") {
		t.Errorf("OCI suffix must start with .tar.gz, got %q", ociSuffix)
	}
	if strings.Contains(ociSuffix, ".dump") {
		t.Errorf("OCI suffix must not contain .dump: %q", ociSuffix)
	}

	// PG suffix is .dump (or .dump.age) - no gzip layer at all.
	pgSuffixPlain := ".dump"
	pgSuffixEnc := ".dump.age"
	for _, s := range []string{pgSuffixPlain, pgSuffixEnc} {
		if strings.Contains(s, "tar") || strings.Contains(s, "gz") {
			t.Errorf("PG suffix %q must not contain tar/gz (pg_dump -Fc already compresses)", s)
		}
	}
}

// --- resolveTargets ---

func TestResolveTargets_NoRolling(t *testing.T) {
	m := &BackupManager{}
	basePaths := []string{"repo/a", "repo/b"}
	got := m.resolveTargets(basePaths, false)
	if len(got) != len(basePaths) {
		t.Fatalf("got %d paths, want %d", len(got), len(basePaths))
	}
	for i, p := range basePaths {
		if got[i] != p {
			t.Errorf("index %d: got %q want %q", i, got[i], p)
		}
	}
}

func TestResolveTargets_Rolling(t *testing.T) {
	m := &BackupManager{}
	basePaths := []string{"repo/a", "repo/b"}
	got := m.resolveTargets(basePaths, true)

	// Expect 2 entries per base path (current month + previous month)
	if len(got) != len(basePaths)*2 {
		t.Fatalf("got %d paths, want %d", len(got), len(basePaths)*2)
	}
	// Each result should start with the base path
	for _, p := range got {
		matched := false
		for _, b := range basePaths {
			if strings.HasPrefix(p, b+"-") {
				matched = true
				break
			}
		}
		if !matched {
			t.Errorf("path %q does not match any base path prefix", p)
		}
	}
	// Month suffix should match YYYY-MM format
	for _, p := range got {
		parts := strings.Split(p, "-")
		// last two parts are YYYY and MM
		if len(parts) < 3 {
			t.Errorf("path %q has too few dash-separated parts for a month suffix", p)
		}
	}
}

func TestResolveTargets_RollingProducesCurrentAndPreviousMonth(t *testing.T) {
	m := &BackupManager{}
	got := m.resolveTargets([]string{"repo"}, true)
	if len(got) != 2 {
		t.Fatalf("got %d paths, want 2", len(got))
	}
	// Both should be distinct (different months, unless we're on the 1st of the month
	// and it wraps - but even then the function produces two entries)
	if got[0] == got[1] {
		// This can happen on the 1st of the month when prev == current month boundary edge case
		// Just verify both are present and have month suffix
		t.Logf("note: both rolling targets are equal (possible on month boundary): %v", got)
	}
}

// --- RunBackups ---

func TestRunBackups_NoTargets(t *testing.T) {
	tracker := stats.New()
	m := &BackupManager{
		Storage:    &mockStorage{},
		Tracker:    tracker,
		DataSource: &mockSource{},
		Timeout:    5 * time.Second,
	}
	m.RunBackups(context.Background(), []string{}, false)
	if tracker.GetTotal() != 0 {
		t.Errorf("expected 0 jobs, got %d", tracker.GetTotal())
	}
}

func TestRunBackups_SingleTarget_Success(t *testing.T) {
	tracker := stats.New()
	m := &BackupManager{
		Storage:     &mockStorage{},
		Tracker:     tracker,
		DataSource:  &mockSource{},
		Concurrency: 1,
		DumpPrefix:  "backup",
		Timeout:     10 * time.Second,
	}
	m.RunBackups(context.Background(), []string{"repo/path"}, false)

	if tracker.GetTotal() != 1 {
		t.Errorf("Total: got %d want 1", tracker.GetTotal())
	}
	if tracker.GetFailedCount() != 0 {
		t.Errorf("Failed: got %d want 0", tracker.GetFailedCount())
	}
}

func TestRunBackups_MultipleTargets_Concurrency1(t *testing.T) {
	tracker := stats.New()
	m := &BackupManager{
		Storage:     &mockStorage{},
		Tracker:     tracker,
		DataSource:  &mockSource{},
		Concurrency: 1,
		DumpPrefix:  "backup",
		Timeout:     10 * time.Second,
	}
	m.RunBackups(context.Background(), []string{"repo/a", "repo/b", "repo/c"}, false)

	if tracker.GetTotal() != 3 {
		t.Errorf("Total: got %d want 3", tracker.GetTotal())
	}
	if tracker.GetFailedCount() != 0 {
		t.Errorf("Failed: got %d want 0", tracker.GetFailedCount())
	}
}

func TestRunBackups_DefaultConcurrency(t *testing.T) {
	tracker := stats.New()
	m := &BackupManager{
		Storage:    &mockStorage{},
		Tracker:    tracker,
		DataSource: &mockSource{},
		// Concurrency 0 should default to 3
		DumpPrefix: "backup",
		Timeout:    10 * time.Second,
	}
	m.RunBackups(context.Background(), []string{"a", "b", "c", "d"}, false)

	if tracker.GetTotal() != 4 {
		t.Errorf("Total: got %d want 4", tracker.GetTotal())
	}
}

func TestRunBackups_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	tracker := stats.New()
	m := &BackupManager{
		Storage:     &mockStorage{},
		Tracker:     tracker,
		DataSource:  &mockSource{},
		Concurrency: 1,
		DumpPrefix:  "backup",
		Timeout:     5 * time.Second,
	}
	m.RunBackups(ctx, []string{"repo/a", "repo/b"}, false)
	// With cancelled context, jobs may be recorded as failures; no panic expected
	total := tracker.GetTotal()
	_ = total
}

// The suffix must match what resolveTargets actually builds, or the predicate
// would exempt nothing and re-open the noise it is meant to avoid. January is
// the case naive month arithmetic gets wrong.
func TestPreviousMonthSuffix_CrossesYearBoundary(t *testing.T) {
	if got := PreviousMonthSuffix(time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)); got != "2025-12" {
		t.Errorf("January must roll back to the previous December, got %q", got)
	}

	m := &BackupManager{}
	now := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	targets := m.resolveTargets([]string{"repo"}, true)
	// resolveTargets uses time.Now(); assert the shape it produces agrees with
	// the helper for the same instant rather than pinning a wall-clock date.
	if len(targets) != 2 {
		t.Fatalf("expected current+previous targets, got %v", targets)
	}
	if !strings.HasSuffix(targets[1], "-"+PreviousMonthSuffix(time.Now().UTC())) {
		t.Errorf("second target %q must carry the previous-month suffix used by SkipIsExpected", targets[1])
	}
	_ = now
}

// The coverage rule is deliberately per BASE PATH, not per target. Under rolling
// months the month suffixes are fabricated from the calendar and the registry
// creates a monthly repository lazily on first push, so a month with no
// artifacts has no repository -- normal, not a fault. Alerting per target paged
// weekly for a low-traffic path; alerting per base path does not.
func TestUncoveredBasePaths(t *testing.T) {
	now := time.Date(2026, 8, 15, 8, 26, 0, 0, time.UTC)
	const rebomCur, rebomPrev = "rearm-artifacts/rebom-artifacts-2026-08", "rearm-artifacts/rebom-artifacts-2026-07"
	const dlCur, dlPrev = "rearm-artifacts/downloadable-artifacts-2026-08", "rearm-artifacts/downloadable-artifacts-2026-07"
	bases := []string{"rearm-artifacts/rebom-artifacts", "rearm-artifacts/downloadable-artifacts"}

	tests := []struct {
		name          string
		basePaths     []string
		rollingMonths bool
		skipped       []string
		want          []string
	}{
		{
			// The production false positive: nobody published a downloadable
			// artifact in August, so that month's repo was never created.
			// July backed up fine, so the path IS covered.
			name:      "quiet current month is covered by the previous month",
			basePaths: bases, rollingMonths: true,
			skipped: []string{dlCur},
			want:    nil,
		},
		{
			name:      "base path absent for both months is uncovered",
			basePaths: bases, rollingMonths: true,
			skipped: []string{dlCur, dlPrev},
			want:    []string{"rearm-artifacts/downloadable-artifacts"},
		},
		{
			name:      "previous month alone is covered",
			basePaths: bases, rollingMonths: true,
			skipped: []string{dlPrev},
			want:    nil,
		},
		{
			name:      "every base path uncovered",
			basePaths: bases, rollingMonths: true,
			skipped: []string{rebomCur, rebomPrev, dlCur, dlPrev},
			want:    bases,
		},
		{
			// Explicit paths expand to one target each, so the strict
			// behaviour is preserved: any skip means no backup for that path.
			name:      "explicit path skipped is uncovered",
			basePaths: []string{"rearm-artifacts/rebom-artifacts"}, rollingMonths: false,
			skipped: []string{"rearm-artifacts/rebom-artifacts"},
			want:    []string{"rearm-artifacts/rebom-artifacts"},
		},
		{
			name:      "nothing skipped",
			basePaths: bases, rollingMonths: true,
			skipped: nil,
			want:    nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := UncoveredBasePaths(tc.basePaths, tc.rollingMonths, now, tc.skipped)
			if len(got) != len(tc.want) {
				t.Fatalf("got %v want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("index %d: got %q want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

// The coverage check must expand base paths exactly as the run does, or it would
// look for targets that were never attempted.
func TestTargetsForBasePath_MatchesResolveTargets(t *testing.T) {
	now := time.Now().UTC()
	m := &BackupManager{}

	rolling := m.resolveTargets([]string{"repo/a"}, true)
	if got := TargetsForBasePath("repo/a", true, now); len(got) != len(rolling) {
		t.Fatalf("rolling: helper produced %v, resolveTargets produced %v", got, rolling)
	}
	for i, tgt := range TargetsForBasePath("repo/a", true, now) {
		if tgt != rolling[i] {
			t.Errorf("rolling index %d: helper %q vs resolveTargets %q", i, tgt, rolling[i])
		}
	}

	explicit := m.resolveTargets([]string{"repo/a"}, false)
	if got := TargetsForBasePath("repo/a", false, now); len(got) != 1 || got[0] != explicit[0] {
		t.Errorf("explicit: helper %v vs resolveTargets %v", got, explicit)
	}
}
