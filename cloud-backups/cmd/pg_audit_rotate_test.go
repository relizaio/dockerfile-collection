package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/relizaio/cloud-backup/internal/config"
	"github.com/relizaio/cloud-backup/internal/storage"
)

// fakeStore is a storage.Provider for unit-testing the post-upload verification and
// the pre-drop gate. If objects is non-nil, Head answers per-key (present -> size,
// absent -> ErrNotFound), which the drop-gate tests use. Otherwise it falls back to
// the single headSize/headErr (the verifyUploadedObject test).
type fakeStore struct {
	headSize  int64
	headErr   error
	uploadErr error
	uploaded  map[string]bool
	objects   map[string]int64
}

func (f *fakeStore) UploadStream(_ context.Context, path string, r io.Reader) error {
	_, _ = io.Copy(io.Discard, r)
	if f.uploadErr == nil {
		if f.uploaded == nil {
			f.uploaded = map[string]bool{}
		}
		f.uploaded[path] = true
	}
	return f.uploadErr
}
func (f *fakeStore) DownloadStream(_ context.Context, _ string, _ io.Writer) error { return nil }
func (f *fakeStore) Head(_ context.Context, path string) (*storage.ObjectInfo, error) {
	if f.objects != nil {
		if sz, ok := f.objects[path]; ok {
			return &storage.ObjectInfo{Size: sz}, nil
		}
		return nil, fmt.Errorf("head %q: %w", path, storage.ErrNotFound)
	}
	if f.headErr != nil {
		return nil, f.headErr
	}
	return &storage.ObjectInfo{Size: f.headSize}, nil
}

func TestVerifyUploadedObject(t *testing.T) {
	const streamed = int64(100)
	cases := []struct {
		name        string
		headSize    int64
		headErr     error
		uploadErr   error
		wantErr     bool
		wantSidecar bool
	}{
		{"exists + size match -> sidecar written, ok", 100, nil, nil, false, true},
		{"head error -> error, no sidecar", 0, errors.New("no head"), nil, true, false},
		{"size mismatch -> error, no sidecar", 99, nil, nil, true, false},
		{"sidecar write fails -> error", 100, nil, errors.New("up fail"), true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := &fakeStore{headSize: tc.headSize, headErr: tc.headErr, uploadErr: tc.uploadErr}
			b := &pgArchiveBackend{store: fs, cfg: &config.AppConfig{PGSchema: "rearm", DumpPrefix: "p"}}
			err := b.verifyUploadedObject(context.Background(), "p-arch.dump", streamed, "deadbeef")
			if (err != nil) != tc.wantErr {
				t.Errorf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			if fs.uploaded["p-arch.dump.sha256"] != tc.wantSidecar {
				t.Errorf("sidecar written = %v, want %v", fs.uploaded["p-arch.dump.sha256"], tc.wantSidecar)
			}
		})
	}
}

func TestRotateSQL(t *testing.T) {
	got := rotateSQL("rearm", "audit", "audit_archive_20260719t120000z_deadbeef", "5s", 1234567890, "audit_archive_20260601t120000z_old")
	for _, want := range []string{
		"SET LOCAL lock_timeout = '5s';",
		"ALTER TABLE rearm.audit RENAME TO audit_archive_20260719t120000z_deadbeef;",
		"'rearm.audit_archive_20260719t120000z_deadbeef'::regclass",
		"CREATE TABLE rearm.audit (LIKE rearm.audit_archive_20260719t120000z_deadbeef INCLUDING ALL);",
		"COMMIT;",
		// concurrency guard: advisory lock + supersession check, before the rename
		"pg_try_advisory_xact_lock(1234567890)",
		"tablename > 'audit_archive_20260601t120000z_old'",
		"AUDIT_ROTATE_SKIP",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("rotateSQL missing %q in:\n%s", want, got)
		}
	}
	// the guard must run BEFORE the rename, or a concurrent run could already have renamed
	if strings.Index(got, "pg_try_advisory_xact_lock") > strings.Index(got, "ALTER TABLE rearm.audit RENAME") {
		t.Errorf("advisory-lock guard must precede the RENAME in:\n%s", got)
	}
}

// The rename-aside suffix MUST derive from the (unique) archive name, not the
// constant original constraint/index name -- otherwise two coexisting un-dropped
// archives (the norm under retention) collide on `audit_pkey_<sfx>` and the second
// rotation fails with `relation "audit_pkey_..." already exists`.
func TestRotateSQL_RenameSuffixIsPerArchive(t *testing.T) {
	a1 := rotateSQL("rearm", "audit", "audit_archive_20260720t100100z_05196fcc", "5s", 1, "")
	a2 := rotateSQL("rearm", "audit", "audit_archive_20260720t112820z_d2337e60", "5s", 1, "")

	if !strings.Contains(a1, "substr(md5('audit_archive_20260720t100100z_05196fcc'), 1, 8)") {
		t.Errorf("rename suffix not derived from archive name in:\n%s", a1)
	}
	if strings.Contains(a1, "md5(r.conname)") || strings.Contains(a1, "md5(r.relname)") {
		t.Errorf("rename suffix still derived from the (constant) constraint/index name:\n%s", a1)
	}
	sfx := func(s string) string {
		const marker = "DECLARE sfx text := "
		i := strings.Index(s, marker)
		if i < 0 {
			t.Fatalf("no suffix declaration in:\n%s", s)
		}
		return s[i : i+len(marker)+60]
	}
	if sfx(a1) == sfx(a2) {
		t.Errorf("two distinct archives produced the SAME rename suffix expression:\n%s", sfx(a1))
	}
}

func TestListArchivesSQL_Anchored(t *testing.T) {
	got := listArchivesSQL("rearm", "audit")
	if !strings.Contains(got, `tablename ~ '^audit_archive_[0-9]{8}t[0-9]{6}z(_[0-9a-f]+)?$'`) {
		t.Errorf("listArchivesSQL not anchored:\n%s", got)
	}
	if strings.Contains(got, "LIKE") {
		t.Errorf("listArchivesSQL should anchor with a regex, not LIKE:\n%s", got)
	}
}

func TestDropArchiveSQL(t *testing.T) {
	got := dropArchiveSQL("rearm", "audit_archive_x", "5s")
	// IF EXISTS makes a concurrent double-drop (two overlapping runs) a benign no-op.
	for _, want := range []string{"SET LOCAL lock_timeout = '5s';", "DROP TABLE IF EXISTS rearm.audit_archive_x;", "COMMIT;"} {
		if !strings.Contains(got, want) {
			t.Errorf("dropArchiveSQL missing %q in:\n%s", want, got)
		}
	}
}

func TestNewestArchive(t *testing.T) {
	archives := []string{
		"audit_archive_20260601t120000z_a", // older
		"audit_archive_20260719t120000z_b", // newest
		"audit_archive_20260610t120000z_c",
		"audit_archive_notatimestamp_d", // unparseable -> skipped
	}
	name, rot, ok := newestArchive(archives, "audit")
	if !ok || name != "audit_archive_20260719t120000z_b" {
		t.Fatalf("newestArchive = %q, %v; want the 07-19 archive", name, ok)
	}
	if !rot.Equal(time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)) {
		t.Errorf("rot = %v, want 2026-07-19T12:00:00Z", rot)
	}
	if _, _, ok := newestArchive(nil, "audit"); ok {
		t.Error("newestArchive(nil) should report ok=false")
	}
	if _, _, ok := newestArchive([]string{"audit_archive_notatimestamp_x"}, "audit"); ok {
		t.Error("newestArchive with only unparseable names should report ok=false")
	}
}

// rotationDecision is the gate that decouples rotation from cron cadence.
func TestRotationDecision(t *testing.T) {
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	newest := func(daysAgo float64) time.Time { return now.Add(-time.Duration(daysAgo*24) * time.Hour) }
	cases := []struct {
		name       string
		cfg        *config.AppConfig
		haveNewest bool
		newestRot  time.Time
		want       bool
	}{
		{"interval off (0) -> always rotate", &config.AppConfig{RotationInterval: 0}, true, newest(1), true},
		{"drain-backlog -> always rotate", &config.AppConfig{RotationInterval: 30, DrainBacklog: true}, true, newest(1), true},
		{"no archive -> rotate (bootstrap)", &config.AppConfig{RotationInterval: 30}, false, time.Time{}, true},
		{"newest younger than interval -> skip", &config.AppConfig{RotationInterval: 30}, true, newest(29), false},
		{"newest older than interval -> rotate", &config.AppConfig{RotationInterval: 30}, true, newest(31), true},
		{"newest just under interval -> skip", &config.AppConfig{RotationInterval: 14}, true, newest(13.9), false},
		{"newest just over interval -> rotate", &config.AppConfig{RotationInterval: 14}, true, newest(14.1), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, reason := rotationDecision(tc.cfg, now, tc.newestRot, tc.haveNewest)
			if got != tc.want {
				t.Errorf("rotate = %v, want %v (reason %q)", got, tc.want, reason)
			}
			if !got && reason == "" {
				t.Error("a skip must carry a human-readable reason")
			}
		})
	}
}

func TestAdvisoryLockKey(t *testing.T) {
	// stable and table-scoped: same input -> same key; different table -> different key.
	if advisoryLockKey("rearm", "audit") != advisoryLockKey("rearm", "audit") {
		t.Error("advisoryLockKey not stable for the same schema.table")
	}
	if advisoryLockKey("rearm", "audit") == advisoryLockKey("rearm", "other") {
		t.Error("advisoryLockKey should differ for a different table (else unrelated rotations block each other)")
	}
}

func TestIsRotateSkip(t *testing.T) {
	if !isRotateSkip(fmt.Errorf("psql exec failed: ERROR: AUDIT_ROTATE_SKIP: superseded")) {
		t.Error("isRotateSkip should recognize the skip token in a wrapped psql error")
	}
	if isRotateSkip(errors.New("some other failure")) {
		t.Error("isRotateSkip must not match an unrelated error")
	}
	if isRotateSkip(nil) {
		t.Error("isRotateSkip(nil) must be false")
	}
}

func TestCountInstancesSQL(t *testing.T) {
	got := countInstancesSQL("rearm", "audit")
	for _, want := range []string{"count(*)", "rearm.audit", "entity_name = 'instances'"} {
		if !strings.Contains(got, want) {
			t.Errorf("countInstancesSQL missing %q in:\n%s", want, got)
		}
	}
}

func TestNonOwnerGrantsSQL(t *testing.T) {
	got := nonOwnerGrantsSQL("rearm", "audit")
	for _, want := range []string{"aclexplode(c.relacl)", "'rearm.audit'::regclass", "acl.grantee <> c.relowner", "'PUBLIC'"} {
		if !strings.Contains(got, want) {
			t.Errorf("nonOwnerGrantsSQL missing %q in:\n%s", want, got)
		}
	}
}

func TestOldestArchiveAgeDays(t *testing.T) {
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	archives := []string{
		"audit_archive_20260719t120000z_a", // 1 day
		"audit_archive_20260601t120000z_b", // 49 days
		"audit_archive_notatimestamp_c",    // unparseable -> skipped
	}
	if got := oldestArchiveAgeDays(archives, "audit", now); got != 49 {
		t.Errorf("oldestArchiveAgeDays = %d, want 49", got)
	}
	if got := oldestArchiveAgeDays(nil, "audit", now); got != 0 {
		t.Errorf("oldestArchiveAgeDays(nil) = %d, want 0", got)
	}
}

func TestNewArchiveName(t *testing.T) {
	ts := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	a, err := newArchiveName("audit", ts)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(a, "audit_archive_20260719t120000z_") {
		t.Errorf("unexpected archive name: %s", a)
	}
	if b, _ := newArchiveName("audit", ts); a == b {
		t.Errorf("archive names for the same second collided: %s == %s", a, b)
	}
}

// --- the retention drop-gate oracle: parsed from the name, must fail SAFE ---

func TestArchiveRotationTime(t *testing.T) {
	cases := []struct {
		name    string
		archive string
		want    time.Time
		wantErr bool
	}{
		{"valid", "audit_archive_20260719t120000z_deadbeef", time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC), false},
		{"valid no hex suffix", "audit_archive_20260719t120000z", time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC), false},
		{"not an archive name", "audit", time.Time{}, true},
		{"wrong prefix", "other_archive_20260719t120000z_x", time.Time{}, true},
		{"impossible date (month 13) -> parse error, NOT zero-time-as-ancient", "audit_archive_20261301t120000z_x", time.Time{}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := archiveRotationTime(tc.archive, "audit")
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && !got.Equal(tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestAgedOut(t *testing.T) {
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name          string
		archive       string
		retentionDays int
		wantAged      bool
		wantErr       bool
	}{
		{"1 day old, 30d window -> retain", "audit_archive_20260719t120000z_a", 30, false, false},
		{"49 days old, 30d window -> aged", "audit_archive_20260601t120000z_a", 30, true, false},
		{"exactly at boundary (30d ago) -> not yet strictly past -> retain", "audit_archive_20260620t120000z_a", 30, false, false},
		{"retention 0 -> any prior archive aged", "audit_archive_20260719t120000z_a", 0, true, false},
		{"unparseable name -> error, caller must not drop", "audit_archive_99999999t999999z_a", 30, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			aged, err := agedOut(tc.archive, "audit", now, tc.retentionDays)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && aged != tc.wantAged {
				t.Errorf("aged = %v, want %v", aged, tc.wantAged)
			}
		})
	}
}

// --- the pre-drop gate: the decision guarding the sole irreversible step ---

func TestBackupIsDroppable_CheapGate(t *testing.T) {
	const archive = "audit_archive_20260720t100100z_dead"
	// cfg without encryption -> suffix ".dump"
	cfg := &config.AppConfig{PGSchema: "rearm", DumpPrefix: "p"}
	dumpKey := "p-" + archive + ".dump"
	sidecarKey := dumpKey + ".sha256"

	cases := []struct {
		name    string
		objects map[string]int64
		wantErr bool
	}{
		{"dump + sidecar present -> droppable", map[string]int64{dumpKey: 100, sidecarKey: 65}, false},
		{"dump present, sidecar absent -> NOT droppable", map[string]int64{dumpKey: 100}, true},
		{"dump absent -> NOT droppable", map[string]int64{sidecarKey: 65}, true},
		{"nothing present -> NOT droppable", map[string]int64{}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := &pgArchiveBackend{store: &fakeStore{objects: tc.objects}, cfg: cfg}
			err := b.backupIsDroppable(context.Background(), archive, false)
			if (err != nil) != tc.wantErr {
				t.Errorf("err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

// A transient (non-NotFound) Head error must NOT be read as "safe to drop".
func TestBackupIsDroppable_TransientHeadErrorDoesNotDrop(t *testing.T) {
	cfg := &config.AppConfig{PGSchema: "rearm", DumpPrefix: "p"}
	b := &pgArchiveBackend{store: &fakeStore{headErr: errors.New("throttled")}, cfg: cfg}
	if err := b.backupIsDroppable(context.Background(), "audit_archive_20260720t100100z_dead", false); err == nil {
		t.Error("transient Head error must fail the gate (not droppable), got nil")
	}
}

// hasBackup keys on the sidecar (written last) and must map a definitive ErrNotFound
// to "not backed up" while propagating a transient error.
func TestHasBackup(t *testing.T) {
	const archive = "audit_archive_20260720t100100z_dead"
	cfg := &config.AppConfig{PGSchema: "rearm", DumpPrefix: "p"}
	dumpKey := "p-" + archive + ".dump"
	sidecarKey := dumpKey + ".sha256"

	t.Run("dump + sidecar present -> backed up", func(t *testing.T) {
		b := &pgArchiveBackend{store: &fakeStore{objects: map[string]int64{dumpKey: 100, sidecarKey: 65}}, cfg: cfg}
		ok, err := b.hasBackup(context.Background(), archive)
		if err != nil || !ok {
			t.Errorf("ok=%v err=%v, want true,nil", ok, err)
		}
	})
	t.Run("sidecar present but dump missing -> not backed up (self-heal re-dump)", func(t *testing.T) {
		b := &pgArchiveBackend{store: &fakeStore{objects: map[string]int64{sidecarKey: 65}}, cfg: cfg}
		ok, err := b.hasBackup(context.Background(), archive)
		if err != nil || ok {
			t.Errorf("ok=%v err=%v, want false,nil", ok, err)
		}
	})
	t.Run("both absent -> not backed up, no error", func(t *testing.T) {
		b := &pgArchiveBackend{store: &fakeStore{objects: map[string]int64{}}, cfg: cfg}
		ok, err := b.hasBackup(context.Background(), archive)
		if err != nil || ok {
			t.Errorf("ok=%v err=%v, want false,nil", ok, err)
		}
	})
	t.Run("transient error -> propagated, not treated as absence", func(t *testing.T) {
		b := &pgArchiveBackend{store: &fakeStore{headErr: errors.New("throttled")}, cfg: cfg}
		if _, err := b.hasBackup(context.Background(), archive); err == nil {
			t.Error("transient Head error must propagate, got nil")
		}
	})
}
