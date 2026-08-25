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
	"github.com/relizaio/cloud-backup/internal/stats"
)

// fakeStore is a storage.Provider for unit-testing the upload-finalize path.
type fakeStore struct {
	failOnKey string // fail only this key (e.g. the sidecar), succeed on others
	uploaded  map[string]bool
}

func (f *fakeStore) UploadStream(_ context.Context, path string, r io.Reader) error {
	_, _ = io.Copy(io.Discard, r)
	if f.failOnKey != "" && path == f.failOnKey {
		return fmt.Errorf("simulated upload failure for %s", path)
	}
	if f.uploaded == nil {
		f.uploaded = map[string]bool{}
	}
	f.uploaded[path] = true
	return nil
}
func (f *fakeStore) DownloadStream(_ context.Context, _ string, _ io.Writer) error { return nil }

func TestFinalizeUpload(t *testing.T) {
	const key = "prefix-audit_archive_20260701t030500z_abc12345.dump.age"
	cases := []struct {
		name        string
		failOnKey   string
		wantErr     bool
		wantSidecar bool
	}{
		{name: "writes the sidecar", wantSidecar: true},
		{
			// The sidecar is the durable record that this archive was backed up. If it
			// cannot be written the caller must NOT go on to drop the source table.
			name:      "sidecar write failure is an error, so the drop never happens",
			failOnKey: key + ".sha256",
			wantErr:   true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := &fakeStore{failOnKey: tc.failOnKey}
			b := &pgArchiveBackend{store: fs, cfg: &config.AppConfig{}}
			err := b.finalizeUpload(context.Background(), key, 100, "abc123")
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if got := fs.uploaded[key+".sha256"]; got != tc.wantSidecar {
				t.Errorf("sidecar written = %v, want %v", got, tc.wantSidecar)
			}
		})
	}
}

// The whole design rests on the steady-state credential being write-only, so assert on the
// CALLS rather than the outcome: a reintroduced read would otherwise be invisible to tests
// that only check results, and would surface as a broadened IAM policy in production.
func TestBackupAndDropIsWriteOnly(t *testing.T) {
	rs := &readRejectingStore{}
	rec := &recordingExec{}
	b := &pgArchiveBackend{store: rs, cfg: &config.AppConfig{PGSchema: "rearm"}, exec: rec}
	b.backupArchive = func(context.Context, string, *stats.Tracker) error { return nil }

	if err := b.backupAndDrop(context.Background(), "audit_archive_20260701t030500z_abc12345", stats.New()); err != nil {
		t.Fatalf("backupAndDrop: %v", err)
	}
	if rs.downloads > 0 {
		t.Errorf("the drop path issued %d bucket read(s); it must be write-only (--verify-restore is the only exception)", rs.downloads)
	}
}

// readRejectingStore fails loudly on any read, so a reintroduced bucket lookup shows up as
// a test failure rather than as a silently broadened credential requirement.
type readRejectingStore struct{ downloads int }

func (r *readRejectingStore) UploadStream(_ context.Context, _ string, rd io.Reader) error {
	_, _ = io.Copy(io.Discard, rd)
	return nil
}
func (r *readRejectingStore) DownloadStream(_ context.Context, path string, _ io.Writer) error {
	r.downloads++
	return fmt.Errorf("unexpected read of %q: the steady-state path must be write-only", path)
}

func TestRotateSQL(t *testing.T) {
	got := rotateSQL("rearm", "audit", "audit_archive_20260719t120000z_deadbeef", "5s", 1234567890, "audit_archive_20260601t120000z_old", 0, "revision_created_date")
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
	// keep-tail-days == 0 (the write-only-table default) must emit NO seed at all.
	if strings.Contains(got, "INSERT INTO") || strings.Contains(got, "make_interval") {
		t.Errorf("keep-tail-days=0 must not seed the fresh table, but rotateSQL contains a seed:\n%s", got)
	}
}

// keep-tail-days > 0 seeds the fresh table, in the rotate transaction, with the recent tail
// of the archive so a live reader's rolling-window read survives the rotation boundary. It
// must land AFTER the fresh CREATE TABLE (the target exists) and BEFORE COMMIT (atomic with
// the rename), filter on the given column, and copy from the archive into the live table.
func TestRotateSQL_KeepTailSeed(t *testing.T) {
	got := rotateSQL("rearm", "metrics_audit", "metrics_audit_archive_20260825t120000z_deadbeef", "5s", 1, "", 4, "revision_created_date")
	seed := "INSERT INTO rearm.metrics_audit SELECT * FROM rearm.metrics_audit_archive_20260825t120000z_deadbeef WHERE revision_created_date >= now() - make_interval(days => 4);"
	if !strings.Contains(got, seed) {
		t.Errorf("keep-tail seed missing or malformed; want %q in:\n%s", seed, got)
	}
	create := strings.Index(got, "CREATE TABLE rearm.metrics_audit (LIKE")
	seedIdx := strings.Index(got, seed)
	commit := strings.Index(got, "COMMIT;")
	if !(create >= 0 && create < seedIdx && seedIdx < commit) {
		t.Errorf("seed must fall between CREATE TABLE (%d) and COMMIT (%d) but is at %d:\n%s", create, commit, seedIdx, got)
	}
}

// The rename-aside suffix MUST derive from the (unique) archive name, not the
// constant original constraint/index name -- otherwise two coexisting un-dropped
// archives (the norm under retention) collide on `audit_pkey_<sfx>` and the second
// rotation fails with `relation "audit_pkey_..." already exists`.
func TestRotateSQL_RenameSuffixIsPerArchive(t *testing.T) {
	a1 := rotateSQL("rearm", "audit", "audit_archive_20260720t100100z_05196fcc", "5s", 1, "", 0, "revision_created_date")
	a2 := rotateSQL("rearm", "audit", "audit_archive_20260720t112820z_d2337e60", "5s", 1, "", 0, "revision_created_date")

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

func TestGeneratedColumnsSQL(t *testing.T) {
	got := generatedColumnsSQL("rearm", "metrics_audit")
	for _, want := range []string{
		"count(*)",
		"table_schema = 'rearm'",
		"table_name = 'metrics_audit'",
		"is_generated = 'ALWAYS'",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("generatedColumnsSQL missing %q in:\n%s", want, got)
		}
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

// columnExistsSQL scopes the entity_name instances guard to tables that have the column
// (the generic audit table), so a table without it (e.g. metrics_audit) skips the guard
// instead of erroring on the missing column.
func TestColumnExistsSQL(t *testing.T) {
	got := columnExistsSQL("rearm", "metrics_audit", "entity_name")
	for _, want := range []string{"SELECT EXISTS", "information_schema.columns", "table_schema = 'rearm'", "table_name = 'metrics_audit'", "column_name = 'entity_name'"} {
		if !strings.Contains(got, want) {
			t.Errorf("columnExistsSQL missing %q in:\n%s", want, got)
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

// recordingExec captures DDL instead of running it, so the sole irreversible step can be
// asserted on without a live Postgres.
type recordingExec struct{ stmts []string }

func (r *recordingExec) Exec(_ context.Context, sql string) error {
	r.stmts = append(r.stmts, sql)
	return nil
}

// THE test for this package: a DROP must be unreachable unless that archive's own backup
// succeeded. Nothing re-reads the bucket afterwards, so this ordering is the only thing
// between a failed upload and irreversible data loss.
func TestBackupAndDropNeverDropsWhenBackupFails(t *testing.T) {
	const archive = "audit_archive_20260701t030500z_abc12345"
	cases := []struct {
		name      string
		backupErr error
		wantDrops int
	}{
		{"dump/upload failed -> no DROP", errors.New("upload did not complete"), 0},
		{"sidecar write failed -> no DROP", errors.New("writing sha256 sidecar"), 0},
		{"verify-restore failed -> no DROP", errors.New("pg_restore -l rejected the archive"), 0},
		{"backup succeeded -> exactly one DROP", nil, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recordingExec{}
			b := &pgArchiveBackend{
				store: &fakeStore{},
				cfg:   &config.AppConfig{PGSchema: "rearm", LockTimeout: "5s"},
				exec:  rec,
			}
			b.backupArchive = func(context.Context, string, *stats.Tracker) error { return tc.backupErr }

			err := b.backupAndDrop(context.Background(), archive, stats.New())
			if (err != nil) != (tc.backupErr != nil) {
				t.Fatalf("err = %v, want error: %v", err, tc.backupErr != nil)
			}
			drops := 0
			for _, st := range rec.stmts {
				if strings.Contains(st, "DROP TABLE") {
					drops++
					if want := "rearm." + archive; !strings.Contains(st, want) {
						t.Errorf("DROP targeted the wrong table, want %q in %q", want, st)
					}
				}
			}
			if drops != tc.wantDrops {
				t.Errorf("DROP statements executed = %d, want %d (stmts: %v)", drops, tc.wantDrops, rec.stmts)
			}
		})
	}
}

// An unwired seam must fail closed rather than nil-panic: pgArchiveBackend embeds
// *pg.Client, which promotes Exec, so a future caller could otherwise construct a partly
// wired backend and reach the DROP by a path that bypasses the injected executor.
func TestBackupAndDropRefusesWhenUnwired(t *testing.T) {
	cases := []struct {
		name string
		b    *pgArchiveBackend
	}{
		{"no executor", &pgArchiveBackend{store: &fakeStore{}, cfg: &config.AppConfig{}, backupArchive: func(context.Context, string, *stats.Tracker) error { return nil }}},
		{"no backup step", &pgArchiveBackend{store: &fakeStore{}, cfg: &config.AppConfig{}, exec: &recordingExec{}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.b.backupAndDrop(context.Background(), "audit_archive_20260701t030500z_abc12345", stats.New())
			if err == nil {
				t.Fatal("expected a refusal, got nil -- an unwired backend must never reach the DROP")
			}
			if !strings.Contains(err.Error(), "refusing to drop") {
				t.Errorf("error should name the refusal, got %v", err)
			}
		})
	}
}
