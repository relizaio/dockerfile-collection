package cmd

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/relizaio/cloud-backup/internal/config"
	"github.com/relizaio/cloud-backup/internal/stats"
)

func TestRotateSQL(t *testing.T) {
	got := rotateSQL("rearm", "audit", "audit_archive_20260719t120000z_deadbeef", "5s")
	for _, want := range []string{
		"SET LOCAL lock_timeout = '5s';",
		"ALTER TABLE rearm.audit RENAME TO audit_archive_20260719t120000z_deadbeef;",
		"'rearm.audit_archive_20260719t120000z_deadbeef'::regclass",
		"CREATE TABLE rearm.audit (LIKE rearm.audit_archive_20260719t120000z_deadbeef INCLUDING ALL);",
		"COMMIT;",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("rotateSQL missing %q in:\n%s", want, got)
		}
	}
}

func TestKeepCopySQL_InstancesOnly(t *testing.T) {
	cols := []string{"uuid", "entity_name", "revision_record_data"}
	got := keepCopySQL("rearm", "audit", "audit_archive_x", 0, "5s", cols)
	for _, want := range []string{
		"SET statement_timeout = 0;",
		"SET lock_timeout = '5s';",
		`INSERT INTO rearm.audit ("uuid", "entity_name", "revision_record_data")`,
		`SELECT "uuid", "entity_name", "revision_record_data" FROM rearm.audit_archive_x`,
		"entity_name = 'instances'",
		"ON CONFLICT DO NOTHING;",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("keepCopySQL(0) missing %q in:\n%s", want, got)
		}
	}
	if strings.Contains(got, "revision_created_date") {
		t.Errorf("keepCopySQL(0) should be INSTANCES-only, but references revision_created_date:\n%s", got)
	}
}

func TestKeepCopySQL_WithTail(t *testing.T) {
	got := keepCopySQL("rearm", "audit", "a", 30, "5s", []string{"uuid"})
	if !strings.Contains(got, "make_interval(days => 30)") {
		t.Errorf("keepCopySQL(30) did not honor keepTailDays:\n%s", got)
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
	for _, want := range []string{"SET LOCAL lock_timeout = '5s';", "DROP TABLE rearm.audit_archive_x;", "COMMIT;"} {
		if !strings.Contains(got, want) {
			t.Errorf("dropArchiveSQL missing %q in:\n%s", want, got)
		}
	}
}

func TestAssertHasUniqueSQL(t *testing.T) {
	got := assertHasUniqueSQL("rearm", "audit")
	for _, want := range []string{"table_constraints", "table_name = 'audit'", "'PRIMARY KEY', 'UNIQUE'"} {
		if !strings.Contains(got, want) {
			t.Errorf("assertHasUniqueSQL missing %q in:\n%s", want, got)
		}
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

// --- drop-gate: the one irreversible step, unit-tested via the archiveBackend seam ---

type fakeBackend struct {
	cols      []string
	queryErr  error
	execErr   error
	backupErr error // BackupAndVerify result: nil = fully verified
	execs     []string
}

func (f *fakeBackend) QueryRows(_ context.Context, _ string) ([]string, error) {
	return f.cols, f.queryErr
}
func (f *fakeBackend) Exec(_ context.Context, sql string) error {
	f.execs = append(f.execs, sql)
	return f.execErr
}
func (f *fakeBackend) BackupAndVerify(_ context.Context, _ string, _ *stats.Tracker) error {
	return f.backupErr
}
func (f *fakeBackend) dropped() bool {
	for _, s := range f.execs {
		if strings.Contains(s, "DROP TABLE") {
			return true
		}
	}
	return false
}

func TestBackupAndDropArchive_Gate(t *testing.T) {
	cases := []struct {
		name        string
		cols        []string
		queryErr    error
		backupErr   error
		noDrop      bool
		wantErr     bool
		wantDropped bool
	}{
		{"verified backup -> drop", []string{"uuid"}, nil, nil, false, false, true},
		{"backup/verify failed -> no drop", []string{"uuid"}, nil, errors.New("upload failed"), false, true, false},
		{"verified but --no-drop -> no drop, no error", []string{"uuid"}, nil, nil, true, false, false},
		{"empty shared columns -> no drop, no backup", nil, nil, nil, false, true, false},
		{"query error -> no drop", []string{"uuid"}, errors.New("boom"), nil, false, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.AppConfig{PGSchema: "rearm", AuditTable: "audit", LockTimeout: "5s", NoDrop: tc.noDrop}
			b := &fakeBackend{cols: tc.cols, queryErr: tc.queryErr, backupErr: tc.backupErr}
			err := backupAndDropArchive(context.Background(), b, cfg, "audit_archive_x", stats.New())
			if (err != nil) != tc.wantErr {
				t.Errorf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			if b.dropped() != tc.wantDropped {
				t.Errorf("dropped = %v, want %v (execs: %v)", b.dropped(), tc.wantDropped, b.execs)
			}
			// When a drop happens, keep-copy must have run first.
			if tc.wantDropped {
				if len(b.execs) < 2 || !strings.Contains(b.execs[0], "INSERT INTO") || !strings.Contains(b.execs[len(b.execs)-1], "DROP TABLE") {
					t.Errorf("keep-copy must precede drop; execs: %v", b.execs)
				}
			}
		})
	}
}
