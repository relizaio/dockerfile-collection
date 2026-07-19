package cmd

import (
	"strings"
	"testing"
	"time"
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
	got := keepCopySQL("rearm", "audit", "audit_archive_x", 0, cols)
	for _, want := range []string{
		"SET statement_timeout = 0;",
		`INSERT INTO rearm.audit ("uuid", "entity_name", "revision_record_data")`,
		`SELECT "uuid", "entity_name", "revision_record_data" FROM rearm.audit_archive_x`,
		"entity_name = 'instances'",
		"ON CONFLICT DO NOTHING;",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("keepCopySQL(0) missing %q in:\n%s", want, got)
		}
	}
	// keepTailDays==0 must NOT reference revision_created_date (avoids full seq-scan)
	if strings.Contains(got, "revision_created_date") {
		t.Errorf("keepCopySQL(0) should be INSTANCES-only, but references revision_created_date:\n%s", got)
	}
}

func TestKeepCopySQL_WithTail(t *testing.T) {
	got := keepCopySQL("rearm", "audit", "a", 30, []string{"uuid"})
	if !strings.Contains(got, "make_interval(days => 30)") {
		t.Errorf("keepCopySQL(30) did not honor keepTailDays:\n%s", got)
	}
}

func TestListArchivesSQL_Anchored(t *testing.T) {
	got := listArchivesSQL("rearm", "audit")
	for _, want := range []string{
		"schemaname = 'rearm'",
		`tablename ~ '^audit_archive_[0-9]{8}t[0-9]{6}z(_[0-9a-f]+)?$'`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("listArchivesSQL missing %q in:\n%s", want, got)
		}
	}
	// must not use an open-ended LIKE that could match an unrelated table
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

func TestNewArchiveName(t *testing.T) {
	ts := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	a, err := newArchiveName("audit", ts)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(a, "audit_archive_20260719t120000z_") {
		t.Errorf("unexpected archive name: %s", a)
	}
	b, _ := newArchiveName("audit", ts)
	if a == b {
		t.Errorf("archive names for the same second collided: %s == %s", a, b)
	}
}
