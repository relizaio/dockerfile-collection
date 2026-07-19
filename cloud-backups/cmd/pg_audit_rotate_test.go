package cmd

import (
	"strings"
	"testing"
)

func TestRotateSQL(t *testing.T) {
	got := rotateSQL("rearm", "audit", "audit_archive_20260719t120000z", "5s")
	for _, want := range []string{
		"SET LOCAL lock_timeout = '5s';",
		"ALTER TABLE rearm.audit RENAME TO audit_archive_20260719t120000z;",
		"'rearm.audit_archive_20260719t120000z'::regclass",
		"CREATE TABLE rearm.audit (LIKE rearm.audit_archive_20260719t120000z INCLUDING ALL);",
		"COMMIT;",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("rotateSQL missing %q in:\n%s", want, got)
		}
	}
}

func TestKeepCopySQL(t *testing.T) {
	got := keepCopySQL("rearm", "audit", "audit_archive_x", 0)
	for _, want := range []string{
		"INSERT INTO rearm.audit",
		"FROM rearm.audit_archive_x",
		"entity_name = 'instances'",
		"make_interval(days => 0)",
		"ON CONFLICT DO NOTHING;",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("keepCopySQL missing %q in:\n%s", want, got)
		}
	}
	if !strings.Contains(keepCopySQL("rearm", "audit", "a", 30), "make_interval(days => 30)") {
		t.Error("keepCopySQL did not honor keepTailDays=30")
	}
}

func TestListArchivesSQL(t *testing.T) {
	got := listArchivesSQL("rearm", "audit")
	for _, want := range []string{
		"schemaname = 'rearm'",
		`tablename LIKE 'audit\_archive\_%'`,
		`ESCAPE '\'`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("listArchivesSQL missing %q in:\n%s", want, got)
		}
	}
	// underscores in the audit name must be escaped so they match literally
	if !strings.Contains(listArchivesSQL("rearm", "my_audit"), `my\_audit\_archive\_%`) {
		t.Error("listArchivesSQL did not escape underscores in the audit table name")
	}
}
