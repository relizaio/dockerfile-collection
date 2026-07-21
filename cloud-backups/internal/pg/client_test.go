package pg

import (
	"strings"
	"testing"
)

func TestDumpArgs_ExcludeTables(t *testing.T) {
	c := &Client{User: "u", Host: "h", Port: "5432", ExcludeTables: []string{"rearm.audit_archive_*", "", "rearm.tmp_*"}}
	got := strings.Join(c.dumpArgs("demo"), " ")
	for _, want := range []string{
		"--exclude-table=rearm.audit_archive_*",
		"--exclude-table=rearm.tmp_*",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("dumpArgs missing %q in: %s", want, got)
		}
	}
	// blank patterns are skipped
	if strings.Contains(got, "--exclude-table= ") || strings.HasSuffix(got, "--exclude-table=") {
		t.Errorf("blank exclude pattern was not skipped: %s", got)
	}
	// database name is last
	if !strings.HasSuffix(got, " demo") {
		t.Errorf("database not last in: %s", got)
	}
}

func TestDumpArgs_SingleTableIgnoresExcludes(t *testing.T) {
	// When Table is set, --exclude-table must NOT be emitted (single-table archive dump).
	c := &Client{User: "u", Host: "h", Port: "5432", Table: "rearm.audit_archive_x", ExcludeTables: []string{"rearm.audit_archive_*"}}
	got := strings.Join(c.dumpArgs("demo"), " ")
	if strings.Contains(got, "--exclude-table") {
		t.Errorf("exclude-table must be ignored when Table is set: %s", got)
	}
	if !strings.Contains(got, "-t rearm.audit_archive_x") {
		t.Errorf("single-table -t missing: %s", got)
	}
}
