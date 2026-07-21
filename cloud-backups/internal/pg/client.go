package pg

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

// Client wraps pg_dump / pg_restore / pg_isready for use as a datasource.Source.
type Client struct {
	Host     string
	Port     string
	Database string
	User     string
	// Table, when non-empty, scopes Backup to a single table (pg_dump -t <Table>)
	// instead of dumping the whole database. Used by the audit-rotate mode to dump
	// a rotated-out archive table. Empty means whole-database dump (default).
	Table string
	// ExcludeTables, when non-empty, are pg_dump --exclude-table patterns (each may
	// use wildcards, e.g. rearm.audit_archive_*) omitted from a whole-database Backup.
	// Used to keep the retained audit archive tables out of the regular full-DB backup
	// (they have their own permanent-bucket backups) and to avoid a DROP-vs-pg_dump
	// race against a table the rotation may drop mid-dump. Ignored when Table is set.
	ExcludeTables []string
}

// dumpArgs builds the pg_dump argv for a dump of database. When Table is set, only
// that table is dumped (-t) and ExcludeTables is ignored; otherwise each non-empty
// ExcludeTables pattern is passed as a discrete --exclude-table=<pat> argv element
// (no shell, so a leading '-' in a pattern can't be reparsed as a flag). The
// database name is always last. Extracted for unit-testability.
func (c *Client) dumpArgs(database string) []string {
	args := []string{
		"-Fc",
		"-U", c.User,
		"-h", c.Host,
		"-p", c.port(),
	}
	if c.Table != "" {
		args = append(args, "-t", c.Table)
	} else {
		for _, pat := range c.ExcludeTables {
			if pat != "" {
				args = append(args, "--exclude-table="+pat)
			}
		}
	}
	return append(args, database)
}

// Backup runs pg_dump -Fc and streams its stdout to out.
// target is the database name to dump (overrides c.Database if non-empty).
// When c.Table is set, only that table is dumped (pg_dump -t).
// PGPASSWORD must be set in the environment by the caller.
func (c *Client) Backup(ctx context.Context, target string, out io.Writer) error {
	database := target
	if database == "" {
		database = c.Database
	}
	cmd := exec.CommandContext(ctx, "pg_dump", c.dumpArgs(database)...)
	cmd.Stdout = out
	cmd.Env = os.Environ()

	var stderrBuf strings.Builder
	cmd.Stderr = &stderrBuf

	if err := cmd.Run(); err != nil {
		stderr := strings.TrimSpace(stderrBuf.String())
		return fmt.Errorf("pg_dump failed: %w | stderr: %s", err, stderr)
	}
	return nil
}

// Restore runs pg_restore --clean reading from in.
// target is the database name to restore into (overrides c.Database if non-empty).
// PGPASSWORD must be set in the environment by the caller.
func (c *Client) Restore(ctx context.Context, target string, in io.Reader) error {
	database := target
	if database == "" {
		database = c.Database
	}
	args := []string{
		"--clean",
		"--if-exists",
		"-U", c.User,
		"-h", c.Host,
		"-p", c.port(),
		"-d", database,
	}
	cmd := exec.CommandContext(ctx, "pg_restore", args...)
	cmd.Stdin = in
	cmd.Env = os.Environ()

	var stderrBuf strings.Builder
	cmd.Stderr = &stderrBuf

	if err := cmd.Run(); err != nil {
		stderr := strings.TrimSpace(stderrBuf.String())
		return fmt.Errorf("pg_restore failed: %w | stderr: %s", err, stderr)
	}
	return nil
}

// RestoreList runs `pg_restore -l` reading a custom-format archive from in,
// discarding the listed TOC. It validates that the archive is a structurally
// valid, non-truncated, restorable pg_dump (it parses the header + TOC + offset
// table) WITHOUT touching any database. Returns an error if the archive is
// invalid/corrupt/truncated. No DB connection is used.
func (c *Client) RestoreList(ctx context.Context, in io.Reader) error {
	cmd := exec.CommandContext(ctx, "pg_restore", "-l")
	cmd.Stdin = in
	cmd.Env = os.Environ()
	cmd.Stdout = io.Discard

	var stderrBuf strings.Builder
	cmd.Stderr = &stderrBuf

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("pg_restore -l failed (archive not a valid restorable dump): %w | stderr: %s", err, strings.TrimSpace(stderrBuf.String()))
	}
	return nil
}

// PreflightCheck runs pg_isready to probe connectivity before the full pipeline.
func (c *Client) PreflightCheck(ctx context.Context, target string) error {
	args := []string{
		"-h", c.Host,
		"-p", c.port(),
		"-U", c.User,
	}
	cmd := exec.CommandContext(ctx, "pg_isready", args...)
	cmd.Env = os.Environ()

	var stderrBuf strings.Builder
	cmd.Stderr = &stderrBuf

	if err := cmd.Run(); err != nil {
		stderr := strings.TrimSpace(stderrBuf.String())
		return fmt.Errorf("postgresql not reachable at %s:%s: %w | %s", c.Host, c.port(), err, stderr)
	}
	return nil
}

// Exec runs a (possibly multi-statement) SQL script via psql with ON_ERROR_STOP,
// reading the script from stdin so multi-statement DDL and DO-blocks work reliably.
// PGPASSWORD must be set in the environment by the caller. Used by the audit-rotate
// mode for the rotate/drop steps.
func (c *Client) Exec(ctx context.Context, sql string) error {
	args := []string{
		"-v", "ON_ERROR_STOP=1",
		"-U", c.User,
		"-h", c.Host,
		"-p", c.port(),
		"-d", c.Database,
		"-f", "-",
	}
	cmd := exec.CommandContext(ctx, "psql", args...)
	cmd.Stdin = strings.NewReader(sql)
	cmd.Env = os.Environ()

	var stderrBuf strings.Builder
	cmd.Stderr = &stderrBuf

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("psql exec failed: %w | stderr: %s", err, strings.TrimSpace(stderrBuf.String()))
	}
	return nil
}

// QueryRows runs a single SELECT via psql in unaligned tuples-only mode and returns
// the non-empty result lines. PGPASSWORD must be set in the environment by the caller.
func (c *Client) QueryRows(ctx context.Context, sql string) ([]string, error) {
	args := []string{
		"-v", "ON_ERROR_STOP=1",
		"-A", "-t",
		"-U", c.User,
		"-h", c.Host,
		"-p", c.port(),
		"-d", c.Database,
		"-c", sql,
	}
	cmd := exec.CommandContext(ctx, "psql", args...)
	cmd.Env = os.Environ()

	var stderrBuf strings.Builder
	cmd.Stderr = &stderrBuf

	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("psql query failed: %w | stderr: %s", err, strings.TrimSpace(stderrBuf.String()))
	}
	var rows []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if s := strings.TrimSpace(line); s != "" {
			rows = append(rows, s)
		}
	}
	return rows, nil
}

func (c *Client) port() string {
	if c.Port != "" {
		return c.Port
	}
	return "5432"
}
