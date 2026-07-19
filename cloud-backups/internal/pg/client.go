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
	args := []string{
		"-Fc",
		"-U", c.User,
		"-h", c.Host,
		"-p", c.port(),
	}
	if c.Table != "" {
		args = append(args, "-t", c.Table)
	}
	args = append(args, database)
	cmd := exec.CommandContext(ctx, "pg_dump", args...)
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
// mode for the rotate/keep-copy/drop steps.
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
