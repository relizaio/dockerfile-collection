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
}

// Backup runs pg_dump -Fc and streams its stdout to out.
// target is the database name to dump (overrides c.Database if non-empty).
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
		database,
	}
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

func (c *Client) port() string {
	if c.Port != "" {
		return c.Port
	}
	return "5432"
}
