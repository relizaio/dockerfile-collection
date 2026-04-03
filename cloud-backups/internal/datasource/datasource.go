package datasource

import (
	"context"
	"io"
)

// Source abstracts any data source that can produce or consume a binary stream.
// The target parameter is source-specific: a registry path for OCI, a database name for PG.
// Implementations must be safe for concurrent use.
type Source interface {
	// Backup streams the data from target and writes it as a binary archive to out.
	Backup(ctx context.Context, target string, out io.Writer) error

	// Restore reads a binary archive from in and restores it to target.
	Restore(ctx context.Context, target string, in io.Reader) error

	// PreflightCheck performs a lightweight connectivity/access probe against target.
	PreflightCheck(ctx context.Context, target string) error
}
