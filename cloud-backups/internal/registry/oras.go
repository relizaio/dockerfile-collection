package registry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

// OrasClient implements Client by shelling out to the oras CLI.
type OrasClient struct {
	host      string
	authDir   string
	plainHTTP bool
}

// New returns an OrasClient for the given registry host and docker config directory.
func New(host, authDir string, plainHTTP bool) *OrasClient {
	return &OrasClient{host: host, authDir: authDir, plainHTTP: plainHTTP}
}

// Backup runs `oras backup` and streams the resulting tar archive to out.
func (c *OrasClient) Backup(ctx context.Context, registryPath string, out io.Writer) error {
	fullPath := fmt.Sprintf("%s/%s", c.host, registryPath)

	randBytes, err := makeRandBytes()
	if err != nil {
		return err
	}

	osReader, osWriter, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("failed to create pipe: %w", err)
	}

	virtualTarPath := filepath.Join(os.TempDir(), fmt.Sprintf("oras-backup-%s.tar", hex.EncodeToString(randBytes)))
	if err := os.Symlink("/proc/self/fd/3", virtualTarPath); err != nil {
		osReader.Close()
		osWriter.Close()
		return fmt.Errorf("failed to create virtual tar path: %w", err)
	}
	defer os.Remove(virtualTarPath)

	backupArgs := []string{"backup", fullPath, "--output", virtualTarPath}
	if c.plainHTTP {
		backupArgs = append(backupArgs, "--plain-http")
	}
	cmd := exec.CommandContext(ctx, "oras", backupArgs...)
	cmd.ExtraFiles = []*os.File{osWriter}
	cmd.Env = append(os.Environ(), fmt.Sprintf("DOCKER_CONFIG=%s", c.authDir))

	stderrBuf := &tailBuffer{max: 8192}
	cmd.Stdout = stderrBuf
	cmd.Stderr = stderrBuf

	if startErr := cmd.Start(); startErr != nil {
		osReader.Close()
		osWriter.Close()
		return fmt.Errorf("failed to start oras backup: %w", startErr)
	}
	osWriter.Close() // oras subprocess holds its own fd copy

	_, copyErr := io.Copy(out, osReader)
	osReader.Close()

	if copyErr != nil && cmd.Process != nil {
		cmd.Process.Signal(syscall.SIGTERM)
	}

	waitErr := cmd.Wait()
	if waitErr != nil || copyErr != nil {
		logs := strings.TrimSpace(stderrBuf.String())
		if strings.Contains(logs, "unauthorized") || strings.Contains(logs, "authentication required") {
			return fmt.Errorf("unauthorized to access %s: check token scopes", fullPath)
		}
		if repositoryAbsent(logs) {
			slog.Warn("oras_backup_repository_not_found", "path", fullPath)
			return fmt.Errorf("repository name not known to registry: %s", fullPath)
		}
		return errors.Join(copyErr, fmt.Errorf("oras backup failed: %w | Logs: %s", waitErr, logs))
	}
	return nil
}

// Restore runs `oras restore` feeding it the tar archive from in.
// oras restore validates that --input is a regular file (rejects pipes/symlinks),
// so the stream is buffered to a temp file before invoking oras.
func (c *OrasClient) Restore(ctx context.Context, registryPath string, in io.Reader) error {
	fullPath := fmt.Sprintf("%s/%s", c.host, registryPath)

	tmpFile, err := os.CreateTemp("", "oras-restore-*.tar")
	if err != nil {
		return fmt.Errorf("failed to create temp file for restore: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	if _, err := io.Copy(tmpFile, in); err != nil {
		tmpFile.Close()
		return fmt.Errorf("failed to buffer tar for oras restore: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to flush temp file: %w", err)
	}

	restoreArgs := []string{"restore", "--input", tmpPath, fullPath}
	if c.plainHTTP {
		restoreArgs = append(restoreArgs, "--plain-http")
	}
	cmd := exec.CommandContext(ctx, "oras", restoreArgs...)
	cmd.Env = append(os.Environ(), fmt.Sprintf("DOCKER_CONFIG=%s", c.authDir))

	stderrBuf := &tailBuffer{max: 8192}
	cmd.Stdout = stderrBuf
	cmd.Stderr = stderrBuf

	if err := cmd.Run(); err != nil {
		logs := strings.TrimSpace(stderrBuf.String())
		if strings.Contains(logs, "unauthorized") || strings.Contains(logs, "authentication required") {
			return fmt.Errorf("unauthorized to push to %s: check token scopes", fullPath)
		}
		return fmt.Errorf("oras restore failed: %w | Logs: %s", err, logs)
	}
	return nil
}

// PreflightCheck runs `oras repo tags` as a lightweight pull-access probe.
func (c *OrasClient) PreflightCheck(ctx context.Context, registryPath string) error {
	fullPath := fmt.Sprintf("%s/%s", c.host, registryPath)
	preflightArgs := []string{"repo", "tags", fullPath}
	if c.plainHTTP {
		preflightArgs = append(preflightArgs, "--plain-http")
	}
	cmd := exec.CommandContext(ctx, "oras", preflightArgs...)
	cmd.Env = append(os.Environ(), fmt.Sprintf("DOCKER_CONFIG=%s", c.authDir))

	// Deliberately NOT a tailBuffer: `logs` is what the not-found and auth
	// predicates below match on, so dropping the head can change the
	// CLASSIFICATION, not just the message -- a probe whose "not found" token
	// scrolls out of a bounded buffer stops being a tolerated absence and aborts
	// the entire run. The size of the surfaced string is bounded at the call
	// site instead, where it is only used for display.
	var stderrBuf strings.Builder
	cmd.Stderr = &stderrBuf

	if err := cmd.Run(); err != nil {
		logs := stderrBuf.String()
		if strings.Contains(logs, "unauthorized") || strings.Contains(logs, "authentication required") {
			return fmt.Errorf("unauthorized to access %s: check token scopes", fullPath)
		}
		if repositoryAbsent(logs) {
			slog.Warn("oras_preflight_repository_not_found", "path", fullPath)
			return nil
		}
		return fmt.Errorf("preflight check failed for %s: %w | Logs: %s", fullPath, err, strings.TrimSpace(logs))
	}
	return nil
}

// transportFailureMarkers are substrings that only a connectivity failure
// produces. A repository that is genuinely absent never emits them.
var transportFailureMarkers = []string{
	"connection refused",
	"connection reset",
	"dial tcp",
	"i/o timeout",
	"no such host",
	"tls handshake",
	"unexpected eof",
	"context deadline exceeded",
}

// repositoryAbsent reports whether the tool's output means "this repository
// does not exist" rather than "we could not reach the registry".
//
// The distinction decides whether the caller SKIPS the target (no backup, no
// alert, exit 0) or FAILS it, so a false positive silently loses a backup. Two
// traps, both observed:
//
//   - Bare "404" is not evidence. Content digests are hex, and "404" appears in
//     roughly 1.5% of them, so a refused dial on a repo with enough blobs in the
//     log tail gets misread as an absence. Any transport marker therefore vetoes
//     the classification outright.
//   - Matching was case-sensitive and missed the canonical distribution error
//     ("name unknown: repository name not known to registry") entirely, which
//     made preflight abort whole runs against distribution-compatible
//     registries. Match case-insensitively and include the canonical phrasing.
func repositoryAbsent(logs string) bool {
	l := strings.ToLower(logs)
	for _, marker := range transportFailureMarkers {
		if strings.Contains(l, marker) {
			return false
		}
	}
	return strings.Contains(l, "name unknown") ||
		strings.Contains(l, "repository name not known") ||
		strings.Contains(l, "not found") ||
		strings.Contains(l, "404")
}

func makeRandBytes() ([]byte, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return nil, fmt.Errorf("failed to generate random bytes: %w", err)
	}
	return b, nil
}

// tailBuffer keeps only the last max bytes written to it.
type tailBuffer struct {
	buf []byte
	max int
	mu  sync.Mutex
}

func (w *tailBuffer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf = append(w.buf, p...)
	if len(w.buf) > w.max {
		shift := len(w.buf) - w.max
		w.buf = append([]byte(nil), w.buf[shift:]...)
	}
	return len(p), nil
}

func (w *tailBuffer) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return string(w.buf)
}
