package registry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

// OrasClient implements Client by shelling out to the oras CLI.
type OrasClient struct {
	host    string
	authDir string
}

// New returns an OrasClient for the given registry host and docker config directory.
func New(host, authDir string) *OrasClient {
	return &OrasClient{host: host, authDir: authDir}
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

	cmd := exec.CommandContext(ctx, "oras", "backup", fullPath, "--output", virtualTarPath)
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

	cmd := exec.CommandContext(ctx, "oras", "restore", "--input", tmpPath, fullPath)
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
	cmd := exec.CommandContext(ctx, "oras", "repo", "tags", fullPath)
	cmd.Env = append(os.Environ(), fmt.Sprintf("DOCKER_CONFIG=%s", c.authDir))

	var stderrBuf strings.Builder
	cmd.Stderr = &stderrBuf

	if err := cmd.Run(); err != nil {
		if logs := stderrBuf.String(); strings.Contains(logs, "unauthorized") || strings.Contains(logs, "authentication required") {
			return fmt.Errorf("unauthorized to access %s: check token scopes", fullPath)
		}
		// Repo-not-found is acceptable; the pipeline handles missing repos gracefully.
	}
	return nil
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
