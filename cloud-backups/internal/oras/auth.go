package oras

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// AuthContext holds the secure temporary directory for this specific CLI run
type AuthContext struct {
	ConfigDir string
}

// Login creates a secure sandbox and authenticates
func Login(ctx context.Context, host, username, token string) (*AuthContext, error) {
	// 1. Create a dynamic, secure sandbox for this session
	tmpDir, err := os.MkdirTemp("", "oci-backup-auth-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create auth sandbox: %w", err)
	}

	cmd := exec.CommandContext(ctx, "oras", "login", host, "--username", username, "--password-stdin")
	cmd.Env = append(os.Environ(), fmt.Sprintf("DOCKER_CONFIG=%s", tmpDir))
	cmd.Stdin = strings.NewReader(token)

	var stderrBuf strings.Builder
	cmd.Stderr = &stderrBuf

	if err := cmd.Run(); err != nil {
		os.RemoveAll(tmpDir) // Cleanup on failure
		return nil, fmt.Errorf("oras login failed: %w\nLogs: %s", err, strings.TrimSpace(stderrBuf.String()))
	}

	return &AuthContext{ConfigDir: tmpDir}, nil
}

// Cleanup permanently deletes the credentials from disk
func (a *AuthContext) Cleanup() {
	if a.ConfigDir != "" {
		os.RemoveAll(a.ConfigDir)
	}
}
