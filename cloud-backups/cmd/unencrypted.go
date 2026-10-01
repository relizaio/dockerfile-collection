package cmd

import (
	"log/slog"

	"github.com/relizaio/cloud-backup/internal/config"
)

// warnIfUnencrypted logs, on every run, that a backup is being written without encryption. Only
// reachable after validation, i.e. with --allow-unencrypted set: an explicit opt-in, but one that
// must stay visible in the logs rather than become the silent default it used to be.
func warnIfUnencrypted(cfg *config.AppConfig) {
	if cfg.EncryptionPassword == "" {
		slog.Warn("backup_unencrypted", "reason", "no encryption password; --allow-unencrypted is set")
	}
}
