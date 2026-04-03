package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/relizaio/cloud-backup/internal/config"
	"github.com/relizaio/cloud-backup/internal/oras"
	"github.com/relizaio/cloud-backup/internal/pipeline"
	"github.com/relizaio/cloud-backup/internal/registry"
	"github.com/relizaio/cloud-backup/internal/storage"
)

var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore an OCI artifact backup from cloud storage to a registry",
	Run: func(cmd *cobra.Command, args []string) {
		// runRestore owns all defers; os.Exit is only called here, after defers have run.
		if err := runRestore(cmd); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	},
}

func runRestore(cmd *cobra.Command) error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// 1. Build and validate typed config
	cfg := &config.AppConfig{
		RegistryHost:        viper.GetString("registry-host"),
		RegistryUsername:    viper.GetString("registry-username"),
		RegistryToken:       viper.GetString("registry-token"),
		StorageType:         viper.GetString("backup-storage-type"),
		EncryptionPassword:  viper.GetString("encryption-password"),
		Timeout:             viper.GetDuration("timeout"),
		AWSBucket:           viper.GetString("aws-bucket"),
		AWSRegion:           viper.GetString("aws-region"),
		AWSAccessKeyID:      viper.GetString("aws-access-key-id"),
		AWSSecretAccessKey:  viper.GetString("aws-secret-access-key"),
		AzureStorageAccount: viper.GetString("azure-storage-account"),
		AzureTenantID:       viper.GetString("azure-tenant-id"),
		AzureClientID:       viper.GetString("azure-client-id"),
		AzureClientSecret:   viper.GetString("azure-client-secret"),
		AzureContainer:      viper.GetString("azure-container"),
		BackupFile:          mustGetString(cmd, "backup-file"),
		RestoreTo:           mustGetString(cmd, "restore-to"),
	}
	if err := cfg.ValidateRestore(); err != nil {
		slog.Error("validation_error", "error", err.Error())
		return err
	}

	// 2. Setup context & graceful shutdown
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(fmt.Errorf("runRestore exited"))

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	go func() {
		select {
		case sig := <-sigCh:
			slog.Error("received_termination_signal", "signal", sig.String())
			cancel(fmt.Errorf("received OS signal: %v", sig))
		case <-ctx.Done():
		}
	}()

	// 3. Auth & storage
	authCtx, err := oras.Login(ctx, cfg.RegistryHost, cfg.RegistryUsername, cfg.RegistryToken)
	if err != nil {
		slog.Error("registry_login_failed", "error", err.Error())
		return err
	}
	defer authCtx.Cleanup() // guaranteed to run — no os.Exit below this point

	regClient := registry.New(cfg.RegistryHost, authCtx.ConfigDir)

	storeProvider, err := storage.New(ctx, cfg.StorageConfig())
	if err != nil {
		slog.Error("storage_initialization_failed", "error", err.Error())
		return err
	}

	// 4. Build reader modifier chain and run restore pipeline
	var readerMods []pipeline.ReaderModifier
	baseName := cfg.BackupFile
	if strings.HasSuffix(baseName, ".age") {
		readerMods = append(readerMods, pipeline.WithAgeDecryption(cfg.EncryptionPassword))
		baseName = strings.TrimSuffix(baseName, ".age")
	}
	if strings.HasSuffix(baseName, ".tar.gz") {
		readerMods = append(readerMods, pipeline.WithGunzip())
	}

	slog.Info("restore_started", "backup_file", cfg.BackupFile, "restore_to", cfg.RestoreTo)

	if err := pipeline.RunRestore(ctx, regClient, storeProvider, cfg.BackupFile, cfg.RestoreTo, readerMods, cfg.Timeout); err != nil {
		slog.Error("restore_failed", "error", err.Error())
		return err
	}

	slog.Info("restore_completed_successfully", "backup_file", cfg.BackupFile, "restore_to", cfg.RestoreTo)
	return nil
}

func init() {
	ociCmd.AddCommand(restoreCmd)
	restoreCmd.Flags().String("backup-file", "", "Remote path of the backup file in cloud storage (ENV: BACKUP_FILE)")
	restoreCmd.Flags().String("restore-to", "", "Target repository path to restore to, e.g. namespace/repo (ENV: RESTORE_TO)")

}
