package cmd

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/relizaio/cloud-backup/internal/config"
	"github.com/relizaio/cloud-backup/internal/pg"
	"github.com/relizaio/cloud-backup/internal/pipeline"
	"github.com/relizaio/cloud-backup/internal/storage"
)

var pgRestoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore a PostgreSQL backup from cloud storage",
	Long: `Restore a PostgreSQL backup from cloud storage.

Default mode: downloads, decrypts, and pipes directly into pg_restore.
With --download-only: downloads and decrypts to a local file for manual pg_restore.`,
	Run: func(cmd *cobra.Command, args []string) {
		downloadOnly, _ := cmd.Flags().GetBool("download-only")
		if err := runPGRestore(cmd, downloadOnly); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	},
}

func runPGRestore(cmd *cobra.Command, downloadOnly bool) error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// Support host:port syntax in --pg-host (net.SplitHostPort handles IPv6 correctly)
	pgHost := viper.GetString("pg-host")
	pgPort := viper.GetString("pg-port")
	if host, port, err := net.SplitHostPort(pgHost); err == nil {
		pgHost = host
		pgPort = port
	}

	cfg := &config.AppConfig{
		PGHost:              pgHost,
		PGPort:              pgPort,
		PGDatabase:          viper.GetString("pg-database"),
		PGUser:              viper.GetString("pg-user"),
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
		OutputFile:          mustGetString(cmd, "output"),
	}
	if err := cfg.ValidatePGRestore(downloadOnly); err != nil {
		slog.Error("validation_error", "error", err.Error())
		return err
	}

	// 2. Setup context & graceful shutdown
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(fmt.Errorf("runPGRestore exited"))

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

	// 3. Init storage
	storeProvider, err := storage.New(ctx, cfg.StorageConfig())
	if err != nil {
		slog.Error("storage_initialization_failed", "error", err.Error())
		return err
	}

	// 4. Build reader modifier chain from file extension (.age → decrypt; .dump has no gunzip)
	var readerMods []pipeline.ReaderModifier
	baseName := cfg.BackupFile
	if strings.HasSuffix(baseName, ".age") {
		readerMods = append(readerMods, pipeline.WithAgeDecryption(cfg.EncryptionPassword))
		baseName = strings.TrimSuffix(baseName, ".age")
	}
	_ = baseName // remaining suffix (.dump) needs no further decompression

	if downloadOnly {
		return runPGDownloadOnly(ctx, storeProvider, cfg, readerMods)
	}
	return runPGFullRestore(ctx, storeProvider, cfg, readerMods)
}

// runPGFullRestore downloads + decrypts + pipes directly into pg_restore.
func runPGFullRestore(ctx context.Context, storeProvider storage.Provider, cfg *config.AppConfig, readerMods []pipeline.ReaderModifier) error {
	pgClient := &pg.Client{
		Host:     cfg.PGHost,
		Port:     cfg.PGPort,
		Database: cfg.PGDatabase,
		User:     cfg.PGUser,
	}

	restoreTarget := cfg.RestoreTo
	if restoreTarget == "" {
		restoreTarget = cfg.PGDatabase
	}

	slog.Info("pg_restore_started", "backup_file", cfg.BackupFile, "restore_to", restoreTarget)

	if err := pipeline.RunRestore(ctx, pgClient, storeProvider, cfg.BackupFile, restoreTarget, readerMods, cfg.Timeout); err != nil {
		slog.Error("pg_restore_failed", "error", err.Error())
		return err
	}

	slog.Info("pg_restore_completed_successfully", "backup_file", cfg.BackupFile, "restore_to", restoreTarget)
	return nil
}

// runPGDownloadOnly downloads + decrypts to a local file for manual pg_restore.
func runPGDownloadOnly(ctx context.Context, storeProvider storage.Provider, cfg *config.AppConfig, readerMods []pipeline.ReaderModifier) error {
	slog.Info("pg_download_started", "backup_file", cfg.BackupFile, "output", cfg.OutputFile)

	outFile, err := os.Create(cfg.OutputFile)
	if err != nil {
		slog.Error("failed_to_create_output_file", "error", err.Error())
		return fmt.Errorf("failed to create output file %q: %w", cfg.OutputFile, err)
	}
	defer outFile.Close()

	// Download → apply reader modifiers → write to local file
	cloudR, cloudW := io.Pipe()
	errChan := make(chan error, 1)

	go func() {
		if dlErr := storeProvider.DownloadStream(ctx, cfg.BackupFile, cloudW); dlErr != nil {
			cloudW.CloseWithError(dlErr)
			errChan <- dlErr
			return
		}
		cloudW.Close()
		errChan <- nil
	}()

	reader, applyErr := pipeline.ApplyReaderModifiers(cloudR, readerMods)
	if applyErr != nil {
		cloudR.Close()
		<-errChan
		return fmt.Errorf("failed to apply reader modifiers: %w", applyErr)
	}

	if _, copyErr := io.Copy(outFile, reader); copyErr != nil {
		cloudR.Close()
		<-errChan
		return fmt.Errorf("failed to write output file: %w", copyErr)
	}

	cloudR.Close()
	if dlErr := <-errChan; dlErr != nil {
		return fmt.Errorf("download failed: %w", dlErr)
	}

	slog.Info("pg_download_completed_successfully", "output", cfg.OutputFile)
	return nil
}

func init() {
	pgCmd.AddCommand(pgRestoreCmd)
	pgRestoreCmd.Flags().String("backup-file", "", "Remote path of the backup file in cloud storage (ENV: BACKUP_FILE)")
	pgRestoreCmd.Flags().String("restore-to", "", "Target database name for pg_restore (ENV: RESTORE_TO)")
	pgRestoreCmd.Flags().Bool("download-only", false, "Download and decrypt to a local file instead of running pg_restore")
	pgRestoreCmd.Flags().String("output", "", "Local output file path when using --download-only (ENV: OUTPUT)")

}
