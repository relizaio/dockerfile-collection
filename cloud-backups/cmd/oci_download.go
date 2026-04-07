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
	"github.com/relizaio/cloud-backup/internal/pipeline"
	"github.com/relizaio/cloud-backup/internal/storage"
)

var ociDownloadCmd = &cobra.Command{
	Use:   "download",
	Short: "Download and decrypt an OCI backup archive from cloud storage to a local file",
	Long: `Download an OCI backup archive from cloud storage to a local file.

Decrypts the archive if it has a .age extension (requires --encryption-password).
The output is a .tar.gz file that can be manually pushed with:
  gunzip backup.tar.gz
  oras restore --input ./backup.tar registry.example.com/namespace/repo`,
	Run: func(cmd *cobra.Command, args []string) {
		if err := runOCIDownload(cmd); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	},
}

func runOCIDownload(cmd *cobra.Command) error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg := &config.AppConfig{
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
		OutputFile:          mustGetString(cmd, "output"),
	}
	if err := cfg.ValidateDownload(); err != nil {
		slog.Error("validation_error", "error", err.Error())
		return err
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(fmt.Errorf("runOCIDownload exited"))

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

	storeProvider, err := storage.New(ctx, cfg.StorageConfig())
	if err != nil {
		slog.Error("storage_initialization_failed", "error", err.Error())
		return err
	}

	var readerMods []pipeline.ReaderModifier
	if strings.HasSuffix(cfg.BackupFile, ".age") {
		readerMods = append(readerMods, pipeline.WithAgeDecryption(cfg.EncryptionPassword))
	}

	return runFileDownload(ctx, storeProvider, cfg, readerMods)
}

func init() {
	ociCmd.AddCommand(ociDownloadCmd)
	ociDownloadCmd.Flags().String("backup-file", "", "Remote backup file name in cloud storage (ENV: BACKUP_FILE)")
	ociDownloadCmd.Flags().String("output", "", "Local output file path (ENV: OUTPUT)")
}
