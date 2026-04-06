package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/relizaio/cloud-backup/internal/config"
	"github.com/relizaio/cloud-backup/internal/pg"
	"github.com/relizaio/cloud-backup/internal/pipeline"
	"github.com/relizaio/cloud-backup/internal/stats"
	"github.com/relizaio/cloud-backup/internal/storage"
)

var pgBackupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Stream a PostgreSQL database dump to cloud storage",
	Run: func(cmd *cobra.Command, args []string) {
		if err := runPGBackup(); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	},
}

func runPGBackup() error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// 1. Build and validate typed config
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
		DumpPrefix:          viper.GetString("dump-prefix"),
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
	}
	if err := cfg.ValidatePGBackup(); err != nil {
		slog.Error("validation_error", "error", err.Error())
		return err
	}

	// 2. Setup context & graceful shutdown
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(fmt.Errorf("runPGBackup exited"))

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

	// 4. Create PG client and preflight check
	pgClient := &pg.Client{
		Host:     cfg.PGHost,
		Port:     cfg.PGPort,
		Database: cfg.PGDatabase,
		User:     cfg.PGUser,
	}
	slog.Info("running_preflight_check", "host", cfg.PGHost, "port", cfg.PGPort)
	if err := pgClient.PreflightCheck(ctx, cfg.PGDatabase); err != nil {
		slog.Error("preflight_check_failed", "error", err.Error())
		return err
	}
	slog.Info("preflight_check_passed")

	// 5. Build writer modifier chain — NO gzip (pg_dump -Fc already compresses)
	nameSuffix := ".dump"
	var writerMods []pipeline.WriterModifier
	if cfg.EncryptionPassword != "" {
		nameSuffix += ".age"
		writerMods = append(writerMods, pipeline.WithAgeEncryption(cfg.EncryptionPassword))
	}

	// 6. Run backup pipeline (single target: the database name)
	tracker := stats.New()
	pipelineStart := time.Now()

	slog.Info("pg_backup_started", "database", cfg.PGDatabase, "storage", cfg.StorageType, "encryption_enabled", cfg.EncryptionPassword != "")
	pipeline.RunWithRetry(ctx, pgClient, storeProvider, cfg.PGDatabase, cfg.DumpPrefix, nameSuffix, writerMods, tracker, cfg.Timeout, false)

	stats.PrintSummary("pg_backup_completed", tracker, cfg.StorageType, time.Since(pipelineStart))
	if tracker.GetFailedCount() > 0 {
		return fmt.Errorf("pg backup failed")
	}
	return nil
}

func init() {
	pgCmd.AddCommand(pgBackupCmd)
}
