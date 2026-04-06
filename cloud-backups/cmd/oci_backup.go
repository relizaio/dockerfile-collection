package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/relizaio/cloud-backup/internal/config"
	"github.com/relizaio/cloud-backup/internal/oras"
	"github.com/relizaio/cloud-backup/internal/orchestrator"
	"github.com/relizaio/cloud-backup/internal/registry"
	"github.com/relizaio/cloud-backup/internal/stats"
	"github.com/relizaio/cloud-backup/internal/storage"
)

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Stream OCI artifacts to cloud storage",
	Run: func(cmd *cobra.Command, args []string) {
		// runBackup owns all defers; os.Exit is only called here, after defers have run.
		if err := runBackup(); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	},
}

func runBackup() error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// 1. Build and validate typed config
	cfg := &config.AppConfig{
		RegistryHost:        viper.GetString("registry-host"),
		RegistryUsername:    viper.GetString("registry-username"),
		RegistryToken:       viper.GetString("registry-token"),
		StorageType:         viper.GetString("backup-storage-type"),
		MaxConcurrentJobs:   viper.GetInt("max-concurrent-jobs"),
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
		RegistryBasePaths:   viper.GetStringSlice("registry-base-paths"),
		AppendRollingMonths: viper.GetBool("append-rolling-months"),
		PlainHTTP:           viper.GetBool("plain-http"),
	}
	if err := cfg.ValidateBackup(); err != nil {
		slog.Error("validation_error", "error", err.Error())
		return err
	}

	// 2. Setup context & graceful shutdown
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(fmt.Errorf("runBackup exited"))

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
	authCtx, err := oras.Login(ctx, cfg.RegistryHost, cfg.RegistryUsername, cfg.RegistryToken, cfg.PlainHTTP)
	if err != nil {
		slog.Error("registry_login_failed", "error", err.Error())
		return err
	}
	defer authCtx.Cleanup() // guaranteed to run — no os.Exit below this point

	regClient := registry.New(cfg.RegistryHost, authCtx.ConfigDir, cfg.PlainHTTP)

	storeProvider, err := storage.New(ctx, cfg.StorageConfig())
	if err != nil {
		slog.Error("storage_initialization_failed", "error", err.Error())
		return err
	}

	// 4. Pre-flight auth check
	basePaths := cfg.CleanBasePaths()
	if len(basePaths) > 0 {
		slog.Info("running_preflight_auth_check", "target", basePaths[0])
		if err := regClient.PreflightCheck(ctx, basePaths[0]); err != nil {
			slog.Error("preflight_check_failed", "error", err.Error())
			return err
		}
		slog.Info("preflight_check_passed")
	}

	// 5. Run backup pipeline
	tracker := stats.New()
	pipelineStart := time.Now()

	manager := &orchestrator.BackupManager{
		Storage:           storeProvider,
		StorageType:       cfg.StorageType,
		Tracker:           tracker,
		Concurrency:       cfg.MaxConcurrentJobs,
		DataSource:        regClient,
		EncPassword:       cfg.EncryptionPassword,
		DumpPrefix:        cfg.DumpPrefix,
		Timeout:           cfg.Timeout,
		DeterministicName: true,
	}
	manager.RunBackups(ctx, basePaths, cfg.AppendRollingMonths)

	// 6. Report result
	stats.PrintSummary("backup_pipeline_completed", tracker, cfg.StorageType, time.Since(pipelineStart))
	if tracker.GetFailedCount() > 0 || (tracker.GetTotal() > 0 && tracker.GetTotal() == tracker.GetSkippedCount()) {
		return fmt.Errorf("backup pipeline completed with failures")
	}
	return nil
}

func init() {
	ociCmd.AddCommand(backupCmd)
	backupCmd.Flags().StringSlice("registry-base-paths", []string{}, "Comma-separated list of target repositories (ENV: REGISTRY_BASE_PATHS)")
	backupCmd.Flags().Bool("append-rolling-months", false, "Append current and previous YYYY-MM to paths (ENV: APPEND_ROLLING_MONTHS)")

	for key, flag := range map[string]string{
		"registry-base-paths":   "registry-base-paths",
		"append-rolling-months": "append-rolling-months",
	} {
		if err := viper.BindPFlag(key, backupCmd.Flags().Lookup(flag)); err != nil {
			panic(fmt.Sprintf("failed to bind flag %q: %v", flag, err))
		}
	}
}
