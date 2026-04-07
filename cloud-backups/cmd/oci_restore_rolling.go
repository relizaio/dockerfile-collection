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

var restoreRollingCmd = &cobra.Command{
	Use:   "restore-rolling",
	Short: "Restore the most recent OCI backup for each repo/month in a rolling window",
	Run: func(cmd *cobra.Command, args []string) {
		if err := runRestoreRolling(cmd); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	},
}

func runRestoreRolling(cmd *cobra.Command) error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	months, _ := cmd.Flags().GetInt("months")
	repos := mustGetStringSlice(cmd, "repos")

	cfg := &config.AppConfig{
		RegistryHost:        viper.GetString("registry-host"),
		RegistryUsername:    viper.GetString("registry-username"),
		RegistryToken:       viper.GetString("registry-token"),
		StorageType:         viper.GetString("backup-storage-type"),
		EncryptionPassword:  viper.GetString("encryption-password"),
		MaxConcurrentJobs:   viper.GetInt("max-concurrent-jobs"),
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
		RestoreNamespace:    mustGetString(cmd, "restore-namespace"),
		RestoreRepos:        repos,
		Months:              months,
		PlainHTTP:           viper.GetBool("plain-http"),
	}

	if raw := mustGetString(cmd, "cutoff-date"); raw != "" {
		t, err := time.Parse("2006-01-02", raw)
		if err != nil {
			slog.Error("validation_error", "error", fmt.Sprintf("invalid --cutoff-date %q: use YYYY-MM-DD format", raw))
			return fmt.Errorf("invalid --cutoff-date: %w", err)
		}
		cfg.CutoffDate = t
	}
	if raw := mustGetString(cmd, "from"); raw != "" {
		t, err := time.Parse("2006-01-02", raw)
		if err != nil {
			slog.Error("validation_error", "error", fmt.Sprintf("invalid --from %q: use YYYY-MM-DD format", raw))
			return fmt.Errorf("invalid --from: %w", err)
		}
		cfg.FromDate = t
	}
	if raw := mustGetString(cmd, "to"); raw != "" {
		t, err := time.Parse("2006-01-02", raw)
		if err != nil {
			slog.Error("validation_error", "error", fmt.Sprintf("invalid --to %q: use YYYY-MM-DD format", raw))
			return fmt.Errorf("invalid --to: %w", err)
		}
		cfg.ToDate = t
	}

	if err := cfg.ValidateRollingRestore(); err != nil {
		slog.Error("validation_error", "error", err.Error())
		return err
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(fmt.Errorf("runRestoreRolling exited"))

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

	authCtx, err := oras.Login(ctx, cfg.RegistryHost, cfg.RegistryUsername, cfg.RegistryToken, cfg.PlainHTTP)
	if err != nil {
		slog.Error("registry_login_failed", "error", err.Error())
		return err
	}
	defer authCtx.Cleanup()

	regClient := registry.New(cfg.RegistryHost, authCtx.ConfigDir, cfg.PlainHTTP)

	storeProvider, err := storage.New(ctx, cfg.StorageConfig())
	if err != nil {
		slog.Error("storage_initialization_failed", "error", err.Error())
		return err
	}

	tracker := stats.New()
	pipelineStart := time.Now()

	slog.Info("rolling_restore_pipeline_started",
		"restore_namespace", cfg.RestoreNamespace,
		"repos", cfg.RestoreRepos,
	)

	orchestrator.RunRollingRestore(ctx, regClient, storeProvider, cfg, tracker)

	stats.PrintSummary("rolling_restore_pipeline_completed", tracker, cfg.StorageType, time.Since(pipelineStart))
	if tracker.GetFailedCount() > 0 {
		return fmt.Errorf("rolling restore completed with failures")
	}
	return nil
}

func init() {
	ociCmd.AddCommand(restoreRollingCmd)

	restoreRollingCmd.Flags().Int("months", 0, "Number of recent months to restore (Mode A; mutually exclusive with --from/--to); defaults to 2 when neither --from/--to nor --months is set")
	restoreRollingCmd.Flags().StringSlice("repos", []string{}, "Repo names to restore (without namespace), e.g. rebom-artifacts,downloadable-artifacts (ENV: REPOS, comma-separated)")
	restoreRollingCmd.Flags().String("restore-namespace", "", "Registry namespace to restore into (ENV: RESTORE_NAMESPACE)")
	restoreRollingCmd.Flags().String("cutoff-date", "", "Anchor date for --months, format YYYY-MM-DD; defaults to today (Mode A)")
	restoreRollingCmd.Flags().String("from", "", "Start of date range, format YYYY-MM-DD (Mode B; mutually exclusive with --months)")
	restoreRollingCmd.Flags().String("to", "", "End of date range, format YYYY-MM-DD (Mode B; mutually exclusive with --months)")
}
