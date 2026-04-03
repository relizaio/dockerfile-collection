package config

import (
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/relizaio/cloud-backup/internal/storage"
)

// AppConfig is a strongly-typed, viper-agnostic representation of all CLI/env configuration.
type AppConfig struct {
	// OCI fields
	RegistryHost        string   `mapstructure:"registry-host"`
	RegistryUsername    string   `mapstructure:"registry-username"`
	RegistryToken       string   `mapstructure:"registry-token"`
	MaxConcurrentJobs   int      `mapstructure:"max-concurrent-jobs"`
	RegistryBasePaths   []string `mapstructure:"registry-base-paths"`
	AppendRollingMonths bool     `mapstructure:"append-rolling-months"`

	// PG fields
	PGHost     string `mapstructure:"pg-host"`
	PGPort     string `mapstructure:"pg-port"`
	PGDatabase string `mapstructure:"pg-database"`
	PGUser     string `mapstructure:"pg-user"`

	// Shared fields
	StorageType         string        `mapstructure:"backup-storage-type"`
	EncryptionPassword  string        `mapstructure:"encryption-password"`
	DumpPrefix          string        `mapstructure:"dump-prefix"`
	Timeout             time.Duration `mapstructure:"timeout"`
	AWSBucket           string        `mapstructure:"aws-bucket"`
	AWSRegion           string        `mapstructure:"aws-region"`
	AWSAccessKeyID      string        `mapstructure:"aws-access-key-id"`
	AWSSecretAccessKey  string        `mapstructure:"aws-secret-access-key"`
	AzureStorageAccount string        `mapstructure:"azure-storage-account"`
	AzureTenantID       string        `mapstructure:"azure-tenant-id"`
	AzureClientID       string        `mapstructure:"azure-client-id"`
	AzureClientSecret   string        `mapstructure:"azure-client-secret"`
	AzureContainer      string        `mapstructure:"azure-container"`
	BackupFile          string        `mapstructure:"backup-file"`
	RestoreTo           string        `mapstructure:"restore-to"`
	OutputFile          string        `mapstructure:"output"`
}

// StorageConfig projects the storage-related fields into the storage.Config struct.
func (c *AppConfig) StorageConfig() *storage.Config {
	return &storage.Config{
		Type:                c.StorageType,
		AWSBucket:           c.AWSBucket,
		AWSRegion:           c.AWSRegion,
		AWSAccessKeyID:      c.AWSAccessKeyID,
		AWSSecretAccessKey:  c.AWSSecretAccessKey,
		AzureStorageAccount: c.AzureStorageAccount,
		AzureTenantID:       c.AzureTenantID,
		AzureClientID:       c.AzureClientID,
		AzureClientSecret:   c.AzureClientSecret,
		AzureContainer:      c.AzureContainer,
	}
}

// CleanBasePaths returns registry base paths with whitespace trimmed and blanks removed.
// It also splits elements that contain commas, which occurs when the value arrives
// via the REGISTRY_BASE_PATHS environment variable as a comma-separated string that
// viper.GetStringSlice does not automatically split.
func (c *AppConfig) CleanBasePaths() []string {
	var valid []string
	for _, p := range c.RegistryBasePaths {
		for _, sub := range strings.Split(p, ",") {
			if trimmed := strings.TrimSpace(sub); trimmed != "" {
				valid = append(valid, trimmed)
			}
		}
	}
	return valid
}

// ValidateBackup checks all fields required for the OCI backup command.
func (c *AppConfig) ValidateBackup() error {
	if err := c.validateOCICommon(); err != nil {
		return err
	}
	if len(c.CleanBasePaths()) == 0 {
		return fmt.Errorf("--registry-base-paths / REGISTRY_BASE_PATHS must contain at least one non-empty path")
	}
	return nil
}

// ValidateRestore checks all fields required for the OCI restore command.
func (c *AppConfig) ValidateRestore() error {
	if err := c.validateOCICommon(); err != nil {
		return err
	}
	if c.BackupFile == "" {
		return fmt.Errorf("--backup-file / BACKUP_FILE is required")
	}
	if c.RestoreTo == "" {
		return fmt.Errorf("--restore-to / RESTORE_TO is required")
	}
	return nil
}

// ValidatePGBackup checks all fields required for the PG backup command.
func (c *AppConfig) ValidatePGBackup() error {
	if err := c.validatePGCommon(); err != nil {
		return err
	}
	if _, err := exec.LookPath("pg_dump"); err != nil {
		return fmt.Errorf("pg_dump not found in PATH: %w", err)
	}
	return c.validateStorage()
}

// ValidatePGRestore checks all fields required for the PG restore command.
// If downloadOnly is true, PG connection fields are not required.
func (c *AppConfig) ValidatePGRestore(downloadOnly bool) error {
	if c.BackupFile == "" {
		return fmt.Errorf("--backup-file / BACKUP_FILE is required")
	}
	if downloadOnly {
		if c.OutputFile == "" {
			return fmt.Errorf("--output is required when --download-only is set")
		}
		return c.validateStorage()
	}
	if _, err := exec.LookPath("pg_restore"); err != nil {
		return fmt.Errorf("pg_restore not found in PATH: %w", err)
	}
	if err := c.validatePGCommon(); err != nil {
		return err
	}
	return c.validateStorage()
}

func (c *AppConfig) validateOCICommon() error {
	if c.RegistryHost == "" {
		return fmt.Errorf("--registry-host / REGISTRY_HOST is required")
	}
	if c.RegistryUsername == "" {
		return fmt.Errorf("--registry-username / REGISTRY_USERNAME is required")
	}
	if c.RegistryToken == "" {
		return fmt.Errorf("--registry-token / REGISTRY_TOKEN is required")
	}
	return c.validateStorage()
}

func (c *AppConfig) validatePGCommon() error {
	if c.PGHost == "" {
		return fmt.Errorf("--pg-host / PG_HOST is required")
	}
	if c.PGDatabase == "" {
		return fmt.Errorf("--pg-database / PG_DATABASE is required")
	}
	if c.PGUser == "" {
		return fmt.Errorf("--pg-user / PG_USER is required")
	}
	return nil
}

func (c *AppConfig) validateStorage() error {
	switch c.StorageType {
	case "s3":
		if c.AWSBucket == "" {
			return fmt.Errorf("--aws-bucket / AWS_BUCKET is required for s3 storage")
		}
		if c.AWSRegion == "" {
			return fmt.Errorf("--aws-region / AWS_REGION is required for s3 storage")
		}
		if c.AWSAccessKeyID == "" {
			return fmt.Errorf("--aws-access-key-id / AWS_ACCESS_KEY_ID is required for s3 storage")
		}
		if c.AWSSecretAccessKey == "" {
			return fmt.Errorf("--aws-secret-access-key / AWS_SECRET_ACCESS_KEY is required for s3 storage")
		}
	case "azure":
		if c.AzureStorageAccount == "" {
			return fmt.Errorf("--azure-storage-account / AZURE_STORAGE_ACCOUNT is required for azure storage")
		}
		if c.AzureTenantID == "" {
			return fmt.Errorf("--azure-tenant-id / AZURE_TENANT_ID is required for azure storage")
		}
		if c.AzureClientID == "" {
			return fmt.Errorf("--azure-client-id / AZURE_CLIENT_ID is required for azure storage")
		}
		if c.AzureClientSecret == "" {
			return fmt.Errorf("--azure-client-secret / AZURE_CLIENT_SECRET is required for azure storage")
		}
		if c.AzureContainer == "" {
			return fmt.Errorf("--azure-container / AZURE_CONTAINER is required for azure storage")
		}
	default:
		return fmt.Errorf("unsupported --backup-storage-type / BACKUP_STORAGE_TYPE: %q (must be s3 or azure)", c.StorageType)
	}
	return nil
}
