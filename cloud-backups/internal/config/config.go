package config

import (
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/relizaio/cloud-backup/internal/storage"
)

// sqlIdent matches a safe unquoted SQL identifier (schema / table name). The
// audit-rotate mode interpolates these into DDL, so they must be validated to
// prevent injection and malformed statements.
var sqlIdent = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// pgDuration matches a PostgreSQL lock_timeout value (e.g. "5s", "500ms", "0",
// "2min"). It is interpolated into a SET statement, so it must be a safe token.
var pgDuration = regexp.MustCompile(`^[0-9]+\s?(us|ms|s|min|h|d)?$`)

// AppConfig is a strongly-typed, viper-agnostic representation of all CLI/env configuration.
type AppConfig struct {
	// OCI fields
	RegistryHost        string   `mapstructure:"registry-host"`
	RegistryUsername    string   `mapstructure:"registry-username"`
	RegistryToken       string   `mapstructure:"registry-token"`
	MaxConcurrentJobs   int      `mapstructure:"max-concurrent-jobs"`
	RegistryBasePaths   []string `mapstructure:"registry-base-paths"`
	AppendRollingMonths bool     `mapstructure:"append-rolling-months"`
	PlainHTTP           bool     `mapstructure:"plain-http"`

	// PG fields
	PGHost       string `mapstructure:"pg-host"`
	PGPort       string `mapstructure:"pg-port"`
	PGDatabase   string `mapstructure:"pg-database"`
	PGUser       string `mapstructure:"pg-user"`
	ExcludeTable string `mapstructure:"exclude-table"`

	// PG audit-rotate fields
	PGSchema         string `mapstructure:"pg-schema"`
	AuditTable       string `mapstructure:"audit-table"`
	RetentionDays    int    `mapstructure:"audit-retention-days"`
	RotationInterval int    `mapstructure:"rotation-interval-days"`
	LockTimeout      string `mapstructure:"lock-timeout"`
	AllowUnencrypted bool   `mapstructure:"allow-unencrypted"`
	VerifyRestore    bool   `mapstructure:"verify-restore"`
	DrainBacklog     bool   `mapstructure:"drain-backlog"`
	DropInstanceRows bool   `mapstructure:"drop-instance-rows"`

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

	// Rolling restore fields
	RestoreNamespace string
	RestoreRepos     []string
	Months           int
	CutoffDate       time.Time
	FromDate         time.Time
	ToDate           time.Time
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

// ValidateRollingRestore checks all fields required for the oci restore-rolling command.
func (c *AppConfig) ValidateRollingRestore() error {
	if err := c.validateOCICommon(); err != nil {
		return err
	}
	if c.RestoreNamespace == "" {
		return fmt.Errorf("--restore-namespace / RESTORE_NAMESPACE is required")
	}
	if len(c.RestoreRepos) == 0 {
		return fmt.Errorf("--repos must contain at least one repo name")
	}
	rangeSet := !c.FromDate.IsZero() || !c.ToDate.IsZero()
	monthsSet := c.Months > 0 || !c.CutoffDate.IsZero()
	if rangeSet && monthsSet {
		return fmt.Errorf("--from/--to and --months/--cutoff-date are mutually exclusive")
	}
	if rangeSet {
		if c.FromDate.IsZero() || c.ToDate.IsZero() {
			return fmt.Errorf("--from and --to must both be provided together")
		}
		if c.ToDate.Before(c.FromDate) {
			return fmt.Errorf("--to must be on or after --from")
		}
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

// ValidatePGAuditRotate checks all fields required for the pg audit-rotate command.
// maxAuditDays bounds retention / rotation-interval day counts so the "now - N days"
// cutoff can't overflow an int64 time.Duration (nanoseconds overflow at ~106,751 days /
// 292 years). 100 years is far beyond any real retention and safely clear of the limit.
const maxAuditDays = 36500

func (c *AppConfig) ValidatePGAuditRotate() error {
	if _, err := exec.LookPath("psql"); err != nil {
		return fmt.Errorf("psql not found in PATH: %w", err)
	}
	if _, err := exec.LookPath("pg_dump"); err != nil {
		return fmt.Errorf("pg_dump not found in PATH: %w", err)
	}
	if err := c.validatePGCommon(); err != nil {
		return err
	}
	if !sqlIdent.MatchString(c.PGSchema) {
		return fmt.Errorf("--pg-schema / PG_SCHEMA must be a valid SQL identifier, got %q", c.PGSchema)
	}
	if !sqlIdent.MatchString(c.AuditTable) {
		return fmt.Errorf("--audit-table / AUDIT_TABLE must be a valid SQL identifier, got %q", c.AuditTable)
	}
	// The archive name is "<audit>_archive_<16-char utc>_<8 hex>" = <audit>+34 chars.
	// Postgres truncates identifiers at 63 bytes; truncation would desync the Go name
	// from the stored name and wedge drop/recovery. Bound the input.
	if len(c.AuditTable) > 63-34 {
		return fmt.Errorf("--audit-table / AUDIT_TABLE too long (%d chars); max 29 so the archive name stays within Postgres's 63-byte identifier limit", len(c.AuditTable))
	}
	if c.RetentionDays < 0 || c.RetentionDays > maxAuditDays {
		return fmt.Errorf("--audit-retention-days / AUDIT_RETENTION_DAYS must be between 0 and %d, got %d", maxAuditDays, c.RetentionDays)
	}
	// rotation-interval-days decouples ROTATION cadence from the CRON cadence: the cron
	// reconciles every run, but a new archive is cut only when the newest existing one is
	// >= this many days old (0 = OFF = rotate every run, today's behavior). This is what
	// lets a fast cron (per-minute .. daily) keep ~retention/interval coexisting archives
	// instead of one per run.
	if c.RotationInterval < 0 {
		return fmt.Errorf("--rotation-interval-days / ROTATION_INTERVAL_DAYS must be >= 0, got %d", c.RotationInterval)
	}
	// interval > retention is degenerate: the lone archive is dropped at retention age
	// (before the interval elapses), so the "no archive -> rotate" arm fires and the
	// effective interval collapses back to retention. Reject rather than silently mislead.
	// interval <= retention (which is itself capped at maxAuditDays above) also bounds the
	// "now - interval days" cutoff away from int64 time.Duration overflow, so no separate cap.
	if c.RotationInterval > c.RetentionDays {
		return fmt.Errorf("--rotation-interval-days / ROTATION_INTERVAL_DAYS (%d) must be <= --audit-retention-days (%d): a larger interval is degenerate (retention drops the archive before the interval elapses)", c.RotationInterval, c.RetentionDays)
	}
	if !pgDuration.MatchString(c.LockTimeout) {
		return fmt.Errorf("--lock-timeout / LOCK_TIMEOUT must be a PostgreSQL duration like 5s or 500ms, got %q", c.LockTimeout)
	}
	// The archive is written to a permanent-retention bucket where it cannot be
	// deleted; refuse to write it in plaintext unless explicitly allowed.
	if c.EncryptionPassword == "" && !c.AllowUnencrypted {
		return fmt.Errorf("audit data would be written UNENCRYPTED to a permanent bucket; set --encryption-password / ENCRYPTION_PASSWORD, or pass --allow-unencrypted / ALLOW_UNENCRYPTED=true to opt in")
	}
	return c.validateStorage()
}

// ValidateDownload checks all fields required for the oci/pg download commands.
// No registry or PG connection is needed — only storage credentials and file paths.
func (c *AppConfig) ValidateDownload() error {
	if c.BackupFile == "" {
		return fmt.Errorf("--backup-file / BACKUP_FILE is required")
	}
	if c.OutputFile == "" {
		return fmt.Errorf("--output / OUTPUT is required")
	}
	return c.validateStorage()
}

// ValidatePGRestore checks all fields required for the PG restore command.
func (c *AppConfig) ValidatePGRestore() error {
	if c.BackupFile == "" {
		return fmt.Errorf("--backup-file / BACKUP_FILE is required")
	}
	if c.RestoreTo == "" {
		return fmt.Errorf("--restore-to / RESTORE_TO is required")
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
