package config

import (
	"testing"
)

func TestCleanBasePaths(t *testing.T) {
	tests := []struct {
		name  string
		paths []string
		want  []string
	}{
		{"empty list", []string{}, nil},
		{"whitespace only", []string{"  ", "\t", ""}, nil},
		{"mixed valid and blank", []string{"foo", "  ", "bar", ""}, []string{"foo", "bar"}},
		{"trims whitespace", []string{"  foo  ", " bar"}, []string{"foo", "bar"}},
		{"comma in single element", []string{"foo,bar"}, []string{"foo", "bar"}},
		{"comma with whitespace", []string{"foo , bar , baz"}, []string{"foo", "bar", "baz"}},
		{"mixed slice and comma", []string{"foo,bar", "baz"}, []string{"foo", "bar", "baz"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := &AppConfig{RegistryBasePaths: tc.paths}
			got := c.CleanBasePaths()
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("index %d: got %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestStorageConfig(t *testing.T) {
	c := &AppConfig{
		StorageType:         "s3",
		AWSBucket:           "bucket",
		AWSRegion:           "us-east-1",
		AWSAccessKeyID:      "kid",
		AWSSecretAccessKey:  "secret",
		AzureStorageAccount: "acct",
		AzureTenantID:       "tid",
		AzureClientID:       "cid",
		AzureClientSecret:   "csec",
		AzureContainer:      "container",
	}
	sc := c.StorageConfig()
	if sc.Type != c.StorageType {
		t.Errorf("Type: got %q want %q", sc.Type, c.StorageType)
	}
	if sc.AWSBucket != c.AWSBucket {
		t.Errorf("AWSBucket: got %q want %q", sc.AWSBucket, c.AWSBucket)
	}
	if sc.AWSRegion != c.AWSRegion {
		t.Errorf("AWSRegion: got %q want %q", sc.AWSRegion, c.AWSRegion)
	}
	if sc.AWSAccessKeyID != c.AWSAccessKeyID {
		t.Errorf("AWSAccessKeyID mismatch")
	}
	if sc.AWSSecretAccessKey != c.AWSSecretAccessKey {
		t.Errorf("AWSSecretAccessKey mismatch")
	}
	if sc.AzureStorageAccount != c.AzureStorageAccount {
		t.Errorf("AzureStorageAccount mismatch")
	}
	if sc.AzureTenantID != c.AzureTenantID {
		t.Errorf("AzureTenantID mismatch")
	}
	if sc.AzureClientID != c.AzureClientID {
		t.Errorf("AzureClientID mismatch")
	}
	if sc.AzureClientSecret != c.AzureClientSecret {
		t.Errorf("AzureClientSecret mismatch")
	}
	if sc.AzureContainer != c.AzureContainer {
		t.Errorf("AzureContainer mismatch")
	}
}

// --- validateStorage ---

func validS3Config() *AppConfig {
	return &AppConfig{
		StorageType:        "s3",
		AWSBucket:          "bucket",
		AWSRegion:          "us-east-1",
		AWSAccessKeyID:     "kid",
		AWSSecretAccessKey: "secret",
	}
}

func validAzureConfig() *AppConfig {
	return &AppConfig{
		StorageType:         "azure",
		AzureStorageAccount: "acct",
		AzureTenantID:       "tid",
		AzureClientID:       "cid",
		AzureClientSecret:   "csec",
		AzureContainer:      "ctr",
	}
}

func TestValidateStorage_S3_MissingFields(t *testing.T) {
	fields := []struct {
		name  string
		apply func(*AppConfig)
	}{
		{"missing bucket", func(c *AppConfig) { c.AWSBucket = "" }},
		{"missing region", func(c *AppConfig) { c.AWSRegion = "" }},
		{"missing access key id", func(c *AppConfig) { c.AWSAccessKeyID = "" }},
		{"missing secret access key", func(c *AppConfig) { c.AWSSecretAccessKey = "" }},
	}
	for _, tc := range fields {
		t.Run(tc.name, func(t *testing.T) {
			c := validS3Config()
			tc.apply(c)
			if err := c.validateStorage(); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestValidateStorage_S3_Valid(t *testing.T) {
	if err := validS3Config().validateStorage(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateStorage_Azure_MissingFields(t *testing.T) {
	fields := []struct {
		name  string
		apply func(*AppConfig)
	}{
		{"missing account", func(c *AppConfig) { c.AzureStorageAccount = "" }},
		{"missing tenant id", func(c *AppConfig) { c.AzureTenantID = "" }},
		{"missing client id", func(c *AppConfig) { c.AzureClientID = "" }},
		{"missing client secret", func(c *AppConfig) { c.AzureClientSecret = "" }},
		{"missing container", func(c *AppConfig) { c.AzureContainer = "" }},
	}
	for _, tc := range fields {
		t.Run(tc.name, func(t *testing.T) {
			c := validAzureConfig()
			tc.apply(c)
			if err := c.validateStorage(); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestValidateStorage_Azure_Valid(t *testing.T) {
	if err := validAzureConfig().validateStorage(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateStorage_UnknownType(t *testing.T) {
	c := &AppConfig{StorageType: "gcs"}
	if err := c.validateStorage(); err == nil {
		t.Fatal("expected error for unknown storage type")
	}
}

// --- OCI ValidateBackup ---

func validOCIBackupConfig() *AppConfig {
	c := validS3Config()
	c.RegistryHost = "registry.example.com"
	c.RegistryUsername = "user"
	c.RegistryToken = "tok"
	c.RegistryBasePaths = []string{"org/repo"}
	return c
}

func TestValidateBackup_OCI_MissingHost(t *testing.T) {
	c := validOCIBackupConfig()
	c.RegistryHost = ""
	if err := c.ValidateBackup(); err == nil {
		t.Fatal("expected error for missing registry host")
	}
}

func TestValidateBackup_OCI_MissingUsername(t *testing.T) {
	c := validOCIBackupConfig()
	c.RegistryUsername = ""
	if err := c.ValidateBackup(); err == nil {
		t.Fatal("expected error for missing registry username")
	}
}

func TestValidateBackup_OCI_MissingToken(t *testing.T) {
	c := validOCIBackupConfig()
	c.RegistryToken = ""
	if err := c.ValidateBackup(); err == nil {
		t.Fatal("expected error for missing registry token")
	}
}

func TestValidateBackup_OCI_EmptyBasePaths(t *testing.T) {
	c := validOCIBackupConfig()
	c.RegistryBasePaths = []string{"  ", ""}
	if err := c.ValidateBackup(); err == nil {
		t.Fatal("expected error for empty base paths")
	}
}

func TestValidateBackup_OCI_StorageError(t *testing.T) {
	c := validOCIBackupConfig()
	c.AWSBucket = ""
	if err := c.ValidateBackup(); err == nil {
		t.Fatal("expected error for missing storage config")
	}
}

// --- OCI ValidateRestore ---

func validOCIRestoreConfig() *AppConfig {
	c := validS3Config()
	c.RegistryHost = "registry.example.com"
	c.RegistryUsername = "user"
	c.RegistryToken = "tok"
	c.BackupFile = "backup.tar.gz"
	c.RestoreTo = "org/repo"
	return c
}

func TestValidateRestore_OCI_MissingBackupFile(t *testing.T) {
	c := validOCIRestoreConfig()
	c.BackupFile = ""
	if err := c.ValidateRestore(); err == nil {
		t.Fatal("expected error for missing backup file")
	}
}

func TestValidateRestore_OCI_MissingRestoreTo(t *testing.T) {
	c := validOCIRestoreConfig()
	c.RestoreTo = ""
	if err := c.ValidateRestore(); err == nil {
		t.Fatal("expected error for missing restore-to")
	}
}

// --- ValidatePGRestore (field-level, before exec.LookPath) ---

func validPGRestoreConfig() *AppConfig {
	c := validS3Config()
	c.PGHost = "localhost"
	c.PGDatabase = "mydb"
	c.PGUser = "admin"
	c.BackupFile = "backup.dump"
	c.RestoreTo = "mydb"
	return c
}

func TestValidatePGRestore_MissingBackupFile(t *testing.T) {
	c := validPGRestoreConfig()
	c.BackupFile = ""
	if err := c.ValidatePGRestore(false); err == nil {
		t.Fatal("expected error for missing backup file")
	}
}

func TestValidatePGRestore_DownloadOnly_MissingOutput(t *testing.T) {
	c := validPGRestoreConfig()
	c.OutputFile = ""
	if err := c.ValidatePGRestore(true); err == nil {
		t.Fatal("expected error for missing output in download-only mode")
	}
}

func TestValidatePGRestore_MissingRestoreTo(t *testing.T) {
	c := validPGRestoreConfig()
	c.RestoreTo = ""
	// pg_restore may not be in PATH in CI; we expect an error about restore-to OR pg_restore binary
	err := c.ValidatePGRestore(false)
	if err == nil {
		t.Fatal("expected error for missing restore-to")
	}
}

func TestValidatePGRestore_MissingPGHost(t *testing.T) {
	c := validPGRestoreConfig()
	c.PGHost = ""
	// pg_restore may or may not be in PATH; error should mention pg-host
	err := c.ValidatePGRestore(false)
	if err == nil {
		t.Fatal("expected error for missing pg-host")
	}
}

func TestValidatePGRestore_MissingDatabase(t *testing.T) {
	c := validPGRestoreConfig()
	c.PGDatabase = ""
	err := c.ValidatePGRestore(false)
	if err == nil {
		t.Fatal("expected error for missing pg-database")
	}
}

func TestValidatePGRestore_MissingUser(t *testing.T) {
	c := validPGRestoreConfig()
	c.PGUser = ""
	err := c.ValidatePGRestore(false)
	if err == nil {
		t.Fatal("expected error for missing pg-user")
	}
}
