package main

import (
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"filippo.io/age"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	AzureBlockSize     = 10 * 1024 * 1024
	AzureConcurrency   = 3
	MaxRetries         = 5
	MaxBackupAttempts  = 3
	BackupTimeout      = 2 * time.Hour
	RetryBackoffBase   = 10 * time.Second
	MaxBackoffDuration = 5 * time.Minute
)

type Config struct {
	PGHost     string
	PGPort     string
	PGDatabase string
	PGUser     string

	DumpPrefix         string
	StorageType        string
	EncryptionPassword string

	AWSBucket          string
	AWSRegion          string
	AWSAccessKeyID     string
	AWSSecretAccessKey string

	AzureStorageAccount string
	AzureTenantID       string
	AzureClientID       string
	AzureClientSecret   string
	AzureContainer      string
}

type StorageProvider struct {
	Type     string
	S3Client *s3.Client
	AzClient *azblob.Client
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(fmt.Errorf("main function exited"))

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

	cfg, err := loadConfig()
	if err != nil {
		slog.Error("configuration_error", "error", err.Error())
		os.Exit(1)
	}

	if err := validateConfig(cfg); err != nil {
		slog.Error("validation_error", "error", err.Error())
		os.Exit(1)
	}

	storage, err := initStorage(ctx, cfg)
	if err != nil {
		slog.Error("storage_initialization_failed", "error", err.Error())
		os.Exit(1)
	}

	if err := runBackupWithRetry(ctx, cfg, storage); err != nil {
		slog.Error("backup_failed", "error", err.Error())
		os.Exit(1)
	}

	slog.Info("backup_completed_successfully")
}

func loadConfig() (*Config, error) {
	pgHost := os.Getenv("PG_HOST")
	pgPort := os.Getenv("PG_PORT")

	// Support host:port in PG_HOST for convenience in local/dev usage
	if idx := strings.LastIndex(pgHost, ":"); idx != -1 {
		pgPort = pgHost[idx+1:]
		pgHost = pgHost[:idx]
	}
	if pgPort == "" {
		pgPort = "5432"
	}

	return &Config{
		PGHost:     pgHost,
		PGPort:     pgPort,
		PGDatabase: os.Getenv("PG_DATABASE"),
		PGUser:     os.Getenv("PG_USER"),

		DumpPrefix:         os.Getenv("DUMP_PREFIX"),
		StorageType:        os.Getenv("BACKUP_STORAGE_TYPE"),
		EncryptionPassword: os.Getenv("ENCRYPTION_PASSWORD"),

		AWSBucket:          os.Getenv("AWS_BUCKET"),
		AWSRegion:          os.Getenv("AWS_REGION"),
		AWSAccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		AWSSecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),

		AzureStorageAccount: os.Getenv("AZURE_STORAGE_ACCOUNT"),
		AzureTenantID:       os.Getenv("AZURE_TENANT_ID"),
		AzureClientID:       os.Getenv("AZURE_CLIENT_ID"),
		AzureClientSecret:   os.Getenv("AZURE_CLIENT_SECRET"),
		AzureContainer:      os.Getenv("AZURE_CONTAINER"),
	}, nil
}

func validateConfig(cfg *Config) error {
	if cfg.PGHost == "" {
		return fmt.Errorf("PG_HOST is required")
	}
	if cfg.PGDatabase == "" {
		return fmt.Errorf("PG_DATABASE is required")
	}
	if cfg.PGUser == "" {
		return fmt.Errorf("PG_USER is required")
	}
	if cfg.DumpPrefix == "" {
		return fmt.Errorf("DUMP_PREFIX is required")
	}
	if cfg.StorageType == "" {
		return fmt.Errorf("BACKUP_STORAGE_TYPE is required")
	}

	switch cfg.StorageType {
	case "s3":
		if cfg.AWSBucket == "" {
			return fmt.Errorf("AWS_BUCKET is required for s3 storage")
		}
		if cfg.AWSRegion == "" {
			return fmt.Errorf("AWS_REGION is required for s3 storage")
		}
		if cfg.AWSAccessKeyID == "" {
			return fmt.Errorf("AWS_ACCESS_KEY_ID is required for s3 storage")
		}
		if cfg.AWSSecretAccessKey == "" {
			return fmt.Errorf("AWS_SECRET_ACCESS_KEY is required for s3 storage")
		}
	case "azure":
		if cfg.AzureStorageAccount == "" {
			return fmt.Errorf("AZURE_STORAGE_ACCOUNT is required for azure storage")
		}
		if cfg.AzureTenantID == "" {
			return fmt.Errorf("AZURE_TENANT_ID is required for azure storage")
		}
		if cfg.AzureClientID == "" {
			return fmt.Errorf("AZURE_CLIENT_ID is required for azure storage")
		}
		if cfg.AzureClientSecret == "" {
			return fmt.Errorf("AZURE_CLIENT_SECRET is required for azure storage")
		}
		if cfg.AzureContainer == "" {
			return fmt.Errorf("AZURE_CONTAINER is required for azure storage")
		}
	default:
		return fmt.Errorf("BACKUP_STORAGE_TYPE must be 's3' or 'azure', got %q", cfg.StorageType)
	}

	if _, err := exec.LookPath("pg_dump"); err != nil {
		return fmt.Errorf("pg_dump not found in PATH: %w", err)
	}

	return nil
}

func initStorage(ctx context.Context, cfg *Config) (*StorageProvider, error) {
	provider := &StorageProvider{Type: cfg.StorageType}

	switch cfg.StorageType {
	case "s3":
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
			awsconfig.WithRegion(cfg.AWSRegion),
			awsconfig.WithRetryer(func() aws.Retryer {
				return retry.AddWithMaxAttempts(retry.NewStandard(), MaxRetries)
			}),
			awsconfig.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, "")),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to load AWS config: %w", err)
		}
		provider.S3Client = s3.NewFromConfig(awsCfg)

	case "azure":
		serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", cfg.AzureStorageAccount)
		credential, err := azidentity.NewClientSecretCredential(cfg.AzureTenantID, cfg.AzureClientID, cfg.AzureClientSecret, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure credential: %w", err)
		}
		opts := &azblob.ClientOptions{
			ClientOptions: policy.ClientOptions{
				Retry: policy.RetryOptions{
					MaxRetries: MaxRetries,
					TryTimeout: time.Minute * 2,
					RetryDelay: time.Second * 5,
				},
			},
		}
		client, err := azblob.NewClient(serviceURL, credential, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure client: %w", err)
		}
		provider.AzClient = client
	}

	return provider, nil
}

func runBackupWithRetry(ctx context.Context, cfg *Config, storage *StorageProvider) error {
	randBytes := make([]byte, 8)
	if _, err := rand.Read(randBytes); err != nil {
		return fmt.Errorf("failed to generate random suffix: %w", err)
	}

	timestamp := time.Now().UTC().Format("2006-01-02-15-04-05")
	remotePath := fmt.Sprintf("%s-%s-%s.dump.gz", cfg.DumpPrefix, timestamp, hex.EncodeToString(randBytes))
	if cfg.EncryptionPassword != "" {
		remotePath += ".age"
	}

	slog.Info("backup_starting",
		"database", cfg.PGDatabase,
		"host", cfg.PGHost,
		"port", cfg.PGPort,
		"storage", cfg.StorageType,
		"encryption_enabled", cfg.EncryptionPassword != "",
		"remote_path", remotePath,
	)

	var lastErr error
	for attempt := 1; attempt <= MaxBackupAttempts; attempt++ {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}

		slog.Info("backup_attempt", "attempt", attempt, "max_attempts", MaxBackupAttempts)

		if err := performStreamBackup(ctx, cfg, storage, remotePath); err == nil {
			slog.Info("backup_successful", "remote_path", remotePath)
			return nil
		} else {

			lastErr = err
			slog.Error("backup_attempt_failed", "attempt", attempt, "error", err.Error())
		}

		if attempt < MaxBackupAttempts {
			backoff := RetryBackoffBase * time.Duration(1<<uint(attempt))
			if backoff > MaxBackoffDuration {
				backoff = MaxBackoffDuration
			}
			slog.Info("backup_retrying", "backoff_seconds", backoff.Seconds())
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return context.Cause(ctx)
			case <-timer.C:
			}
		}
	}

	return fmt.Errorf("all %d backup attempts failed: %w", MaxBackupAttempts, lastErr)
}

func performStreamBackup(parentCtx context.Context, cfg *Config, storage *StorageProvider, remotePath string) error {
	ctx, cancel := context.WithTimeoutCause(parentCtx, BackupTimeout, fmt.Errorf("backup timed out after %s", BackupTimeout))
	defer cancel()

	cloudReader, cloudWriter := io.Pipe()
	defer cloudReader.Close()

	errChan := make(chan error, 1)

	go func() {
		var gErr error

		defer func() {
			if r := recover(); r != nil {
				gErr = fmt.Errorf("CRITICAL: goroutine panicked: %v", r)
			}
			cloudWriter.CloseWithError(gErr)
			errChan <- gErr
		}()

		if ctx.Err() != nil {
			gErr = context.Cause(ctx)
			return
		}

		var currentWriter io.WriteCloser = cloudWriter

		if cfg.EncryptionPassword != "" {
			recipient, ageErr := age.NewScryptRecipient(cfg.EncryptionPassword)
			if ageErr != nil {
				gErr = fmt.Errorf("failed to create age recipient: %w", ageErr)
				return
			}
			ageWriter, ageErr := age.Encrypt(currentWriter, recipient)
			if ageErr != nil {
				gErr = fmt.Errorf("failed to initialize age encryptor: %w", ageErr)
				return
			}
			currentWriter = ageWriter
			defer func() {
				if closeErr := ageWriter.Close(); closeErr != nil && !errors.Is(closeErr, io.ErrClosedPipe) {
					gErr = errors.Join(gErr, fmt.Errorf("failed to close age writer: %w", closeErr))
				}
			}()
		}

		gzipWriter := gzip.NewWriter(currentWriter)
		defer func() {
			if closeErr := gzipWriter.Close(); closeErr != nil && !errors.Is(closeErr, io.ErrClosedPipe) {
				gErr = errors.Join(gErr, fmt.Errorf("failed to close gzip writer: %w", closeErr))
			}
		}()

		pgDumpArgs := []string{
			"-Fc",
			"-U", cfg.PGUser,
			"-h", cfg.PGHost,
			"-p", cfg.PGPort,
			cfg.PGDatabase,
		}
		cmd := exec.CommandContext(ctx, "pg_dump", pgDumpArgs...)
		cmd.Stdout = gzipWriter
		cmd.Env = os.Environ()

		var stderrBuf strings.Builder
		cmd.Stderr = &stderrBuf

		if startErr := cmd.Start(); startErr != nil {
			gErr = fmt.Errorf("failed to start pg_dump: %w", startErr)
			return
		}

		if waitErr := cmd.Wait(); waitErr != nil {
			stderr := strings.TrimSpace(stderrBuf.String())
			gErr = fmt.Errorf("pg_dump failed: %w | stderr: %s", waitErr, stderr)
			return
		}
	}()

	var uploadErr error
	switch storage.Type {
	case "s3":
		uploadErr = uploadToS3(ctx, cfg, storage.S3Client, cloudReader, remotePath)
	case "azure":
		uploadErr = uploadToAzure(ctx, cfg, storage.AzClient, cloudReader, remotePath)
	default:
		uploadErr = fmt.Errorf("unsupported storage type: %s", storage.Type)
	}

	cloudReader.Close()

	if uploadErr != nil {
		cancel()
	}

	goroutineErr := <-errChan

	if uploadErr != nil {
		if goroutineErr != nil && !strings.Contains(goroutineErr.Error(), "stream closed prematurely") &&
			!strings.Contains(goroutineErr.Error(), "broken pipe") {
			return fmt.Errorf("upload failed: %w (root cause: %v)", uploadErr, goroutineErr)
		}
		return fmt.Errorf("upload failed: %w", uploadErr)
	}

	if goroutineErr != nil {
		return fmt.Errorf("backup stream failed: %w", goroutineErr)
	}

	return nil
}

func uploadToS3(ctx context.Context, cfg *Config, client *s3.Client, reader io.Reader, remotePath string) error {
	if client == nil {
		return fmt.Errorf("S3 client is nil")
	}

	tm := transfermanager.New(client)
	_, err := tm.UploadObject(ctx, &transfermanager.UploadObjectInput{
		Bucket: aws.String(cfg.AWSBucket),
		Key:    aws.String(remotePath),
		Body:   reader,
	})
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("upload interrupted by context cancellation (%v): %w", context.Cause(ctx), err)
		}
		return err
	}
	return nil
}

func uploadToAzure(ctx context.Context, cfg *Config, client *azblob.Client, reader io.Reader, remotePath string) error {
	if client == nil {
		return fmt.Errorf("Azure client is nil")
	}

	opts := &azblob.UploadStreamOptions{
		BlockSize:   AzureBlockSize,
		Concurrency: AzureConcurrency,
	}
	_, err := client.UploadStream(ctx, cfg.AzureContainer, remotePath, reader, opts)
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("upload interrupted by context cancellation (%v): %w", context.Cause(ctx), err)
		}
		return err
	}
	return nil
}
