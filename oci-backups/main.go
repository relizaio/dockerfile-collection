package main

import (
	"bytes"
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
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	AzureBlockSize        = 10 * 1024 * 1024
	AzureConcurrency      = 3
	MaxRetries            = 5
	MaxBackupAttempts     = 3
	BackupTimeout         = 2 * time.Hour
	RetryBackoffBase      = 10 * time.Second
	MaxBackoffDuration    = 5 * time.Minute
	MaxFailedPathsTracked = 100
)

type Config struct {
	RegistryHost       string
	RegistryUsername   string
	RegistryToken      string
	RegistryBasePaths  []string
	DumpPrefix         string
	StorageType        string
	EncryptionPassword string
	MaxConcurrentJobs  int

	AWSBucket           string
	AWSRegion           string
	AWSAccessKeyID      string
	AWSSecretAccessKey  string
	AzureStorageAccount string
	AzureTenantID       string
	AzureClientID       string
	AzureClientSecret   string
	AzureContainer      string

	UsePlainHTTP bool
}

type BackupStats struct {
	mu           sync.Mutex
	Total        int64
	Success      int64
	FailureCount int64
	SkippedCount int64
	TotalBytes   int64
	Failed       []string
	Skipped      []string
}

func (s *BackupStats) RecordJob() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Total++
}

func (s *BackupStats) RecordSuccess() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Success++
}

func (s *BackupStats) RecordSkipped(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.SkippedCount++

	if len(s.Skipped) < MaxFailedPathsTracked {
		s.Skipped = append(s.Skipped, path)
	}
}

func (s *BackupStats) RecordFailure(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.FailureCount++

	if len(s.Failed) < MaxFailedPathsTracked {
		s.Failed = append(s.Failed, path)
	}
}

func (s *BackupStats) AddBytes(b int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.TotalBytes += b
}

func (s *BackupStats) GetTotal() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Total
}

func (s *BackupStats) GetSuccess() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Success
}

func (s *BackupStats) GetSkippedCount() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.SkippedCount
}

func (s *BackupStats) GetFailedCount() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.FailureCount
}

func (s *BackupStats) GetTotalBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.TotalBytes
}

func (s *BackupStats) GetFailedSample() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.Failed)
}

func (s *BackupStats) GetSkippedSample() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.Skipped)
}

type StorageProvider struct {
	Type     string
	S3Client *s3.Client
	AzClient *azblob.Client
}

type tailBuffer struct {
	buf []byte
	max int
	mu  sync.Mutex
}

func (w *tailBuffer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.buf = append(w.buf, p...)
	if len(w.buf) > w.max {
		shift := len(w.buf) - w.max
		copy(w.buf, w.buf[shift:])
		w.buf = w.buf[:w.max]
	}
	return len(p), nil
}

func (w *tailBuffer) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return string(w.buf)
}

type byteCounter struct {
	io.Reader
	bytesRead atomic.Int64
}

func (bc *byteCounter) Read(p []byte) (int, error) {
	n, err := bc.Reader.Read(p)
	bc.bytesRead.Add(int64(n))
	return n, err
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(fmt.Errorf("main function exited naturally"))

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	go func() {
		select {
		case sig := <-sigCh:
			slog.Error("received_termination_signal", "msg", "Canceling in-flight backups due to OS signal", "signal", sig.String())
			cancel(fmt.Errorf("received OS signal: %v", sig))
		case <-ctx.Done():
		}
	}()

	config, err := loadConfig()
	if err != nil {
		slog.Error("configuration_error", "error", err.Error())
		os.Exit(1)
	}

	if err := validateConfig(config); err != nil {
		slog.Error("validation_error", "error", err.Error())
		os.Exit(1)
	}

	if err := performOrasLogin(ctx, config); err != nil {
		slog.Error("registry_login_failed", "error", err.Error())
		os.Exit(1)
	}

	storage, err := initStorage(ctx, config)
	if err != nil {
		slog.Error("storage_initialization_failed", "error", err.Error())
		os.Exit(1)
	}

	stats := &BackupStats{}
	currentMonth := time.Now().UTC().Format("2006-01")
	previousMonth := time.Now().UTC().AddDate(0, -1, 0).Format("2006-01")

	slog.Info("backup_pipeline_initialized",
		"current_month", currentMonth,
		"previous_month", previousMonth,
		"concurrency_limit", config.MaxConcurrentJobs,
	)

	sem := make(chan struct{}, config.MaxConcurrentJobs)
	var wg sync.WaitGroup

	pipelineStart := time.Now()

	for _, basePath := range config.RegistryBasePaths {
		tasks := []struct {
			path       string
			backupName string
		}{
			{
				path:       fmt.Sprintf("%s-%s", basePath, currentMonth),
				backupName: fmt.Sprintf("%s-%s-%s", config.DumpPrefix, basePath, currentMonth),
			},
			{
				path:       fmt.Sprintf("%s-%s", basePath, previousMonth),
				backupName: fmt.Sprintf("%s-%s-%s", config.DumpPrefix, basePath, previousMonth),
			},
		}

		for _, task := range tasks {
			wg.Add(1)

			go func() {
				defer wg.Done()

				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					stats.RecordJob()
					stats.RecordFailure(task.path)
					return
				}

				defer func() { <-sem }()

				executeBackupWithRetry(ctx, config, storage, task.path, task.backupName, stats)
			}()
		}
	}

	wg.Wait()

	// Did the entire pipeline process zero valid repositories?
	allSkipped := stats.GetTotal() > 0 && stats.GetSkippedCount() == stats.GetTotal()

	printSummary(stats, config, time.Since(pipelineStart), allSkipped)

	if stats.GetFailedCount() > 0 || allSkipped {
		os.Exit(1)
	}
	os.Exit(0)
}

func performOrasLogin(ctx context.Context, config *Config) error {
	slog.Info("authenticating_with_registry", "host", config.RegistryHost)

	loginArgs := []string{"login", config.RegistryHost,
		"--username", config.RegistryUsername,
		"--password-stdin",
	}
	if config.UsePlainHTTP {
		loginArgs = append(loginArgs, "--plain-http")
	}
	cmd := exec.CommandContext(ctx, "oras", loginArgs...)

	cmd.Env = append(os.Environ(), "DOCKER_CONFIG=/tmp/docker")
	cmd.Stdin = strings.NewReader(config.RegistryToken)

	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("oras login failed: %w\nLogs: %s", err, strings.TrimSpace(stderrBuf.String()))
	}

	slog.Info("registry_authentication_successful")
	return nil
}

func initStorage(ctx context.Context, config *Config) (*StorageProvider, error) {
	provider := &StorageProvider{Type: config.StorageType}

	if config.StorageType == "s3" {
		cfg, err := awsconfig.LoadDefaultConfig(ctx,
			awsconfig.WithRegion(config.AWSRegion),
			awsconfig.WithRetryer(func() aws.Retryer {
				return retry.AddWithMaxAttempts(retry.NewStandard(), MaxRetries)
			}),
			awsconfig.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(config.AWSAccessKeyID, config.AWSSecretAccessKey, "")))
		if err != nil {
			return nil, fmt.Errorf("failed to load AWS config: %w", err)
		}
		provider.S3Client = s3.NewFromConfig(cfg)
	} else if config.StorageType == "azure" {
		serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", config.AzureStorageAccount)
		credential, err := azidentity.NewClientSecretCredential(config.AzureTenantID, config.AzureClientID, config.AzureClientSecret, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure credential: %w", err)
		}

		opts := &azblob.ClientOptions{
			ClientOptions: policy.ClientOptions{
				Retry: policy.RetryOptions{MaxRetries: MaxRetries, TryTimeout: time.Minute * 2, RetryDelay: time.Second * 5},
			},
		}
		client, err := azblob.NewClient(serviceURL, credential, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure client: %w", err)
		}
		provider.AzClient = client
	} else {
		return nil, fmt.Errorf("unsupported storage type: %s", config.StorageType)
	}
	return provider, nil
}

func executeBackupWithRetry(parentCtx context.Context, config *Config, storage *StorageProvider, registryPath, backupName string, stats *BackupStats) {
	stats.RecordJob()
	startTimer := time.Now()

	jobHandled := false
	defer func() {
		if !jobHandled {
			stats.RecordFailure(registryPath)
		}
	}()

	for attempt := 1; attempt <= MaxBackupAttempts; attempt++ {
		if parentCtx.Err() != nil {
			slog.Warn("backup_aborted", "registry_path", registryPath, "reason", context.Cause(parentCtx).Error())
			return
		}

		slog.Info("backup_started", "registry_path", registryPath, "attempt", attempt, "max_attempts", MaxBackupAttempts)

		bytesUploaded, err := performStreamBackup(parentCtx, config, storage, registryPath, backupName)
		if err == nil {
			duration := time.Since(startTimer).Round(time.Second)
			slog.Info("backup_successful",
				"registry_path", registryPath,
				"duration", duration.String(),
				"size_bytes", bytesUploaded,
				"size_human", formatBytes(bytesUploaded))

			jobHandled = true
			stats.RecordSuccess()
			stats.AddBytes(bytesUploaded)
			return
		}

		// GRACEFUL SKIP: If the repository doesn't exist, log it, record it, and exit immediately. No retries.
		if strings.Contains(err.Error(), "repository name not known to registry") {
			slog.Warn("repository_not_found_skipping", "registry_path", registryPath, "msg", "No artifacts exist for this repository/period.")
			jobHandled = true
			stats.RecordSkipped(registryPath)
			return
		}

		slog.Error("backup_attempt_failed", "registry_path", registryPath, "attempt", attempt, "error", err.Error())
		if attempt < MaxBackupAttempts {
			backoff := RetryBackoffBase * time.Duration(1<<uint(attempt))
			if backoff > MaxBackoffDuration {
				backoff = MaxBackoffDuration
			}

			timer := time.NewTimer(backoff)
			select {
			case <-parentCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}

	slog.Error("backup_exhausted", "registry_path", registryPath, "msg", "all retry attempts failed")
}

func performStreamBackup(parentCtx context.Context, config *Config, storage *StorageProvider, registryPath, backupName string) (int64, error) {
	ctx, cancel := context.WithTimeoutCause(parentCtx, BackupTimeout, fmt.Errorf("backup timed out after %s", BackupTimeout))
	defer cancel()

	fullPath := fmt.Sprintf("%s/%s", config.RegistryHost, registryPath)
	timestamp := time.Now().UTC().Format("2006-01-02-15-04-05")
	randBytes := make([]byte, 8)
	if _, err := rand.Read(randBytes); err != nil {
		return 0, fmt.Errorf("failed to generate random suffix: %w", err)
	}

	osReader, osWriter, err := os.Pipe()
	if err != nil {
		return 0, fmt.Errorf("failed to create OS pipe: %w", err)
	}

	virtualTarPath := filepath.Join(os.TempDir(), fmt.Sprintf("stream-%s.tar", hex.EncodeToString(randBytes)))
	if err := os.Symlink("/proc/self/fd/3", virtualTarPath); err != nil {
		osReader.Close()
		osWriter.Close()
		return 0, fmt.Errorf("failed to create fd symlink: %w", err)
	}
	defer os.Remove(virtualTarPath)

	remotePath := fmt.Sprintf("%s-%s-%s.tar.gz", backupName, timestamp, hex.EncodeToString(randBytes))
	if config.EncryptionPassword != "" {
		remotePath += ".age"
	}

	cloudReader, cloudWriter := io.Pipe()
	defer cloudReader.Close()

	counter := &byteCounter{Reader: cloudReader}
	errChan := make(chan error, 1)

	go func() {
		var gErr error

		defer func() {
			if r := recover(); r != nil {
				gErr = fmt.Errorf("CRITICAL: goroutine panicked: %v", r)
			}
			cloudWriter.CloseWithError(gErr)
			osReader.Close()
			errChan <- gErr
		}()

		if ctx.Err() != nil {
			gErr = context.Cause(ctx)
			return
		}

		backupArgs := []string{"backup", fullPath, "--output", virtualTarPath}
		if config.UsePlainHTTP {
			backupArgs = append(backupArgs, "--plain-http")
		}
		cmd := exec.CommandContext(ctx, "oras", backupArgs...)

		cmd.ExtraFiles = []*os.File{osWriter}
		cmd.Env = append(os.Environ(), "DOCKER_CONFIG=/tmp/docker")

		stderrBuf := &tailBuffer{max: 8192}
		cmd.Stdout = stderrBuf
		cmd.Stderr = stderrBuf

		cmdStarted := false
		if startErr := cmd.Start(); startErr != nil {
			gErr = fmt.Errorf("failed to start oras command: %w", startErr)
			return
		}
		cmdStarted = true

		osWriter.Close()

		defer func() {
			if cmdStarted {
				if gErr != nil && cmd.Process != nil {
					if sigErr := cmd.Process.Signal(syscall.SIGTERM); sigErr != nil && !errors.Is(sigErr, os.ErrProcessDone) {
						slog.Warn("failed_to_signal_process", "error", sigErr.Error())
					}
				}
				if waitErr := cmd.Wait(); waitErr != nil {
					waitWrap := fmt.Errorf("oras backup command failed: %w | ORAS Logs: %s", waitErr, strings.TrimSpace(stderrBuf.String()))
					gErr = errors.Join(gErr, waitWrap)
				}
			}
		}()

		var currentWriter io.WriteCloser = cloudWriter

		if config.EncryptionPassword != "" {
			recipient, ageErr := age.NewScryptRecipient(config.EncryptionPassword)
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

		if ctx.Err() != nil {
			gErr = context.Cause(ctx)
			return
		}

		if _, copyErr := io.Copy(gzipWriter, osReader); copyErr != nil {
			if errors.Is(copyErr, io.ErrClosedPipe) {
				gErr = fmt.Errorf("stream closed prematurely by main thread")
			} else {
				gErr = fmt.Errorf("failed during stream copy: %w", copyErr)
			}
			return
		}
	}()

	var uploadErr error
	if storage.Type == "s3" {
		uploadErr = uploadToS3Stream(ctx, config, storage.S3Client, counter, remotePath)
	} else if storage.Type == "azure" {
		uploadErr = uploadToAzureStream(ctx, config, storage.AzClient, counter, remotePath)
	} else {
		uploadErr = fmt.Errorf("unsupported storage type: %s", storage.Type)
	}

	cloudReader.Close()

	if uploadErr != nil {
		cancel()
	}

	goroutineErr := <-errChan

	if uploadErr != nil {
		if goroutineErr != nil && !strings.Contains(goroutineErr.Error(), "stream closed prematurely") {
			return 0, fmt.Errorf("upload failed: %w (root cause: %v)", uploadErr, goroutineErr)
		}
		return 0, fmt.Errorf("upload failed: %w", uploadErr)
	}

	if goroutineErr != nil {
		return 0, fmt.Errorf("backup stream failed: %w", goroutineErr)
	}

	return counter.bytesRead.Load(), nil
}

func uploadToS3Stream(ctx context.Context, config *Config, client *s3.Client, reader io.Reader, remotePath string) error {
	if client == nil {
		return fmt.Errorf("S3 client is nil")
	}

	tm := transfermanager.New(client)
	_, err := tm.UploadObject(ctx, &transfermanager.UploadObjectInput{
		Bucket: aws.String(config.AWSBucket),
		Key:    aws.String(remotePath),
		Body:   reader,
	})

	if err != nil && ctx.Err() != nil {
		return fmt.Errorf("upload interrupted by context cancellation (%v): %w", context.Cause(ctx), err)
	}
	return err
}

func uploadToAzureStream(ctx context.Context, config *Config, client *azblob.Client, reader io.Reader, remotePath string) error {
	if client == nil {
		return fmt.Errorf("Azure client is nil")
	}

	opts := &azblob.UploadStreamOptions{
		BlockSize:   AzureBlockSize,
		Concurrency: AzureConcurrency,
	}
	_, err := client.UploadStream(ctx, config.AzureContainer, remotePath, reader, opts)

	if err != nil && ctx.Err() != nil {
		return fmt.Errorf("upload interrupted by context cancellation (%v): %w", context.Cause(ctx), err)
	}
	return err
}

func loadConfig() (*Config, error) {
	config := &Config{
		RegistryHost:        os.Getenv("REGISTRY_HOST"),
		RegistryUsername:    os.Getenv("REGISTRY_USERNAME"),
		RegistryToken:       os.Getenv("REGISTRY_TOKEN"),
		DumpPrefix:          os.Getenv("DUMP_PREFIX"),
		StorageType:         os.Getenv("BACKUP_STORAGE_TYPE"),
		EncryptionPassword:  os.Getenv("ENCRYPTION_PASSWORD"),
		AWSBucket:           os.Getenv("AWS_BUCKET"),
		AWSRegion:           os.Getenv("AWS_REGION"),
		AWSAccessKeyID:      os.Getenv("AWS_ACCESS_KEY_ID"),
		AWSSecretAccessKey:  os.Getenv("AWS_SECRET_ACCESS_KEY"),
		AzureStorageAccount: os.Getenv("AZURE_STORAGE_ACCOUNT"),
		AzureTenantID:       os.Getenv("AZURE_TENANT_ID"),
		AzureClientID:       os.Getenv("AZURE_CLIENT_ID"),
		AzureClientSecret:   os.Getenv("AZURE_CLIENT_SECRET"),
		AzureContainer:      os.Getenv("AZURE_CONTAINER"),
	}

	concurrencyStr := os.Getenv("MAX_CONCURRENT_JOBS")
	if concurrencyStr != "" {
		if val, err := strconv.Atoi(concurrencyStr); err == nil && val > 0 {
			config.MaxConcurrentJobs = val
		} else {
			return nil, fmt.Errorf("MAX_CONCURRENT_JOBS must be a positive integer")
		}
	} else {
		config.MaxConcurrentJobs = 3
	}

	rawPaths := os.Getenv("REGISTRY_BASE_PATHS")
	if rawPaths != "" {
		config.RegistryBasePaths = strings.Split(rawPaths, ",")
	}

	config.UsePlainHTTP = strings.EqualFold(os.Getenv("USE_PLAIN_HTTP"), "true")

	return config, nil
}

func validateConfig(config *Config) error {
	if _, err := exec.LookPath("oras"); err != nil {
		return fmt.Errorf("oras command not found in PATH: %w", err)
	}

	if config.RegistryHost == "" {
		return fmt.Errorf("REGISTRY_HOST is required")
	}
	if config.RegistryUsername == "" {
		return fmt.Errorf("REGISTRY_USERNAME is required")
	}
	if config.RegistryToken == "" {
		return fmt.Errorf("REGISTRY_TOKEN is required")
	}
	if config.DumpPrefix == "" {
		return fmt.Errorf("DUMP_PREFIX is required")
	}
	if config.StorageType == "" {
		return fmt.Errorf("BACKUP_STORAGE_TYPE is required")
	}

	for i, p := range config.RegistryBasePaths {
		config.RegistryBasePaths[i] = strings.TrimSpace(p)
	}

	config.RegistryBasePaths = slices.DeleteFunc(config.RegistryBasePaths, func(p string) bool {
		return p == ""
	})

	if len(config.RegistryBasePaths) == 0 {
		return fmt.Errorf("REGISTRY_BASE_PATHS is required and must contain valid paths")
	}

	if config.StorageType == "s3" {
		if config.AWSBucket == "" {
			return fmt.Errorf("AWS_BUCKET is required")
		}
		if config.AWSRegion == "" {
			return fmt.Errorf("AWS_REGION is required (e.g., us-east-1, eu-west-1)")
		}
		if config.AWSAccessKeyID == "" {
			return fmt.Errorf("AWS_ACCESS_KEY_ID is required")
		}
		if config.AWSSecretAccessKey == "" {
			return fmt.Errorf("AWS_SECRET_ACCESS_KEY is required")
		}
	} else if config.StorageType == "azure" {
		if config.AzureStorageAccount == "" {
			return fmt.Errorf("AZURE_STORAGE_ACCOUNT is required")
		}
		if config.AzureTenantID == "" {
			return fmt.Errorf("AZURE_TENANT_ID is required")
		}
		if config.AzureClientID == "" {
			return fmt.Errorf("AZURE_CLIENT_ID is required")
		}
		if config.AzureClientSecret == "" {
			return fmt.Errorf("AZURE_CLIENT_SECRET is required")
		}
		if config.AzureContainer == "" {
			return fmt.Errorf("AZURE_CONTAINER is required")
		}
	} else {
		return fmt.Errorf("BACKUP_STORAGE_TYPE must be 's3' or 'azure'")
	}
	return nil
}

func printSummary(stats *BackupStats, config *Config, duration time.Duration, allSkipped bool) {
	failedCount := stats.GetFailedCount()
	skippedCount := stats.GetSkippedCount()
	totalBytes := stats.GetTotalBytes()

	summary := map[string]any{
		"event":               "backup_pipeline_completed",
		"total_targeted":      stats.GetTotal(),
		"success_count":       stats.GetSuccess(),
		"failed_count":        failedCount,
		"skipped_count":       skippedCount,
		"total_duration_secs": duration.Seconds(),
		"total_bytes_raw":     totalBytes,
		"total_bytes_human":   formatBytes(totalBytes),
		"storage_destination": config.StorageType,
		"concurrency_limit":   config.MaxConcurrentJobs,
		"encryption_enabled":  config.EncryptionPassword != "",
	}

	if skippedCount > 0 {
		summary["skipped_paths_sample"] = stats.GetSkippedSample()
	}

	if failedCount > 0 {
		summary["failed_paths_sample"] = stats.GetFailedSample()
	}

	if allSkipped {
		slog.Error("pipeline_failed_all_repos_missing", "summary", summary, "msg", "CRITICAL: No repositories were found on the registry to backup.")
	} else if failedCount > 0 {
		slog.Error("pipeline_completed_with_failures", "summary", summary)
	} else if skippedCount > 0 {
		slog.Info("pipeline_completed_with_skips", "summary", summary)
	} else {
		slog.Info("pipeline_completed_successfully", "summary", summary)
	}
}
