package storage

import (
	"context"
	"fmt"
	"io"
)

const MaxRetries = 5

// Provider is the interface all cloud storage backends must implement.
type Provider interface {
	UploadStream(ctx context.Context, remotePath string, reader io.Reader) error
	DownloadStream(ctx context.Context, remotePath string, writer io.Writer) error
}

// Config holds credentials passed down from the CLI.
type Config struct {
	Type                string // "s3" or "azure"
	AWSBucket           string
	AWSRegion           string
	AWSAccessKeyID      string
	AWSSecretAccessKey  string
	AzureStorageAccount string
	AzureTenantID       string
	AzureClientID       string
	AzureClientSecret   string
	AzureContainer      string
}

// New is the factory that returns the correct Provider for the given config.
func New(ctx context.Context, cfg *Config) (Provider, error) {
	switch cfg.Type {
	case "s3":
		return newS3Provider(ctx, cfg)
	case "azure":
		return newAzureProvider(ctx, cfg)
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", cfg.Type)
	}
}
