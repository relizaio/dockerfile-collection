package storage

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
)

const (
	AzureBlockSize   = 10 * 1024 * 1024
	AzureConcurrency = 3
)

type azureProvider struct {
	client    *azblob.Client
	container string
}

func newAzureProvider(ctx context.Context, cfg *Config) (*azureProvider, error) {
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
	return &azureProvider{client: client, container: cfg.AzureContainer}, nil
}

func (p *azureProvider) UploadStream(ctx context.Context, remotePath string, reader io.Reader) error {
	opts := &azblob.UploadStreamOptions{
		BlockSize:   AzureBlockSize,
		Concurrency: AzureConcurrency,
	}
	_, err := p.client.UploadStream(ctx, p.container, remotePath, reader, opts)
	if err != nil && ctx.Err() != nil {
		return fmt.Errorf("upload interrupted: %w", err)
	}
	return err
}

// mapAzureNotFound maps a GetProperties error to ErrNotFound when it is a definitive
// missing blob (404 / BlobNotFound), so callers can distinguish a confirmed absence
// from a transient/credential error. Any other error is passed through as a generic
// failure. Azure sets the x-ms-error-code header on a HEAD 404, which bloberror.HasCode
// reads off the *azcore.ResponseError.
func mapAzureNotFound(remotePath string, err error) error {
	if bloberror.HasCode(err, bloberror.BlobNotFound) {
		return fmt.Errorf("get properties for %q: %w", remotePath, ErrNotFound)
	}
	return fmt.Errorf("get properties for %q failed: %w", remotePath, err)
}

// Head returns the stored blob's size via GetProperties (no body download).
func (p *azureProvider) Head(ctx context.Context, remotePath string) (*ObjectInfo, error) {
	blobClient := p.client.ServiceClient().NewContainerClient(p.container).NewBlobClient(remotePath)
	props, err := blobClient.GetProperties(ctx, nil)
	if err != nil {
		return nil, mapAzureNotFound(remotePath, err)
	}
	if props.ContentLength == nil {
		return nil, fmt.Errorf("get properties for %q returned no ContentLength", remotePath)
	}
	return &ObjectInfo{Size: *props.ContentLength}, nil
}

func (p *azureProvider) DownloadStream(ctx context.Context, remotePath string, writer io.Writer) error {
	stream, err := p.client.DownloadStream(ctx, p.container, remotePath, nil)
	if err != nil {
		return fmt.Errorf("failed to start Azure download: %w", err)
	}
	// NewRetryReader reconnects mid-stream on a transient drop, so a whole-blob
	// read (e.g. verify-restore) is not killed by the client's per-try timeout.
	body := stream.NewRetryReader(ctx, &azblob.RetryReaderOptions{MaxRetries: MaxRetries})
	defer body.Close()
	_, err = io.Copy(writer, body)
	return err
}
