package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
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

// Have the SDK compute a CRC64 and send it as x-ms-content-crc64; Azure validates it
// server-side and rejects the payload on mismatch. Without it Azure receives NO checksum
// at all, so a "successful" upload means only that bytes arrived, not that they arrived
// intact -- and this tool DROPs the source table once an upload verifies. The S3 path
// opts into the same protection (see newS3Provider/UploadStream). This is not a
// nice-to-have: nothing re-reads the object afterwards, so on Azure this checksum is the
// SOLE integrity check standing between the upload and an irreversible DROP.
//
// Cost: ComputeCRC64 hashes from a buffer rather than in-line, and it is applied per
// BLOCK, so peak memory stays flat in archive size (a 4.5 GB archive costs the same as a
// 100 MB one). It is not free though: io.ReadAll's append growth re-buffers each block at
// roughly 2x, which measured ~+100-200 MiB RSS at BlockSize 10 MiB x Concurrency 3 --
// well inside the cronjob's 1Gi limit, but above a naive BlockSize*Concurrency estimate.
func azureValidation() blob.TransferValidationType {
	return blob.TransferValidationTypeComputeCRC64()
}

func azureUploadStreamOptions() *azblob.UploadStreamOptions {
	return &azblob.UploadStreamOptions{
		BlockSize:               AzureBlockSize,
		Concurrency:             AzureConcurrency,
		TransactionalValidation: azureValidation(),
	}
}

// UploadStream streams the payload to a block blob, checksummed whatever its size.
//
// REMOVABLE LATER: the size split below works around an SDK bug that upstream has since
// fixed -- as of azblob v1.8.1-beta.1 (and main) UploadStreamOptions.getUploadOptions()
// does copy TransactionalValidation. Do NOT remove it before the pinned version actually
// contains that fix; v1.8.0, which we pin, does not.
//
// The size split is NOT an optimization -- it is required for the checksum to exist at
// all. azblob's UploadStream short-circuits to a single-shot Put Blob whenever the whole
// payload fit in block 0 and is smaller than BlockSize (blockblob/chunkwriting.go
// commitBlocks), and that path silently DROPS TransactionalValidation, because
// UploadStreamOptions.getUploadOptions does not copy it. So without this split every
// .sha256 sidecar and every archive under 10 MiB would upload with no server-side
// integrity check whatsoever -- exactly the case where a length-preserving corruption
// would survive the size gate and authorize an irreversible DROP. blockblob.Upload does
// honour the option, so undersized payloads go through it directly.
func (p *azureProvider) UploadStream(ctx context.Context, remotePath string, reader io.Reader) error {
	// Buffer at most one block to find out which path applies. A bytes.Buffer (rather
	// than a fixed AzureBlockSize array) keeps a 65-byte sidecar from allocating 10 MiB.
	var head bytes.Buffer
	_, err := io.CopyN(&head, reader, AzureBlockSize)
	switch {
	case errors.Is(err, io.EOF):
		// Fewer than BlockSize bytes available: the whole payload is in hand.
		return p.uploadSingleBlock(ctx, remotePath, head.Bytes())
	case err != nil:
		return fmt.Errorf("reading first block of %q: %w", remotePath, err)
	}
	_, err = p.client.UploadStream(ctx, p.container, remotePath,
		io.MultiReader(bytes.NewReader(head.Bytes()), reader), azureUploadStreamOptions())
	if err != nil && ctx.Err() != nil {
		return fmt.Errorf("upload interrupted: %w", err)
	}
	return err
}

// uploadSingleBlock puts a payload smaller than one block in a single request, keeping
// the CRC64 that UploadStream's own single-shot path would have discarded.
func (p *azureProvider) uploadSingleBlock(ctx context.Context, remotePath string, data []byte) error {
	blobClient := p.client.ServiceClient().NewContainerClient(p.container).NewBlockBlobClient(remotePath)
	_, err := blobClient.Upload(ctx, streaming.NopCloser(bytes.NewReader(data)), &blockblob.UploadOptions{
		TransactionalValidation: azureValidation(),
	})
	if err != nil && ctx.Err() != nil {
		return fmt.Errorf("upload interrupted: %w", err)
	}
	return err
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
