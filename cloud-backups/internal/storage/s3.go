package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	tmtypes "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type s3Provider struct {
	client      *s3.Client
	bucket      string
	useChecksum bool
}

func newS3Provider(ctx context.Context, cfg *Config) (*s3Provider, error) {
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
	// Only force an explicit checksum algorithm on real AWS. A custom endpoint
	// (MinIO/Ceph/R2/B2/Wasabi via AWS_ENDPOINT_URL[_S3]) may reject a forced
	// checksum, which would break EVERY upload mode -- too high a price, so those
	// keep the SDK's own default (CRC32, which for a multipart upload is COMPOSITE).
	// CONSEQUENCE: the assembled-object verification below is real-AWS only; on an
	// S3-compatible endpoint a multipart upload is still per-part checksummed but the
	// assembly itself is not verified. MinIO was measured to accept CRC32C +
	// FULL_OBJECT, so this could be relaxed per-endpoint later if a customer needs it.
	customEndpoint := os.Getenv("AWS_ENDPOINT_URL_S3") != "" || os.Getenv("AWS_ENDPOINT_URL") != ""
	return &s3Provider{client: s3.NewFromConfig(awsCfg), bucket: cfg.AWSBucket, useChecksum: !customEndpoint}, nil
}

func (p *s3Provider) UploadStream(ctx context.Context, remotePath string, reader io.Reader) error {
	tm := transfermanager.New(p.client)
	in := &transfermanager.UploadObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(remotePath),
		Body:   reader,
	}
	if p.useChecksum {
		// FULL_OBJECT (not the default COMPOSITE) is what makes S3 verify the ASSEMBLED
		// object server-side. CompleteMultipartUpload validates the parts you LIST, not
		// that you listed them all: measured, listing only part 1 of 2 uploaded parts is
		// accepted and yields a silently half-length object. A composite checksum is a
		// hash OF the part hashes and cannot detect that; a full-object checksum is a
		// hash of the assembled bytes and does (an under-listed completion is rejected
		// with XAmzContentChecksumMismatch). That matters because nothing re-reads the
		// object afterwards -- this upload is the only integrity check before the DROP.
		//
		// CRC32C rather than SHA-256 because SHA-256 does not support FULL_OBJECT, and
		// transfermanager does not expose CRC64NVME. The whole-object SHA-256 the tool
		// computes itself still goes into the .sha256 sidecar for --verify-restore, so
		// cryptographic-strength verification remains available on the audit path.
		in.ChecksumAlgorithm = tmtypes.ChecksumAlgorithmCrc32c
		in.ChecksumType = tmtypes.ChecksumTypeFullObject
	}
	_, err := tm.UploadObject(ctx, in)
	if err != nil && ctx.Err() != nil {
		return fmt.Errorf("upload interrupted: %w", err)
	}
	return uploadAccessHint(remotePath, err)
}

// uploadAccessHint annotates a denial with the permission the caller is missing. The
// steady-state path needs only write access, so an unannotated 403 here is the most
// likely misconfiguration by far -- and an opaque one previously cost a live debugging
// session.
func uploadAccessHint(remotePath string, err error) error {
	if err == nil {
		return nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "AccessDenied", "Forbidden", "AllAccessDisabled":
			return fmt.Errorf("upload of %q denied -- the bucket credentials need s3:PutObject on the objects (arn:aws:s3:::<bucket>/<prefix>-*) and s3:AbortMultipartUpload for interrupted multipart uploads: %w", remotePath, err)
		}
	}
	return err
}

func (p *s3Provider) DownloadStream(ctx context.Context, remotePath string, writer io.Writer) error {
	result, err := p.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(remotePath),
	})
	if err != nil {
		return fmt.Errorf("failed to start S3 download: %w", err)
	}
	defer result.Body.Close()
	_, err = io.Copy(writer, result.Body)
	return err
}
