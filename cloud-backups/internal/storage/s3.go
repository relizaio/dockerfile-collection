package storage

import (
	"context"
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
	// Only ask S3 to verify a SHA-256 per part on real AWS. A custom endpoint
	// (MinIO/Ceph/R2/B2/Wasabi via AWS_ENDPOINT_URL[_S3]) may reject the SHA-256
	// composite/streaming-trailer checksum, which would break EVERY upload mode --
	// so we don't force it there.
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
		// S3 verifies a SHA-256 of every (multipart) part server-side and refuses
		// the object on any mismatch -- a "completed" upload is integrity-checked.
		in.ChecksumAlgorithm = tmtypes.ChecksumAlgorithmSha256
	}
	_, err := tm.UploadObject(ctx, in)
	if err != nil && ctx.Err() != nil {
		return fmt.Errorf("upload interrupted: %w", err)
	}
	return err
}

// Head returns the stored object's size via a HeadObject call (no body download).
func (p *s3Provider) Head(ctx context.Context, remotePath string) (*ObjectInfo, error) {
	out, err := p.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(remotePath),
	})
	if err != nil {
		return nil, fmt.Errorf("head object %q failed: %w", remotePath, err)
	}
	if out.ContentLength == nil {
		return nil, fmt.Errorf("head object %q returned no ContentLength", remotePath)
	}
	return &ObjectInfo{Size: *out.ContentLength}, nil
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
