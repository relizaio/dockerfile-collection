package storage

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type s3Provider struct {
	client *s3.Client
	bucket string
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
	return &s3Provider{client: s3.NewFromConfig(awsCfg), bucket: cfg.AWSBucket}, nil
}

func (p *s3Provider) UploadStream(ctx context.Context, remotePath string, reader io.Reader) error {
	tm := transfermanager.New(p.client)
	_, err := tm.UploadObject(ctx, &transfermanager.UploadObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(remotePath),
		Body:   reader,
	})
	if err != nil && ctx.Err() != nil {
		return fmt.Errorf("upload interrupted: %w", err)
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
