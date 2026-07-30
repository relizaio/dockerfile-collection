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
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

const (
	// statListMaxKeys bounds each listing page used to look a single key up. At most
	// four keys can share a dump key as their prefix (the dump, its .sha256 sidecar,
	// and the same pair with an .age suffix if --encryption-password was toggled), so
	// one page always suffices in practice; statListMaxPages is a safety net for a
	// server that pads pages with entries we filter out.
	statListMaxKeys  = 10
	statListMaxPages = 8
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

// statFromListing scans a listing page (taken with Prefix set to the full key) for the
// EXACT key. It deliberately does not assume any ordering: AWS documents that general
// purpose buckets list lexicographically but directory buckets do not, and an
// S3-compatible gateway need not sort either -- so picking contents[0] could mistake the
// "<key>.sha256" sidecar for the dump and report a phantom absence. Not-in-this-page is
// reported as ErrNotFound; the caller decides whether that is definitive by looking at
// whether the listing was truncated.
func statFromListing(remotePath string, contents []types.Object) (*ObjectInfo, error) {
	for _, o := range contents {
		if o.Key == nil || *o.Key != remotePath {
			continue
		}
		// An exact key with no size is an unusable answer, not an absence: reporting
		// ErrNotFound would tell the caller "not backed up" about an object that exists.
		if o.Size == nil {
			return nil, fmt.Errorf("list objects for %q returned no Size", remotePath)
		}
		return &ObjectInfo{Size: *o.Size}, nil
	}
	return nil, fmt.Errorf("list objects for %q: %w", remotePath, ErrNotFound)
}

// listAccessHint annotates a denial with the permission the caller is missing. This
// lookup is the first bucket call every run makes, so an unannotated 403 here is the
// single most likely misconfiguration -- and it previously surfaced as an opaque error
// that cost a live debugging session.
func listAccessHint(remotePath string, err error) error {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "AccessDenied", "Forbidden", "AllAccessDisabled":
			return fmt.Errorf("list objects for %q denied -- the bucket credentials need s3:ListBucket on the BUCKET arn (arn:aws:s3:::<bucket>, no /*): %w", remotePath, err)
		}
	}
	return fmt.Errorf("list objects for %q failed: %w", remotePath, err)
}

// Head returns the stored object's size without reading its body. It deliberately uses
// ListObjectsV2 (IAM: s3:ListBucket) instead of HeadObject: there is no s3:HeadObject
// action -- HeadObject is authorized by s3:GetObject, which ALSO grants reading every
// archive's contents. The steady-state rotate/backup/drop path only ever asks "does this
// key exist, and how big is it", so it must not require content-read on the permanent
// bucket; GetObject stays confined to the opt-in --verify-restore path (DownloadStream /
// readSidecar).
//
// It also makes absence unambiguous: HeadObject returns 403 AccessDenied rather than 404
// for a MISSING key when the caller lacks s3:ListBucket, and a 403 cannot safely be read
// as "not backed up" -- so on a least-privilege policy the self-healing
// "no backup -> re-dump instead of drop" path was unreachable.
//
// Absence is only reported once a listing says so without being truncated (an empty but
// truncated page is legal, e.g. when the page's keys are all delete markers). This
// assumes list-after-write consistency, which AWS has guaranteed since Dec 2020 and
// MinIO/Ceph/R2 also document; on a store without it, an upload could momentarily look
// absent (harmless: a re-dump, never a drop -- see backupIsDroppable).
func (p *s3Provider) Head(ctx context.Context, remotePath string) (*ObjectInfo, error) {
	var token *string
	for page := 0; page < statListMaxPages; page++ {
		out, err := p.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(p.bucket),
			Prefix:            aws.String(remotePath),
			MaxKeys:           aws.Int32(statListMaxKeys),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, listAccessHint(remotePath, err)
		}
		info, err := statFromListing(remotePath, out.Contents)
		if !errors.Is(err, ErrNotFound) {
			return info, err // a hit, or an unusable answer that must not read as absence
		}
		if !aws.ToBool(out.IsTruncated) {
			return nil, err
		}
		token = out.NextContinuationToken
	}
	// Never claim absence we did not confirm: an unconfirmed answer must stay ambiguous
	// so callers neither re-dump blindly nor drop.
	return nil, fmt.Errorf("list objects for %q: no definitive answer after %d pages", remotePath, statListMaxPages)
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
