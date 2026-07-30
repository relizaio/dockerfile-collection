package storage

import (
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

func listObj(key string, size int64) types.Object {
	return types.Object{Key: aws.String(key), Size: aws.Int64(size)}
}

func TestStatFromListing(t *testing.T) {
	const base = "prefix-audit_archive_20260701t030500z_abc12345.dump"
	const key = base + ".age"
	cases := []struct {
		name         string
		lookup       string
		contents     []types.Object
		wantSize     int64
		wantNotFound bool
		wantErr      bool
	}{
		{
			name:     "exact key -> size",
			contents: []types.Object{listObj(key, 4711)},
			wantSize: 4711,
		},
		{
			// Directory buckets (and any unsorted S3-compatible gateway) may return the
			// sidecar first; scanning must still find the dump rather than call it absent.
			name:     "exact key NOT first -> still found",
			contents: []types.Object{listObj(key+".sha256", 65), listObj(key, 4711)},
			wantSize: 4711,
		},
		{
			// The sidecar shares the dump's key as a prefix, so a listing containing only
			// it means the dump itself is gone -- that must not read as a hit.
			name:         "only the .sha256 sidecar -> absence",
			contents:     []types.Object{listObj(key+".sha256", 65)},
			wantNotFound: true,
		},
		{
			// Looking up the UNencrypted key while an .age pair exists (--encryption-password
			// toggled between runs): both keys carry the lookup as a prefix, neither IS it.
			name:         "only longer .age-suffixed keys -> absence",
			lookup:       base,
			contents:     []types.Object{listObj(key, 4711), listObj(key+".sha256", 65)},
			wantNotFound: true,
		},
		{
			name:         "empty page -> absence",
			contents:     nil,
			wantNotFound: true,
		},
		{
			name:         "nil key -> absence, never a panic",
			contents:     []types.Object{{Size: aws.Int64(1)}},
			wantNotFound: true,
		},
		{
			// An exact key with no size is an unusable answer, NOT an absence: reporting
			// ErrNotFound here would tell the caller "no backup" about an object that
			// exists, which on the recovery path means a needless re-dump.
			name:     "exact key without Size -> generic error, not absence",
			contents: []types.Object{{Key: aws.String(key)}},
			wantErr:  true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lookup := tc.lookup
			if lookup == "" {
				lookup = key
			}
			info, err := statFromListing(lookup, tc.contents)
			if got := errors.Is(err, ErrNotFound); got != tc.wantNotFound {
				t.Fatalf("errors.Is(ErrNotFound) = %v, want %v (err: %v)", got, tc.wantNotFound, err)
			}
			if tc.wantNotFound || tc.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got info %+v", info)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if info.Size != tc.wantSize {
				t.Errorf("Size = %d, want %d", info.Size, tc.wantSize)
			}
		})
	}
}

func TestListAccessHint(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		wantHint bool
	}{
		{"AccessDenied names the missing permission", &smithy.GenericAPIError{Code: "AccessDenied"}, true},
		{"Forbidden names the missing permission", &smithy.GenericAPIError{Code: "Forbidden"}, true},
		{"NoSuchBucket is not a permission problem", &smithy.GenericAPIError{Code: "NoSuchBucket"}, false},
		{"transport error is not a permission problem", errors.New("connection reset"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := listAccessHint("k", tc.err)
			if hinted := strings.Contains(got.Error(), "s3:ListBucket"); hinted != tc.wantHint {
				t.Errorf("hint present = %v, want %v (err: %v)", hinted, tc.wantHint, got)
			}
			if !errors.Is(got, tc.err) {
				t.Errorf("underlying error must stay unwrappable, got %v", got)
			}
			// Absence is never inferred from a FAILED call, only from a successful listing.
			if errors.Is(got, ErrNotFound) {
				t.Errorf("a failed listing must not report ErrNotFound: %v", got)
			}
		})
	}
}
