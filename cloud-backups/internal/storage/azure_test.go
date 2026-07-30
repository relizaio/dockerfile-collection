package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

func TestAzureUploadStreamOptions(t *testing.T) {
	opts := azureUploadStreamOptions()
	if opts.TransactionalValidation == nil {
		t.Error("TransactionalValidation is nil: blocks would be staged with no x-ms-content-crc64, so Azure cannot reject a corrupted block")
	}
	if opts.BlockSize != AzureBlockSize {
		t.Errorf("BlockSize = %d, want %d", opts.BlockSize, AzureBlockSize)
	}
	if opts.Concurrency != AzureConcurrency {
		t.Errorf("Concurrency = %d, want %d", opts.Concurrency, AzureConcurrency)
	}
}

// Every upload must carry a server-verifiable checksum REGARDLESS of size, because a
// verified upload is what authorizes an irreversible DROP. This asserts it on the wire
// rather than on the options struct: setting TransactionalValidation is necessary but NOT
// sufficient, since azblob's UploadStream silently drops it on the single-shot Put Blob
// path it takes for payloads below BlockSize. Without the size split in UploadStream this
// test fails for the small cases while the options-struct test above still passes.
func TestAzureUploadStreamChecksumsEverySize(t *testing.T) {
	cases := []struct {
		name      string
		size      int
		wantParts int // staged blocks; 0 => single-shot Put Blob
	}{
		{"sha256 sidecar sized", 65, 0},
		{"just under one block", AzureBlockSize - 1, 0},
		{"exactly one block", AzureBlockSize, 1},
		{"spans two blocks", AzureBlockSize + 1024, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var (
				mu       sync.Mutex
				bodies   int
				unsigned []string
			)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				comp := r.URL.Query().Get("comp")
				if comp == "block" || comp == "" { // a staged block, or a single-shot Put Blob
					mu.Lock()
					bodies++
					if r.Header.Get("x-ms-content-crc64") == "" {
						unsigned = append(unsigned, fmt.Sprintf("comp=%q len=%s", comp, r.Header.Get("Content-Length")))
					}
					mu.Unlock()
				}
				w.Header().Set("ETag", `"0x1"`)
				w.Header().Set("Last-Modified", "Wed, 30 Jul 2026 00:00:00 GMT")
				w.WriteHeader(http.StatusCreated)
			}))
			defer srv.Close()

			client, err := azblob.NewClientWithNoCredential(srv.URL, nil)
			if err != nil {
				t.Fatalf("client: %v", err)
			}
			p := &azureProvider{client: client, container: "archives"}
			// A pipe, not a file: the real caller streams pg_dump | age, so the source is
			// not seekable and its length is unknown up front.
			pr, pw := io.Pipe()
			go func() {
				pw.Write(bytes.Repeat([]byte("a"), tc.size))
				pw.Close()
			}()
			if err := p.UploadStream(context.Background(), "audit_archive.dump.age", pr); err != nil {
				t.Fatalf("UploadStream: %v", err)
			}

			mu.Lock()
			defer mu.Unlock()
			if len(unsigned) > 0 {
				t.Errorf("%d of %d payload request(s) carried NO x-ms-content-crc64: %v", len(unsigned), bodies, unsigned)
			}
			wantBodies := tc.wantParts
			if wantBodies == 0 {
				wantBodies = 1 // one single-shot Put Blob
			}
			if bodies != wantBodies {
				t.Errorf("payload requests = %d, want %d", bodies, wantBodies)
			}
		})
	}
}

func TestMapAzureNotFound(t *testing.T) {
	cases := []struct {
		name         string
		err          error
		wantNotFound bool
	}{
		{"GetProperties 404 BlobNotFound -> ErrNotFound", &azcore.ResponseError{ErrorCode: "BlobNotFound", StatusCode: 404}, true},
		{"AuthenticationFailed -> generic (NOT NotFound)", &azcore.ResponseError{ErrorCode: "AuthenticationFailed", StatusCode: 403}, false},
		{"ContainerNotFound -> generic (NOT BlobNotFound)", &azcore.ResponseError{ErrorCode: "ContainerNotFound", StatusCode: 404}, false},
		{"plain error / timeout -> generic", errors.New("dial tcp: i/o timeout"), false},
		{"wrapped BlobNotFound -> ErrNotFound", fmt.Errorf("op failed: %w", &azcore.ResponseError{ErrorCode: "BlobNotFound", StatusCode: 404}), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mapAzureNotFound("k", tc.err)
			if errors.Is(got, ErrNotFound) != tc.wantNotFound {
				t.Errorf("errors.Is(ErrNotFound) = %v, want %v (err: %v)", errors.Is(got, ErrNotFound), tc.wantNotFound, got)
			}
		})
	}
}
