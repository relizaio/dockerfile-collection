package storage

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
)

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
