package storage

import (
	"errors"
	"fmt"
	"testing"

	"github.com/aws/smithy-go"
)

func TestMapS3NotFound(t *testing.T) {
	cases := []struct {
		name         string
		err          error
		wantNotFound bool
	}{
		{"HeadObject 404 NotFound -> ErrNotFound", &smithy.GenericAPIError{Code: "NotFound"}, true},
		{"NoSuchKey -> ErrNotFound", &smithy.GenericAPIError{Code: "NoSuchKey"}, true},
		{"AccessDenied -> generic (NOT NotFound)", &smithy.GenericAPIError{Code: "AccessDenied"}, false},
		{"throttling / plain error -> generic (NOT NotFound)", errors.New("connection reset"), false},
		{"wrapped 404 -> ErrNotFound", fmt.Errorf("op failed: %w", &smithy.GenericAPIError{Code: "NotFound"}), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mapS3NotFound("k", tc.err)
			if errors.Is(got, ErrNotFound) != tc.wantNotFound {
				t.Errorf("errors.Is(ErrNotFound) = %v, want %v (err: %v)", errors.Is(got, ErrNotFound), tc.wantNotFound, got)
			}
		})
	}
}
