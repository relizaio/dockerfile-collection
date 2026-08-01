package storage

import (
	"errors"
	"strings"
	"testing"

	"github.com/aws/smithy-go"
)

// The steady-state path is write-only, so a denied upload is the most likely
// misconfiguration and must say which permission is missing rather than surfacing a bare
// 403 -- an opaque one previously cost a live debugging session.
func TestUploadAccessHint(t *testing.T) {
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
			got := uploadAccessHint("k", tc.err)
			if hinted := strings.Contains(got.Error(), "s3:PutObject"); hinted != tc.wantHint {
				t.Errorf("hint present = %v, want %v (err: %v)", hinted, tc.wantHint, got)
			}
			if !errors.Is(got, tc.err) {
				t.Errorf("underlying error must stay unwrappable, got %v", got)
			}
		})
	}
}

func TestUploadAccessHintPassesThroughSuccess(t *testing.T) {
	if err := uploadAccessHint("k", nil); err != nil {
		t.Errorf("nil error must stay nil, got %v", err)
	}
}
