package pipeline

import (
	"compress/gzip"
	"fmt"
	"io"
)

func wrapGzipWriter(w io.Writer) *gzip.Writer {
	return gzip.NewWriter(w)
}

func wrapGzipReader(r io.Reader) (*gzip.Reader, error) {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}
	return gr, nil
}

// WithGzip returns a WriterModifier that compresses data with gzip.
func WithGzip() WriterModifier {
	return func(w io.WriteCloser) (io.WriteCloser, error) {
		return wrapGzipWriter(w), nil
	}
}

// WithGunzip returns a ReaderModifier that decompresses gzip data.
func WithGunzip() ReaderModifier {
	return func(r io.Reader) (io.Reader, error) {
		return wrapGzipReader(r)
	}
}
