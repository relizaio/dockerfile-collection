package pipeline

import "io"

// WriterModifier wraps an io.WriteCloser with an additional layer (e.g. compression, encryption).
// Implementations must close the returned writer to flush any buffered data.
type WriterModifier func(io.WriteCloser) (io.WriteCloser, error)

// ReaderModifier wraps an io.Reader with an additional layer (e.g. decompression, decryption).
type ReaderModifier func(io.Reader) (io.Reader, error)

// applyWriterModifiers applies each modifier in order, returning the outermost writer
// and a list of intermediate closers that must be closed in reverse order.
func applyWriterModifiers(base io.WriteCloser, mods []WriterModifier) (io.WriteCloser, []io.Closer, error) {
	current := base
	var closers []io.Closer
	for _, mod := range mods {
		next, err := mod(current)
		if err != nil {
			return nil, closers, err
		}
		closers = append(closers, next)
		current = next
	}
	return current, closers, nil
}

// ApplyReaderModifiers applies each modifier in order, returning the outermost reader.
func ApplyReaderModifiers(base io.Reader, mods []ReaderModifier) (io.Reader, error) {
	current := base
	for _, mod := range mods {
		next, err := mod(current)
		if err != nil {
			return nil, err
		}
		current = next
	}
	return current, nil
}
