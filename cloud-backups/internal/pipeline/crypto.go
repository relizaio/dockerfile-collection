package pipeline

import (
	"fmt"
	"io"

	"filippo.io/age"
)

func wrapEncryptWriter(w io.WriteCloser, password string) (io.WriteCloser, error) {
	recipient, err := age.NewScryptRecipient(password)
	if err != nil {
		return nil, fmt.Errorf("failed to create age recipient: %w", err)
	}
	aw, err := age.Encrypt(w, recipient)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize age encryption: %w", err)
	}
	return aw, nil
}

func wrapDecryptReader(r io.Reader, password string) (io.Reader, error) {
	identity, err := age.NewScryptIdentity(password)
	if err != nil {
		return nil, fmt.Errorf("failed to create age identity: %w", err)
	}
	dr, err := age.Decrypt(r, identity)
	if err != nil {
		return nil, fmt.Errorf("age decryption failed: %w", err)
	}
	return dr, nil
}

// WithAgeEncryption returns a WriterModifier that encrypts data with age scrypt.
func WithAgeEncryption(password string) WriterModifier {
	return func(w io.WriteCloser) (io.WriteCloser, error) {
		return wrapEncryptWriter(w, password)
	}
}

// WithAgeDecryption returns a ReaderModifier that decrypts age-encrypted data.
func WithAgeDecryption(password string) ReaderModifier {
	return func(r io.Reader) (io.Reader, error) {
		return wrapDecryptReader(r, password)
	}
}
