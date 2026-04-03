package pipeline

import (
	"bytes"
	"io"
	"testing"
)

const testPassword = "test-password-for-unit-tests"

func TestAgeRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping age round-trip: scrypt is intentionally slow")
	}
	original := []byte("secret payload that must survive encrypt/decrypt round-trip")

	var encrypted bytes.Buffer
	base := &pipeWriteCloser{Writer: &encrypted}
	encWriter, err := wrapEncryptWriter(base, testPassword)
	if err != nil {
		t.Fatalf("wrapEncryptWriter: %v", err)
	}
	if _, err := encWriter.Write(original); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := encWriter.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	decReader, err := wrapDecryptReader(&encrypted, testPassword)
	if err != nil {
		t.Fatalf("wrapDecryptReader: %v", err)
	}
	got, err := io.ReadAll(decReader)
	if err != nil {
		t.Fatalf("read decrypted: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Errorf("round-trip mismatch: got %q want %q", got, original)
	}
}

func TestWithAgeEncryption_WriterModifier(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping age modifier test: scrypt is intentionally slow")
	}
	original := []byte("modifier age encrypt test")

	var encrypted bytes.Buffer
	base := &pipeWriteCloser{Writer: &encrypted}
	encMod := WithAgeEncryption(testPassword)
	wc, err := encMod(base)
	if err != nil {
		t.Fatalf("WithAgeEncryption modifier: %v", err)
	}
	if _, err := wc.Write(original); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := wc.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	decMod := WithAgeDecryption(testPassword)
	r, err := decMod(&encrypted)
	if err != nil {
		t.Fatalf("WithAgeDecryption modifier: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Errorf("got %q want %q", got, original)
	}
}

func TestAgeDecrypt_WrongPassword(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping age wrong-password test: scrypt is intentionally slow")
	}
	original := []byte("payload")
	var encrypted bytes.Buffer
	base := &pipeWriteCloser{Writer: &encrypted}
	enc, err := wrapEncryptWriter(base, testPassword)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	enc.Write(original)
	enc.Close()

	_, err = wrapDecryptReader(&encrypted, "wrong-password")
	if err == nil {
		t.Fatal("expected decryption error with wrong password, got nil")
	}
}
