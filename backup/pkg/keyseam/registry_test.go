package keyseam

import (
	"context"
	"errors"
	"slices"
	"testing"

	enclave "github.com/jeremyhahn/go-enclave"
)

func TestRegistry(t *testing.T) {
	if !slices.Contains(Providers(), PassphraseName) {
		t.Fatalf("Providers() = %v, want %q", Providers(), PassphraseName)
	}

	h, err := enclave.NewMemory([]byte("registry passphrase"))
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProvider(PassphraseName, ProviderConfig{Secret: h})
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	defer func() { _ = p.Close() }()
	dk, wrapped, err := NewDataKey(context.Background(), p)
	if err != nil {
		t.Fatalf("NewDataKey: %v", err)
	}
	_ = dk.Close()
	if len(wrapped) == 0 {
		t.Fatal("empty wrapped key")
	}

	_, err = NewProvider("xkms", ProviderConfig{})
	var e *Error
	if !errors.As(err, &e) || !errors.Is(err, ErrUnknownProvider) {
		t.Fatalf("unknown provider: %v", err)
	}
	if _, err := NewProvider(PassphraseName, ProviderConfig{}); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("no secret: %v", err)
	}

	if err := Register(PassphraseName, func(ProviderConfig) (ProviderCloser, error) { return nil, nil }); !errors.Is(err, ErrProviderRegistered) {
		t.Fatalf("duplicate: %v", err)
	}
	if err := Register("", nil); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("empty: %v", err)
	}
	called := false
	if err := Register("test-registry", func(ProviderConfig) (ProviderCloser, error) {
		called = true
		return nil, ErrUnwrap
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := NewProvider("test-registry", ProviderConfig{}); !called || !errors.Is(err, ErrUnwrap) {
		t.Fatalf("registered factory: called %v, %v", called, err)
	}
}
