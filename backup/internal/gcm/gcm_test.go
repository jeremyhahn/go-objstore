package gcm

import (
	"bytes"
	"errors"
	"testing"

	enclave "github.com/jeremyhahn/go-enclave"
)

func newKey(t *testing.T, n int) enclave.ByteAccess {
	t.Helper()
	k, err := enclave.NewMemory(bytes.Repeat([]byte{0x42}, n))
	if err != nil {
		t.Fatalf("NewMemory: %v", err)
	}
	t.Cleanup(func() {
		if err := k.Destroy(); err != nil {
			t.Errorf("Destroy: %v", err)
		}
	})
	return k
}

func TestNewRoundTrip(t *testing.T) {
	aead, err := New(newKey(t, KeySize))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if aead.NonceSize() != 0 || aead.Overhead() != Overhead {
		t.Fatalf("NonceSize %d Overhead %d, want 0 and %d", aead.NonceSize(), aead.Overhead(), Overhead)
	}
	sealed := aead.Seal(nil, nil, []byte("plaintext"), []byte("ad"))
	got, err := aead.Open(nil, nil, sealed, []byte("ad"))
	if err != nil || string(got) != "plaintext" {
		t.Fatalf("Open = %q, %v", got, err)
	}
	if again := aead.Seal(nil, nil, []byte("plaintext"), []byte("ad")); bytes.Equal(again[:12], sealed[:12]) {
		t.Fatal("two seals drew the same nonce")
	}
}

func TestNewRejects(t *testing.T) {
	if _, err := New(nil); !errors.Is(err, ErrKeySize) {
		t.Errorf("nil key: %v, want ErrKeySize", err)
	}
	if _, err := New(newKey(t, 16)); !errors.Is(err, ErrKeySize) {
		t.Errorf("16-byte key: %v, want ErrKeySize", err)
	}
	k, err := enclave.NewMemory(bytes.Repeat([]byte{1}, KeySize))
	if err != nil {
		t.Fatalf("NewMemory: %v", err)
	}
	if err := k.Destroy(); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
	if _, err := New(k); !errors.Is(err, enclave.ErrEnclaveDestroyed) {
		t.Errorf("destroyed key: %v, want ErrEnclaveDestroyed", err)
	}
}
