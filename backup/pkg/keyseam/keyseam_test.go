package keyseam

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	enclave "github.com/jeremyhahn/go-enclave"
)

const testPassphrase = "correct horse battery staple"

func newPassphrase(t *testing.T, s string) enclave.ByteAccess {
	t.Helper()
	h, err := enclave.NewMemory([]byte(s))
	if err != nil {
		t.Fatalf("NewMemory: %v", err)
	}
	return h
}

func newProvider(t *testing.T, s string) *PassphraseProvider {
	t.Helper()
	p, err := NewPassphraseProvider(newPassphrase(t, s), 0)
	if err != nil {
		t.Fatalf("NewPassphraseProvider: %v", err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return p
}

func newDataKey(t *testing.T, p Provider) (*DataKey, []byte) {
	t.Helper()
	k, wrapped, err := NewDataKey(context.Background(), p)
	if err != nil {
		t.Fatalf("NewDataKey: %v", err)
	}
	t.Cleanup(func() {
		if err := k.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return k, wrapped
}

func keyBytes(t *testing.T, h enclave.ByteAccess) []byte {
	t.Helper()
	var out []byte
	if err := h.WithBytes(func(b []byte) error {
		out = append(out, b...)
		return nil
	}); err != nil {
		t.Fatalf("WithBytes: %v", err)
	}
	return out
}

// assertTyped checks err matches want, is an *Error and carries no key material.
func assertTyped(t *testing.T, err, want error, secrets ...[]byte) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("error %v does not match %v", err, want)
	}
	var e *Error
	if !errors.As(err, &e) || e.Op == "" {
		t.Fatalf("error %v is not an *Error with an Op", err)
	}
	msg := err.Error()
	for _, s := range secrets {
		if strings.Contains(msg, string(s)) || strings.Contains(msg, hex.EncodeToString(s)) {
			t.Fatalf("error %q carries key material", msg)
		}
	}
}

func TestWrapUnwrapRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	p := newProvider(t, testPassphrase)
	k, wrapped := newDataKey(t, p)
	if len(wrapped) != passphraseWrapSize {
		t.Fatalf("wrapped key is %d bytes, want %d", len(wrapped), passphraseWrapSize)
	}
	want := keyBytes(t, k.Handle())
	if strings.Contains(string(wrapped), string(want)) {
		t.Fatal("wrapped form contains the data key")
	}

	// A second provider over the same passphrase opens it.
	opened, err := OpenDataKey(ctx, newProvider(t, testPassphrase), wrapped)
	if err != nil {
		t.Fatalf("OpenDataKey: %v", err)
	}
	defer func() {
		if err := opened.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	if eq, err := opened.Handle().EqualTo(want); err != nil || !eq {
		t.Fatalf("unwrapped key differs from the generated key (err %v)", err)
	}

	// Wrapping again draws a new salt and nonce.
	again, err := k.Wrap(ctx, p)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	if string(again[:passphraseHeaderSize]) == string(wrapped[:passphraseHeaderSize]) {
		t.Fatal("rewrap reused the salt")
	}
}

func TestUnwrapWrongPassphrase(t *testing.T) {
	t.Parallel()
	p := newProvider(t, testPassphrase)
	k, wrapped := newDataKey(t, p)
	_, err := newProvider(t, "wrong passphrase").UnwrapKey(context.Background(), wrapped)
	assertTyped(t, err, ErrUnwrap, keyBytes(t, k.Handle()), []byte(testPassphrase))
}

func TestUnwrapTampered(t *testing.T) {
	t.Parallel()
	p := newProvider(t, testPassphrase)
	k, wrapped := newDataKey(t, p)
	secret := keyBytes(t, k.Handle())

	flip := func(i int) []byte {
		w := append([]byte(nil), wrapped...)
		w[i] ^= 0x01
		return w
	}
	iterOff := len(passphraseMagic) + 1
	low := append([]byte(nil), wrapped...)
	low[iterOff], low[iterOff+1], low[iterOff+2], low[iterOff+3] = 0, 0, 0x03, 0xe8 // 1000
	high := append([]byte(nil), wrapped...)
	high[iterOff] = 0xff

	tests := []struct {
		name    string
		wrapped []byte
		want    error
	}{
		{"magic", flip(0), ErrMalformedWrappedKey},
		{"version", flip(len(passphraseMagic)), ErrMalformedWrappedKey},
		{"iterations below minimum", low, ErrMalformedWrappedKey},
		{"iterations above maximum", high, ErrMalformedWrappedKey},
		{"truncated", wrapped[:len(wrapped)-1], ErrMalformedWrappedKey},
		{"extended", append(append([]byte(nil), wrapped...), 0), ErrMalformedWrappedKey},
		{"empty", nil, ErrMalformedWrappedKey},
		{"iterations", flip(iterOff + 3), ErrUnwrap},
		{"salt", flip(passphraseHeaderSize - 1), ErrUnwrap},
		{"nonce", flip(passphraseHeaderSize), ErrUnwrap},
		{"ciphertext", flip(passphraseHeaderSize + 12), ErrUnwrap},
		{"tag", flip(len(wrapped) - 1), ErrUnwrap},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, err := p.UnwrapKey(context.Background(), tt.wrapped)
			if h != nil {
				t.Fatal("tampered wrapped key returned a key")
			}
			assertTyped(t, err, tt.want, secret)
		})
	}
}

func TestDestroyedKeys(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	p := newProvider(t, testPassphrase)

	k, wrapped, err := NewDataKey(ctx, p)
	if err != nil {
		t.Fatalf("NewDataKey: %v", err)
	}
	if err := k.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := k.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	_, err = k.Wrap(ctx, p)
	assertTyped(t, err, ErrKeyDestroyed)
	if err := k.Handle().WithBytes(func([]byte) error { return nil }); !errors.Is(err, enclave.ErrEnclaveDestroyed) {
		t.Fatalf("closed data key is still readable: %v", err)
	}

	closed, err := NewPassphraseProvider(newPassphrase(t, testPassphrase), 0)
	if err != nil {
		t.Fatalf("NewPassphraseProvider: %v", err)
	}
	if err := closed.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err = closed.UnwrapKey(ctx, wrapped)
	assertTyped(t, err, ErrKeyDestroyed, []byte(testPassphrase))
	live, _ := newDataKey(t, p)
	_, err = closed.WrapKey(ctx, live.Handle())
	assertTyped(t, err, ErrKeyDestroyed, []byte(testPassphrase))
}

func TestWrapRejectsBadDataKey(t *testing.T) {
	t.Parallel()
	p := newProvider(t, testPassphrase)
	_, err := p.WrapKey(context.Background(), nil)
	assertTyped(t, err, ErrInvalidKey)
	short := newPassphrase(t, "sixteen byte key")
	defer func() {
		if err := short.Destroy(); err != nil {
			t.Errorf("Destroy: %v", err)
		}
	}()
	_, err = p.WrapKey(context.Background(), short)
	assertTyped(t, err, ErrInvalidKey)
}

func TestContextCancelled(t *testing.T) {
	t.Parallel()
	p := newProvider(t, testPassphrase)
	k, wrapped := newDataKey(t, p)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := p.WrapKey(ctx, k.Handle())
	assertTyped(t, err, context.Canceled)
	_, err = p.UnwrapKey(ctx, wrapped)
	assertTyped(t, err, context.Canceled)
	_, err = p.deriveKEK(ctx, opUnwrap, make([]byte, SaltSize), MinIterations)
	assertTyped(t, err, context.Canceled)
}

func TestNewPassphraseProviderRejects(t *testing.T) {
	t.Parallel()
	_, err := NewPassphraseProvider(nil, 0)
	assertTyped(t, err, ErrInvalidKey)

	h := newPassphrase(t, testPassphrase)
	for _, n := range []int{1, MinIterations - 1, MaxIterations + 1} {
		_, err = NewPassphraseProvider(h, n)
		assertTyped(t, err, ErrIterations)
	}
	if err := h.Destroy(); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
	_, err = NewPassphraseProvider(h, 0)
	assertTyped(t, err, ErrKeyDestroyed)
}

// fakeProvider returns what it is told and remembers the handle it wrapped.
type fakeProvider struct {
	wrapErr   error
	unwrapKey enclave.ByteAccess
	unwrapErr error
	wrapped   enclave.ByteAccess
}

func (f *fakeProvider) WrapKey(_ context.Context, k enclave.ByteAccess) ([]byte, error) {
	f.wrapped = k
	return []byte("wrapped"), f.wrapErr
}

func (f *fakeProvider) UnwrapKey(context.Context, []byte) (enclave.ByteAccess, error) {
	return f.unwrapKey, f.unwrapErr
}

func TestDataKeyWithFakeProvider(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	errFake := errors.New("fake provider failure")

	f := &fakeProvider{wrapErr: errFake}
	if _, _, err := NewDataKey(ctx, f); !errors.Is(err, errFake) {
		t.Fatalf("NewDataKey: %v, want the provider's error", err)
	}
	if f.wrapped == nil || f.wrapped.IsAlive() {
		t.Fatal("NewDataKey left the generated key alive after the provider failed")
	}

	if _, err := OpenDataKey(ctx, &fakeProvider{unwrapErr: errFake}, nil); !errors.Is(err, errFake) {
		t.Fatalf("OpenDataKey: %v, want the provider's error", err)
	}
	_, err := OpenDataKey(ctx, &fakeProvider{}, nil)
	assertTyped(t, err, ErrInvalidKey)
	short := newPassphrase(t, "short")
	_, err = OpenDataKey(ctx, &fakeProvider{unwrapKey: short}, nil)
	assertTyped(t, err, ErrInvalidKey)
	if short.IsAlive() {
		t.Fatal("OpenDataKey left a wrong-size key alive")
	}

	_, _, err = NewDataKey(ctx, nil)
	assertTyped(t, err, ErrNilProvider)
	_, err = OpenDataKey(ctx, nil, nil)
	assertTyped(t, err, ErrNilProvider)
	k, _ := newDataKey(t, newProvider(t, testPassphrase))
	_, err = k.Wrap(ctx, nil)
	assertTyped(t, err, ErrNilProvider)
}

func TestErrorText(t *testing.T) {
	t.Parallel()
	cause := errors.New("cause")
	e := &Error{Op: "op", Err: ErrUnwrap, Cause: cause}
	if got, want := e.Error(), "op: "+ErrUnwrap.Error()+": cause"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
	if !errors.Is(e, cause) || !errors.Is(e, ErrUnwrap) {
		t.Fatal("Error does not unwrap to both Err and Cause")
	}
	if got := (&Error{Op: "op", Err: ErrUnwrap}).Error(); got != "op: "+ErrUnwrap.Error() {
		t.Fatalf("Error() without cause = %q", got)
	}
}
