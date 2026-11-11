package local

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// keyedEncrypter binds its ciphertext to its key id, so decrypting with any
// other key fails the way a real cipher would.
type keyedEncrypter struct{ id string }

func (k *keyedEncrypter) prefix() []byte { return []byte("ENC[" + k.id + "]:") }

func (k *keyedEncrypter) Encrypt(_ context.Context, r io.Reader) (io.ReadCloser, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(append(k.prefix(), b...))), nil
}

func (k *keyedEncrypter) Decrypt(_ context.Context, r io.Reader) (io.ReadCloser, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if !bytes.HasPrefix(b, k.prefix()) {
		return nil, fmt.Errorf("ciphertext not sealed with key %q", k.id)
	}
	return io.NopCloser(bytes.NewReader(bytes.TrimPrefix(b, k.prefix()))), nil
}

func (k *keyedEncrypter) Algorithm() string { return "TEST-KEYED" }
func (k *keyedEncrypter) KeyID() string     { return k.id }

// rotatingFactory is an at-rest key store whose default key can rotate.
type rotatingFactory struct {
	def  string
	keys map[string]common.Encrypter
}

func newRotatingFactory(def string, ids ...string) *rotatingFactory {
	f := &rotatingFactory{def: def, keys: map[string]common.Encrypter{}}
	for _, id := range ids {
		f.keys[id] = &keyedEncrypter{id: id}
	}
	return f
}

func (f *rotatingFactory) GetEncrypter(id string) (common.Encrypter, error) {
	if id == "" {
		id = f.def
	}
	if e, ok := f.keys[id]; ok {
		return e, nil
	}
	return nil, errors.New("unknown key id")
}
func (f *rotatingFactory) DefaultKeyID() string { return f.def }
func (f *rotatingFactory) Close() error         { return nil }

func newAtRestLocal(t *testing.T, f common.EncrypterFactory) *Local {
	t.Helper()
	l := New().(*Local)
	if err := l.Configure(map[string]string{"path": t.TempDir()}, noCreds); err != nil {
		t.Fatalf("configure: %v", err)
	}
	l.SetAtRestEncrypterFactory(f)
	return l
}

func readAll(t *testing.T, l *Local, key string) (string, error) {
	t.Helper()
	r, err := l.GetWithContext(context.Background(), key)
	if err != nil {
		return "", err
	}
	defer func() { _ = r.Close() }()
	b, err := io.ReadAll(r)
	return string(b), err
}

// TestAtRestReadUsesRecordedKeyID: an object written under key A still
// reads after the default rotates to key B (#243).
func TestAtRestReadUsesRecordedKeyID(t *testing.T) {
	f := newRotatingFactory("A", "A", "B")
	l := newAtRestLocal(t, f)
	if err := l.Put("old.txt", strings.NewReader("under A")); err != nil {
		t.Fatalf("put: %v", err)
	}

	f.def = "B"
	if err := l.Put("new.txt", strings.NewReader("under B")); err != nil {
		t.Fatalf("put: %v", err)
	}

	for key, want := range map[string]string{"old.txt": "under A", "new.txt": "under B"} {
		got, err := readAll(t, l, key)
		if err != nil {
			t.Fatalf("get %s after rotation: %v", key, err)
		}
		if got != want {
			t.Fatalf("get %s = %q, want %q", key, got, want)
		}
	}
}

// TestAtRestReadUnknownKeyIDFails: a recorded key id the factory does not
// know fails the read instead of falling back to the default key.
func TestAtRestReadUnknownKeyIDFails(t *testing.T) {
	f := newRotatingFactory("A", "A")
	l := newAtRestLocal(t, f)
	if err := l.Put("obj.txt", strings.NewReader("data")); err != nil {
		t.Fatalf("put: %v", err)
	}
	md, err := l.loadMetadata("obj.txt")
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	md.Custom["at_rest_encryption_key_id"] = "gone"
	if err := l.saveMetadata("obj.txt", md); err != nil {
		t.Fatalf("save metadata: %v", err)
	}

	_, err = readAll(t, l, "obj.txt")
	if err == nil || !strings.Contains(err.Error(), `"gone"`) {
		t.Fatalf("get with unknown key id: err = %v, want failure naming the id", err)
	}
}

// TestAtRestReadNoKeyIDUsesDefault: an object whose metadata records no key
// id (written before ids were recorded) decrypts with the default key.
func TestAtRestReadNoKeyIDUsesDefault(t *testing.T) {
	f := newRotatingFactory("A", "A")
	l := newAtRestLocal(t, f)
	if err := l.Put("obj.txt", strings.NewReader("data")); err != nil {
		t.Fatalf("put: %v", err)
	}
	md, err := l.loadMetadata("obj.txt")
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	delete(md.Custom, "at_rest_encryption_key_id")
	if err := l.saveMetadata("obj.txt", md); err != nil {
		t.Fatalf("save metadata: %v", err)
	}
	if got, err := readAll(t, l, "obj.txt"); err != nil || got != "data" {
		t.Fatalf("get = %q, %v; want %q", got, err, "data")
	}
}

// TestAtRestMultipartPartsSurviveRotation: parts staged under key A are
// decrypted with key A when the upload completes after a rotation to B.
func TestAtRestMultipartPartsSurviveRotation(t *testing.T) {
	ctx := context.Background()
	f := newRotatingFactory("A", "A", "B")
	l := newAtRestLocal(t, f)

	id, err := l.InitiateMultipartUpload(ctx, "mp.txt", nil)
	if err != nil {
		t.Fatalf("initiate: %v", err)
	}
	one := strings.Repeat("1", 5<<20)
	p1, err := l.UploadPart(ctx, "mp.txt", id, 1, strings.NewReader(one), int64(len(one)))
	if err != nil {
		t.Fatalf("upload part 1: %v", err)
	}
	f.def = "B"
	p2, err := l.UploadPart(ctx, "mp.txt", id, 2, strings.NewReader("part-two"), 8)
	if err != nil {
		t.Fatalf("upload part 2: %v", err)
	}
	parts := []common.CompletedPart{{PartNumber: 1, ETag: p1.ETag}, {PartNumber: 2, ETag: p2.ETag}}
	if err := l.CompleteMultipartUpload(ctx, "mp.txt", id, parts); err != nil {
		t.Fatalf("complete after rotation: %v", err)
	}
	if got, err := readAll(t, l, "mp.txt"); err != nil || got != one+"part-two" {
		t.Fatalf("get: len %d, err %v; want len %d", len(got), err, len(one)+8)
	}
	md, err := l.loadMetadata("mp.txt")
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	if md.Custom["at_rest_encryption_key_id"] != "B" {
		t.Fatalf("completed object key id = %q, want B", md.Custom["at_rest_encryption_key_id"])
	}
}
