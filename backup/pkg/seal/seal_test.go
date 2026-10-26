package seal

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	enclave "github.com/jeremyhahn/go-enclave"
)

func newDataKey(t testing.TB, fill byte) enclave.ByteAccess {
	t.Helper()
	h, err := enclave.NewMemory(bytes.Repeat([]byte{fill}, DataKeySize))
	if err != nil {
		t.Fatalf("NewMemory: %v", err)
	}
	t.Cleanup(func() {
		if err := h.Destroy(); err != nil {
			t.Errorf("Destroy: %v", err)
		}
	})
	return h
}

func newKeys(t testing.TB, fill byte) *Keys {
	t.Helper()
	k, err := DeriveKeys(newDataKey(t, fill))
	if err != nil {
		t.Fatalf("DeriveKeys: %v", err)
	}
	t.Cleanup(func() {
		if err := k.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return k
}

func readKey(t *testing.T, h enclave.ByteAccess) []byte {
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

func assertTyped(t *testing.T, err, want error) {
	t.Helper()
	var e *Error
	if !errors.Is(err, want) || !errors.As(err, &e) || e.Op == "" {
		t.Fatalf("error %v is not an *Error matching %v", err, want)
	}
}

var allTypes = []Type{TypePack, TypeIndex, TypeSnapshot, TypeConfig, TypeMark}

func TestSealOpenRoundTrip(t *testing.T) {
	k := newKeys(t, 1)
	for _, typ := range allTypes {
		for _, plain := range [][]byte{nil, []byte("x"), bytes.Repeat([]byte("chunk data "), 1000)} {
			sealed, err := k.Seal(typ, []byte("object-id"), CompressionNone, plain)
			if err != nil {
				t.Fatalf("Seal(%v): %v", typ, err)
			}
			if len(sealed) != len(plain)+Overhead {
				t.Fatalf("sealed %d bytes for %d of plaintext, want overhead %d", len(sealed), len(plain), Overhead)
			}
			if len(plain) > 16 && bytes.Contains(sealed, plain[:16]) {
				t.Fatal("sealed object contains plaintext")
			}
			got, err := k.Open(typ, []byte("object-id"), sealed)
			if err != nil {
				t.Fatalf("Open(%v): %v", typ, err)
			}
			if !bytes.Equal(got, plain) {
				t.Fatalf("Open(%v) = %d bytes, want %d", typ, len(got), len(plain))
			}
		}
	}
	a, _ := k.Seal(TypePack, nil, CompressionNone, []byte("same"))
	b, _ := k.Seal(TypePack, nil, CompressionNone, []byte("same"))
	if bytes.Equal(a, b) {
		t.Fatal("two seals of the same plaintext are equal: the nonce repeated")
	}
}

func TestOpenRejectsEveryFlippedBit(t *testing.T) {
	k := newKeys(t, 2)
	id := []byte("pack-0001")
	sealed, err := k.Seal(TypePack, id, CompressionNone, []byte("sixteen bytes!!!"))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	// Every bit of the header (additional data), nonce, ciphertext and tag.
	for i := range sealed {
		for bit := range 8 {
			tampered := append([]byte(nil), sealed...)
			tampered[i] ^= 1 << bit
			got, err := k.Open(TypePack, id, tampered)
			if err == nil || got != nil {
				t.Fatalf("byte %d bit %d flipped: opened", i, bit)
			}
			if !errors.Is(err, ErrOpen) && !errors.Is(err, ErrMalformed) {
				t.Fatalf("byte %d bit %d flipped: %v", i, bit, err)
			}
		}
	}
	// Every bit of the ID, the rest of the additional data.
	for i := range id {
		for bit := range 8 {
			other := append([]byte(nil), id...)
			other[i] ^= 1 << bit
			_, err := k.Open(TypePack, other, sealed)
			assertTyped(t, err, ErrOpen)
		}
	}
	_, err = k.Open(TypePack, append(append([]byte(nil), id...), 0), sealed)
	assertTyped(t, err, ErrOpen)
	_, err = k.Open(TypePack, id, sealed[:len(sealed)-1])
	assertTyped(t, err, ErrOpen)
	_, err = k.Open(TypePack, id, sealed[:Overhead-1])
	assertTyped(t, err, ErrMalformed)
}

func TestNoSwapAcrossTypesIDsOrKeys(t *testing.T) {
	k := newKeys(t, 3)
	other := newKeys(t, 4)
	for _, typ := range allTypes {
		sealed, err := k.Seal(typ, []byte("a"), CompressionNone, []byte("payload"))
		if err != nil {
			t.Fatalf("Seal: %v", err)
		}
		for _, as := range allTypes {
			if as == typ {
				continue
			}
			_, err = k.Open(as, []byte("a"), sealed)
			assertTyped(t, err, ErrOpen)
			// Relabel the header too: the subkey still differs.
			relabeled := append([]byte(nil), sealed...)
			relabeled[offType] = byte(as)
			_, err = k.Open(as, []byte("a"), relabeled)
			assertTyped(t, err, ErrOpen)
		}
		_, err = k.Open(typ, []byte("b"), sealed)
		assertTyped(t, err, ErrOpen)
		_, err = other.Open(typ, []byte("a"), sealed)
		assertTyped(t, err, ErrOpen)
	}
}

func TestSubkeysDiffer(t *testing.T) {
	k := newKeys(t, 5)
	dk := newDataKey(t, 5)
	seen := map[string]string{string(readKey(t, dk)): "data key"}
	for name, h := range map[string]enclave.ByteAccess{
		"chunk-id": k.chunkID, "object-id": k.objectID, "pack": k.pack, "index": k.index,
		"snapshot": k.snapshot, "config": k.config,
	} {
		b := string(readKey(t, h))
		if prev, ok := seen[b]; ok {
			t.Fatalf("subkey %s equals %s", name, prev)
		}
		seen[b] = name
	}
	// Derivation is deterministic: the same data key gives the same subkeys.
	again := newKeys(t, 5)
	if !bytes.Equal(readKey(t, k.pack), readKey(t, again.pack)) {
		t.Fatal("deriving twice from one data key gave different subkeys")
	}
}

func TestChunkID(t *testing.T) {
	k := newKeys(t, 6)
	a1, err := k.ChunkID([]byte("chunk"))
	if err != nil {
		t.Fatalf("ChunkID: %v", err)
	}
	a2, _ := k.ChunkID([]byte("chunk"))
	b, _ := k.ChunkID([]byte("chunk!"))
	if a1 != a2 {
		t.Fatal("ChunkID is not stable for one key")
	}
	if a1 == b {
		t.Fatal("different chunks have the same ID")
	}
	if a1 == (ID{}) || len(a1.String()) != 2*IDSize {
		t.Fatalf("ID %s is empty or malformed", a1)
	}
	other, _ := newKeys(t, 7).ChunkID([]byte("chunk"))
	if a1 == other {
		t.Fatal("ChunkID is the same under another key")
	}
}

func TestObjectID(t *testing.T) {
	k := newKeys(t, 12)
	a, err := k.ObjectID(TypePack, []byte("content"))
	if err != nil {
		t.Fatalf("ObjectID: %v", err)
	}
	again, _ := k.ObjectID(TypePack, []byte("content"))
	asIndex, _ := k.ObjectID(TypeIndex, []byte("content"))
	chunk, _ := k.ChunkID(append([]byte{byte(TypePack)}, "content"...))
	other, _ := newKeys(t, 13).ObjectID(TypePack, []byte("content"))
	if a != again {
		t.Fatal("ObjectID is not stable for one key")
	}
	if a == asIndex || a == chunk || a == other {
		t.Fatal("ObjectID collides across types, with a chunk ID, or across keys")
	}
}

func TestParseID(t *testing.T) {
	k := newKeys(t, 14)
	id, _ := k.ChunkID([]byte("x"))
	got, err := ParseID(id.String())
	if err != nil || got != id {
		t.Fatalf("ParseID(%s) = %s, %v", id, got, err)
	}
	text, err := id.MarshalText()
	if err != nil {
		t.Fatalf("MarshalText: %v", err)
	}
	var back ID
	if err := back.UnmarshalText(text); err != nil || back != id {
		t.Fatalf("UnmarshalText = %s, %v", back, err)
	}
	upper := strings.ToUpper(id.String())
	for _, bad := range []string{"", "abc", id.String()[1:], upper, strings.Repeat("zz", IDSize)} {
		if _, err := ParseID(bad); err == nil {
			t.Fatalf("ParseID(%q) succeeded", bad)
		} else {
			assertTyped(t, err, ErrMalformedID)
		}
		if err := back.UnmarshalText([]byte(bad)); !errors.Is(err, ErrMalformedID) {
			t.Fatalf("UnmarshalText(%q) = %v", bad, err)
		}
	}
}

func TestDestroyedKeys(t *testing.T) {
	k, err := DeriveKeys(newDataKey(t, 8))
	if err != nil {
		t.Fatalf("DeriveKeys: %v", err)
	}
	sealed, err := k.Seal(TypeIndex, nil, CompressionNone, []byte("x"))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := k.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := k.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	_, err = k.Seal(TypeIndex, nil, CompressionNone, []byte("x"))
	assertTyped(t, err, ErrKeysDestroyed)
	_, err = k.Open(TypeIndex, nil, sealed)
	assertTyped(t, err, ErrKeysDestroyed)
	_, err = k.ChunkID([]byte("x"))
	assertTyped(t, err, ErrKeysDestroyed)
	_, err = k.ObjectID(TypePack, []byte("x"))
	assertTyped(t, err, ErrKeysDestroyed)

	dead, err := enclave.NewMemory(bytes.Repeat([]byte{9}, DataKeySize))
	if err != nil {
		t.Fatalf("NewMemory: %v", err)
	}
	if err := dead.Destroy(); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
	_, err = DeriveKeys(dead)
	assertTyped(t, err, ErrKeysDestroyed)
}

func TestRejects(t *testing.T) {
	_, err := DeriveKeys(nil)
	assertTyped(t, err, ErrInvalidKey)
	short, err := enclave.NewMemory([]byte("short"))
	if err != nil {
		t.Fatalf("NewMemory: %v", err)
	}
	defer func() {
		if err := short.Destroy(); err != nil {
			t.Errorf("Destroy: %v", err)
		}
	}()
	_, err = DeriveKeys(short)
	assertTyped(t, err, ErrInvalidKey)

	k := newKeys(t, 10)
	_, err = k.Seal(Type(0), nil, CompressionNone, nil)
	assertTyped(t, err, ErrUnknownType)
	_, err = k.Open(Type(9), nil, nil)
	assertTyped(t, err, ErrUnknownType)
	_, err = k.Seal(TypePack, nil, Compression(1), nil)
	assertTyped(t, err, ErrUnsupportedCompression)

	sealed, _ := k.Seal(TypePack, nil, CompressionNone, []byte("x"))
	for _, bad := range [][]byte{nil, []byte("OBKS"), append([]byte("XBKS"), sealed[4:]...)} {
		_, err = k.Open(TypePack, nil, bad)
		assertTyped(t, err, ErrMalformed)
	}
	version := append([]byte(nil), sealed...)
	version[len(magic)] = formatVersion + 1
	_, err = k.Open(TypePack, nil, version)
	assertTyped(t, err, ErrMalformed)
	_, err = decompress("open", Compression(2), nil)
	assertTyped(t, err, ErrUnsupportedCompression)
}

func TestStrings(t *testing.T) {
	for want, s := range map[string]interface{ String() string }{
		"pack": TypePack, "index": TypeIndex, "snapshot": TypeSnapshot, "config": TypeConfig, "mark": TypeMark, "type(7)": Type(7),
		"none": CompressionNone, "compression(3)": Compression(3),
	} {
		if s.String() != want {
			t.Errorf("String() = %q, want %q", s.String(), want)
		}
	}
	e := &Error{Op: "open", Err: ErrOpen, Cause: errors.New("cause")}
	if !strings.HasSuffix(e.Error(), ": cause") || !errors.Is(e, ErrOpen) {
		t.Fatalf("Error() = %q", e.Error())
	}
}
