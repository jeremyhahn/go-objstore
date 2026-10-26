package seal

import (
	"bytes"
	"testing"
)

// FuzzOpen feeds arbitrary bytes, types and IDs to Open. It must never panic,
// and must open nothing but the one object it was seeded with, under its own
// type and ID.
func FuzzOpen(f *testing.F) {
	k := newKeys(f, 11)
	id := []byte("seed-id")
	plain := []byte("seed plaintext")
	sealed, err := k.Seal(TypeSnapshot, id, CompressionNone, plain)
	if err != nil {
		f.Fatalf("Seal: %v", err)
	}
	f.Add(sealed, uint8(TypeSnapshot), id)
	f.Add(sealed[:Overhead], uint8(TypePack), []byte{})
	f.Add([]byte("OBKS\x01\x01\x00"), uint8(TypePack), []byte("x"))
	f.Add([]byte{}, uint8(0), []byte{})

	f.Fuzz(func(t *testing.T, data []byte, typ uint8, objID []byte) {
		got, err := k.Open(Type(typ), objID, data)
		if err != nil {
			if got != nil {
				t.Fatal("Open returned plaintext with an error")
			}
			return
		}
		// Each fuzz worker reruns the setup, sealing the seed under a fresh
		// nonce, so the oracle is the type, the ID and the plaintext.
		if Type(typ) != TypeSnapshot || !bytes.Equal(objID, id) || len(data) != len(sealed) {
			t.Fatalf("Open accepted a forged object (type %d, %d bytes)", typ, len(data))
		}
		if !bytes.Equal(got, plain) {
			t.Fatal("Open returned the wrong plaintext")
		}
	})
}
