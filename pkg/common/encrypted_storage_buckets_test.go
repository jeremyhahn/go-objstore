// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed:
//
// 1. GNU Affero General Public License v3.0 (AGPL-3.0)
//    See LICENSE file or visit https://www.gnu.org/licenses/agpl-3.0.html
//
// 2. Commercial License
//    Contact licensing@automatethethings.com for commercial licensing options.

package common

import "testing"

// capableStorage is an underlying storage that offers both capabilities.
type capableStorage struct {
	*mockUnderlyingStorage
	MultipartUploader
	BucketManager
}

// The wrapper encrypts each object as one stream, which parts encrypted
// separately cannot form, so it must not expose the capabilities of the
// storage it wraps; callers then see ErrNotSupported.
func TestEncryptedStorage_HidesBucketAndMultipartCapabilities(t *testing.T) {
	underlying := &capableStorage{mockUnderlyingStorage: newMockUnderlyingStorage()}
	factory := &mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters:   map[string]Encrypter{"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256"}},
	}

	var plain Storage = underlying
	if _, ok := plain.(MultipartUploader); !ok {
		t.Fatal("the underlying storage must offer multipart for this test to mean anything")
	}
	if _, ok := plain.(BucketManager); !ok {
		t.Fatal("the underlying storage must offer buckets for this test to mean anything")
	}

	wrapped := NewEncryptedStorage(underlying, factory)
	if _, ok := wrapped.(MultipartUploader); ok {
		t.Error("encryptedStorage exposes MultipartUploader")
	}
	if _, ok := wrapped.(BucketManager); ok {
		t.Error("encryptedStorage exposes BucketManager")
	}
}
