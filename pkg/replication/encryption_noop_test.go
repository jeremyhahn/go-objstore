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

package replication_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNoopEncrypter_Encrypt(t *testing.T) {
	tests := []struct {
		name           string
		input          io.Reader
		expectedData   string
		expectReadable bool
		description    string
	}{
		{
			name:           "PassthroughPlainReader",
			input:          strings.NewReader("test data"),
			expectedData:   "test data",
			expectReadable: true,
			description:    "Should return data unmodified when given plain io.Reader",
		},
		{
			name:           "PassthroughReadCloser",
			input:          io.NopCloser(strings.NewReader("sensitive data")),
			expectedData:   "sensitive data",
			expectReadable: true,
			description:    "Should preserve io.ReadCloser interface",
		},
		{
			name:           "EmptyData",
			input:          strings.NewReader(""),
			expectedData:   "",
			expectReadable: true,
			description:    "Should handle empty data correctly",
		},
		{
			name:           "LargeData",
			input:          strings.NewReader(strings.Repeat("a", 10000)),
			expectedData:   strings.Repeat("a", 10000),
			expectReadable: true,
			description:    "Should handle large data without modification",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encrypter := replication.NewNoopEncrypter()
			ctx := context.Background()

			result, err := encrypter.Encrypt(ctx, tt.input)
			require.NoError(t, err, "Encrypt should not return an error")
			require.NotNil(t, result, "Result should not be nil")

			// Read and verify data
			data, err := io.ReadAll(result)
			require.NoError(t, err, "Should be able to read all data")
			assert.Equal(t, tt.expectedData, string(data), "Data should be unchanged")

			// Ensure Close doesn't error
			err = result.Close()
			assert.NoError(t, err, "Close should not return an error")
		})
	}
}

func TestNoopEncrypter_Decrypt(t *testing.T) {
	tests := []struct {
		name           string
		input          io.Reader
		expectedData   string
		expectReadable bool
		description    string
	}{
		{
			name:           "PassthroughPlainReader",
			input:          strings.NewReader("encrypted data"),
			expectedData:   "encrypted data",
			expectReadable: true,
			description:    "Should return data unmodified when given plain io.Reader",
		},
		{
			name:           "PassthroughReadCloser",
			input:          io.NopCloser(strings.NewReader("cipher text")),
			expectedData:   "cipher text",
			expectReadable: true,
			description:    "Should preserve io.ReadCloser interface",
		},
		{
			name:           "EmptyData",
			input:          strings.NewReader(""),
			expectedData:   "",
			expectReadable: true,
			description:    "Should handle empty data correctly",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encrypter := replication.NewNoopEncrypter()
			ctx := context.Background()

			result, err := encrypter.Decrypt(ctx, tt.input)
			require.NoError(t, err, "Decrypt should not return an error")
			require.NotNil(t, result, "Result should not be nil")

			// Read and verify data
			data, err := io.ReadAll(result)
			require.NoError(t, err, "Should be able to read all data")
			assert.Equal(t, tt.expectedData, string(data), "Data should be unchanged")

			// Ensure Close doesn't error
			err = result.Close()
			assert.NoError(t, err, "Close should not return an error")
		})
	}
}

func TestNoopEncrypter_PreservesReadCloser(t *testing.T) {
	encrypter := replication.NewNoopEncrypter()
	ctx := context.Background()

	// Test with an actual ReadCloser
	actualRC := io.NopCloser(strings.NewReader("test"))
	encResult, err := encrypter.Encrypt(ctx, actualRC)
	require.NoError(t, err)
	assert.Equal(t, actualRC, encResult, "Encrypt should return the same ReadCloser instance")

	// Test Decrypt with an actual ReadCloser
	actualRC2 := io.NopCloser(strings.NewReader("test2"))
	decResult, err := encrypter.Decrypt(ctx, actualRC2)
	require.NoError(t, err)

	// Verify it's the same instance
	assert.Equal(t, actualRC2, decResult, "Decrypt should return the same ReadCloser instance")
}

func TestNoopEncrypter_RoundTrip(t *testing.T) {
	encrypter := replication.NewNoopEncrypter()
	ctx := context.Background()

	originalData := "round trip test data"
	reader := strings.NewReader(originalData)

	// Encrypt
	encrypted, err := encrypter.Encrypt(ctx, reader)
	require.NoError(t, err)
	defer encrypted.Close()

	// Decrypt
	decrypted, err := encrypter.Decrypt(ctx, encrypted)
	require.NoError(t, err)
	defer decrypted.Close()

	// Verify data is unchanged
	data, err := io.ReadAll(decrypted)
	require.NoError(t, err)
	assert.Equal(t, originalData, string(data), "Round trip should preserve data")
}

func TestNoopEncrypter_Algorithm(t *testing.T) {
	encrypter := replication.NewNoopEncrypter()
	algorithm := encrypter.Algorithm()
	assert.Equal(t, "none", algorithm, "Algorithm should return 'none'")
}

func TestNoopEncrypter_KeyID(t *testing.T) {
	encrypter := replication.NewNoopEncrypter()
	keyID := encrypter.KeyID()
	assert.Equal(t, "", keyID, "KeyID should return empty string")
}

func TestNoopEncrypterFactory_GetEncrypter(t *testing.T) {
	factory := replication.NewNoopEncrypterFactory()

	tests := []struct {
		name  string
		keyID string
	}{
		{
			name:  "EmptyKeyID",
			keyID: "",
		},
		{
			name:  "NonEmptyKeyID",
			keyID: "test-key-123",
		},
		{
			name:  "AnotherKeyID",
			keyID: "another-key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encrypter, err := factory.GetEncrypter(tt.keyID)
			require.NoError(t, err, "GetEncrypter should not return an error")
			require.NotNil(t, encrypter, "Encrypter should not be nil")

			// Verify it returns a NoopEncrypter
			_, ok := encrypter.(*replication.NoopEncrypter)
			assert.True(t, ok, "Should return a *NoopEncrypter instance")

			// Verify the encrypter works
			ctx := context.Background()
			testData := "factory test"
			result, err := encrypter.Encrypt(ctx, strings.NewReader(testData))
			require.NoError(t, err)
			defer result.Close()

			data, err := io.ReadAll(result)
			require.NoError(t, err)
			assert.Equal(t, testData, string(data), "Encrypter should work correctly")
		})
	}
}

func TestNoopEncrypterFactory_GetEncrypter_ReturnsSameInstance(t *testing.T) {
	factory := replication.NewNoopEncrypterFactory()

	// Get encrypter multiple times
	enc1, err := factory.GetEncrypter("")
	require.NoError(t, err)

	enc2, err := factory.GetEncrypter("key1")
	require.NoError(t, err)

	enc3, err := factory.GetEncrypter("key2")
	require.NoError(t, err)

	// All should be the same instance (NoopEncrypterFactory reuses the same encrypter)
	assert.Equal(t, enc1, enc2, "Should return the same encrypter instance")
	assert.Equal(t, enc2, enc3, "Should return the same encrypter instance")
}

func TestNoopEncrypterFactory_DefaultKeyID(t *testing.T) {
	factory := replication.NewNoopEncrypterFactory()
	keyID := factory.DefaultKeyID()
	assert.Equal(t, "", keyID, "DefaultKeyID should return empty string")
}

func TestNoopEncrypterFactory_Close(t *testing.T) {
	factory := replication.NewNoopEncrypterFactory()
	err := factory.Close()
	assert.NoError(t, err, "Close should not return an error")

	// Verify factory still works after Close (since it has no resources to release)
	encrypter, err := factory.GetEncrypter("")
	require.NoError(t, err)
	assert.NotNil(t, encrypter, "Factory should still work after Close")
}

func TestNoopEncrypterFactory_MultipleClose(t *testing.T) {
	factory := replication.NewNoopEncrypterFactory()

	// Close multiple times should not error
	err := factory.Close()
	assert.NoError(t, err, "First Close should not error")

	err = factory.Close()
	assert.NoError(t, err, "Second Close should not error")

	err = factory.Close()
	assert.NoError(t, err, "Third Close should not error")
}

func TestNoopEncrypter_ConcurrentAccess(t *testing.T) {
	// Test that NoopEncrypter is safe for concurrent use
	encrypter := replication.NewNoopEncrypter()
	ctx := context.Background()

	const goroutines = 100
	done := make(chan bool, goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer func() { done <- true }()

			data := strings.NewReader("concurrent test")

			// Encrypt
			encrypted, err := encrypter.Encrypt(ctx, data)
			assert.NoError(t, err)
			if encrypted != nil {
				encrypted.Close()
			}

			// Decrypt
			data2 := strings.NewReader("concurrent test")
			decrypted, err := encrypter.Decrypt(ctx, data2)
			assert.NoError(t, err)
			if decrypted != nil {
				decrypted.Close()
			}

			// Algorithm and KeyID
			_ = encrypter.Algorithm()
			_ = encrypter.KeyID()
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < goroutines; i++ {
		<-done
	}
}

func TestNoopEncrypterFactory_ConcurrentAccess(t *testing.T) {
	// Test that NoopEncrypterFactory is safe for concurrent use
	factory := replication.NewNoopEncrypterFactory()

	const goroutines = 100
	done := make(chan bool, goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer func() { done <- true }()

			encrypter, err := factory.GetEncrypter("")
			assert.NoError(t, err)
			assert.NotNil(t, encrypter)

			_ = factory.DefaultKeyID()
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < goroutines; i++ {
		<-done
	}

	// Close should still work
	err := factory.Close()
	assert.NoError(t, err)
}

func TestNoopEncrypter_BinaryData(t *testing.T) {
	// Test with binary data (not just strings)
	encrypter := replication.NewNoopEncrypter()
	ctx := context.Background()

	binaryData := []byte{0x00, 0xFF, 0x42, 0xAA, 0x55, 0x01, 0x02, 0x03}
	reader := bytes.NewReader(binaryData)

	// Encrypt
	encrypted, err := encrypter.Encrypt(ctx, reader)
	require.NoError(t, err)
	defer encrypted.Close()

	// Read result
	result, err := io.ReadAll(encrypted)
	require.NoError(t, err)

	// Verify binary data is unchanged
	assert.Equal(t, binaryData, result, "Binary data should be unchanged")
}

func TestNoopEncrypter_ContextCancellation(t *testing.T) {
	// Test behavior with cancelled context
	encrypter := replication.NewNoopEncrypter()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	reader := strings.NewReader("test data")

	// NoopEncrypter should not check context, so this should still work
	result, err := encrypter.Encrypt(ctx, reader)
	require.NoError(t, err, "Should succeed even with cancelled context")
	require.NotNil(t, result)

	data, err := io.ReadAll(result)
	require.NoError(t, err)
	assert.Equal(t, "test data", string(data))
	result.Close()
}
