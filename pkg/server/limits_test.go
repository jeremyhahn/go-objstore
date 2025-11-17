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

package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Test constants are properly defined and have expected values
func TestLimitsConstants(t *testing.T) {
	t.Run("MaxListLimit is set correctly", func(t *testing.T) {
		assert.Equal(t, 1000, MaxListLimit)
		assert.Greater(t, MaxListLimit, 0)
	})

	t.Run("DefaultListLimit is set correctly", func(t *testing.T) {
		assert.Equal(t, 100, DefaultListLimit)
		assert.Greater(t, DefaultListLimit, 0)
		assert.LessOrEqual(t, DefaultListLimit, MaxListLimit)
	})

	t.Run("MaxUploadSize is set correctly", func(t *testing.T) {
		// 1 GB in bytes
		expectedSize := int64(1 * 1024 * 1024 * 1024)
		assert.Equal(t, expectedSize, int64(MaxUploadSize))
		assert.Greater(t, MaxUploadSize, 0)
	})

	t.Run("MaxMetadataSize is set correctly", func(t *testing.T) {
		// 1 MB in bytes
		expectedSize := int(1 * 1024 * 1024)
		assert.Equal(t, expectedSize, MaxMetadataSize)
		assert.Greater(t, MaxMetadataSize, 0)
		assert.Less(t, MaxMetadataSize, MaxUploadSize)
	})

	t.Run("MaxKeyLength is set correctly", func(t *testing.T) {
		assert.Equal(t, 1024, MaxKeyLength)
		assert.Greater(t, MaxKeyLength, 0)
	})

	t.Run("MaxPrefixLength is set correctly", func(t *testing.T) {
		assert.Equal(t, 512, MaxPrefixLength)
		assert.Greater(t, MaxPrefixLength, 0)
		assert.LessOrEqual(t, MaxPrefixLength, MaxKeyLength)
	})

	t.Run("MaxDelimiterLength is set correctly", func(t *testing.T) {
		assert.Equal(t, 10, MaxDelimiterLength)
		assert.Greater(t, MaxDelimiterLength, 0)
	})

	t.Run("MaxContinueTokenLength is set correctly", func(t *testing.T) {
		assert.Equal(t, 2048, MaxContinueTokenLength)
		assert.Greater(t, MaxContinueTokenLength, 0)
	})

	t.Run("HealthCheckTimeout is set correctly", func(t *testing.T) {
		assert.Equal(t, 5000, HealthCheckTimeout)
		assert.Greater(t, HealthCheckTimeout, 0)
	})

	t.Run("DefaultRequestTimeout is set correctly", func(t *testing.T) {
		assert.Equal(t, 30, DefaultRequestTimeout)
		assert.Greater(t, DefaultRequestTimeout, 0)
	})

	t.Run("MaxConcurrentRequests is set correctly", func(t *testing.T) {
		assert.Equal(t, 1000, MaxConcurrentRequests)
		assert.Greater(t, MaxConcurrentRequests, 0)
	})

	t.Run("BufferSize is set correctly", func(t *testing.T) {
		// 64 KB in bytes
		expectedSize := 64 * 1024
		assert.Equal(t, expectedSize, BufferSize)
		assert.Greater(t, BufferSize, 0)
		assert.Less(t, BufferSize, MaxMetadataSize)
	})
}

// Test relationships between limits
func TestLimitsRelationships(t *testing.T) {
	t.Run("DefaultListLimit should be less than MaxListLimit", func(t *testing.T) {
		assert.Less(t, DefaultListLimit, MaxListLimit)
	})

	t.Run("MaxMetadataSize should be less than MaxUploadSize", func(t *testing.T) {
		assert.Less(t, MaxMetadataSize, MaxUploadSize)
	})

	t.Run("MaxPrefixLength should be less than MaxKeyLength", func(t *testing.T) {
		assert.Less(t, MaxPrefixLength, MaxKeyLength)
	})

	t.Run("BufferSize should be less than MaxMetadataSize", func(t *testing.T) {
		assert.Less(t, BufferSize, MaxMetadataSize)
	})

	t.Run("MaxDelimiterLength should be less than MaxKeyLength", func(t *testing.T) {
		assert.Less(t, MaxDelimiterLength, MaxKeyLength)
	})
}

// Test individual constant values
func TestIndividualConstants(t *testing.T) {
	t.Run("all constants are positive", func(t *testing.T) {
		assert.Greater(t, MaxListLimit, 0)
		assert.Greater(t, DefaultListLimit, 0)
		assert.Greater(t, MaxUploadSize, 0)
		assert.Greater(t, MaxMetadataSize, 0)
		assert.Greater(t, MaxKeyLength, 0)
		assert.Greater(t, MaxPrefixLength, 0)
		assert.Greater(t, MaxDelimiterLength, 0)
		assert.Greater(t, MaxContinueTokenLength, 0)
		assert.Greater(t, HealthCheckTimeout, 0)
		assert.Greater(t, DefaultRequestTimeout, 0)
		assert.Greater(t, MaxConcurrentRequests, 0)
		assert.Greater(t, BufferSize, 0)
	})

	t.Run("size constants are reasonable", func(t *testing.T) {
		// MaxUploadSize should be much larger than MaxMetadataSize
		assert.Greater(t, MaxUploadSize, MaxMetadataSize*100)

		// Timeouts should be in reasonable range (ms and seconds)
		assert.GreaterOrEqual(t, HealthCheckTimeout, 1)
		assert.GreaterOrEqual(t, DefaultRequestTimeout, 1)

		// Request limits should be reasonable
		assert.LessOrEqual(t, MaxListLimit, 10000)
		assert.LessOrEqual(t, MaxConcurrentRequests, 100000)
	})

	t.Run("token and key lengths are reasonable", func(t *testing.T) {
		// Token should be larger than key (tokens encode data)
		assert.Greater(t, MaxContinueTokenLength, MaxKeyLength)

		// Prefix should be smaller or equal to key
		assert.LessOrEqual(t, MaxPrefixLength, MaxKeyLength)

		// Delimiter should be small
		assert.Less(t, MaxDelimiterLength, 100)
	})
}
