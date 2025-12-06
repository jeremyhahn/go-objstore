// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed under AGPL-3.0 and a Commercial License.

package objstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRetryWrapper_DisabledRetry(t *testing.T) {
	ctx := context.Background()
	callCount := 0

	// With nil config, should execute once
	result, err := retryWrapper(ctx, nil, func() (int, error) {
		callCount++
		return 42, nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 42, result)
	assert.Equal(t, 1, callCount)
}

func TestRetryWrapper_DisabledRetryWithConfig(t *testing.T) {
	ctx := context.Background()
	config := &RetryConfig{
		Enabled: false,
	}
	callCount := 0

	// With disabled config, should execute once
	result, err := retryWrapper(ctx, config, func() (int, error) {
		callCount++
		return 42, nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 42, result)
	assert.Equal(t, 1, callCount)
}

func TestRetryWrapper_SuccessOnFirstAttempt(t *testing.T) {
	ctx := context.Background()
	config := &RetryConfig{
		Enabled:        true,
		MaxRetries:     3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
	}
	callCount := 0

	result, err := retryWrapper(ctx, config, func() (string, error) {
		callCount++
		return "success", nil
	})

	assert.NoError(t, err)
	assert.Equal(t, "success", result)
	assert.Equal(t, 1, callCount)
}

func TestRetryWrapper_SuccessAfterRetries(t *testing.T) {
	ctx := context.Background()
	config := &RetryConfig{
		Enabled:        true,
		MaxRetries:     3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
	}
	callCount := 0

	result, err := retryWrapper(ctx, config, func() (string, error) {
		callCount++
		if callCount < 3 {
			return "", ErrConnectionFailed
		}
		return "success", nil
	})

	assert.NoError(t, err)
	assert.Equal(t, "success", result)
	assert.Equal(t, 3, callCount)
}

func TestRetryWrapper_ExhaustedRetries(t *testing.T) {
	ctx := context.Background()
	config := &RetryConfig{
		Enabled:        true,
		MaxRetries:     2,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
	}
	callCount := 0

	result, err := retryWrapper(ctx, config, func() (string, error) {
		callCount++
		return "", ErrConnectionFailed
	})

	assert.Error(t, err)
	assert.Equal(t, "", result)
	assert.Equal(t, 3, callCount) // Initial attempt + 2 retries
}

func TestRetryWrapper_NonRetryableError(t *testing.T) {
	ctx := context.Background()
	config := &RetryConfig{
		Enabled:        true,
		MaxRetries:     3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
	}
	callCount := 0
	nonRetryableErr := errors.New("invalid input")

	result, err := retryWrapper(ctx, config, func() (string, error) {
		callCount++
		return "", nonRetryableErr
	})

	assert.Error(t, err)
	assert.Equal(t, "", result)
	assert.Equal(t, 1, callCount) // Should not retry
}

func TestRetryWrapper_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	config := &RetryConfig{
		Enabled:        true,
		MaxRetries:     5,
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     500 * time.Millisecond,
	}
	callCount := 0

	// Cancel context after first failure
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	result, err := retryWrapper(ctx, config, func() (string, error) {
		callCount++
		return "", ErrConnectionFailed
	})

	assert.Error(t, err)
	assert.Equal(t, "", result)
	// Should stop retrying after context cancellation
	// The initial call happens, then it may attempt one retry before checking context
	assert.LessOrEqual(t, callCount, 3)
}

func TestRetryWrapper_CustomRetryableErrors(t *testing.T) {
	ctx := context.Background()
	customErr := errors.New("custom retryable error")
	config := &RetryConfig{
		Enabled:         true,
		MaxRetries:      3,
		InitialBackoff:  10 * time.Millisecond,
		MaxBackoff:      100 * time.Millisecond,
		RetryableErrors: []error{customErr},
	}
	callCount := 0

	result, err := retryWrapper(ctx, config, func() (string, error) {
		callCount++
		if callCount < 2 {
			return "", customErr
		}
		return "success", nil
	})

	assert.NoError(t, err)
	assert.Equal(t, "success", result)
	assert.Equal(t, 2, callCount)
}

func TestIsRetryable_SentinelErrors(t *testing.T) {
	retryableErrors := []error{ErrConnectionFailed, ErrTimeout}

	assert.True(t, isRetryable(ErrConnectionFailed, retryableErrors))
	assert.True(t, isRetryable(ErrTimeout, retryableErrors))
	assert.False(t, isRetryable(ErrInvalidKey, retryableErrors))
}

func TestIsRetryable_GRPCStatusCodes(t *testing.T) {
	retryableErrors := []error{}

	unavailableErr := status.Error(codes.Unavailable, "service unavailable")
	assert.True(t, isRetryable(unavailableErr, retryableErrors))

	deadlineErr := status.Error(codes.DeadlineExceeded, "deadline exceeded")
	assert.True(t, isRetryable(deadlineErr, retryableErrors))

	notFoundErr := status.Error(codes.NotFound, "not found")
	assert.False(t, isRetryable(notFoundErr, retryableErrors))
}

func TestIsRetryable_ErrorStrings(t *testing.T) {
	retryableErrors := []error{}

	timeoutErr := errors.New("operation timeout occurred")
	assert.True(t, isRetryable(timeoutErr, retryableErrors))

	connRefusedErr := errors.New("connection refused by server")
	assert.True(t, isRetryable(connRefusedErr, retryableErrors))

	rateLimitErr := errors.New("rate limit exceeded")
	assert.True(t, isRetryable(rateLimitErr, retryableErrors))

	invalidErr := errors.New("invalid input")
	assert.False(t, isRetryable(invalidErr, retryableErrors))
}

func TestCalculateBackoff(t *testing.T) {
	initial := 100 * time.Millisecond
	max := 5 * time.Second

	// Test that backoff increases exponentially
	backoff0 := calculateBackoff(0, initial, max)
	assert.LessOrEqual(t, backoff0, initial)

	backoff1 := calculateBackoff(1, initial, max)
	assert.LessOrEqual(t, backoff1, 2*initial)

	backoff2 := calculateBackoff(2, initial, max)
	assert.LessOrEqual(t, backoff2, 4*initial)

	// Test that backoff is capped at max
	backoff10 := calculateBackoff(10, initial, max)
	assert.LessOrEqual(t, backoff10, max)
}

func TestCalculateBackoff_Jitter(t *testing.T) {
	initial := 100 * time.Millisecond
	max := 5 * time.Second

	// Test that jitter produces different values
	backoffs := make(map[time.Duration]bool)
	for i := 0; i < 10; i++ {
		backoff := calculateBackoff(1, initial, max)
		backoffs[backoff] = true
	}

	// With jitter, we should have multiple different values
	// (there's a small chance this could fail if random produces same values)
	assert.Greater(t, len(backoffs), 1)
}
