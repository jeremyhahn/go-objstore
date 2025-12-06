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
	"math"
	"math/rand"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// retryWrapper wraps an operation with retry logic using exponential backoff with jitter.
// It returns the result of the operation or the last error encountered.
func retryWrapper[T any](ctx context.Context, config *RetryConfig, operation func() (T, error)) (T, error) {
	var zero T

	// If retry is not enabled, execute once
	if config == nil || !config.Enabled {
		return operation()
	}

	// Set defaults
	maxRetries := config.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 3
	}

	initialBackoff := config.InitialBackoff
	if initialBackoff <= 0 {
		initialBackoff = 100 * time.Millisecond
	}

	maxBackoff := config.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = 5 * time.Second
	}

	retryableErrors := config.RetryableErrors
	if len(retryableErrors) == 0 {
		// Default retryable errors
		retryableErrors = []error{
			ErrConnectionFailed,
			ErrTimeout,
			ErrTemporaryFailure,
		}
	}

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		// Check if context is cancelled
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return zero, lastErr
			}
			return zero, ctx.Err()
		default:
		}

		// Execute the operation
		result, err := operation()
		if err == nil {
			return result, nil
		}

		lastErr = err

		// Don't retry on last attempt
		if attempt == maxRetries {
			break
		}

		// Check if error is retryable
		if !isRetryable(err, retryableErrors) {
			return zero, err
		}

		// Calculate backoff with exponential growth and jitter
		backoff := calculateBackoff(attempt, initialBackoff, maxBackoff)

		// Wait with context awareness
		select {
		case <-ctx.Done():
			return zero, lastErr
		case <-time.After(backoff):
			// Continue to next attempt
		}
	}

	return zero, lastErr
}

// isRetryable checks if an error should trigger a retry.
func isRetryable(err error, retryableErrors []error) bool {
	if err == nil {
		return false
	}

	// Check against configured retryable errors
	for _, retryableErr := range retryableErrors {
		if errors.Is(err, retryableErr) {
			return true
		}
	}

	// Check for gRPC status codes that indicate transient failures
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted:
			return true
		}
	}

	// Check for common transient error strings
	errMsg := strings.ToLower(err.Error())
	transientKeywords := []string{
		"timeout",
		"connection refused",
		"connection reset",
		"temporary failure",
		"unavailable",
		"deadline exceeded",
		"resource exhausted",
		"too many requests",
		"rate limit",
	}

	for _, keyword := range transientKeywords {
		if strings.Contains(errMsg, keyword) {
			return true
		}
	}

	return false
}

// calculateBackoff computes the backoff duration for a given attempt using
// exponential backoff with jitter.
func calculateBackoff(attempt int, initial, max time.Duration) time.Duration {
	// Exponential backoff: initial * 2^attempt
	backoff := float64(initial) * math.Pow(2, float64(attempt))

	// Cap at max backoff
	if backoff > float64(max) {
		backoff = float64(max)
	}

	// Add jitter: random value between 0 and backoff
	jitter := rand.Float64() * backoff

	return time.Duration(jitter)
}
