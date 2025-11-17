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

package common_test

import (
	"bytes"
	"io"
	"testing"
)

func TestArchiver_Put(t *testing.T) {
	// Test case 1: Successful Put
	called := false
	mockArchiver := &MockArchiver{
		PutFunc: func(key string, data io.Reader) error {
			called = true
			if key != "test-key" {
				t.Errorf("Expected key 'test-key', got '%s'", key)
			}
			content, _ := io.ReadAll(data)
			if string(content) != "test-data" {
				t.Errorf("Expected data 'test-data', got '%s'", string(content))
			}
			return nil
		},
	}

	err := mockArchiver.Put("test-key", bytes.NewBufferString("test-data"))
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if !called {
		t.Error("Expected PutFunc to be called, but it wasn't")
	}

	// Test case 2: Error during Put
	mockArchiverWithError := &MockArchiver{
		PutFunc: func(key string, data io.Reader) error {
			return io.ErrUnexpectedEOF // Simulate an error
		},
	}

	err = mockArchiverWithError.Put("test-key", bytes.NewBufferString("test-data"))
	if err == nil || err.Error() != io.ErrUnexpectedEOF.Error() {
		t.Errorf("Expected error '%v', got '%v'", io.ErrUnexpectedEOF, err)
	}
}
