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

package cli

import (
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/version"
)

func TestFormatOperationResult(t *testing.T) {
	t.Run("successful result text format", func(t *testing.T) {
		result := &OperationResult{
			Success: true,
			Message: "Operation completed",
		}
		output := FormatOperationResult(result, FormatText)
		if !strings.Contains(output, "Operation completed") {
			t.Error("Expected message in output")
		}
	})

	t.Run("successful result without message", func(t *testing.T) {
		result := &OperationResult{
			Success: true,
		}
		output := FormatOperationResult(result, FormatText)
		if !strings.Contains(output, "successfully") {
			t.Error("Expected success message")
		}
	})

	t.Run("failed result text format", func(t *testing.T) {
		result := &OperationResult{
			Success: false,
			Error:   "Something went wrong",
		}
		output := FormatOperationResult(result, FormatText)
		if !strings.Contains(output, "Error:") {
			t.Error("Expected error prefix")
		}
		if !strings.Contains(output, "Something went wrong") {
			t.Error("Expected error message in output")
		}
	})

	t.Run("json format", func(t *testing.T) {
		result := &OperationResult{
			Success: true,
			Message: "Test message",
			Data:    map[string]string{"key": "value"},
		}
		output := FormatOperationResult(result, FormatJSON)
		if !strings.Contains(output, `"success": true`) {
			t.Error("Expected success field in JSON")
		}
		if !strings.Contains(output, `"message": "Test message"`) {
			t.Error("Expected message in JSON")
		}
	})

	t.Run("table format success", func(t *testing.T) {
		result := &OperationResult{
			Success: true,
			Message: "Operation completed successfully",
		}
		output := FormatOperationResult(result, FormatTable)
		if !strings.Contains(output, "SUCCESS") {
			t.Error("Expected SUCCESS status in table")
		}
		if !strings.Contains(output, "Operation completed successfully") {
			t.Error("Expected message in table")
		}
	})

	t.Run("table format failure", func(t *testing.T) {
		result := &OperationResult{
			Success: false,
			Error:   "Test error",
		}
		output := FormatOperationResult(result, FormatTable)
		if !strings.Contains(output, "FAILED") {
			t.Error("Expected FAILED status in table")
		}
		if !strings.Contains(output, "Test error") {
			t.Error("Expected error message in table")
		}
	})
}

func TestFormatListResult(t *testing.T) {
	t.Run("empty list text format", func(t *testing.T) {
		objects := []ObjectInfo{}
		output := FormatListResult(objects, FormatText)
		if !strings.Contains(output, "No objects found") {
			t.Error("Expected 'No objects found' message")
		}
	})

	t.Run("list with objects text format", func(t *testing.T) {
		objects := []ObjectInfo{
			{
				Key:          "test/file1.txt",
				Size:         1024,
				LastModified: time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC),
			},
			{
				Key:          "test/file2.txt",
				Size:         2048,
				LastModified: time.Date(2025, 1, 2, 12, 0, 0, 0, time.UTC),
			},
		}
		output := FormatListResult(objects, FormatText)
		if !strings.Contains(output, "Found 2 object(s)") {
			t.Error("Expected count in output")
		}
		if !strings.Contains(output, "test/file1.txt") {
			t.Error("Expected first file in output")
		}
		if !strings.Contains(output, "test/file2.txt") {
			t.Error("Expected second file in output")
		}
	})

	t.Run("empty list json format", func(t *testing.T) {
		objects := []ObjectInfo{}
		output := FormatListResult(objects, FormatJSON)
		if !strings.Contains(output, `"count": 0`) {
			t.Error("Expected count 0 in JSON")
		}
	})

	t.Run("list with objects json format", func(t *testing.T) {
		objects := []ObjectInfo{
			{
				Key:          "test/file1.txt",
				Size:         1024,
				LastModified: time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC),
			},
		}
		output := FormatListResult(objects, FormatJSON)
		if !strings.Contains(output, `"count": 1`) {
			t.Error("Expected count 1 in JSON")
		}
		if !strings.Contains(output, "test/file1.txt") {
			t.Error("Expected file key in JSON")
		}
	})

	t.Run("empty list table format", func(t *testing.T) {
		objects := []ObjectInfo{}
		output := FormatListResult(objects, FormatTable)
		if !strings.Contains(output, "No objects found") {
			t.Error("Expected 'No objects found' message")
		}
	})

	t.Run("list with objects table format", func(t *testing.T) {
		objects := []ObjectInfo{
			{
				Key:          "test/file1.txt",
				Size:         1024,
				LastModified: time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC),
			},
			{
				Key:          "test/file2.txt",
				Size:         2048,
				LastModified: time.Date(2025, 1, 2, 12, 0, 0, 0, time.UTC),
			},
		}
		output := FormatListResult(objects, FormatTable)
		if !strings.Contains(output, "Key") {
			t.Error("Expected 'Key' header in table")
		}
		if !strings.Contains(output, "Size") {
			t.Error("Expected 'Size' header in table")
		}
		if !strings.Contains(output, "Total: 2 object(s)") {
			t.Error("Expected total count in table")
		}
	})

	t.Run("list with storage class", func(t *testing.T) {
		objects := []ObjectInfo{
			{
				Key:          "test/file1.txt",
				Size:         1024,
				LastModified: time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC),
				StorageClass: "STANDARD",
			},
		}
		output := FormatListResult(objects, FormatText)
		if !strings.Contains(output, "Storage Class: STANDARD") {
			t.Error("Expected storage class in output")
		}
	})
}

func TestFormatExistsResult(t *testing.T) {
	t.Run("exists true text format", func(t *testing.T) {
		output := FormatExistsResult("test/file.txt", true, FormatText)
		if !strings.Contains(output, "test/file.txt") {
			t.Error("Expected key in output")
		}
		if !strings.Contains(output, "true") {
			t.Error("Expected 'true' in output")
		}
	})

	t.Run("exists false text format", func(t *testing.T) {
		output := FormatExistsResult("test/file.txt", false, FormatText)
		if !strings.Contains(output, "false") {
			t.Error("Expected 'false' in output")
		}
	})

	t.Run("exists json format", func(t *testing.T) {
		output := FormatExistsResult("test/file.txt", true, FormatJSON)
		if !strings.Contains(output, `"exists": true`) {
			t.Error("Expected exists field in JSON")
		}
		if !strings.Contains(output, `"key": "test/file.txt"`) {
			t.Error("Expected key field in JSON")
		}
	})

	t.Run("exists table format", func(t *testing.T) {
		output := FormatExistsResult("test/file.txt", true, FormatTable)
		if !strings.Contains(output, "SUCCESS") {
			t.Error("Expected SUCCESS in table")
		}
	})
}

func TestFormatError(t *testing.T) {
	t.Run("error text format", func(t *testing.T) {
		err := &testError{msg: "test error"}
		output := FormatError(err, FormatText)
		if !strings.Contains(output, "Error:") {
			t.Error("Expected error prefix")
		}
		if !strings.Contains(output, "test error") {
			t.Error("Expected error message")
		}
	})

	t.Run("error json format", func(t *testing.T) {
		err := &testError{msg: "test error"}
		output := FormatError(err, FormatJSON)
		if !strings.Contains(output, `"success": false`) {
			t.Error("Expected success false in JSON")
		}
		if !strings.Contains(output, `"error": "test error"`) {
			t.Error("Expected error message in JSON")
		}
	})

	t.Run("error table format", func(t *testing.T) {
		err := &testError{msg: "test error"}
		output := FormatError(err, FormatTable)
		if !strings.Contains(output, "FAILED") {
			t.Error("Expected FAILED in table")
		}
		if !strings.Contains(output, "test error") {
			t.Error("Expected error message in table")
		}
	})
}

func TestConvertListResultToObjectInfo(t *testing.T) {
	t.Run("nil result", func(t *testing.T) {
		objects := ConvertListResultToObjectInfo(nil)
		if len(objects) != 0 {
			t.Error("Expected empty slice for nil result")
		}
	})

	t.Run("empty result", func(t *testing.T) {
		result := &common.ListResult{
			Objects: []*common.ObjectInfo{},
		}
		objects := ConvertListResultToObjectInfo(result)
		if len(objects) != 0 {
			t.Error("Expected empty slice")
		}
	})

	t.Run("with objects", func(t *testing.T) {
		now := time.Now()
		result := &common.ListResult{
			Objects: []*common.ObjectInfo{
				{
					Key: "test/file1.txt",
					Metadata: &common.Metadata{
						Size:         1024,
						LastModified: now,
						Custom: map[string]string{
							"storage_class": "STANDARD",
						},
					},
				},
				{
					Key: "test/file2.txt",
					Metadata: &common.Metadata{
						Size:         2048,
						LastModified: now.Add(time.Hour),
					},
				},
			},
		}
		objects := ConvertListResultToObjectInfo(result)
		if len(objects) != 2 {
			t.Errorf("Expected 2 objects, got %d", len(objects))
		}
		if objects[0].Key != "test/file1.txt" {
			t.Error("Incorrect key for first object")
		}
		if objects[0].Size != 1024 {
			t.Error("Incorrect size for first object")
		}
		if objects[0].StorageClass != "STANDARD" {
			t.Error("Incorrect storage class for first object")
		}
		if objects[1].Key != "test/file2.txt" {
			t.Error("Incorrect key for second object")
		}
	})
}

func TestFormatSize(t *testing.T) {
	tests := []struct {
		size     int64
		expected string
	}{
		{0, "0 B"},
		{100, "100 B"},
		{1023, "1023 B"},
		{1024, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{1048576, "1.0 MiB"},
		{1572864, "1.5 MiB"},
		{1073741824, "1.0 GiB"},
		{1610612736, "1.5 GiB"},
		{1099511627776, "1.0 TiB"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := formatSize(tt.size)
			if result != tt.expected {
				t.Errorf("formatSize(%d) = %q, want %q", tt.size, result, tt.expected)
			}
		})
	}
}

func TestWrapText(t *testing.T) {
	t.Run("short text", func(t *testing.T) {
		lines := wrapText("hello", 10)
		if len(lines) != 1 {
			t.Errorf("Expected 1 line, got %d", len(lines))
		}
		if lines[0] != "hello" {
			t.Error("Text was modified")
		}
	})

	t.Run("long text with spaces", func(t *testing.T) {
		lines := wrapText("this is a very long text that needs wrapping", 15)
		if len(lines) < 2 {
			t.Error("Expected multiple lines")
		}
		for _, line := range lines {
			if len(line) > 15 {
				t.Errorf("Line too long: %q (len=%d)", line, len(line))
			}
		}
	})

	t.Run("long text without spaces", func(t *testing.T) {
		text := "verylongtextwithoutanyspaces"
		lines := wrapText(text, 10)
		if len(lines) < 2 {
			t.Errorf("Expected multiple lines, got %d: %v", len(lines), lines)
		}
		// The function wraps at maxWidth when there are no spaces
		expectedLines := 3 // 28 chars / 10 = 2.8, so 3 lines
		if len(lines) != expectedLines {
			t.Errorf("Expected %d lines, got %d", expectedLines, len(lines))
		}
	})

	t.Run("exact length", func(t *testing.T) {
		lines := wrapText("exactly10c", 10)
		if len(lines) != 1 {
			t.Errorf("Expected 1 line, got %d", len(lines))
		}
	})

	t.Run("empty text", func(t *testing.T) {
		lines := wrapText("", 10)
		if len(lines) != 1 || lines[0] != "" {
			t.Error("Expected single empty line")
		}
	})

	t.Run("wrapping at word boundaries", func(t *testing.T) {
		lines := wrapText("hello world test", 11)
		// Should wrap to keep words together
		if len(lines) < 2 {
			t.Error("Expected multiple lines")
		}
	})
}

// testError is a simple error implementation for testing.
type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}

func TestFormatJSON_ErrorHandling(t *testing.T) {
	// Test with a type that can't be marshaled to JSON
	type unmarshalable struct {
		Ch chan int // channels can't be marshaled
	}

	output := formatJSON(unmarshalable{Ch: make(chan int)})
	if !strings.Contains(output, "error") {
		t.Error("Expected error message in output")
	}
	if !strings.Contains(output, "failed to marshal JSON") {
		t.Error("Expected marshal error message")
	}
}

func TestTableFormatLongMessages(t *testing.T) {
	t.Run("long message wrapping", func(t *testing.T) {
		result := &OperationResult{
			Success: true,
			Message: "This is a very long message that should be wrapped to fit within the table column width limit",
		}
		output := FormatOperationResult(result, FormatTable)
		lines := strings.Split(output, "\n")
		// Verify table structure is maintained
		if !strings.Contains(output, "│") {
			t.Error("Expected table borders")
		}
		// Each line should be properly formatted
		for _, line := range lines {
			if strings.Contains(line, "│") {
				// Table content lines should have consistent structure
				if strings.Count(line, "│") < 2 {
					t.Errorf("Invalid table row: %q", line)
				}
			}
		}
	})

	t.Run("long error wrapping", func(t *testing.T) {
		result := &OperationResult{
			Success: false,
			Error:   "This is a very long error message that should be wrapped to fit within the table column width limit",
		}
		output := FormatOperationResult(result, FormatTable)
		if !strings.Contains(output, "FAILED") {
			t.Error("Expected FAILED status")
		}
		// Should contain wrapped content
		lines := strings.Split(output, "\n")
		if len(lines) < 5 {
			t.Error("Expected multiple lines for wrapped error")
		}
	})
}

func TestFormatPoliciesResult(t *testing.T) {
	t.Run("empty policies text format", func(t *testing.T) {
		policies := []common.LifecyclePolicy{}
		output := FormatPoliciesResult(policies, FormatText)
		if !strings.Contains(output, "No lifecycle policies found") {
			t.Error("Expected 'No lifecycle policies found' message")
		}
	})

	t.Run("policies with data text format", func(t *testing.T) {
		policies := []common.LifecyclePolicy{
			{
				ID:        "policy1",
				Prefix:    "logs/",
				Retention: 24 * time.Hour,
				Action:    "delete",
			},
			{
				ID:        "policy2",
				Prefix:    "data/",
				Retention: 30 * 24 * time.Hour,
				Action:    "archive",
			},
		}
		output := FormatPoliciesResult(policies, FormatText)
		if !strings.Contains(output, "Found 2 lifecycle policy(ies)") {
			t.Error("Expected count in output")
		}
		if !strings.Contains(output, "policy1") {
			t.Error("Expected first policy ID")
		}
		if !strings.Contains(output, "policy2") {
			t.Error("Expected second policy ID")
		}
		if !strings.Contains(output, "logs/") {
			t.Error("Expected first prefix")
		}
		if !strings.Contains(output, "delete") {
			t.Error("Expected first action")
		}
		if !strings.Contains(output, "archive") {
			t.Error("Expected second action")
		}
	})

	t.Run("empty policies json format", func(t *testing.T) {
		policies := []common.LifecyclePolicy{}
		output := FormatPoliciesResult(policies, FormatJSON)
		if !strings.Contains(output, `"count": 0`) {
			t.Error("Expected count 0 in JSON")
		}
	})

	t.Run("policies with data json format", func(t *testing.T) {
		policies := []common.LifecyclePolicy{
			{
				ID:        "policy1",
				Prefix:    "logs/",
				Retention: 24 * time.Hour,
				Action:    "delete",
			},
		}
		output := FormatPoliciesResult(policies, FormatJSON)
		if !strings.Contains(output, `"count": 1`) {
			t.Error("Expected count 1 in JSON")
		}
		if !strings.Contains(output, "policy1") {
			t.Error("Expected policy ID in JSON")
		}
		if !strings.Contains(output, "logs/") {
			t.Error("Expected prefix in JSON")
		}
	})

	t.Run("empty policies table format", func(t *testing.T) {
		policies := []common.LifecyclePolicy{}
		output := FormatPoliciesResult(policies, FormatTable)
		if !strings.Contains(output, "No lifecycle policies found") {
			t.Error("Expected 'No lifecycle policies found' message")
		}
	})

	t.Run("policies with data table format", func(t *testing.T) {
		policies := []common.LifecyclePolicy{
			{
				ID:        "policy1",
				Prefix:    "logs/",
				Retention: 24 * time.Hour,
				Action:    "delete",
			},
			{
				ID:        "policy2",
				Prefix:    "data/",
				Retention: 30 * 24 * time.Hour,
				Action:    "archive",
			},
		}
		output := FormatPoliciesResult(policies, FormatTable)
		if !strings.Contains(output, "ID") {
			t.Error("Expected 'ID' header in table")
		}
		if !strings.Contains(output, "Prefix") {
			t.Error("Expected 'Prefix' header in table")
		}
		if !strings.Contains(output, "Retention") {
			t.Error("Expected 'Retention' header in table")
		}
		if !strings.Contains(output, "Action") {
			t.Error("Expected 'Action' header in table")
		}
		if !strings.Contains(output, "Total: 2 policy(ies)") {
			t.Error("Expected total count in table")
		}
	})
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		expected string
	}{
		{
			name:     "days",
			duration: 5 * 24 * time.Hour,
			expected: "5 days",
		},
		{
			name:     "single day",
			duration: 24 * time.Hour,
			expected: "1 days",
		},
		{
			name:     "hours",
			duration: 12 * time.Hour,
			expected: "12 hours",
		},
		{
			name:     "single hour",
			duration: 1 * time.Hour,
			expected: "1 hours",
		},
		{
			name:     "minutes",
			duration: 45 * time.Minute,
			expected: "45 minutes",
		},
		{
			name:     "single minute",
			duration: 1 * time.Minute,
			expected: "1 minutes",
		},
		{
			name:     "seconds",
			duration: 30 * time.Second,
			expected: "30 seconds",
		},
		{
			name:     "zero",
			duration: 0,
			expected: "0 seconds",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatDuration(tt.duration)
			if result != tt.expected {
				t.Errorf("formatDuration(%v) = %q, want %q", tt.duration, result, tt.expected)
			}
		})
	}
}

func TestFormatMetadataResult(t *testing.T) {
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

	t.Run("basic metadata text format", func(t *testing.T) {
		metadata := &common.Metadata{
			Size:         1024,
			LastModified: now,
		}
		output := FormatMetadataResult(metadata, FormatText)
		if !strings.Contains(output, "Metadata:") {
			t.Error("Expected 'Metadata:' header")
		}
		if !strings.Contains(output, "1.0 KiB") {
			t.Error("Expected formatted size")
		}
		if !strings.Contains(output, "2025-01-01") {
			t.Error("Expected formatted date")
		}
	})

	t.Run("metadata with content type text format", func(t *testing.T) {
		metadata := &common.Metadata{
			Size:         1024,
			LastModified: now,
			ContentType:  "application/json",
		}
		output := FormatMetadataResult(metadata, FormatText)
		if !strings.Contains(output, "Content Type: application/json") {
			t.Error("Expected content type in output")
		}
	})

	t.Run("metadata with content encoding text format", func(t *testing.T) {
		metadata := &common.Metadata{
			Size:            1024,
			LastModified:    now,
			ContentEncoding: "gzip",
		}
		output := FormatMetadataResult(metadata, FormatText)
		if !strings.Contains(output, "Content Encoding: gzip") {
			t.Error("Expected content encoding in output")
		}
	})

	t.Run("metadata with custom fields text format", func(t *testing.T) {
		metadata := &common.Metadata{
			Size:         1024,
			LastModified: now,
			Custom: map[string]string{
				"author":  "test",
				"version": "1.0",
			},
		}
		output := FormatMetadataResult(metadata, FormatText)
		if !strings.Contains(output, "Custom Fields:") {
			t.Error("Expected 'Custom Fields:' section")
		}
		if !strings.Contains(output, "author") {
			t.Error("Expected author in custom fields")
		}
		if !strings.Contains(output, "version") {
			t.Error("Expected version in custom fields")
		}
	})

	t.Run("metadata json format", func(t *testing.T) {
		metadata := &common.Metadata{
			Size:         1024,
			LastModified: now,
			ContentType:  "application/json",
		}
		output := FormatMetadataResult(metadata, FormatJSON)
		if !strings.Contains(output, `"size": 1024`) {
			t.Error("Expected size in JSON")
		}
		if !strings.Contains(output, `"content_type": "application/json"`) {
			t.Error("Expected content type in JSON")
		}
	})

	t.Run("metadata table format", func(t *testing.T) {
		metadata := &common.Metadata{
			Size:         1024,
			LastModified: now,
			ContentType:  "text/plain",
		}
		output := FormatMetadataResult(metadata, FormatTable)
		if !strings.Contains(output, "Field") {
			t.Error("Expected 'Field' header in table")
		}
		if !strings.Contains(output, "Value") {
			t.Error("Expected 'Value' header in table")
		}
		if !strings.Contains(output, "Size") {
			t.Error("Expected Size field in table")
		}
		if !strings.Contains(output, "1.0 KiB") {
			t.Error("Expected formatted size in table")
		}
	})
}

func TestFormatHealthResult(t *testing.T) {
	t.Run("health text format", func(t *testing.T) {
		health := map[string]any{
			"status":  "healthy",
			"version": version.Get(),
			"backend": "test",
		}
		output := FormatHealthResult(health, FormatText)
		if !strings.Contains(output, "Health Check:") {
			t.Error("Expected 'Health Check:' header")
		}
		if !strings.Contains(output, "status") {
			t.Error("Expected status in output")
		}
		if !strings.Contains(output, "healthy") {
			t.Error("Expected healthy status")
		}
		if !strings.Contains(output, "version") {
			t.Error("Expected version in output")
		}
	})

	t.Run("health json format", func(t *testing.T) {
		health := map[string]any{
			"status":  "healthy",
			"version": version.Get(),
		}
		output := FormatHealthResult(health, FormatJSON)
		if !strings.Contains(output, `"status": "healthy"`) {
			t.Error("Expected status in JSON")
		}
		if !strings.Contains(output, `"version": "`+version.Get()+`"`) {
			t.Error("Expected version in JSON")
		}
	})

	t.Run("health table format", func(t *testing.T) {
		health := map[string]any{
			"status":  "healthy",
			"backend": "test",
		}
		output := FormatHealthResult(health, FormatTable)
		if !strings.Contains(output, "Health Check") {
			t.Error("Expected 'Health Check' header in table")
		}
		if !strings.Contains(output, "status") {
			t.Error("Expected status field in table")
		}
		if !strings.Contains(output, "healthy") {
			t.Error("Expected healthy value in table")
		}
	})

	t.Run("health with various types", func(t *testing.T) {
		health := map[string]any{
			"status":       "healthy",
			"uptime":       12345,
			"requests":     100,
			"error_rate":   0.01,
			"is_available": true,
		}
		output := FormatHealthResult(health, FormatText)
		if !strings.Contains(output, "uptime") {
			t.Error("Expected uptime in output")
		}
		if !strings.Contains(output, "requests") {
			t.Error("Expected requests in output")
		}
		if !strings.Contains(output, "error_rate") {
			t.Error("Expected error_rate in output")
		}
		if !strings.Contains(output, "is_available") {
			t.Error("Expected is_available in output")
		}
	})
}
