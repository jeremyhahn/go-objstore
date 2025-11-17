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
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// OutputFormat defines the output format type.
type OutputFormat string

const (
	FormatText  OutputFormat = "text"
	FormatJSON  OutputFormat = "json"
	FormatTable OutputFormat = "table"
)

// ObjectInfo holds information about an object for output formatting.
type ObjectInfo struct {
	Key          string    `json:"key"`
	Size         int64     `json:"size"`
	LastModified time.Time `json:"last_modified"`
	StorageClass string    `json:"storage_class,omitempty"`
}

// OperationResult holds the result of an operation.
type OperationResult struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
	Error   string `json:"error,omitempty"`
	Data    any    `json:"data,omitempty"`
}

// FormatOperationResult formats an operation result in the specified format.
func FormatOperationResult(result *OperationResult, format OutputFormat) string {
	switch format {
	case FormatJSON:
		return formatJSON(result)
	case FormatTable:
		return formatResultTable(result)
	default:
		return formatResultText(result)
	}
}

// FormatListResult formats a list of objects in the specified format.
func FormatListResult(objects []ObjectInfo, format OutputFormat) string {
	switch format {
	case FormatJSON:
		return formatListJSON(objects)
	case FormatTable:
		return formatListTable(objects)
	default:
		return formatListText(objects)
	}
}

// FormatExistsResult formats an exists check result.
func FormatExistsResult(key string, exists bool, format OutputFormat) string {
	result := &OperationResult{
		Success: true,
		Message: fmt.Sprintf("Object '%s' exists: %v", key, exists),
		Data:    map[string]any{"key": key, "exists": exists},
	}
	return FormatOperationResult(result, format)
}

// FormatError formats an error message in the specified format.
func FormatError(err error, format OutputFormat) string {
	result := &OperationResult{
		Success: false,
		Error:   err.Error(),
	}
	return FormatOperationResult(result, format)
}

func formatResultText(result *OperationResult) string {
	if result.Success {
		if result.Message != "" {
			return result.Message + "\n"
		}
		return "Operation completed successfully\n"
	}
	return fmt.Sprintf("Error: %s\n", result.Error)
}

func formatResultTable(result *OperationResult) string {
	if result.Success {
		output := "┌────────────────────────────────────────────────────────┐\n"
		output += "│ Operation Result                                       │\n"
		output += "├────────────────────────────────────────────────────────┤\n"
		output += fmt.Sprintf("│ Status: %-47s │\n", "SUCCESS")
		if result.Message != "" {
			// Split message into lines and wrap if needed
			lines := wrapText(result.Message, 47)
			for _, line := range lines {
				output += fmt.Sprintf("│ %-54s │\n", line)
			}
		}
		output += "└────────────────────────────────────────────────────────┘\n"
		return output
	}

	output := "┌────────────────────────────────────────────────────────┐\n"
	output += "│ Operation Result                                       │\n"
	output += "├────────────────────────────────────────────────────────┤\n"
	output += fmt.Sprintf("│ Status: %-47s │\n", "FAILED")
	lines := wrapText(result.Error, 47)
	for _, line := range lines {
		output += fmt.Sprintf("│ %-54s │\n", line)
	}
	output += "└────────────────────────────────────────────────────────┘\n"
	return output
}

func formatJSON(v any) string {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprintf("{\"error\": \"failed to marshal JSON: %s\"}\n", err)
	}
	return string(data) + "\n"
}

func formatListText(objects []ObjectInfo) string {
	if len(objects) == 0 {
		return "No objects found\n"
	}

	var output string
	output += fmt.Sprintf("Found %d object(s):\n\n", len(objects))
	for _, obj := range objects {
		output += fmt.Sprintf("Key: %s\n", obj.Key)
		output += fmt.Sprintf("  Size: %s\n", formatSize(obj.Size))
		output += fmt.Sprintf("  Last Modified: %s\n", obj.LastModified.Format(time.RFC3339))
		if obj.StorageClass != "" {
			output += fmt.Sprintf("  Storage Class: %s\n", obj.StorageClass)
		}
		output += "\n"
	}
	return output
}

func formatListTable(objects []ObjectInfo) string {
	if len(objects) == 0 {
		return "No objects found\n"
	}

	var output string
	output += "┌────────────────────────────────────┬──────────────┬──────────────────────┐\n"
	output += "│ Key                                │ Size         │ Last Modified        │\n"
	output += "├────────────────────────────────────┼──────────────┼──────────────────────┤\n"

	for _, obj := range objects {
		key := truncate(obj.Key, 34)
		size := formatSize(obj.Size)
		modified := obj.LastModified.Format("2006-01-02 15:04:05")
		output += fmt.Sprintf("│ %-34s │ %-12s │ %-20s │\n", key, size, modified)
	}

	output += "└────────────────────────────────────┴──────────────┴──────────────────────┘\n"
	output += fmt.Sprintf("Total: %d object(s)\n", len(objects))
	return output
}

func formatListJSON(objects []ObjectInfo) string {
	result := map[string]any{
		"count":   len(objects),
		"objects": objects,
	}
	return formatJSON(result)
}

// ConvertListResultToObjectInfo converts common.ListResult to []ObjectInfo.
func ConvertListResultToObjectInfo(result *common.ListResult) []ObjectInfo {
	if result == nil {
		return []ObjectInfo{}
	}

	objects := make([]ObjectInfo, len(result.Objects))
	for i, obj := range result.Objects {
		var size int64
		var lastModified time.Time
		var storageClass string

		if obj.Metadata != nil {
			size = obj.Metadata.Size
			lastModified = obj.Metadata.LastModified
			// Storage class is typically in custom metadata
			if obj.Metadata.Custom != nil {
				storageClass = obj.Metadata.Custom["storage_class"]
			}
		}

		objects[i] = ObjectInfo{
			Key:          obj.Key,
			Size:         size,
			LastModified: lastModified,
			StorageClass: storageClass,
		}
	}
	return objects
}

// formatSize formats a byte size into a human-readable string.
func formatSize(size int64) string {
	const unit = 1024
	if size < unit {
		return fmt.Sprintf("%d B", size)
	}
	div, exp := int64(unit), 0
	for n := size / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(size)/float64(div), "KMGTPE"[exp])
}

// wrapText wraps text to fit within maxWidth characters.
func wrapText(text string, maxWidth int) []string {
	if len(text) <= maxWidth {
		return []string{text}
	}

	// Check if text has no spaces - need to hard wrap
	if !strings.Contains(text, " ") {
		var lines []string
		for len(text) > maxWidth {
			lines = append(lines, text[:maxWidth])
			text = text[maxWidth:]
		}
		if len(text) > 0 {
			lines = append(lines, text)
		}
		return lines
	}

	// Text has spaces - wrap at word boundaries
	var lines []string
	words := strings.Fields(text)
	var currentLine string
	for _, word := range words {
		if len(currentLine) == 0 {
			currentLine = word
		} else if len(currentLine)+1+len(word) <= maxWidth {
			currentLine += " " + word
		} else {
			lines = append(lines, currentLine)
			currentLine = word
		}
	}
	if len(currentLine) > 0 {
		lines = append(lines, currentLine)
	}
	return lines
}

// FormatPoliciesResult formats a list of lifecycle policies in the specified format.
func FormatPoliciesResult(policies []common.LifecyclePolicy, format OutputFormat) string {
	switch format {
	case FormatJSON:
		return formatPoliciesJSON(policies)
	case FormatTable:
		return formatPoliciesTable(policies)
	default:
		return formatPoliciesText(policies)
	}
}

func formatPoliciesText(policies []common.LifecyclePolicy) string {
	if len(policies) == 0 {
		return "No lifecycle policies found\n"
	}

	var output string
	output += fmt.Sprintf("Found %d lifecycle policy(ies):\n\n", len(policies))
	for _, policy := range policies {
		output += fmt.Sprintf("ID: %s\n", policy.ID)
		output += fmt.Sprintf("  Prefix: %s\n", policy.Prefix)
		output += fmt.Sprintf("  Retention: %s\n", formatDuration(policy.Retention))
		output += fmt.Sprintf("  Action: %s\n", policy.Action)
		output += "\n"
	}
	return output
}

func formatPoliciesTable(policies []common.LifecyclePolicy) string {
	if len(policies) == 0 {
		return "No lifecycle policies found\n"
	}

	var output string
	output += "┌──────────────────┬──────────────────┬──────────────┬──────────┐\n"
	output += "│ ID               │ Prefix           │ Retention    │ Action   │\n"
	output += "├──────────────────┼──────────────────┼──────────────┼──────────┤\n"

	for _, policy := range policies {
		id := truncate(policy.ID, 16)
		prefix := truncate(policy.Prefix, 16)
		retention := formatDuration(policy.Retention)
		action := truncate(policy.Action, 8)
		output += fmt.Sprintf("│ %-16s │ %-16s │ %-12s │ %-8s │\n", id, prefix, retention, action)
	}

	output += "└──────────────────┴──────────────────┴──────────────┴──────────┘\n"
	output += fmt.Sprintf("Total: %d policy(ies)\n", len(policies))
	return output
}

func formatPoliciesJSON(policies []common.LifecyclePolicy) string {
	// Convert policies to a JSON-friendly format
	type policyJSON struct {
		ID        string `json:"id"`
		Prefix    string `json:"prefix"`
		Retention string `json:"retention"`
		Action    string `json:"action"`
	}

	jsonPolicies := make([]policyJSON, len(policies))
	for i, policy := range policies {
		jsonPolicies[i] = policyJSON{
			ID:        policy.ID,
			Prefix:    policy.Prefix,
			Retention: formatDuration(policy.Retention),
			Action:    policy.Action,
		}
	}

	result := map[string]any{
		"count":    len(jsonPolicies),
		"policies": jsonPolicies,
	}
	return formatJSON(result)
}

// formatDuration formats a time.Duration into a human-readable string.
func formatDuration(d time.Duration) string {
	days := int(d.Hours() / 24)
	if days > 0 {
		return fmt.Sprintf("%d days", days)
	}
	hours := int(d.Hours())
	if hours > 0 {
		return fmt.Sprintf("%d hours", hours)
	}
	minutes := int(d.Minutes())
	if minutes > 0 {
		return fmt.Sprintf("%d minutes", minutes)
	}
	return fmt.Sprintf("%.0f seconds", d.Seconds())
}

// FormatMetadataResult formats metadata in the specified format.
func FormatMetadataResult(metadata *common.Metadata, format OutputFormat) string {
	if metadata == nil {
		return FormatError(fmt.Errorf("metadata not found"), format)
	}
	switch format {
	case FormatJSON:
		return formatMetadataJSON(metadata)
	case FormatTable:
		return formatMetadataTable(metadata)
	default:
		return formatMetadataText(metadata)
	}
}

func formatMetadataText(metadata *common.Metadata) string {
	var output string
	output += "Metadata:\n"
	output += fmt.Sprintf("  Size: %s\n", formatSize(metadata.Size))
	output += fmt.Sprintf("  Last Modified: %s\n", metadata.LastModified.Format(time.RFC3339))
	if metadata.ContentType != "" {
		output += fmt.Sprintf("  Content Type: %s\n", metadata.ContentType)
	}
	if metadata.ContentEncoding != "" {
		output += fmt.Sprintf("  Content Encoding: %s\n", metadata.ContentEncoding)
	}
	if len(metadata.Custom) > 0 {
		output += "  Custom Fields:\n"
		for k, v := range metadata.Custom {
			output += fmt.Sprintf("    %s: %s\n", k, v)
		}
	}
	return output
}

func formatMetadataTable(metadata *common.Metadata) string {
	var output string
	output += "┌──────────────────────┬────────────────────────────────────────┐\n"
	output += "│ Field                │ Value                                  │\n"
	output += "├──────────────────────┼────────────────────────────────────────┤\n"
	output += fmt.Sprintf("│ %-20s │ %-38s │\n", "Size", formatSize(metadata.Size))
	output += fmt.Sprintf("│ %-20s │ %-38s │\n", "Last Modified", metadata.LastModified.Format(time.RFC3339))
	if metadata.ContentType != "" {
		output += fmt.Sprintf("│ %-20s │ %-38s │\n", "Content Type", truncate(metadata.ContentType, 38))
	}
	if metadata.ContentEncoding != "" {
		output += fmt.Sprintf("│ %-20s │ %-38s │\n", "Content Encoding", truncate(metadata.ContentEncoding, 38))
	}
	if len(metadata.Custom) > 0 {
		for k, v := range metadata.Custom {
			output += fmt.Sprintf("│ %-20s │ %-38s │\n", truncate(k, 20), truncate(v, 38))
		}
	}
	output += "└──────────────────────┴────────────────────────────────────────┘\n"
	return output
}

func formatMetadataJSON(metadata *common.Metadata) string {
	type metadataJSON struct {
		Size            int64             `json:"size"`
		LastModified    string            `json:"last_modified"`
		ContentType     string            `json:"content_type,omitempty"`
		ContentEncoding string            `json:"content_encoding,omitempty"`
		Custom          map[string]string `json:"custom,omitempty"`
	}

	result := metadataJSON{
		Size:            metadata.Size,
		LastModified:    metadata.LastModified.Format(time.RFC3339),
		ContentType:     metadata.ContentType,
		ContentEncoding: metadata.ContentEncoding,
		Custom:          metadata.Custom,
	}
	return formatJSON(result)
}

// FormatHealthResult formats a health check result.
func FormatHealthResult(health map[string]any, format OutputFormat) string {
	switch format {
	case FormatJSON:
		return formatJSON(health)
	case FormatTable:
		return formatHealthTable(health)
	default:
		return formatHealthText(health)
	}
}

func formatHealthText(health map[string]any) string {
	var output string
	output += "Health Check:\n"
	for k, v := range health {
		output += fmt.Sprintf("  %s: %v\n", k, v)
	}
	return output
}

func formatHealthTable(health map[string]any) string {
	var output string
	output += "┌──────────────────────┬────────────────────────────────────────┐\n"
	output += "│ Health Check                                                  │\n"
	output += "├──────────────────────┼────────────────────────────────────────┤\n"
	for k, v := range health {
		output += fmt.Sprintf("│ %-20s │ %-38v │\n", truncate(k, 20), truncate(fmt.Sprint(v), 38))
	}
	output += "└──────────────────────┴────────────────────────────────────────┘\n"
	return output
}
