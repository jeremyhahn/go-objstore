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

package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
	"github.com/jeremyhahn/go-objstore/pkg/version"
)

// Constants
const (
	actionDelete  = "delete"
	actionArchive = "archive"
)

// Tool represents an MCP tool definition
type Tool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

// ToolRegistry manages available tools
type ToolRegistry struct {
	tools map[string]Tool
}

// NewToolRegistry creates a new tool registry
func NewToolRegistry() *ToolRegistry {
	return &ToolRegistry{
		tools: make(map[string]Tool),
	}
}

// RegisterDefaultTools registers all default objstore tools
func (r *ToolRegistry) RegisterDefaultTools() {
	r.tools["objstore_put"] = Tool{
		Name:        "objstore_put",
		Description: "Upload an object to the object store. Stores data with a given key.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"key": map[string]any{
					"type":        "string",
					"description": "The storage key/path for the object",
				},
				"data": map[string]any{
					"type":        "string",
					"description": "The content to store (string or base64 encoded)",
				},
				"metadata": map[string]any{
					"type":        "object",
					"description": "Optional metadata for the object",
					"properties": map[string]any{
						"content_type": map[string]any{
							"type":        "string",
							"description": "MIME type of the content",
						},
						"content_encoding": map[string]any{
							"type":        "string",
							"description": "Content encoding (e.g., gzip)",
						},
						"custom": map[string]any{
							"type":        "object",
							"description": "Custom metadata key-value pairs",
						},
					},
				},
			},
			"required": []string{"key", "data"},
		},
	}

	r.tools["objstore_get"] = Tool{
		Name:        "objstore_get",
		Description: "Download an object from the object store. Retrieves data by key.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"key": map[string]any{
					"type":        "string",
					"description": "The storage key/path of the object to retrieve",
				},
			},
			"required": []string{"key"},
		},
	}

	r.tools["objstore_delete"] = Tool{
		Name:        "objstore_delete",
		Description: "Delete an object from the object store. Removes the object identified by key.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"key": map[string]any{
					"type":        "string",
					"description": "The storage key/path of the object to delete",
				},
			},
			"required": []string{"key"},
		},
	}

	r.tools["objstore_list"] = Tool{
		Name:        "objstore_list",
		Description: "List objects in the object store. Returns objects matching the given prefix.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"prefix": map[string]any{
					"type":        "string",
					"description": "Prefix to filter objects (empty string lists all)",
				},
				"max_results": map[string]any{
					"type":        "integer",
					"description": "Maximum number of results to return (0 for all)",
				},
				"continue_from": map[string]any{
					"type":        "string",
					"description": "Pagination token from previous list operation",
				},
			},
		},
	}

	r.tools["objstore_exists"] = Tool{
		Name:        "objstore_exists",
		Description: "Check if an object exists in the object store. Returns true if the object exists.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"key": map[string]any{
					"type":        "string",
					"description": "The storage key/path to check",
				},
			},
			"required": []string{"key"},
		},
	}

	r.tools["objstore_get_metadata"] = Tool{
		Name:        "objstore_get_metadata",
		Description: "Get metadata for an object without downloading it. Returns size, content type, last modified, etc.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"key": map[string]any{
					"type":        "string",
					"description": "The storage key/path of the object",
				},
			},
			"required": []string{"key"},
		},
	}

	r.tools["objstore_update_metadata"] = Tool{
		Name:        "objstore_update_metadata",
		Description: "Update metadata for an existing object. Allows updating content type, encoding, and custom metadata.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"key": map[string]any{
					"type":        "string",
					"description": "The storage key/path of the object",
				},
				"metadata": map[string]any{
					"type":        "object",
					"description": "Metadata to update",
					"properties": map[string]any{
						"content_type": map[string]any{
							"type":        "string",
							"description": "MIME type of the content",
						},
						"content_encoding": map[string]any{
							"type":        "string",
							"description": "Content encoding (e.g., gzip)",
						},
						"custom": map[string]any{
							"type":        "object",
							"description": "Custom metadata key-value pairs",
						},
					},
				},
			},
			"required": []string{"key", "metadata"},
		},
	}

	r.tools["objstore_health"] = Tool{
		Name:        "objstore_health",
		Description: "Check the health status of the object store. Returns status and version information.",
		InputSchema: map[string]any{
			"type":       "object",
			"properties": map[string]any{},
		},
	}

	r.tools["objstore_archive"] = Tool{
		Name:        "objstore_archive",
		Description: "Archive an object to an archival storage backend (e.g., Glacier, Azure Archive). The object remains in the source but is also copied to long-term storage.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"key": map[string]any{
					"type":        "string",
					"description": "The storage key/path of the object to archive",
				},
				"destination_type": map[string]any{
					"type":        "string",
					"description": "Type of archival backend (e.g., 'glacier', 'azurearchive')",
				},
				"destination_settings": map[string]any{
					"type":        "object",
					"description": "Configuration settings for the archival backend",
				},
			},
			"required": []string{"key", "destination_type"},
		},
	}

	r.tools["objstore_add_policy"] = Tool{
		Name:        "objstore_add_policy",
		Description: "Add a lifecycle policy for automatic object management. Policies can delete or archive objects after a retention period.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"id": map[string]any{
					"type":        "string",
					"description": "Unique identifier for the policy",
				},
				"prefix": map[string]any{
					"type":        "string",
					"description": "Key prefix to apply policy to (empty for all objects)",
				},
				"retention_seconds": map[string]any{
					"type":        "integer",
					"description": "Retention period in seconds before action is taken",
				},
				"action": map[string]any{
					"type":        "string",
					"description": "Action to take: 'delete' or 'archive'",
					"enum":        []string{"delete", "archive"},
				},
				"destination_type": map[string]any{
					"type":        "string",
					"description": "Required for 'archive' action. Type of archival backend.",
				},
				"destination_settings": map[string]any{
					"type":        "object",
					"description": "Configuration settings for archival backend (for 'archive' action)",
				},
			},
			"required": []string{"id", "retention_seconds", "action"},
		},
	}

	r.tools["objstore_remove_policy"] = Tool{
		Name:        "objstore_remove_policy",
		Description: "Remove an existing lifecycle policy by its ID. This stops the policy from being applied to objects.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"id": map[string]any{
					"type":        "string",
					"description": "The ID of the policy to remove",
				},
			},
			"required": []string{"id"},
		},
	}

	r.tools["objstore_get_policies"] = Tool{
		Name:        "objstore_get_policies",
		Description: "List all lifecycle policies with optional prefix filtering. Returns all configured policies and their settings.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"prefix": map[string]any{
					"type":        "string",
					"description": "Optional prefix filter to show only policies matching this prefix",
				},
			},
		},
	}

	r.tools["objstore_apply_policies"] = Tool{
		Name:        "objstore_apply_policies",
		Description: "Apply all lifecycle policies immediately. Executes deletion or archival actions based on configured retention periods.",
		InputSchema: map[string]any{
			"type":       "object",
			"properties": map[string]any{},
		},
	}

	// Register replication tools
	r.RegisterReplicationTools()
}

// ListTools returns all registered tools
func (r *ToolRegistry) ListTools() []Tool {
	tools := make([]Tool, 0, len(r.tools))
	for _, tool := range r.tools {
		tools = append(tools, tool)
	}
	return tools
}

// GetTool retrieves a tool by name
func (r *ToolRegistry) GetTool(name string) (Tool, bool) {
	tool, ok := r.tools[name]
	return tool, ok
}

// ToolExecutor executes tool calls
type ToolExecutor struct {
	backend string // Backend name (empty = default)
}

// NewToolExecutor creates a new tool executor
func NewToolExecutor(backend string) *ToolExecutor {
	return &ToolExecutor{
		backend: backend,
	}
}

// keyRef builds a key reference with optional backend prefix.
func (e *ToolExecutor) keyRef(key string) string {
	if e.backend == "" {
		return key
	}
	return e.backend + ":" + key
}

// Execute executes a tool call
func (e *ToolExecutor) Execute(ctx context.Context, toolName string, args map[string]any) (string, error) {
	switch toolName {
	case "objstore_put":
		return e.executePut(ctx, args)
	case "objstore_get":
		return e.executeGet(ctx, args)
	case "objstore_delete":
		return e.executeDelete(ctx, args)
	case "objstore_list":
		return e.executeList(ctx, args)
	case "objstore_exists":
		return e.executeExists(ctx, args)
	case "objstore_get_metadata":
		return e.executeGetMetadata(ctx, args)
	case "objstore_update_metadata":
		return e.executeUpdateMetadata(ctx, args)
	case "objstore_health":
		return e.executeHealth(ctx, args)
	case "objstore_archive":
		return e.executeArchive(ctx, args)
	case "objstore_add_policy":
		return e.executeAddPolicy(ctx, args)
	case "objstore_remove_policy":
		return e.executeRemovePolicy(ctx, args)
	case "objstore_get_policies":
		return e.executeGetPolicies(ctx, args)
	case "objstore_apply_policies":
		return e.executeApplyPolicies(ctx, args)
	case "objstore_add_replication_policy":
		return e.executeAddReplicationPolicy(ctx, args)
	case "objstore_remove_replication_policy":
		return e.executeRemoveReplicationPolicy(ctx, args)
	case "objstore_list_replication_policies":
		return e.executeListReplicationPolicies(ctx, args)
	case "objstore_get_replication_policy":
		return e.executeGetReplicationPolicy(ctx, args)
	case "objstore_trigger_replication":
		return e.executeTriggerReplication(ctx, args)
	case "objstore_get_replication_status":
		return e.executeGetReplicationStatus(ctx, args)
	default:
		return "", ErrUnknownTool
	}
}

// executePut executes the objstore_put tool
func (e *ToolExecutor) executePut(ctx context.Context, args map[string]any) (string, error) {
	key, ok := args["key"].(string)
	if !ok || key == "" {
		return "", ErrMissingParameter
	}

	data, ok := args["data"].(string)
	if !ok {
		return "", ErrMissingParameter
	}

	reader := strings.NewReader(data)

	// Check for metadata
	var metadata *common.Metadata
	if metaRaw, ok := args["metadata"]; ok {
		if metaMap, ok := metaRaw.(map[string]any); ok {
			metadata = &common.Metadata{}

			if ct, ok := metaMap["content_type"].(string); ok {
				metadata.ContentType = ct
			}
			if ce, ok := metaMap["content_encoding"].(string); ok {
				metadata.ContentEncoding = ce
			}
			if customRaw, ok := metaMap["custom"].(map[string]any); ok {
				metadata.Custom = make(map[string]string)
				for k, v := range customRaw {
					if strVal, ok := v.(string); ok {
						metadata.Custom[k] = strVal
					}
				}
			}
		}
	}

	// Store the object using facade
	var err error
	if metadata != nil {
		err = objstore.PutWithMetadata(ctx, e.keyRef(key), reader, metadata)
	} else {
		err = objstore.PutWithContext(ctx, e.keyRef(key), reader)
	}

	if err != nil {
		return "", err
	}

	result := map[string]any{
		"success": true,
		"key":     key,
		"size":    len(data),
	}

	jsonResult, _ := json.MarshalIndent(result, "", "  ")
	return string(jsonResult), nil
}

// executeGet executes the objstore_get tool
func (e *ToolExecutor) executeGet(ctx context.Context, args map[string]any) (string, error) {
	key, ok := args["key"].(string)
	if !ok || key == "" {
		return "", ErrMissingParameter
	}

	// Get object using facade
	reader, err := objstore.GetWithContext(ctx, e.keyRef(key))
	if err != nil {
		return "", err
	}
	defer func() { _ = reader.Close() }()

	var buf bytes.Buffer
	size, err := io.Copy(&buf, reader)
	if err != nil {
		return "", err
	}

	result := map[string]any{
		"success": true,
		"key":     key,
		"size":    size,
		"data":    buf.String(),
	}

	jsonResult, _ := json.MarshalIndent(result, "", "  ")
	return string(jsonResult), nil
}

// executeDelete executes the objstore_delete tool
func (e *ToolExecutor) executeDelete(ctx context.Context, args map[string]any) (string, error) {
	key, ok := args["key"].(string)
	if !ok || key == "" {
		return "", ErrMissingParameter
	}

	// Delete object using facade
	err := objstore.DeleteWithContext(ctx, e.keyRef(key))
	if err != nil {
		return "", err
	}

	result := map[string]any{
		"success": true,
		"key":     key,
		"deleted": true,
	}

	jsonResult, _ := json.MarshalIndent(result, "", "  ")
	return string(jsonResult), nil
}

// executeList executes the objstore_list tool
func (e *ToolExecutor) executeList(ctx context.Context, args map[string]any) (string, error) {
	prefix, _ := args["prefix"].(string)

	opts := &common.ListOptions{
		Prefix: prefix,
	}

	if maxResults, ok := args["max_results"].(float64); ok {
		opts.MaxResults = int(maxResults)
	}

	if continueFrom, ok := args["continue_from"].(string); ok {
		opts.ContinueFrom = continueFrom
	}

	// List objects using facade
	listResult, err := objstore.ListWithOptions(ctx, e.backend, opts)
	if err != nil {
		return "", err
	}

	keys := make([]string, len(listResult.Objects))
	for i, obj := range listResult.Objects {
		keys[i] = obj.Key
	}

	result := map[string]any{
		"success":    true,
		"prefix":     prefix,
		"count":      len(keys),
		"keys":       keys,
		"truncated":  listResult.Truncated,
		"next_token": listResult.NextToken,
	}

	jsonResult, _ := json.MarshalIndent(result, "", "  ")
	return string(jsonResult), nil
}

// executeExists executes the objstore_exists tool
func (e *ToolExecutor) executeExists(ctx context.Context, args map[string]any) (string, error) {
	key, ok := args["key"].(string)
	if !ok || key == "" {
		return "", ErrMissingParameter
	}

	// Check existence using facade
	exists, err := objstore.Exists(ctx, e.keyRef(key))
	if err != nil {
		return "", err
	}

	result := map[string]any{
		"success": true,
		"key":     key,
		"exists":  exists,
	}

	jsonResult, _ := json.MarshalIndent(result, "", "  ")
	return string(jsonResult), nil
}

// executeGetMetadata executes the objstore_get_metadata tool
func (e *ToolExecutor) executeGetMetadata(ctx context.Context, args map[string]any) (string, error) {
	key, ok := args["key"].(string)
	if !ok || key == "" {
		return "", ErrMissingParameter
	}

	// Get metadata using facade
	metadata, err := objstore.GetMetadata(ctx, e.keyRef(key))
	if err != nil {
		return "", err
	}

	result := map[string]any{
		"success":          true,
		"key":              key,
		"size":             metadata.Size,
		"content_type":     metadata.ContentType,
		"content_encoding": metadata.ContentEncoding,
		"last_modified":    metadata.LastModified.Format("2006-01-02T15:04:05Z07:00"),
		"etag":             metadata.ETag,
		"custom":           metadata.Custom,
	}

	jsonResult, _ := json.MarshalIndent(result, "", "  ")
	return string(jsonResult), nil
}

// executeUpdateMetadata executes the objstore_update_metadata tool
func (e *ToolExecutor) executeUpdateMetadata(ctx context.Context, args map[string]any) (string, error) {
	key, ok := args["key"].(string)
	if !ok || key == "" {
		return "", ErrMissingParameter
	}

	metaRaw, ok := args["metadata"]
	if !ok {
		return "", ErrMissingParameter
	}

	metaMap, ok := metaRaw.(map[string]any)
	if !ok {
		return "", ErrInvalidParameter
	}

	metadata := &common.Metadata{}

	if ct, ok := metaMap["content_type"].(string); ok {
		metadata.ContentType = ct
	}
	if ce, ok := metaMap["content_encoding"].(string); ok {
		metadata.ContentEncoding = ce
	}
	if customRaw, ok := metaMap["custom"].(map[string]any); ok {
		metadata.Custom = make(map[string]string)
		for k, v := range customRaw {
			if strVal, ok := v.(string); ok {
				metadata.Custom[k] = strVal
			}
		}
	}

	// Update metadata using facade
	err := objstore.UpdateMetadata(ctx, e.keyRef(key), metadata)
	if err != nil {
		return "", err
	}

	result := map[string]any{
		"success": true,
		"key":     key,
		"updated": true,
	}

	jsonResult, _ := json.MarshalIndent(result, "", "  ")
	return string(jsonResult), nil
}

// executeHealth executes the objstore_health tool
func (e *ToolExecutor) executeHealth(ctx context.Context, args map[string]any) (string, error) {
	result := map[string]any{
		"status":  "healthy",
		"version": version.Get(),
	}

	jsonResult, _ := json.MarshalIndent(result, "", "  ")
	return string(jsonResult), nil
}

// executeArchive executes the objstore_archive tool
func (e *ToolExecutor) executeArchive(ctx context.Context, args map[string]any) (string, error) {
	key, ok := args["key"].(string)
	if !ok || key == "" {
		return "", ErrMissingParameter
	}

	destinationType, ok := args["destination_type"].(string)
	if !ok || destinationType == "" {
		return "", ErrMissingParameter
	}

	// Extract destination settings
	var destinationSettings map[string]string
	if settingsRaw, ok := args["destination_settings"]; ok {
		if settingsMap, ok := settingsRaw.(map[string]any); ok {
			destinationSettings = make(map[string]string)
			for k, v := range settingsMap {
				if strVal, ok := v.(string); ok {
					destinationSettings[k] = strVal
				}
			}
		}
	}

	// Create archiver from factory
	archiver, err := createArchiver(destinationType, destinationSettings)
	if err != nil {
		return "", err
	}

	// Perform archive operation using facade
	err = objstore.Archive(e.keyRef(key), archiver)
	if err != nil {
		return "", err
	}

	result := map[string]any{
		"success":     true,
		"key":         key,
		"destination": destinationType,
		"archived":    true,
	}

	jsonResult, _ := json.MarshalIndent(result, "", "  ")
	return string(jsonResult), nil
}

// executeAddPolicy executes the objstore_add_policy tool
func (e *ToolExecutor) executeAddPolicy(ctx context.Context, args map[string]any) (string, error) {
	id, ok := args["id"].(string)
	if !ok || id == "" {
		return "", ErrMissingParameter
	}

	action, ok := args["action"].(string)
	if !ok || action == "" {
		return "", ErrMissingParameter
	}

	if action != actionDelete && action != actionArchive {
		return "", ErrInvalidAction
	}

	retentionRaw, ok := args["retention_seconds"]
	if !ok {
		return "", ErrMissingParameter
	}

	// Handle both integer and float64 from JSON
	var retentionSeconds int64
	switch v := retentionRaw.(type) {
	case float64:
		retentionSeconds = int64(v)
	case int64:
		retentionSeconds = v
	case int:
		retentionSeconds = int64(v)
	default:
		return "", ErrInvalidParameter
	}

	if retentionSeconds <= 0 {
		return "", ErrRetentionMustBePositive
	}

	prefix, _ := args["prefix"].(string)

	// Build lifecycle policy
	policy := common.LifecyclePolicy{
		ID:        id,
		Prefix:    prefix,
		Retention: time.Duration(retentionSeconds) * time.Second,
		Action:    action,
	}

	// Create archiver if action is "archive"
	if action == "archive" {
		destinationType, ok := args["destination_type"].(string)
		if !ok || destinationType == "" {
			return "", ErrDestinationTypeRequired
		}

		var destinationSettings map[string]string
		if settingsRaw, ok := args["destination_settings"]; ok {
			if settingsMap, ok := settingsRaw.(map[string]any); ok {
				destinationSettings = make(map[string]string)
				for k, v := range settingsMap {
					if strVal, ok := v.(string); ok {
						destinationSettings[k] = strVal
					}
				}
			}
		}

		archiver, err := createArchiver(destinationType, destinationSettings)
		if err != nil {
			return "", err
		}
		policy.Destination = archiver
	}

	// Add policy using facade
	err := objstore.AddPolicy(e.backend, policy)
	if err != nil {
		return "", err
	}

	result := map[string]any{
		"success": true,
		"id":      id,
		"added":   true,
	}

	jsonResult, _ := json.MarshalIndent(result, "", "  ")
	return string(jsonResult), nil
}

// executeRemovePolicy executes the objstore_remove_policy tool
func (e *ToolExecutor) executeRemovePolicy(ctx context.Context, args map[string]any) (string, error) {
	id, ok := args["id"].(string)
	if !ok || id == "" {
		return "", ErrMissingParameter
	}

	// Remove policy using facade
	err := objstore.RemovePolicy(e.backend, id)
	if err != nil {
		return "", err
	}

	result := map[string]any{
		"success": true,
		"id":      id,
		"removed": true,
	}

	jsonResult, _ := json.MarshalIndent(result, "", "  ")
	return string(jsonResult), nil
}

// executeGetPolicies executes the objstore_get_policies tool
func (e *ToolExecutor) executeGetPolicies(ctx context.Context, args map[string]any) (string, error) {
	prefix, _ := args["prefix"].(string)

	// Get policies using facade
	policies, err := objstore.GetPolicies(e.backend)
	if err != nil {
		return "", err
	}

	// Filter by prefix if specified
	var filteredPolicies []common.LifecyclePolicy
	for _, policy := range policies {
		if prefix == "" || policy.Prefix == prefix {
			filteredPolicies = append(filteredPolicies, policy)
		}
	}

	// Convert policies to JSON-friendly format
	policyResults := make([]map[string]any, len(filteredPolicies))
	for i, policy := range filteredPolicies {
		policyResults[i] = map[string]any{
			"id":                policy.ID,
			"prefix":            policy.Prefix,
			"retention_seconds": int64(policy.Retention),
			"action":            policy.Action,
		}
	}

	result := map[string]any{
		"success":  true,
		"policies": policyResults,
		"count":    len(policyResults),
	}

	jsonResult, _ := json.MarshalIndent(result, "", "  ")
	return string(jsonResult), nil
}

func (e *ToolExecutor) executeApplyPolicies(ctx context.Context, args map[string]any) (string, error) {
	// Get policies using facade
	policies, err := objstore.GetPolicies(e.backend)
	if err != nil {
		return "", err
	}

	if len(policies) == 0 {
		result := map[string]any{
			"success":           true,
			"message":           "no lifecycle policies to apply",
			"policies_count":    0,
			"objects_processed": 0,
		}
		jsonResult, _ := json.MarshalIndent(result, "", "  ")
		return string(jsonResult), nil
	}

	// Apply policies by listing objects and checking retention
	objectsProcessed := 0
	opts := &common.ListOptions{
		Prefix: "",
	}
	// List objects using facade
	listResult, err := objstore.ListWithOptions(ctx, e.backend, opts)
	if err != nil {
		return "", err
	}

	for _, policy := range policies {
		for _, obj := range listResult.Objects {
			// Check if object matches policy prefix
			if policy.Prefix != "" && !strings.HasPrefix(obj.Key, policy.Prefix) {
				continue
			}

			// Get metadata to check last modified time
			if obj.Metadata == nil {
				continue
			}

			// Check if object is older than retention period
			age := time.Since(obj.Metadata.LastModified)
			if age <= policy.Retention {
				continue
			}

			// Apply action using facade
			switch policy.Action {
			case "delete":
				if err := objstore.DeleteWithContext(ctx, e.keyRef(obj.Key)); err != nil {
					// Log error but continue
					continue
				}
				objectsProcessed++
			case "archive":
				if policy.Destination != nil {
					if err := objstore.Archive(e.keyRef(obj.Key), policy.Destination); err != nil {
						// Log error but continue
						continue
					}
					objectsProcessed++
				}
			}
		}
	}

	result := map[string]any{
		"success":           true,
		"message":           "lifecycle policies applied successfully",
		"policies_count":    len(policies),
		"objects_processed": objectsProcessed,
	}

	jsonResult, _ := json.MarshalIndent(result, "", "  ")
	return string(jsonResult), nil
}

// createArchiver creates an archiver from factory based on destination type
func createArchiver(destinationType string, settings map[string]string) (common.Archiver, error) {
	return factory.NewArchiver(destinationType, settings)
}
