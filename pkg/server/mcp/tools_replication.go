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
	"context"
	"encoding/json"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
	"github.com/jeremyhahn/go-objstore/pkg/replication"
)

// RegisterReplicationTools registers replication-related MCP tools
func (r *ToolRegistry) RegisterReplicationTools() {
	r.tools["objstore_add_replication_policy"] = Tool{
		Name:        "objstore_add_replication_policy",
		Description: "Add a replication policy for automatic object replication between storage backends. Supports transparent and opaque replication modes with three-layer encryption.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"id": map[string]any{
					"type":        "string",
					"description": "Unique identifier for the replication policy",
				},
				"source_backend": map[string]any{
					"type":        "string",
					"description": "Type of source storage backend (e.g., 'local', 's3', 'gcs', 'azure')",
				},
				"source_settings": map[string]any{
					"type":        "object",
					"description": "Configuration settings for the source backend (e.g., bucket, path, region)",
				},
				"source_prefix": map[string]any{
					"type":        "string",
					"description": "Optional key prefix to filter objects for replication",
				},
				"destination_backend": map[string]any{
					"type":        "string",
					"description": "Type of destination storage backend (e.g., 'local', 's3', 'gcs', 'azure')",
				},
				"destination_settings": map[string]any{
					"type":        "object",
					"description": "Configuration settings for the destination backend",
				},
				"check_interval": map[string]any{
					"type":        "integer",
					"description": "Check interval in seconds for periodic synchronization",
				},
				"enabled": map[string]any{
					"type":        "boolean",
					"description": "Whether the policy is enabled",
				},
				"replication_mode": map[string]any{
					"type":        "string",
					"description": "Replication mode: 'transparent' (decrypt/re-encrypt) or 'opaque' (copy as-is)",
					"enum":        []string{"transparent", "opaque"},
				},
				"encryption": map[string]any{
					"type":        "object",
					"description": "Three-layer encryption policy configuration",
					"properties": map[string]any{
						"backend": map[string]any{
							"type":        "object",
							"description": "Layer 1: Backend at-rest encryption config",
							"properties": map[string]any{
								"enabled":     map[string]any{"type": "boolean"},
								"provider":    map[string]any{"type": "string"},
								"default_key": map[string]any{"type": "string"},
							},
						},
						"source": map[string]any{
							"type":        "object",
							"description": "Layer 2: Client-side source DEK config",
							"properties": map[string]any{
								"enabled":     map[string]any{"type": "boolean"},
								"provider":    map[string]any{"type": "string"},
								"default_key": map[string]any{"type": "string"},
							},
						},
						"destination": map[string]any{
							"type":        "object",
							"description": "Layer 3: Client-side destination DEK config",
							"properties": map[string]any{
								"enabled":     map[string]any{"type": "boolean"},
								"provider":    map[string]any{"type": "string"},
								"default_key": map[string]any{"type": "string"},
							},
						},
					},
				},
			},
			"required": []string{"id", "source_backend", "destination_backend", "check_interval"},
		},
	}

	r.tools["objstore_remove_replication_policy"] = Tool{
		Name:        "objstore_remove_replication_policy",
		Description: "Remove an existing replication policy by its ID. This stops automatic replication for this policy.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"id": map[string]any{
					"type":        "string",
					"description": "The ID of the replication policy to remove",
				},
			},
			"required": []string{"id"},
		},
	}

	r.tools["objstore_list_replication_policies"] = Tool{
		Name:        "objstore_list_replication_policies",
		Description: "List all replication policies. Returns policy configurations, sync status, and statistics.",
		InputSchema: map[string]any{
			"type":       "object",
			"properties": map[string]any{},
		},
	}

	r.tools["objstore_get_replication_policy"] = Tool{
		Name:        "objstore_get_replication_policy",
		Description: "Get details of a specific replication policy by ID. Returns full configuration and sync status.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"id": map[string]any{
					"type":        "string",
					"description": "The ID of the replication policy to retrieve",
				},
			},
			"required": []string{"id"},
		},
	}

	r.tools["objstore_trigger_replication"] = Tool{
		Name:        "objstore_trigger_replication",
		Description: "Manually trigger replication for all policies or a specific policy. Returns sync results including objects synced, failed, and bytes transferred.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"policy_id": map[string]any{
					"type":        "string",
					"description": "Optional policy ID to sync. If empty, syncs all enabled policies.",
				},
			},
		},
	}

	r.tools["objstore_get_replication_status"] = Tool{
		Name:        "objstore_get_replication_status",
		Description: "Get status and metrics for a specific replication policy. Returns comprehensive statistics including sync counts, errors, and performance metrics.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"policy_id": map[string]any{
					"type":        "string",
					"description": "The ID of the replication policy to get status for",
				},
			},
			"required": []string{"policy_id"},
		},
	}
}

// executeAddReplicationPolicy executes the objstore_add_replication_policy tool
func (e *ToolExecutor) executeAddReplicationPolicy(ctx context.Context, args map[string]any) (string, error) {
	id, ok := args["id"].(string)
	if !ok || id == "" {
		return "", ErrMissingParameter
	}

	sourceBackend, ok := args["source_backend"].(string)
	if !ok || sourceBackend == "" {
		return "", ErrMissingParameter
	}

	destinationBackend, ok := args["destination_backend"].(string)
	if !ok || destinationBackend == "" {
		return "", ErrMissingParameter
	}

	checkIntervalRaw, ok := args["check_interval"]
	if !ok {
		return "", ErrMissingParameter
	}

	// Handle both integer and float64 from JSON
	var checkIntervalSeconds int64
	switch v := checkIntervalRaw.(type) {
	case float64:
		checkIntervalSeconds = int64(v)
	case int64:
		checkIntervalSeconds = v
	case int:
		checkIntervalSeconds = int64(v)
	default:
		return "", ErrInvalidParameter
	}

	if checkIntervalSeconds <= 0 {
		return "", ErrInvalidParameter
	}

	// Extract optional settings
	var sourceSettings map[string]string
	if settingsRaw, ok := args["source_settings"]; ok {
		if settingsMap, ok := settingsRaw.(map[string]any); ok {
			sourceSettings = make(map[string]string)
			for k, v := range settingsMap {
				if strVal, ok := v.(string); ok {
					sourceSettings[k] = strVal
				}
			}
		}
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

	sourcePrefix, _ := args["source_prefix"].(string)
	enabled, _ := args["enabled"].(bool)

	replicationModeStr, _ := args["replication_mode"].(string)
	replicationMode := common.ReplicationMode(replicationModeStr)
	if replicationMode == "" {
		replicationMode = common.ReplicationModeTransparent
	}

	// Validate replication mode
	if replicationMode != common.ReplicationModeTransparent &&
		replicationMode != common.ReplicationModeOpaque {
		return "", ErrInvalidParameter
	}

	// Build encryption policy if provided
	var encryptionPolicy *common.EncryptionPolicy
	if encRaw, ok := args["encryption"]; ok {
		if encMap, ok := encRaw.(map[string]any); ok {
			encryptionPolicy = &common.EncryptionPolicy{}

			if backendRaw, ok := encMap["backend"]; ok {
				if backendMap, ok := backendRaw.(map[string]any); ok {
					encryptionPolicy.Backend = parseEncryptionConfig(backendMap)
				}
			}

			if sourceRaw, ok := encMap["source"]; ok {
				if sourceMap, ok := sourceRaw.(map[string]any); ok {
					encryptionPolicy.Source = parseEncryptionConfig(sourceMap)
				}
			}

			if destRaw, ok := encMap["destination"]; ok {
				if destMap, ok := destRaw.(map[string]any); ok {
					encryptionPolicy.Destination = parseEncryptionConfig(destMap)
				}
			}
		}
	}

	// Build replication policy
	policy := common.ReplicationPolicy{
		ID:                  id,
		SourceBackend:       sourceBackend,
		SourceSettings:      sourceSettings,
		SourcePrefix:        sourcePrefix,
		DestinationBackend:  destinationBackend,
		DestinationSettings: destinationSettings,
		CheckInterval:       time.Duration(checkIntervalSeconds) * time.Second,
		Enabled:             enabled,
		ReplicationMode:     replicationMode,
		Encryption:          encryptionPolicy,
	}

	// Get replication manager from facade
	repMgr, err := objstore.GetReplicationManager(e.backend)
	if err != nil {
		return "", err
	}

	// Add policy
	err = repMgr.AddPolicy(policy)
	if err != nil {
		return "", err
	}

	result := map[string]any{
		"success": true,
		"id":      id,
		"message": "replication policy added successfully",
	}

	jsonResult, _ := json.MarshalIndent(result, "", "  ")
	return string(jsonResult), nil
}

// executeRemoveReplicationPolicy executes the objstore_remove_replication_policy tool
func (e *ToolExecutor) executeRemoveReplicationPolicy(ctx context.Context, args map[string]any) (string, error) {
	id, ok := args["id"].(string)
	if !ok || id == "" {
		return "", ErrMissingParameter
	}

	// Get replication manager from facade
	repMgr, err := objstore.GetReplicationManager(e.backend)
	if err != nil {
		return "", err
	}

	// Remove policy
	err = repMgr.RemovePolicy(id)
	if err != nil {
		return "", err
	}

	result := map[string]any{
		"success": true,
		"id":      id,
		"message": "replication policy removed successfully",
	}

	jsonResult, _ := json.MarshalIndent(result, "", "  ")
	return string(jsonResult), nil
}

// executeListReplicationPolicies executes the objstore_list_replication_policies tool
func (e *ToolExecutor) executeListReplicationPolicies(ctx context.Context, args map[string]any) (string, error) {
	// Get replication manager from facade
	repMgr, err := objstore.GetReplicationManager(e.backend)
	if err != nil {
		return "", err
	}

	// Get all policies
	policies, err := repMgr.GetPolicies()
	if err != nil {
		return "", err
	}

	// Convert policies to JSON-friendly format
	policyResults := make([]map[string]any, len(policies))
	for i, policy := range policies {
		policyResults[i] = map[string]any{
			"id":                   policy.ID,
			"source_backend":       policy.SourceBackend,
			"source_settings":      policy.SourceSettings,
			"source_prefix":        policy.SourcePrefix,
			"destination_backend":  policy.DestinationBackend,
			"destination_settings": policy.DestinationSettings,
			"check_interval":       int64(policy.CheckInterval.Seconds()),
			"last_sync_time":       policy.LastSyncTime.Format(time.RFC3339),
			"enabled":              policy.Enabled,
			"replication_mode":     policy.ReplicationMode,
			"encryption":           policy.Encryption,
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

// executeGetReplicationPolicy executes the objstore_get_replication_policy tool
func (e *ToolExecutor) executeGetReplicationPolicy(ctx context.Context, args map[string]any) (string, error) {
	id, ok := args["id"].(string)
	if !ok || id == "" {
		return "", ErrMissingParameter
	}

	// Get replication manager from facade
	repMgr, err := objstore.GetReplicationManager(e.backend)
	if err != nil {
		return "", err
	}

	// Get policy
	policy, err := repMgr.GetPolicy(id)
	if err != nil {
		return "", err
	}

	result := map[string]any{
		"success":              true,
		"id":                   policy.ID,
		"source_backend":       policy.SourceBackend,
		"source_settings":      policy.SourceSettings,
		"source_prefix":        policy.SourcePrefix,
		"destination_backend":  policy.DestinationBackend,
		"destination_settings": policy.DestinationSettings,
		"check_interval":       int64(policy.CheckInterval.Seconds()),
		"last_sync_time":       policy.LastSyncTime.Format(time.RFC3339),
		"enabled":              policy.Enabled,
		"replication_mode":     policy.ReplicationMode,
		"encryption":           policy.Encryption,
	}

	jsonResult, _ := json.MarshalIndent(result, "", "  ")
	return string(jsonResult), nil
}

// executeTriggerReplication executes the objstore_trigger_replication tool
func (e *ToolExecutor) executeTriggerReplication(ctx context.Context, args map[string]any) (string, error) {
	policyID, _ := args["policy_id"].(string)

	// Get replication manager from facade
	repMgr, err := objstore.GetReplicationManager(e.backend)
	if err != nil {
		return "", err
	}

	var syncResult *common.SyncResult

	// Trigger sync
	if policyID == "" {
		// Sync all policies
		syncResult, err = repMgr.SyncAll(ctx)
	} else {
		// Sync specific policy
		syncResult, err = repMgr.SyncPolicy(ctx, policyID)
	}

	if err != nil {
		return "", err
	}

	result := map[string]any{
		"success": true,
		"result": map[string]any{
			"policy_id":   syncResult.PolicyID,
			"synced":      syncResult.Synced,
			"deleted":     syncResult.Deleted,
			"failed":      syncResult.Failed,
			"bytes_total": syncResult.BytesTotal,
			"duration":    syncResult.Duration.String(),
			"errors":      syncResult.Errors,
		},
		"message": "replication triggered successfully",
	}

	jsonResult, _ := json.MarshalIndent(result, "", "  ")
	return string(jsonResult), nil
}

// executeGetReplicationStatus executes the objstore_get_replication_status tool
func (e *ToolExecutor) executeGetReplicationStatus(ctx context.Context, args map[string]any) (string, error) {
	policyID, ok := args["policy_id"].(string)
	if !ok || policyID == "" {
		return "", ErrMissingParameter
	}

	// Get replication manager from facade
	repMgr, err := objstore.GetReplicationManager(e.backend)
	if err != nil {
		return "", err
	}

	// Get replication status
	replicationStatus, err := repMgr.(interface {
		GetReplicationStatus(id string) (*replication.ReplicationStatus, error)
	}).GetReplicationStatus(policyID)
	if err != nil {
		return "", err
	}

	result := map[string]any{
		"success":               true,
		"policy_id":             replicationStatus.PolicyID,
		"source_backend":        replicationStatus.SourceBackend,
		"destination_backend":   replicationStatus.DestinationBackend,
		"enabled":               replicationStatus.Enabled,
		"total_objects_synced":  replicationStatus.TotalObjectsSynced,
		"total_objects_deleted": replicationStatus.TotalObjectsDeleted,
		"total_bytes_synced":    replicationStatus.TotalBytesSynced,
		"total_errors":          replicationStatus.TotalErrors,
		"last_sync_time":        replicationStatus.LastSyncTime.Format(time.RFC3339),
		"average_sync_duration": replicationStatus.AverageSyncDuration.String(),
		"sync_count":            replicationStatus.SyncCount,
	}

	jsonResult, _ := json.MarshalIndent(result, "", "  ")
	return string(jsonResult), nil
}

// parseEncryptionConfig parses encryption config from map
func parseEncryptionConfig(m map[string]any) *common.EncryptionConfig {
	config := &common.EncryptionConfig{}

	if enabled, ok := m["enabled"].(bool); ok {
		config.Enabled = enabled
	}

	if provider, ok := m["provider"].(string); ok {
		config.Provider = provider
	}

	if defaultKey, ok := m["default_key"].(string); ok {
		config.DefaultKey = defaultKey
	}

	return config
}
