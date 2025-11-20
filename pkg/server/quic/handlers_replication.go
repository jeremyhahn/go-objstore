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

package quic

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/replication"
)

// handleReplicationPolicies handles GET and POST requests for replication policies.
func (h *Handler) handleReplicationPolicies(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.handleGetReplicationPolicies(w, r)
	case http.MethodPost:
		h.handleAddReplicationPolicy(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleGetReplicationPolicies handles GET requests to list replication policies.
func (h *Handler) handleGetReplicationPolicies(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), h.readTimeout)
	defer cancel()

	// Get replication manager from storage
	repSupport, ok := h.storage.(interface {
		GetReplicationManager() (common.ReplicationManager, error)
	})
	if !ok {
		http.Error(w, "replication not supported by this storage backend", http.StatusInternalServerError)
		return
	}

	repMgr, err := repSupport.GetReplicationManager()
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			http.Error(w, "request timeout", http.StatusRequestTimeout)
			return
		}
		if errors.Is(err, common.ErrReplicationNotSupported) {
			http.Error(w, "replication not supported by this storage backend", http.StatusInternalServerError)
		} else {
			http.Error(w, common.SanitizeErrorMessage(err), http.StatusInternalServerError)
		}
		return
	}

	// Get all policies
	policies, err := repMgr.GetPolicies()
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			http.Error(w, "request timeout", http.StatusRequestTimeout)
			return
		}
		http.Error(w, common.SanitizeErrorMessage(err), http.StatusInternalServerError)
		return
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

	response := map[string]any{
		"success":  true,
		"policies": policyResults,
		"count":    len(policyResults),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		h.logger.Error(r.Context(), "failed to encode response", adapters.Field{Key: "error", Value: err.Error()})
	}
}

// handleAddReplicationPolicy handles POST requests to add a replication policy.
func (h *Handler) handleAddReplicationPolicy(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), h.writeTimeout)
	defer cancel()

	// Parse request body
	var req struct {
		ID                  string                   `json:"id"`
		SourceBackend       string                   `json:"source_backend"`
		SourceSettings      map[string]string        `json:"source_settings,omitempty"`
		SourcePrefix        string                   `json:"source_prefix,omitempty"`
		DestinationBackend  string                   `json:"destination_backend"`
		DestinationSettings map[string]string        `json:"destination_settings,omitempty"`
		CheckInterval       int64                    `json:"check_interval"`
		Enabled             bool                     `json:"enabled"`
		ReplicationMode     common.ReplicationMode   `json:"replication_mode,omitempty"`
		Encryption          *common.EncryptionPolicy `json:"encryption,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Validate required fields
	if req.ID == "" {
		http.Error(w, "policy ID is required", http.StatusBadRequest)
		return
	}

	if req.SourceBackend == "" {
		http.Error(w, "source_backend is required", http.StatusBadRequest)
		return
	}

	if req.DestinationBackend == "" {
		http.Error(w, "destination_backend is required", http.StatusBadRequest)
		return
	}

	if req.CheckInterval <= 0 {
		http.Error(w, "check_interval must be positive", http.StatusBadRequest)
		return
	}

	// Validate replication mode
	if req.ReplicationMode != "" {
		if req.ReplicationMode != common.ReplicationModeTransparent &&
			req.ReplicationMode != common.ReplicationModeOpaque {
			http.Error(w, "invalid replication_mode: must be 'transparent' or 'opaque'", http.StatusBadRequest)
			return
		}
	}

	// Build replication policy
	policy := common.ReplicationPolicy{
		ID:                  req.ID,
		SourceBackend:       req.SourceBackend,
		SourceSettings:      req.SourceSettings,
		SourcePrefix:        req.SourcePrefix,
		DestinationBackend:  req.DestinationBackend,
		DestinationSettings: req.DestinationSettings,
		CheckInterval:       time.Duration(req.CheckInterval) * time.Second,
		Enabled:             req.Enabled,
		ReplicationMode:     req.ReplicationMode,
		Encryption:          req.Encryption,
	}

	// Set default replication mode if not specified
	if policy.ReplicationMode == "" {
		policy.ReplicationMode = common.ReplicationModeTransparent
	}

	// Get replication manager from storage
	repSupport, ok := h.storage.(interface {
		GetReplicationManager() (common.ReplicationManager, error)
	})
	if !ok {
		http.Error(w, "replication not supported by this storage backend", http.StatusInternalServerError)
		return
	}

	repMgr, err := repSupport.GetReplicationManager()
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			http.Error(w, "request timeout", http.StatusRequestTimeout)
			return
		}
		if errors.Is(err, common.ErrReplicationNotSupported) {
			http.Error(w, "replication not supported by this storage backend", http.StatusInternalServerError)
		} else {
			http.Error(w, common.SanitizeErrorMessage(err), http.StatusInternalServerError)
		}
		return
	}

	// Add policy
	err = repMgr.AddPolicy(policy)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			http.Error(w, "request timeout", http.StatusRequestTimeout)
			return
		}
		// Check for duplicate policy error
		if err.Error() == "policy already exists" {
			http.Error(w, common.SanitizeErrorMessage(err), http.StatusConflict)
			return
		}
		http.Error(w, common.SanitizeErrorMessage(err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"message": "replication policy added successfully",
		"id":      req.ID,
	}); err != nil {
		h.logger.Error(r.Context(), "failed to encode response", adapters.Field{Key: "error", Value: err.Error()})
	}
}

// handleReplicationPolicyByID handles GET and DELETE requests for individual replication policies.
func (h *Handler) handleReplicationPolicyByID(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.handleGetReplicationPolicy(w, r)
	case http.MethodDelete:
		h.handleDeleteReplicationPolicy(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleGetReplicationPolicy handles GET requests to retrieve a specific replication policy.
func (h *Handler) handleGetReplicationPolicy(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), h.readTimeout)
	defer cancel()

	// Extract policy ID from path
	id := strings.TrimPrefix(r.URL.Path, "/replication/policies/")
	if id == "" {
		http.Error(w, "policy ID is required", http.StatusBadRequest)
		return
	}

	// Get replication manager from storage
	repSupport, ok := h.storage.(interface {
		GetReplicationManager() (common.ReplicationManager, error)
	})
	if !ok {
		http.Error(w, "replication not supported by this storage backend", http.StatusInternalServerError)
		return
	}

	repMgr, err := repSupport.GetReplicationManager()
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			http.Error(w, "request timeout", http.StatusRequestTimeout)
			return
		}
		if errors.Is(err, common.ErrReplicationNotSupported) {
			http.Error(w, "replication not supported by this storage backend", http.StatusInternalServerError)
		} else {
			http.Error(w, common.SanitizeErrorMessage(err), http.StatusInternalServerError)
		}
		return
	}

	// Get policy
	policy, err := repMgr.GetPolicy(id)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			http.Error(w, "request timeout", http.StatusRequestTimeout)
			return
		}
		if errors.Is(err, common.ErrPolicyNotFound) {
			http.Error(w, "policy not found", http.StatusNotFound)
			return
		}
		http.Error(w, common.SanitizeErrorMessage(err), http.StatusInternalServerError)
		return
	}

	policyResult := map[string]any{
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

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(policyResult); err != nil {
		h.logger.Error(r.Context(), "failed to encode response", adapters.Field{Key: "error", Value: err.Error()})
	}
}

// handleDeleteReplicationPolicy handles DELETE requests to remove a replication policy.
func (h *Handler) handleDeleteReplicationPolicy(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), h.writeTimeout)
	defer cancel()

	// Extract policy ID from path
	id := strings.TrimPrefix(r.URL.Path, "/replication/policies/")
	if id == "" {
		http.Error(w, "policy ID is required", http.StatusBadRequest)
		return
	}

	// Get replication manager from storage
	repSupport, ok := h.storage.(interface {
		GetReplicationManager() (common.ReplicationManager, error)
	})
	if !ok {
		http.Error(w, "replication not supported by this storage backend", http.StatusInternalServerError)
		return
	}

	repMgr, err := repSupport.GetReplicationManager()
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			http.Error(w, "request timeout", http.StatusRequestTimeout)
			return
		}
		if errors.Is(err, common.ErrReplicationNotSupported) {
			http.Error(w, "replication not supported by this storage backend", http.StatusInternalServerError)
		} else {
			http.Error(w, common.SanitizeErrorMessage(err), http.StatusInternalServerError)
		}
		return
	}

	// Remove policy
	err = repMgr.RemovePolicy(id)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			http.Error(w, "request timeout", http.StatusRequestTimeout)
			return
		}
		if errors.Is(err, common.ErrPolicyNotFound) {
			http.Error(w, "policy not found", http.StatusNotFound)
			return
		}
		http.Error(w, common.SanitizeErrorMessage(err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"message": "replication policy removed successfully",
		"id":      id,
	}); err != nil {
		h.logger.Error(r.Context(), "failed to encode response", adapters.Field{Key: "error", Value: err.Error()})
	}
}

// handleTriggerReplication handles POST requests to manually trigger replication.
func (h *Handler) handleTriggerReplication(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.writeTimeout)
	defer cancel()

	policyID := r.URL.Query().Get("policy_id")

	// Get replication manager from storage
	repSupport, ok := h.storage.(interface {
		GetReplicationManager() (common.ReplicationManager, error)
	})
	if !ok {
		http.Error(w, "replication not supported by this storage backend", http.StatusInternalServerError)
		return
	}

	repMgr, err := repSupport.GetReplicationManager()
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			http.Error(w, "request timeout", http.StatusRequestTimeout)
			return
		}
		if errors.Is(err, common.ErrReplicationNotSupported) {
			http.Error(w, "replication not supported by this storage backend", http.StatusInternalServerError)
		} else {
			http.Error(w, common.SanitizeErrorMessage(err), http.StatusInternalServerError)
		}
		return
	}

	var result *common.SyncResult

	// Trigger sync
	if policyID == "" {
		// Sync all policies
		result, err = repMgr.SyncAll(ctx)
	} else {
		// Sync specific policy
		result, err = repMgr.SyncPolicy(ctx, policyID)
	}

	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			http.Error(w, "request timeout", http.StatusRequestTimeout)
			return
		}
		if errors.Is(err, common.ErrPolicyNotFound) {
			http.Error(w, "policy not found", http.StatusNotFound)
			return
		}
		http.Error(w, common.SanitizeErrorMessage(err), http.StatusInternalServerError)
		return
	}

	response := map[string]any{
		"success": true,
		"result": map[string]any{
			"policy_id":   result.PolicyID,
			"synced":      result.Synced,
			"deleted":     result.Deleted,
			"failed":      result.Failed,
			"bytes_total": result.BytesTotal,
			"duration":    result.Duration.String(),
			"errors":      result.Errors,
		},
		"message": "replication triggered successfully",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		h.logger.Error(r.Context(), "failed to encode response", adapters.Field{Key: "error", Value: err.Error()})
	}
}

// handleGetReplicationStatus handles GET requests to retrieve replication status.
func (h *Handler) handleGetReplicationStatus(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), h.readTimeout)
	defer cancel()

	// Extract policy ID from path
	id := strings.TrimPrefix(r.URL.Path, "/replication/status/")
	if id == "" {
		http.Error(w, "policy ID is required", http.StatusBadRequest)
		return
	}

	// Get replication manager from storage
	repSupport, ok := h.storage.(interface {
		GetReplicationManager() (common.ReplicationManager, error)
	})
	if !ok {
		http.Error(w, "replication not supported by this storage backend", http.StatusInternalServerError)
		return
	}

	repMgr, err := repSupport.GetReplicationManager()
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			http.Error(w, "request timeout", http.StatusRequestTimeout)
			return
		}
		if errors.Is(err, common.ErrReplicationNotSupported) {
			http.Error(w, "replication not supported by this storage backend", http.StatusInternalServerError)
		} else {
			http.Error(w, common.SanitizeErrorMessage(err), http.StatusInternalServerError)
		}
		return
	}

	// Get replication status
	replicationStatus, err := repMgr.(interface {
		GetReplicationStatus(id string) (*replication.ReplicationStatus, error)
	}).GetReplicationStatus(id)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			http.Error(w, "request timeout", http.StatusRequestTimeout)
			return
		}
		if errors.Is(err, common.ErrPolicyNotFound) {
			http.Error(w, "policy not found", http.StatusNotFound)
			return
		}
		http.Error(w, common.SanitizeErrorMessage(err), http.StatusInternalServerError)
		return
	}

	statusResult := map[string]any{
		"success":                true,
		"policy_id":              replicationStatus.PolicyID,
		"source_backend":         replicationStatus.SourceBackend,
		"destination_backend":    replicationStatus.DestinationBackend,
		"enabled":                replicationStatus.Enabled,
		"total_objects_synced":   replicationStatus.TotalObjectsSynced,
		"total_objects_deleted":  replicationStatus.TotalObjectsDeleted,
		"total_bytes_synced":     replicationStatus.TotalBytesSynced,
		"total_errors":           replicationStatus.TotalErrors,
		"last_sync_time":         replicationStatus.LastSyncTime.Format(time.RFC3339),
		"average_sync_duration":  replicationStatus.AverageSyncDuration.String(),
		"sync_count":             replicationStatus.SyncCount,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(statusResult); err != nil {
		h.logger.Error(r.Context(), "failed to encode response", adapters.Field{Key: "error", Value: err.Error()})
	}
}
