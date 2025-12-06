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

package rest

import (
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
	"github.com/jeremyhahn/go-objstore/pkg/replication"
)

// AddReplicationPolicy handles adding a new replication policy
func (h *Handler) AddReplicationPolicy(c *gin.Context) {
	var req AddReplicationPolicyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		RespondWithError(c, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	// Validate required fields
	if req.ID == "" {
		RespondWithError(c, http.StatusBadRequest, "policy ID is required")
		return
	}

	if req.SourceBackend == "" {
		RespondWithError(c, http.StatusBadRequest, "source_backend is required")
		return
	}

	if req.DestinationBackend == "" {
		RespondWithError(c, http.StatusBadRequest, "destination_backend is required")
		return
	}

	if req.CheckIntervalSeconds <= 0 {
		RespondWithError(c, http.StatusBadRequest, "check_interval_seconds must be positive")
		return
	}

	// Validate replication mode
	if req.ReplicationMode != "" {
		if req.ReplicationMode != common.ReplicationModeTransparent &&
			req.ReplicationMode != common.ReplicationModeOpaque {
			RespondWithError(c, http.StatusBadRequest, "invalid replication_mode: must be 'transparent' or 'opaque'")
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
		CheckInterval:       time.Duration(req.CheckIntervalSeconds) * time.Second,
		Enabled:             req.Enabled,
		ReplicationMode:     req.ReplicationMode,
		Encryption:          req.Encryption,
	}

	// Set default replication mode if not specified
	if policy.ReplicationMode == "" {
		policy.ReplicationMode = common.ReplicationModeTransparent
	}

	// Get replication manager from facade
	repMgr, err := objstore.GetReplicationManager(h.backend)
	if err != nil {
		if errors.Is(err, common.ErrReplicationNotSupported) {
			RespondWithError(c, http.StatusInternalServerError, "replication not supported by this storage backend")
		} else {
			RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		}
		return
	}

	// Add policy
	err = repMgr.AddPolicy(policy)

	// Audit logging
	auditLogger := audit.GetAuditLogger(c.Request.Context())
	principal, userID := extractPrincipal(c)
	requestID := audit.GetRequestID(c.Request.Context())

	if err != nil {
		_ = auditLogger.LogObjectMutation(c.Request.Context(), "replication_policy_add_failed",
			userID, principal, h.backend, req.ID, c.ClientIP(), requestID, 0,
			audit.ResultFailure, err)

		if err.Error() == "policy already exists" {
			RespondWithError(c, http.StatusConflict, common.SanitizeErrorMessage(err))
			return
		}
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	_ = auditLogger.LogObjectMutation(c.Request.Context(), "replication_policy_added",
		userID, principal, h.backend, req.ID, c.ClientIP(), requestID, 0,
		audit.ResultSuccess, nil)

	RespondWithSuccess(c, http.StatusCreated, "replication policy added successfully", gin.H{
		"id": req.ID,
	})
}

// RemoveReplicationPolicy handles removing a replication policy
func (h *Handler) RemoveReplicationPolicy(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		RespondWithError(c, http.StatusBadRequest, "policy ID is required")
		return
	}

	// Remove leading slashes if present
	for len(id) > 0 && id[0] == '/' {
		id = id[1:]
	}

	// Get replication manager from facade
	repMgr, err := objstore.GetReplicationManager(h.backend)
	if err != nil {
		if errors.Is(err, common.ErrReplicationNotSupported) {
			RespondWithError(c, http.StatusInternalServerError, "replication not supported by this storage backend")
		} else {
			RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		}
		return
	}

	// Remove policy
	err = repMgr.RemovePolicy(id)

	// Audit logging
	auditLogger := audit.GetAuditLogger(c.Request.Context())
	principal, userID := extractPrincipal(c)
	requestID := audit.GetRequestID(c.Request.Context())

	if err != nil {
		_ = auditLogger.LogObjectMutation(c.Request.Context(), "replication_policy_remove_failed",
			userID, principal, h.backend, id, c.ClientIP(), requestID, 0,
			audit.ResultFailure, err)

		if errors.Is(err, common.ErrPolicyNotFound) {
			RespondWithError(c, http.StatusNotFound, common.SanitizeErrorMessage(err))
			return
		}
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	_ = auditLogger.LogObjectMutation(c.Request.Context(), "replication_policy_removed",
		userID, principal, h.backend, id, c.ClientIP(), requestID, 0,
		audit.ResultSuccess, nil)

	RespondWithSuccess(c, http.StatusOK, "replication policy removed successfully", gin.H{
		"id": id,
	})
}

// GetReplicationPolicies handles listing all replication policies
func (h *Handler) GetReplicationPolicies(c *gin.Context) {
	// Get replication manager from facade
	repMgr, err := objstore.GetReplicationManager(h.backend)
	if err != nil {
		if errors.Is(err, common.ErrReplicationNotSupported) {
			RespondWithError(c, http.StatusInternalServerError, "replication not supported by this storage backend")
		} else {
			RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		}
		return
	}

	// Get all policies
	policies, err := repMgr.GetPolicies()
	if err != nil {
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	RespondWithReplicationPolicies(c, policies)
}

// GetReplicationPolicy handles retrieving a specific replication policy
func (h *Handler) GetReplicationPolicy(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		RespondWithError(c, http.StatusBadRequest, "policy ID is required")
		return
	}

	// Remove leading slashes if present
	for len(id) > 0 && id[0] == '/' {
		id = id[1:]
	}

	// Get replication manager from facade
	repMgr, err := objstore.GetReplicationManager(h.backend)
	if err != nil {
		if errors.Is(err, common.ErrReplicationNotSupported) {
			RespondWithError(c, http.StatusInternalServerError, "replication not supported by this storage backend")
		} else {
			RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		}
		return
	}

	// Get policy
	policy, err := repMgr.GetPolicy(id)
	if err != nil {
		if errors.Is(err, common.ErrPolicyNotFound) {
			RespondWithError(c, http.StatusNotFound, common.SanitizeErrorMessage(err))
			return
		}
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	RespondWithReplicationPolicy(c, policy)
}

// TriggerReplicationRequest represents a request to trigger replication
type TriggerReplicationRequest struct {
	PolicyID    string `json:"policy_id,omitempty"`
	Parallel    bool   `json:"parallel,omitempty"`
	WorkerCount int    `json:"worker_count,omitempty"`
}

// TriggerReplication handles manually triggering replication
func (h *Handler) TriggerReplication(c *gin.Context) {
	var req TriggerReplicationRequest
	// Try to bind JSON body, but don't fail if empty (allows query param fallback)
	_ = c.ShouldBindJSON(&req)

	// Fallback to query parameter for backwards compatibility
	policyID := req.PolicyID
	if policyID == "" {
		policyID = c.Query("policy_id")
	}

	// Get replication manager from facade
	repMgr, err := objstore.GetReplicationManager(h.backend)
	if err != nil {
		if errors.Is(err, common.ErrReplicationNotSupported) {
			RespondWithError(c, http.StatusInternalServerError, "replication not supported by this storage backend")
		} else {
			RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		}
		return
	}

	var result *common.SyncResult

	// Trigger sync
	if policyID == "" {
		// Sync all policies
		result, err = repMgr.SyncAll(c.Request.Context())
	} else {
		// Sync specific policy
		result, err = repMgr.SyncPolicy(c.Request.Context(), policyID)
	}

	// Audit logging
	auditLogger := audit.GetAuditLogger(c.Request.Context())
	principal, userID := extractPrincipal(c)
	requestID := audit.GetRequestID(c.Request.Context())

	if err != nil {
		_ = auditLogger.LogObjectMutation(c.Request.Context(), "replication_trigger_failed",
			userID, principal, h.backend, policyID, c.ClientIP(), requestID, 0,
			audit.ResultFailure, err)

		if errors.Is(err, common.ErrPolicyNotFound) {
			RespondWithError(c, http.StatusNotFound, common.SanitizeErrorMessage(err))
			return
		}
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	_ = auditLogger.LogObjectMutation(c.Request.Context(), "replication_triggered",
		userID, principal, h.backend, policyID, c.ClientIP(), requestID, result.BytesTotal,
		audit.ResultSuccess, nil)

	RespondWithSyncResult(c, result)
}

// GetReplicationStatus handles retrieving replication status for a specific policy
func (h *Handler) GetReplicationStatus(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		RespondWithError(c, http.StatusBadRequest, "policy ID is required")
		return
	}

	// Remove leading slashes if present
	for len(id) > 0 && id[0] == '/' {
		id = id[1:]
	}

	// Get replication manager from facade
	repMgr, err := objstore.GetReplicationManager(h.backend)
	if err != nil {
		if errors.Is(err, common.ErrReplicationNotSupported) {
			RespondWithError(c, http.StatusInternalServerError, "replication not supported by this storage backend")
		} else {
			RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		}
		return
	}

	// Get replication status - type assert to access GetReplicationStatus method
	statusProvider, ok := repMgr.(interface {
		GetReplicationStatus(id string) (*replication.ReplicationStatus, error)
	})
	if !ok {
		RespondWithError(c, http.StatusInternalServerError, "replication status not supported by this backend")
		return
	}

	replicationStatus, err := statusProvider.GetReplicationStatus(id)
	if err != nil {
		if errors.Is(err, common.ErrPolicyNotFound) {
			RespondWithError(c, http.StatusNotFound, common.SanitizeErrorMessage(err))
			return
		}
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	RespondWithReplicationStatus(c, replicationStatus)
}
