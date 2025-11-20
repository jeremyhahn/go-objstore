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

	"github.com/gin-gonic/gin"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/replication"
)

// AddReplicationPolicy handles adding a new replication policy
// @Summary Add replication policy
// @Description Add a new replication policy for automatic object replication
// @Tags replication
// @Accept json
// @Produce json
// @Param request body AddReplicationPolicyRequest true "Replication policy request"
// @Success 201 {object} SuccessResponse
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /replication/policies [post]
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

	if req.CheckInterval <= 0 {
		RespondWithError(c, http.StatusBadRequest, "check_interval must be positive")
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
		CheckInterval:       req.CheckInterval,
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
		RespondWithError(c, http.StatusInternalServerError, "replication not supported by this storage backend")
		return
	}

	repMgr, err := repSupport.GetReplicationManager()
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
			userID, principal, "default", req.ID, c.ClientIP(), requestID, 0,
			audit.ResultFailure, err) // #nosec G104 -- Audit logging errors are logged internally

		// Check for duplicate policy error
		if err.Error() == "policy already exists" {
			RespondWithError(c, http.StatusConflict, common.SanitizeErrorMessage(err))
			return
		}
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	_ = auditLogger.LogObjectMutation(c.Request.Context(), "replication_policy_added",
		userID, principal, "default", req.ID, c.ClientIP(), requestID, 0,
		audit.ResultSuccess, nil) // #nosec G104 -- Audit logging errors are logged internally

	RespondWithSuccess(c, http.StatusCreated, "replication policy added successfully", gin.H{
		"id": req.ID,
	})
}

// RemoveReplicationPolicy handles removing a replication policy
// @Summary Remove replication policy
// @Description Remove an existing replication policy by ID
// @Tags replication
// @Produce json
// @Param id path string true "Policy ID"
// @Success 200 {object} SuccessResponse
// @Failure 400 {object} ErrorResponse
// @Failure 404 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /replication/policies/{id} [delete]
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

	// Get replication manager from storage
	repSupport, ok := h.storage.(interface {
		GetReplicationManager() (common.ReplicationManager, error)
	})
	if !ok {
		RespondWithError(c, http.StatusInternalServerError, "replication not supported by this storage backend")
		return
	}

	repMgr, err := repSupport.GetReplicationManager()
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
			userID, principal, "default", id, c.ClientIP(), requestID, 0,
			audit.ResultFailure, err) // #nosec G104 -- Audit logging errors are logged internally

		if errors.Is(err, common.ErrPolicyNotFound) {
			RespondWithError(c, http.StatusNotFound, common.SanitizeErrorMessage(err))
			return
		}
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	_ = auditLogger.LogObjectMutation(c.Request.Context(), "replication_policy_removed",
		userID, principal, "default", id, c.ClientIP(), requestID, 0,
		audit.ResultSuccess, nil) // #nosec G104 -- Audit logging errors are logged internally

	RespondWithSuccess(c, http.StatusOK, "replication policy removed successfully", gin.H{
		"id": id,
	})
}

// GetReplicationPolicies handles listing all replication policies
// @Summary List replication policies
// @Description Retrieve all replication policies
// @Tags replication
// @Produce json
// @Success 200 {object} GetReplicationPoliciesResponse
// @Failure 500 {object} ErrorResponse
// @Router /replication/policies [get]
func (h *Handler) GetReplicationPolicies(c *gin.Context) {
	// Get replication manager from storage
	repSupport, ok := h.storage.(interface {
		GetReplicationManager() (common.ReplicationManager, error)
	})
	if !ok {
		RespondWithError(c, http.StatusInternalServerError, "replication not supported by this storage backend")
		return
	}

	repMgr, err := repSupport.GetReplicationManager()
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
// @Summary Get replication policy
// @Description Retrieve a specific replication policy by ID
// @Tags replication
// @Produce json
// @Param id path string true "Policy ID"
// @Success 200 {object} ReplicationPolicyResponse
// @Failure 404 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /replication/policies/{id} [get]
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

	// Get replication manager from storage
	repSupport, ok := h.storage.(interface {
		GetReplicationManager() (common.ReplicationManager, error)
	})
	if !ok {
		RespondWithError(c, http.StatusInternalServerError, "replication not supported by this storage backend")
		return
	}

	repMgr, err := repSupport.GetReplicationManager()
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

// TriggerReplication handles manually triggering replication
// @Summary Trigger replication
// @Description Manually trigger replication for all policies or a specific policy
// @Tags replication
// @Produce json
// @Param policy_id query string false "Policy ID (empty for all policies)"
// @Success 200 {object} TriggerReplicationResponse
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /replication/trigger [post]
func (h *Handler) TriggerReplication(c *gin.Context) {
	policyID := c.Query("policy_id")

	// Get replication manager from storage
	repSupport, ok := h.storage.(interface {
		GetReplicationManager() (common.ReplicationManager, error)
	})
	if !ok {
		RespondWithError(c, http.StatusInternalServerError, "replication not supported by this storage backend")
		return
	}

	repMgr, err := repSupport.GetReplicationManager()
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
			userID, principal, "default", policyID, c.ClientIP(), requestID, 0,
			audit.ResultFailure, err) // #nosec G104 -- Audit logging errors are logged internally

		if errors.Is(err, common.ErrPolicyNotFound) {
			RespondWithError(c, http.StatusNotFound, common.SanitizeErrorMessage(err))
			return
		}
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	_ = auditLogger.LogObjectMutation(c.Request.Context(), "replication_triggered",
		userID, principal, "default", policyID, c.ClientIP(), requestID, result.BytesTotal,
		audit.ResultSuccess, nil) // #nosec G104 -- Audit logging errors are logged internally

	RespondWithSyncResult(c, result)
}

// GetReplicationStatus handles retrieving replication status for a specific policy
// @Summary Get replication status
// @Description Retrieve status and metrics for a specific replication policy by ID
// @Tags replication
// @Produce json
// @Param id path string true "Policy ID"
// @Success 200 {object} ReplicationStatusResponse
// @Failure 404 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /replication/status/{id} [get]
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

	// Get replication manager from storage
	repSupport, ok := h.storage.(interface {
		GetReplicationManager() (common.ReplicationManager, error)
	})
	if !ok {
		RespondWithError(c, http.StatusInternalServerError, "replication not supported by this storage backend")
		return
	}

	repMgr, err := repSupport.GetReplicationManager()
	if err != nil {
		if errors.Is(err, common.ErrReplicationNotSupported) {
			RespondWithError(c, http.StatusInternalServerError, "replication not supported by this storage backend")
		} else {
			RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		}
		return
	}

	// Get replication status
	replicationStatus, err := repMgr.(interface {
		GetReplicationStatus(id string) (*replication.ReplicationStatus, error)
	}).GetReplicationStatus(id)
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
