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
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/replication"
)

// ErrorResponse represents a standard error response
type ErrorResponse struct {
	Error   string `json:"error" example:"error message"`
	Code    int    `json:"code" example:"400"`
	Message string `json:"message,omitempty" example:"detailed error description"`
} // @name ErrorResponse

// SuccessResponse represents a standard success response
type SuccessResponse struct {
	Message string `json:"message" example:"operation completed successfully"`
	Data    any    `json:"data,omitempty"`
} // @name SuccessResponse

// ObjectResponse represents an object metadata response
type ObjectResponse struct {
	Key         string            `json:"key" example:"path/to/object.txt"`
	Size        int64             `json:"size" example:"1024"`
	Modified    string            `json:"modified,omitempty" example:"2025-11-05T10:00:00Z"`
	ETag        string            `json:"etag,omitempty" example:"d41d8cd98f00b204e9800998ecf8427e"`
	ContentType string            `json:"content_type,omitempty" example:"text/plain"`
	Metadata    map[string]string `json:"metadata,omitempty"`
} // @name ObjectResponse

// ListObjectsResponse represents a paginated list of objects
type ListObjectsResponse struct {
	Objects        []ObjectResponse `json:"objects"`
	CommonPrefixes []string         `json:"common_prefixes,omitempty"`
	NextToken      string           `json:"next_token,omitempty" example:"token123"`
	Truncated      bool             `json:"truncated" example:"false"`
} // @name ListObjectsResponse

// HealthResponse represents the health check response
type HealthResponse struct {
	Status  string `json:"status" example:"healthy"`
	Version string `json:"version,omitempty" example:"0.1.0-beta"`
} // @name HealthResponse

// ArchiveRequest represents a request to archive an object
type ArchiveRequest struct {
	Key                 string            `json:"key" binding:"required" example:"path/to/object.txt"`
	DestinationType     string            `json:"destination_type" binding:"required" example:"s3"`
	DestinationSettings map[string]string `json:"destination_settings,omitempty"`
} // @name ArchiveRequest

// AddPolicyRequest represents a request to add a lifecycle policy
type AddPolicyRequest struct {
	ID                  string            `json:"id" binding:"required" example:"policy-1"`
	Prefix              string            `json:"prefix,omitempty" example:"logs/"`
	Retention           time.Duration     `json:"retention_seconds" binding:"required" example:"2592000"`
	Action              string            `json:"action" binding:"required" example:"delete"`
	DestinationType     string            `json:"destination_type,omitempty" example:"s3"`
	DestinationSettings map[string]string `json:"destination_settings,omitempty"`
} // @name AddPolicyRequest

// PolicyResponse represents a lifecycle policy response
type PolicyResponse struct {
	ID               string `json:"id" example:"policy-1"`
	Prefix           string `json:"prefix,omitempty" example:"logs/"`
	RetentionSeconds int64  `json:"retention_seconds" example:"2592000"`
	Action           string `json:"action" example:"delete"`
	DestinationType  string `json:"destination_type,omitempty" example:"s3"`
} // @name PolicyResponse

// GetPoliciesResponse represents a list of lifecycle policies
type GetPoliciesResponse struct {
	Policies []PolicyResponse `json:"policies"`
	Count    int              `json:"count" example:"5"`
} // @name GetPoliciesResponse

// AddReplicationPolicyRequest represents a request to add a replication policy
type AddReplicationPolicyRequest struct {
	ID                   string                   `json:"id" binding:"required" example:"repl-policy-1"`
	SourceBackend        string                   `json:"source_backend" binding:"required" example:"local"`
	SourceSettings       map[string]string        `json:"source_settings,omitempty"`
	SourcePrefix         string                   `json:"source_prefix,omitempty" example:"data/"`
	DestinationBackend   string                   `json:"destination_backend" binding:"required" example:"s3"`
	DestinationSettings  map[string]string        `json:"destination_settings,omitempty"`
	CheckIntervalSeconds int64                    `json:"check_interval_seconds" binding:"required" example:"300"`
	Enabled              bool                     `json:"enabled" example:"true"`
	ReplicationMode      common.ReplicationMode   `json:"replication_mode,omitempty" example:"transparent"`
	Encryption           *common.EncryptionPolicy `json:"encryption,omitempty"`
} // @name AddReplicationPolicyRequest

// ReplicationPolicyResponse represents a replication policy response
type ReplicationPolicyResponse struct {
	ID                   string                   `json:"id" example:"repl-policy-1"`
	SourceBackend        string                   `json:"source_backend" example:"local"`
	SourceSettings       map[string]string        `json:"source_settings,omitempty"`
	SourcePrefix         string                   `json:"source_prefix,omitempty" example:"data/"`
	DestinationBackend   string                   `json:"destination_backend" example:"s3"`
	DestinationSettings  map[string]string        `json:"destination_settings,omitempty"`
	CheckIntervalSeconds int64                    `json:"check_interval_seconds" example:"300"`
	LastSyncTime         string                   `json:"last_sync_time,omitempty" example:"2025-11-05T10:00:00Z"`
	Enabled              bool                     `json:"enabled" example:"true"`
	ReplicationMode      common.ReplicationMode   `json:"replication_mode" example:"transparent"`
	Encryption           *common.EncryptionPolicy `json:"encryption,omitempty"`
} // @name ReplicationPolicyResponse

// GetReplicationPoliciesResponse represents a list of replication policies
type GetReplicationPoliciesResponse struct {
	Policies []ReplicationPolicyResponse `json:"policies"`
	Count    int                         `json:"count" example:"3"`
} // @name GetReplicationPoliciesResponse

// SyncResultResponse represents the result of a sync operation
type SyncResultResponse struct {
	PolicyID   string   `json:"policy_id" example:"repl-policy-1"`
	Synced     int      `json:"synced" example:"150"`
	Deleted    int      `json:"deleted" example:"5"`
	Failed     int      `json:"failed" example:"2"`
	BytesTotal int64    `json:"bytes_total" example:"1048576"`
	Duration   string   `json:"duration" example:"5.2s"`
	Errors     []string `json:"errors,omitempty"`
} // @name SyncResultResponse

// TriggerReplicationResponse represents the response from triggering replication
type TriggerReplicationResponse struct {
	Success bool                `json:"success" example:"true"`
	Result  *SyncResultResponse `json:"result,omitempty"`
	Message string              `json:"message" example:"Replication triggered successfully"`
} // @name TriggerReplicationResponse

// ReplicationStatusResponse represents replication status and metrics
type ReplicationStatusResponse struct {
	PolicyID             string `json:"policy_id" example:"repl-policy-1"`
	SourceBackend        string `json:"source_backend" example:"local"`
	DestinationBackend   string `json:"destination_backend" example:"s3"`
	Enabled              bool   `json:"enabled" example:"true"`
	TotalObjectsSynced   int64  `json:"total_objects_synced" example:"1500"`
	TotalObjectsDeleted  int64  `json:"total_objects_deleted" example:"50"`
	TotalBytesSynced     int64  `json:"total_bytes_synced" example:"10485760"`
	TotalErrors          int64  `json:"total_errors" example:"3"`
	LastSyncTime         string `json:"last_sync_time,omitempty" example:"2025-11-05T10:00:00Z"`
	AverageSyncDuration  string `json:"average_sync_duration" example:"2.5s"`
	SyncCount            int64  `json:"sync_count" example:"100"`
} // @name ReplicationStatusResponse

// RespondWithError sends a standard error response
func RespondWithError(c *gin.Context, code int, message string) {
	c.JSON(code, ErrorResponse{
		Error:   http.StatusText(code),
		Code:    code,
		Message: message,
	})
}

// RespondWithSuccess sends a standard success response
func RespondWithSuccess(c *gin.Context, code int, message string, data any) {
	response := SuccessResponse{
		Message: message,
	}
	if data != nil {
		response.Data = data
	}
	c.JSON(code, response)
}

// RespondWithObject sends an object metadata response
func RespondWithObject(c *gin.Context, key string, metadata *common.Metadata) {
	if metadata == nil {
		RespondWithError(c, http.StatusInternalServerError, "metadata is nil")
		return
	}

	response := ObjectResponse{
		Key:         key,
		Size:        metadata.Size,
		ETag:        metadata.ETag,
		ContentType: metadata.ContentType,
	}

	if !metadata.LastModified.IsZero() {
		response.Modified = metadata.LastModified.Format("2006-01-02T15:04:05Z07:00")
	}

	if len(metadata.Custom) > 0 {
		response.Metadata = metadata.Custom
	}

	c.JSON(http.StatusOK, response)
}

// RespondWithListObjects sends a paginated list response
func RespondWithListObjects(c *gin.Context, result *common.ListResult) {
	response := ListObjectsResponse{
		Objects:        make([]ObjectResponse, 0, len(result.Objects)),
		CommonPrefixes: result.CommonPrefixes,
		NextToken:      result.NextToken,
		Truncated:      result.Truncated,
	}

	for _, obj := range result.Objects {
		objResp := ObjectResponse{
			Key:  obj.Key,
			Size: obj.Metadata.Size,
			ETag: obj.Metadata.ETag,
		}

		if !obj.Metadata.LastModified.IsZero() {
			objResp.Modified = obj.Metadata.LastModified.Format("2006-01-02T15:04:05Z07:00")
		}

		if len(obj.Metadata.Custom) > 0 {
			objResp.Metadata = obj.Metadata.Custom
		}

		response.Objects = append(response.Objects, objResp)
	}

	c.JSON(http.StatusOK, response)
}

// RespondWithPolicies sends a policies list response
func RespondWithPolicies(c *gin.Context, policies []common.LifecyclePolicy) {
	response := GetPoliciesResponse{
		Policies: make([]PolicyResponse, 0, len(policies)),
		Count:    len(policies),
	}

	for _, policy := range policies {
		policyResp := PolicyResponse{
			ID:               policy.ID,
			Prefix:           policy.Prefix,
			RetentionSeconds: int64(policy.Retention.Seconds()),
			Action:           policy.Action,
		}

		response.Policies = append(response.Policies, policyResp)
	}

	c.JSON(http.StatusOK, response)
}

// RespondWithReplicationPolicies sends a replication policies list response
func RespondWithReplicationPolicies(c *gin.Context, policies []common.ReplicationPolicy) {
	response := GetReplicationPoliciesResponse{
		Policies: make([]ReplicationPolicyResponse, 0, len(policies)),
		Count:    len(policies),
	}

	for _, policy := range policies {
		policyResp := ReplicationPolicyResponse{
			ID:                   policy.ID,
			SourceBackend:        policy.SourceBackend,
			SourceSettings:       policy.SourceSettings,
			SourcePrefix:         policy.SourcePrefix,
			DestinationBackend:   policy.DestinationBackend,
			DestinationSettings:  policy.DestinationSettings,
			CheckIntervalSeconds: int64(policy.CheckInterval.Seconds()),
			Enabled:              policy.Enabled,
			ReplicationMode:      policy.ReplicationMode,
			Encryption:           policy.Encryption,
		}

		if !policy.LastSyncTime.IsZero() {
			policyResp.LastSyncTime = policy.LastSyncTime.Format("2006-01-02T15:04:05Z07:00")
		}

		response.Policies = append(response.Policies, policyResp)
	}

	c.JSON(http.StatusOK, response)
}

// RespondWithReplicationPolicy sends a single replication policy response
func RespondWithReplicationPolicy(c *gin.Context, policy *common.ReplicationPolicy) {
	if policy == nil {
		RespondWithError(c, http.StatusInternalServerError, "policy is nil")
		return
	}

	response := ReplicationPolicyResponse{
		ID:                   policy.ID,
		SourceBackend:        policy.SourceBackend,
		SourceSettings:       policy.SourceSettings,
		SourcePrefix:         policy.SourcePrefix,
		DestinationBackend:   policy.DestinationBackend,
		DestinationSettings:  policy.DestinationSettings,
		CheckIntervalSeconds: int64(policy.CheckInterval.Seconds()),
		Enabled:              policy.Enabled,
		ReplicationMode:      policy.ReplicationMode,
		Encryption:           policy.Encryption,
	}

	if !policy.LastSyncTime.IsZero() {
		response.LastSyncTime = policy.LastSyncTime.Format("2006-01-02T15:04:05Z07:00")
	}

	c.JSON(http.StatusOK, response)
}

// RespondWithSyncResult sends a sync result response
func RespondWithSyncResult(c *gin.Context, result *common.SyncResult) {
	if result == nil {
		RespondWithError(c, http.StatusInternalServerError, "sync result is nil")
		return
	}

	response := TriggerReplicationResponse{
		Success: true,
		Result: &SyncResultResponse{
			PolicyID:   result.PolicyID,
			Synced:     result.Synced,
			Deleted:    result.Deleted,
			Failed:     result.Failed,
			BytesTotal: result.BytesTotal,
			Duration:   result.Duration.String(),
			Errors:     result.Errors,
		},
		Message: "Replication triggered successfully",
	}

	c.JSON(http.StatusOK, response)
}

// RespondWithReplicationStatus sends a replication status response
func RespondWithReplicationStatus(c *gin.Context, status *replication.ReplicationStatus) {
	if status == nil {
		RespondWithError(c, http.StatusInternalServerError, "replication status is nil")
		return
	}

	response := ReplicationStatusResponse{
		PolicyID:            status.PolicyID,
		SourceBackend:       status.SourceBackend,
		DestinationBackend:  status.DestinationBackend,
		Enabled:             status.Enabled,
		TotalObjectsSynced:  status.TotalObjectsSynced,
		TotalObjectsDeleted: status.TotalObjectsDeleted,
		TotalBytesSynced:    status.TotalBytesSynced,
		TotalErrors:         status.TotalErrors,
		AverageSyncDuration: status.AverageSyncDuration.String(),
		SyncCount:           status.SyncCount,
	}

	if !status.LastSyncTime.IsZero() {
		response.LastSyncTime = status.LastSyncTime.Format("2006-01-02T15:04:05Z07:00")
	}

	c.JSON(http.StatusOK, response)
}
