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

package unix

import "github.com/jeremyhahn/go-objstore/pkg/server/jsonrpc"

const jsonRPCVersion = jsonrpc.Version

// Request, Response, and RPCError are the JSON-RPC 2.0 envelope types shared
// with the MCP transport via pkg/server/jsonrpc. Kept as local aliases for
// source compatibility.
type (
	// Request represents a JSON-RPC 2.0 request.
	Request = jsonrpc.Request
	// Response represents a JSON-RPC 2.0 response.
	Response = jsonrpc.Response
	// RPCError represents a JSON-RPC 2.0 error.
	RPCError = jsonrpc.Error
)

// Method names
const (
	MethodPut              = "put"
	MethodGet              = "get"
	MethodDelete           = "delete"
	MethodExists           = "exists"
	MethodList             = "list"
	MethodGetMetadata      = "get_metadata"
	MethodUpdateMetadata   = "update_metadata"
	MethodArchive          = "archive"
	MethodAddPolicy        = "add_policy"
	MethodRemovePolicy     = "remove_policy"
	MethodGetPolicies      = "get_policies"
	MethodApplyPolicies    = "apply_policies"
	MethodAddReplPolicy    = "add_replication_policy"
	MethodRemoveReplPolicy = "remove_replication_policy"
	MethodGetReplPolicy    = "get_replication_policy"
	MethodGetReplPolicies  = "get_replication_policies"
	MethodTriggerRepl      = "trigger_replication"
	MethodGetReplStatus    = "get_replication_status"
	MethodHealth           = "health"
	MethodPing             = "ping"
)

// PutParams represents parameters for the put method
type PutParams struct {
	Key      string          `json:"key"`
	Data     string          `json:"data"` // Base64 encoded
	Metadata *MetadataParams `json:"metadata,omitempty"`
}

// GetParams represents parameters for the get method
type GetParams struct {
	Key string `json:"key"`
}

// DeleteParams represents parameters for the delete method
type DeleteParams struct {
	Key string `json:"key"`
}

// ExistsParams represents parameters for the exists method
type ExistsParams struct {
	Key string `json:"key"`
}

// ListParams represents parameters for the list method
type ListParams struct {
	Prefix       string `json:"prefix,omitempty"`
	Delimiter    string `json:"delimiter,omitempty"`
	MaxResults   int    `json:"max_results,omitempty"`
	ContinueFrom string `json:"continue_from,omitempty"`
}

// MetadataParams represents object metadata
type MetadataParams struct {
	ContentType     string            `json:"content_type,omitempty"`
	ContentEncoding string            `json:"content_encoding,omitempty"`
	Custom          map[string]string `json:"custom,omitempty"`
}

// GetMetadataParams represents parameters for get_metadata
type GetMetadataParams struct {
	Key string `json:"key"`
}

// UpdateMetadataParams represents parameters for update_metadata
type UpdateMetadataParams struct {
	Key      string          `json:"key"`
	Metadata *MetadataParams `json:"metadata"`
}

// ArchiveParams represents parameters for archive
type ArchiveParams struct {
	Key                 string            `json:"key"`
	DestinationType     string            `json:"destination_type"`
	DestinationSettings map[string]string `json:"destination_settings"`
}

// PolicyParams represents lifecycle policy parameters. RetentionSeconds
// expresses the retention with second granularity; when positive it takes
// precedence over AfterDays in requests, and responses always populate it
// (exact) alongside AfterDays (rounded down to whole days) for backward
// compatibility.
type PolicyParams struct {
	ID               string `json:"id"`
	Prefix           string `json:"prefix"`
	Action           string `json:"action"` // delete, archive, transition
	AfterDays        int    `json:"after_days"`
	RetentionSeconds int64  `json:"retention_seconds,omitempty"`
}

// RemovePolicyParams represents parameters for remove_policy
type RemovePolicyParams struct {
	ID string `json:"id"`
}

// ReplicationPolicyParams represents replication policy parameters
type ReplicationPolicyParams struct {
	ID              string            `json:"id"`
	SourcePrefix    string            `json:"source_prefix"`
	DestinationType string            `json:"destination_type"`
	Destination     map[string]string `json:"destination"`
	Schedule        string            `json:"schedule,omitempty"`
	Enabled         bool              `json:"enabled"`
}

// ReplicationPolicyIDParams represents parameters with policy ID
type ReplicationPolicyIDParams struct {
	ID string `json:"id"`
}

// GetResult represents the result of a get operation
type GetResult struct {
	Data     string          `json:"data"` // Base64 encoded
	Metadata *MetadataParams `json:"metadata"`
}

// ExistsResult represents the result of an exists operation
type ExistsResult struct {
	Exists bool `json:"exists"`
}

// ListResult represents the result of a list operation
type ListResult struct {
	Objects     []ObjectInfo `json:"objects"`
	NextCursor  string       `json:"next_cursor,omitempty"`
	IsTruncated bool         `json:"is_truncated"`
}

// ObjectInfo represents information about an object
type ObjectInfo struct {
	Key          string `json:"key"`
	Size         int64  `json:"size"`
	LastModified string `json:"last_modified"`
	ETag         string `json:"etag,omitempty"`
}

// ApplyPoliciesResult represents the result of apply_policies
type ApplyPoliciesResult struct {
	PoliciesCount    int `json:"policies_count"`
	ObjectsProcessed int `json:"objects_processed"`
}

// TriggerReplicationResult represents the result of trigger_replication
type TriggerReplicationResult struct {
	ObjectsSynced    int      `json:"objects_synced"`
	ObjectsFailed    int      `json:"objects_failed"`
	BytesTransferred int64    `json:"bytes_transferred"`
	Errors           []string `json:"errors,omitempty"`
}

// ReplicationStatusResult represents replication status
type ReplicationStatusResult struct {
	PolicyID       string `json:"policy_id"`
	Status         string `json:"status"`
	LastSyncTime   string `json:"last_sync_time,omitempty"`
	ObjectsSynced  int    `json:"objects_synced"`
	ObjectsPending int    `json:"objects_pending"`
	ObjectsFailed  int    `json:"objects_failed"`
}

// HealthResult represents health check result
type HealthResult struct {
	Status  string `json:"status"`
	Version string `json:"version"`
}
