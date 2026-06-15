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

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
	"github.com/jeremyhahn/go-objstore/pkg/replication"
	servererrors "github.com/jeremyhahn/go-objstore/pkg/server/errors"
	"github.com/jeremyhahn/go-objstore/pkg/server/jsonrpc"
	"github.com/jeremyhahn/go-objstore/pkg/version"
)

// Response and log field keys used across the unix socket server.
const (
	fieldStatus = "status"
	fieldError  = "error"
)

// ErrCodeForbidden is the JSON-RPC error code returned when authorization is
// denied. Shared with the MCP transport via pkg/server/jsonrpc.
const ErrCodeForbidden = jsonrpc.CodeForbidden

// Handler handles JSON-RPC requests
type Handler struct {
	backend       string
	logger        adapters.Logger
	authenticator adapters.Authenticator
	authorizer    adapters.Authorizer
}

// NewHandler creates a new handler.
//
// Unix transport carries no per-request HTTP/gRPC/mTLS credential object, so the
// authenticator is invoked via its HTTP entrypoint with a credential-less request.
// With the default NoOpAuthenticator this yields the anonymous principal; custom
// authenticators that require credentials will deny (since none are present).
func NewHandler(backend string, logger adapters.Logger, authenticator adapters.Authenticator, authorizer adapters.Authorizer) *Handler {
	if authenticator == nil {
		authenticator = adapters.NewNoOpAuthenticator()
	}
	if authorizer == nil {
		authorizer = adapters.NewNoOpAuthorizer()
	}
	return &Handler{
		backend:       backend,
		logger:        logger,
		authenticator: authenticator,
		authorizer:    authorizer,
	}
}

// methodAuthz maps a JSON-RPC method name to its required (action, resource)
// pair per the standard taxonomy. Health/ping are public and not present.
var methodAuthz = map[string]struct {
	action   string
	resource string
}{
	MethodGet:              {adapters.ActionRead, ""},
	MethodExists:           {adapters.ActionRead, ""},
	MethodGetMetadata:      {adapters.ActionRead, ""},
	MethodPut:              {adapters.ActionWrite, ""},
	MethodUpdateMetadata:   {adapters.ActionWrite, ""},
	MethodDelete:           {adapters.ActionDelete, ""},
	MethodList:             {adapters.ActionList, ""},
	MethodArchive:          {adapters.ActionAdmin, adapters.ResourcePolicy},
	MethodAddPolicy:        {adapters.ActionAdmin, adapters.ResourcePolicy},
	MethodRemovePolicy:     {adapters.ActionAdmin, adapters.ResourcePolicy},
	MethodGetPolicies:      {adapters.ActionAdmin, adapters.ResourcePolicy},
	MethodApplyPolicies:    {adapters.ActionAdmin, adapters.ResourcePolicy},
	MethodAddReplPolicy:    {adapters.ActionAdmin, adapters.ResourceReplication},
	MethodRemoveReplPolicy: {adapters.ActionAdmin, adapters.ResourceReplication},
	MethodGetReplPolicy:    {adapters.ActionAdmin, adapters.ResourceReplication},
	MethodGetReplPolicies:  {adapters.ActionAdmin, adapters.ResourceReplication},
	MethodTriggerRepl:      {adapters.ActionAdmin, adapters.ResourceReplication},
	MethodGetReplStatus:    {adapters.ActionAdmin, adapters.ResourceReplication},
}

// authorize authenticates the (credential-less) unix request and enforces
// authorization for the given method. It returns an error response on denial,
// or nil to proceed. Health/ping are public.
func (h *Handler) authorize(ctx context.Context, req *Request) *Response {
	mapping, needsAuthz := methodAuthz[req.Method]
	if !needsAuthz {
		// Health, ping, and unknown methods are not gated here; unknown methods
		// are rejected later by the dispatch switch.
		return nil
	}

	// Prefer a peer-credential principal injected at the connection layer
	// (Unix SO_PEERCRED). This is the natural identity for a Unix socket and
	// takes precedence over the transport authenticator. When absent, fall back
	// to the configured Authenticator (NoOp/anonymous by default), which
	// preserves backward compatibility and lets consumers inject a custom one.
	principal, ok := principalFromContext(ctx)
	if !ok {
		// Derive the principal via the authenticator's HTTP entrypoint with a
		// credential-less request (unix transport carries no credentials).
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "/", nil)
		if err != nil {
			return h.errorResponse(req.ID, ErrCodeInternalError, "failed to build auth context")
		}
		principal, err = h.authenticator.AuthenticateHTTP(ctx, httpReq)
		if err != nil {
			return h.errorResponse(req.ID, ErrCodeForbidden, "forbidden")
		}
	}

	if err := h.authorizer.Authorize(ctx, principal, mapping.action, mapping.resource); err != nil {
		h.logger.Warn(ctx, "unix authorization denied",
			adapters.Field{Key: fieldError, Value: err.Error()},
			adapters.Field{Key: "method", Value: req.Method},
		)
		return h.errorResponse(req.ID, ErrCodeForbidden, "forbidden")
	}
	return nil
}

// keyRef builds a key reference with optional backend prefix.
func (h *Handler) keyRef(key string) string {
	if h.backend == "" {
		return key
	}
	return h.backend + ":" + key
}

// Handle processes a JSON-RPC request and returns a response
func (h *Handler) Handle(ctx context.Context, req *Request) *Response {
	// Enforce authentication + authorization before dispatch. Health/ping are
	// public and pass through. The default NoOpAuthorizer allows everything.
	if denied := h.authorize(ctx, req); denied != nil {
		return denied
	}

	switch req.Method {
	case MethodPut:
		return h.handlePut(ctx, req)
	case MethodGet:
		return h.handleGet(ctx, req)
	case MethodDelete:
		return h.handleDelete(ctx, req)
	case MethodExists:
		return h.handleExists(ctx, req)
	case MethodList:
		return h.handleList(ctx, req)
	case MethodGetMetadata:
		return h.handleGetMetadata(ctx, req)
	case MethodUpdateMetadata:
		return h.handleUpdateMetadata(ctx, req)
	case MethodArchive:
		return h.handleArchive(ctx, req)
	case MethodAddPolicy:
		return h.handleAddPolicy(ctx, req)
	case MethodRemovePolicy:
		return h.handleRemovePolicy(ctx, req)
	case MethodGetPolicies:
		return h.handleGetPolicies(ctx, req)
	case MethodApplyPolicies:
		return h.handleApplyPolicies(ctx, req)
	case MethodAddReplPolicy:
		return h.handleAddReplicationPolicy(ctx, req)
	case MethodRemoveReplPolicy:
		return h.handleRemoveReplicationPolicy(ctx, req)
	case MethodGetReplPolicy:
		return h.handleGetReplicationPolicy(ctx, req)
	case MethodGetReplPolicies:
		return h.handleGetReplicationPolicies(ctx, req)
	case MethodTriggerRepl:
		return h.handleTriggerReplication(ctx, req)
	case MethodGetReplStatus:
		return h.handleGetReplicationStatus(ctx, req)
	case MethodHealth, MethodPing:
		return h.handleHealth(ctx, req)
	default:
		return h.errorResponse(req.ID, ErrCodeMethodNotFound, "method not found: "+req.Method)
	}
}

// handlePut handles the put method
func (h *Handler) handlePut(ctx context.Context, req *Request) *Response {
	var params PutParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return h.errorResponse(req.ID, ErrCodeInvalidParams, "invalid parameters")
	}

	if params.Key == "" {
		return h.errorResponse(req.ID, ErrCodeInvalidParams, "key is required")
	}

	// Decode base64 data
	data, err := base64.StdEncoding.DecodeString(params.Data)
	if err != nil {
		return h.errorResponse(req.ID, ErrCodeInvalidParams, "invalid base64 data")
	}

	// Put object using facade
	if params.Metadata != nil {
		// Convert metadata
		metadata := &common.Metadata{
			ContentType:     params.Metadata.ContentType,
			ContentEncoding: params.Metadata.ContentEncoding,
			Custom:          params.Metadata.Custom,
		}
		if err := objstore.PutWithMetadata(ctx, h.keyRef(params.Key), bytes.NewReader(data), metadata); err != nil {
			return h.backendErrorResponse(req.ID, err)
		}
	} else {
		if err := objstore.PutWithContext(ctx, h.keyRef(params.Key), bytes.NewReader(data)); err != nil {
			return h.backendErrorResponse(req.ID, err)
		}
	}

	return h.successResponse(req.ID, map[string]string{fieldStatus: "ok"})
}

// handleGet handles the get method
func (h *Handler) handleGet(ctx context.Context, req *Request) *Response {
	var params GetParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return h.errorResponse(req.ID, ErrCodeInvalidParams, "invalid parameters")
	}

	if params.Key == "" {
		return h.errorResponse(req.ID, ErrCodeInvalidParams, "key is required")
	}

	// Get object using facade
	reader, err := objstore.GetWithContext(ctx, h.keyRef(params.Key))
	if err != nil {
		return h.backendErrorResponse(req.ID, err)
	}
	defer reader.Close()

	// Read all data
	data, err := io.ReadAll(reader)
	if err != nil {
		return h.backendErrorResponse(req.ID, err)
	}

	result := &GetResult{
		Data: base64.StdEncoding.EncodeToString(data),
	}

	// Get metadata separately
	metadata, err := objstore.GetMetadata(ctx, h.keyRef(params.Key))
	if err == nil && metadata != nil {
		result.Metadata = &MetadataParams{
			ContentType:     metadata.ContentType,
			ContentEncoding: metadata.ContentEncoding,
			Custom:          metadata.Custom,
		}
	}

	return h.successResponse(req.ID, result)
}

// handleDelete handles the delete method
func (h *Handler) handleDelete(ctx context.Context, req *Request) *Response {
	var params DeleteParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return h.errorResponse(req.ID, ErrCodeInvalidParams, "invalid parameters")
	}

	if params.Key == "" {
		return h.errorResponse(req.ID, ErrCodeInvalidParams, "key is required")
	}

	if err := objstore.DeleteWithContext(ctx, h.keyRef(params.Key)); err != nil {
		return h.backendErrorResponse(req.ID, err)
	}

	return h.successResponse(req.ID, map[string]string{fieldStatus: "ok"})
}

// handleExists handles the exists method
func (h *Handler) handleExists(ctx context.Context, req *Request) *Response {
	var params ExistsParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return h.errorResponse(req.ID, ErrCodeInvalidParams, "invalid parameters")
	}

	if params.Key == "" {
		return h.errorResponse(req.ID, ErrCodeInvalidParams, "key is required")
	}

	exists, err := objstore.Exists(ctx, h.keyRef(params.Key))
	if err != nil {
		return h.backendErrorResponse(req.ID, err)
	}

	return h.successResponse(req.ID, &ExistsResult{Exists: exists})
}

// handleList handles the list method
func (h *Handler) handleList(ctx context.Context, req *Request) *Response {
	var params ListParams
	if req.Params != nil {
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return h.errorResponse(req.ID, ErrCodeInvalidParams, "invalid parameters")
		}
	}

	opts := &common.ListOptions{
		Prefix:       params.Prefix,
		Delimiter:    params.Delimiter,
		MaxResults:   params.MaxResults,
		ContinueFrom: params.ContinueFrom,
	}

	result, err := objstore.ListWithOptions(ctx, h.backend, opts)
	if err != nil {
		return h.backendErrorResponse(req.ID, err)
	}

	objects := make([]ObjectInfo, 0, len(result.Objects))
	for _, obj := range result.Objects {
		info := ObjectInfo{
			Key: obj.Key,
		}
		if obj.Metadata != nil {
			info.Size = obj.Metadata.Size
			info.ETag = obj.Metadata.ETag
			info.LastModified = obj.Metadata.LastModified.Format(time.RFC3339)
		}
		objects = append(objects, info)
	}

	return h.successResponse(req.ID, &ListResult{
		Objects:     objects,
		NextCursor:  result.NextToken,
		IsTruncated: result.Truncated,
	})
}

// handleGetMetadata handles the get_metadata method
func (h *Handler) handleGetMetadata(ctx context.Context, req *Request) *Response {
	var params GetMetadataParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return h.errorResponse(req.ID, ErrCodeInvalidParams, "invalid parameters")
	}

	if params.Key == "" {
		return h.errorResponse(req.ID, ErrCodeInvalidParams, "key is required")
	}

	metadata, err := objstore.GetMetadata(ctx, h.keyRef(params.Key))
	if err != nil {
		return h.backendErrorResponse(req.ID, err)
	}

	result := &MetadataParams{
		ContentType:     metadata.ContentType,
		ContentEncoding: metadata.ContentEncoding,
		Custom:          metadata.Custom,
	}

	return h.successResponse(req.ID, result)
}

// handleUpdateMetadata handles the update_metadata method
func (h *Handler) handleUpdateMetadata(ctx context.Context, req *Request) *Response {
	var params UpdateMetadataParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return h.errorResponse(req.ID, ErrCodeInvalidParams, "invalid parameters")
	}

	if params.Key == "" {
		return h.errorResponse(req.ID, ErrCodeInvalidParams, "key is required")
	}

	metadata := &common.Metadata{}
	if params.Metadata != nil {
		metadata.ContentType = params.Metadata.ContentType
		metadata.ContentEncoding = params.Metadata.ContentEncoding
		metadata.Custom = params.Metadata.Custom
	}

	if err := objstore.UpdateMetadata(ctx, h.keyRef(params.Key), metadata); err != nil {
		return h.backendErrorResponse(req.ID, err)
	}

	return h.successResponse(req.ID, map[string]string{fieldStatus: "ok"})
}

// handleArchive handles the archive method
func (h *Handler) handleArchive(ctx context.Context, req *Request) *Response {
	var params ArchiveParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return h.errorResponse(req.ID, ErrCodeInvalidParams, "invalid parameters")
	}

	if params.Key == "" {
		return h.errorResponse(req.ID, ErrCodeInvalidParams, "key is required")
	}

	if params.DestinationType == "" {
		return h.errorResponse(req.ID, ErrCodeInvalidParams, "destination_type is required")
	}

	// Create archiver from factory
	archiver, err := factory.NewArchiver(params.DestinationType, params.DestinationSettings)
	if err != nil {
		return h.backendErrorResponse(req.ID, err)
	}

	if err := objstore.Archive(h.keyRef(params.Key), archiver); err != nil {
		return h.backendErrorResponse(req.ID, err)
	}

	return h.successResponse(req.ID, map[string]string{fieldStatus: "ok"})
}

// handleAddPolicy handles the add_policy method
func (h *Handler) handleAddPolicy(ctx context.Context, req *Request) *Response {
	var params PolicyParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return h.errorResponse(req.ID, ErrCodeInvalidParams, "invalid parameters")
	}

	// retention_seconds takes precedence over after_days when positive,
	// allowing sub-day retention over the unix transport.
	retention := time.Duration(params.AfterDays) * 24 * time.Hour
	if params.RetentionSeconds > 0 {
		retention = time.Duration(params.RetentionSeconds) * time.Second
	}

	policy := common.LifecyclePolicy{
		ID:        params.ID,
		Prefix:    params.Prefix,
		Action:    params.Action,
		Retention: retention,
	}

	if err := objstore.AddPolicy(h.backend, policy); err != nil {
		return h.backendErrorResponse(req.ID, err)
	}

	return h.successResponse(req.ID, map[string]string{fieldStatus: "ok"})
}

// handleRemovePolicy handles the remove_policy method
func (h *Handler) handleRemovePolicy(ctx context.Context, req *Request) *Response {
	var params RemovePolicyParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return h.errorResponse(req.ID, ErrCodeInvalidParams, "invalid parameters")
	}

	if params.ID == "" {
		return h.errorResponse(req.ID, ErrCodeInvalidParams, "id is required")
	}

	if err := objstore.RemovePolicy(h.backend, params.ID); err != nil {
		return h.backendErrorResponse(req.ID, err)
	}

	return h.successResponse(req.ID, map[string]string{fieldStatus: "ok"})
}

// handleGetPolicies handles the get_policies method
func (h *Handler) handleGetPolicies(ctx context.Context, req *Request) *Response {
	policies, err := objstore.GetPolicies(h.backend)
	if err != nil {
		return h.backendErrorResponse(req.ID, err)
	}

	result := make([]PolicyParams, 0, len(policies))
	for _, p := range policies {
		result = append(result, PolicyParams{
			ID:     p.ID,
			Prefix: p.Prefix,
			Action: p.Action,
			// after_days is rounded for backward compatibility;
			// retention_seconds carries the exact retention.
			AfterDays:        int(p.Retention.Hours() / 24),
			RetentionSeconds: int64(p.Retention / time.Second),
		})
	}

	return h.successResponse(req.ID, result)
}

// handleApplyPolicies handles the apply_policies method
func (h *Handler) handleApplyPolicies(ctx context.Context, req *Request) *Response {
	// Get policies
	policies, err := objstore.GetPolicies(h.backend)
	if err != nil {
		return h.backendErrorResponse(req.ID, err)
	}

	if len(policies) == 0 {
		return h.successResponse(req.ID, &ApplyPoliciesResult{
			PoliciesCount:    0,
			ObjectsProcessed: 0,
		})
	}

	// Apply policies by listing objects and checking retention
	objectsProcessed := 0
	opts := &common.ListOptions{
		Prefix: "",
	}

	listResult, err := objstore.ListWithOptions(ctx, h.backend, opts)
	if err != nil {
		return h.backendErrorResponse(req.ID, err)
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

			// Apply action
			switch policy.Action {
			case "delete":
				if err := objstore.DeleteWithContext(ctx, h.keyRef(obj.Key)); err != nil {
					continue
				}
				objectsProcessed++
			case "archive":
				if policy.Destination != nil {
					if err := objstore.Archive(h.keyRef(obj.Key), policy.Destination); err != nil {
						continue
					}
					objectsProcessed++
				}
			}
		}
	}

	return h.successResponse(req.ID, &ApplyPoliciesResult{
		PoliciesCount:    len(policies),
		ObjectsProcessed: objectsProcessed,
	})
}

// handleAddReplicationPolicy handles add_replication_policy
func (h *Handler) handleAddReplicationPolicy(ctx context.Context, req *Request) *Response {
	var params ReplicationPolicyParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return h.errorResponse(req.ID, ErrCodeInvalidParams, "invalid parameters")
	}

	// Get replication manager from facade
	repMgr, err := objstore.GetReplicationManager(h.backend)
	if err != nil {
		return h.backendErrorResponse(req.ID, err)
	}

	policy := common.ReplicationPolicy{
		ID:                  params.ID,
		SourcePrefix:        params.SourcePrefix,
		DestinationBackend:  params.DestinationType,
		DestinationSettings: params.Destination,
		Enabled:             params.Enabled,
	}

	if params.Schedule != "" {
		// Parse schedule as duration
		duration, err := time.ParseDuration(params.Schedule)
		if err == nil {
			policy.CheckInterval = duration
		}
	}

	if err := repMgr.AddPolicy(policy); err != nil {
		return h.backendErrorResponse(req.ID, err)
	}

	return h.successResponse(req.ID, map[string]string{fieldStatus: "ok"})
}

// handleRemoveReplicationPolicy handles remove_replication_policy
func (h *Handler) handleRemoveReplicationPolicy(ctx context.Context, req *Request) *Response {
	var params ReplicationPolicyIDParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return h.errorResponse(req.ID, ErrCodeInvalidParams, "invalid parameters")
	}

	if params.ID == "" {
		return h.errorResponse(req.ID, ErrCodeInvalidParams, "id is required")
	}

	// Get replication manager from facade
	repMgr, err := objstore.GetReplicationManager(h.backend)
	if err != nil {
		return h.backendErrorResponse(req.ID, err)
	}

	if err := repMgr.RemovePolicy(params.ID); err != nil {
		return h.backendErrorResponse(req.ID, err)
	}

	return h.successResponse(req.ID, map[string]string{fieldStatus: "ok"})
}

// handleGetReplicationPolicy handles get_replication_policy
func (h *Handler) handleGetReplicationPolicy(ctx context.Context, req *Request) *Response {
	var params ReplicationPolicyIDParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return h.errorResponse(req.ID, ErrCodeInvalidParams, "invalid parameters")
	}

	if params.ID == "" {
		return h.errorResponse(req.ID, ErrCodeInvalidParams, "id is required")
	}

	// Get replication manager from facade
	repMgr, err := objstore.GetReplicationManager(h.backend)
	if err != nil {
		return h.backendErrorResponse(req.ID, err)
	}

	policy, err := repMgr.GetPolicy(params.ID)
	if err != nil {
		return h.backendErrorResponse(req.ID, err)
	}

	result := &ReplicationPolicyParams{
		ID:              policy.ID,
		SourcePrefix:    policy.SourcePrefix,
		DestinationType: policy.DestinationBackend,
		Destination:     policy.DestinationSettings,
		Schedule:        policy.CheckInterval.String(),
		Enabled:         policy.Enabled,
	}

	return h.successResponse(req.ID, result)
}

// handleGetReplicationPolicies handles get_replication_policies
func (h *Handler) handleGetReplicationPolicies(ctx context.Context, req *Request) *Response {
	// Get replication manager from facade
	repMgr, err := objstore.GetReplicationManager(h.backend)
	if err != nil {
		return h.backendErrorResponse(req.ID, err)
	}

	policies, err := repMgr.GetPolicies()
	if err != nil {
		return h.backendErrorResponse(req.ID, err)
	}

	result := make([]ReplicationPolicyParams, 0, len(policies))
	for _, p := range policies {
		result = append(result, ReplicationPolicyParams{
			ID:              p.ID,
			SourcePrefix:    p.SourcePrefix,
			DestinationType: p.DestinationBackend,
			Destination:     p.DestinationSettings,
			Schedule:        p.CheckInterval.String(),
			Enabled:         p.Enabled,
		})
	}

	return h.successResponse(req.ID, result)
}

// handleTriggerReplication handles trigger_replication
func (h *Handler) handleTriggerReplication(ctx context.Context, req *Request) *Response {
	var params ReplicationPolicyIDParams
	if req.Params != nil {
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return h.errorResponse(req.ID, ErrCodeInvalidParams, "invalid parameters")
		}
	}

	// Get replication manager from facade
	repMgr, err := objstore.GetReplicationManager(h.backend)
	if err != nil {
		return h.backendErrorResponse(req.ID, err)
	}

	var syncResult *common.SyncResult

	if params.ID == "" {
		// Sync all policies
		syncResult, err = repMgr.SyncAll(ctx)
	} else {
		// Sync specific policy
		syncResult, err = repMgr.SyncPolicy(ctx, params.ID)
	}

	if err != nil {
		return h.backendErrorResponse(req.ID, err)
	}

	result := &TriggerReplicationResult{
		ObjectsSynced:    syncResult.Synced,
		ObjectsFailed:    syncResult.Failed,
		BytesTransferred: syncResult.BytesTotal,
		Errors:           syncResult.Errors,
	}

	return h.successResponse(req.ID, result)
}

// handleGetReplicationStatus handles get_replication_status
func (h *Handler) handleGetReplicationStatus(ctx context.Context, req *Request) *Response {
	var params ReplicationPolicyIDParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return h.errorResponse(req.ID, ErrCodeInvalidParams, "invalid parameters")
	}

	if params.ID == "" {
		return h.errorResponse(req.ID, ErrCodeInvalidParams, "id is required")
	}

	// Get replication manager from facade
	repMgr, err := objstore.GetReplicationManager(h.backend)
	if err != nil {
		return h.backendErrorResponse(req.ID, err)
	}

	// Get replication status using type assertion
	statusGetter, ok := repMgr.(interface {
		GetReplicationStatus(id string) (*replication.ReplicationStatus, error)
	})
	if !ok {
		return h.errorResponse(req.ID, ErrCodeInternalError, "replication manager does not support status")
	}

	status, err := statusGetter.GetReplicationStatus(params.ID)
	if err != nil {
		return h.backendErrorResponse(req.ID, err)
	}

	var lastSyncTime string
	if !status.LastSyncTime.IsZero() {
		lastSyncTime = status.LastSyncTime.Format(time.RFC3339)
	}

	result := &ReplicationStatusResult{
		PolicyID:       status.PolicyID,
		Status:         "active",
		LastSyncTime:   lastSyncTime,
		ObjectsSynced:  int(status.TotalObjectsSynced),
		ObjectsPending: 0,
		ObjectsFailed:  int(status.TotalErrors),
	}

	return h.successResponse(req.ID, result)
}

// handleHealth handles the health/ping method
func (h *Handler) handleHealth(ctx context.Context, req *Request) *Response {
	return h.successResponse(req.ID, &HealthResult{
		Status:  "ok",
		Version: version.Get(),
	})
}

// successResponse creates a success response
func (h *Handler) successResponse(id any, result any) *Response {
	return &Response{
		JSONRPC: jsonRPCVersion,
		Result:  result,
		ID:      id,
	}
}

// errorResponse creates an error response
func (h *Handler) errorResponse(id any, code int, message string) *Response {
	return jsonrpc.NewError(id, code, message)
}

// backendErrorResponse maps a backend error through the shared taxonomy so
// not-found, permission, and rate-limit errors surface with their proper
// JSON-RPC codes instead of a blanket internal error.
func (h *Handler) backendErrorResponse(id any, err error) *Response {
	code, message := servererrors.JSONRPCError(err)
	return jsonrpc.NewError(id, code, message)
}
