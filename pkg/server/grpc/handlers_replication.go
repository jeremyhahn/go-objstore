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

package grpc

import (
	"context"
	"fmt"
	"time"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/replication"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// AddReplicationPolicy adds a new replication policy.
func (s *Server) AddReplicationPolicy(
	ctx context.Context,
	req *objstorepb.AddReplicationPolicyRequest,
) (*objstorepb.AddReplicationPolicyResponse, error) {
	if req.Policy == nil {
		return nil, status.Error(codes.InvalidArgument, "policy is required")
	}

	if req.Policy.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "policy ID is required")
	}

	// Convert protobuf to domain model
	policy, err := protoToReplicationPolicy(req.Policy)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Get replication manager
	repCapable, ok := s.storage.(common.ReplicationCapable)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "replication not supported by this storage backend")
	}

	repMgr, err := repCapable.GetReplicationManager()
	if err != nil {
		return nil, mapError(err)
	}

	// Add policy
	if err := repMgr.AddPolicy(*policy); err != nil {
		logReplicationAudit(ctx, s.opts.AuditLogger, "REPLICATION_POLICY_ADD_FAILED", req.Policy.Id, err)
		return nil, mapError(err)
	}

	logReplicationAudit(ctx, s.opts.AuditLogger, "REPLICATION_POLICY_ADDED", req.Policy.Id, nil)

	return &objstorepb.AddReplicationPolicyResponse{
		Success: true,
		Message: "Replication policy added successfully",
	}, nil
}

// RemoveReplicationPolicy removes an existing replication policy.
func (s *Server) RemoveReplicationPolicy(
	ctx context.Context,
	req *objstorepb.RemoveReplicationPolicyRequest,
) (*objstorepb.RemoveReplicationPolicyResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "policy ID is required")
	}

	repCapable, ok := s.storage.(common.ReplicationCapable)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "replication not supported by this storage backend")
	}

	repMgr, err := repCapable.GetReplicationManager()
	if err != nil {
		return nil, mapError(err)
	}

	if err := repMgr.RemovePolicy(req.Id); err != nil {
		logReplicationAudit(ctx, s.opts.AuditLogger, "REPLICATION_POLICY_REMOVE_FAILED", req.Id, err)
		return nil, mapError(err)
	}

	logReplicationAudit(ctx, s.opts.AuditLogger, "REPLICATION_POLICY_REMOVED", req.Id, nil)

	return &objstorepb.RemoveReplicationPolicyResponse{
		Success: true,
		Message: "Replication policy removed successfully",
	}, nil
}

// GetReplicationPolicies retrieves all replication policies.
func (s *Server) GetReplicationPolicies(
	ctx context.Context,
	req *objstorepb.GetReplicationPoliciesRequest,
) (*objstorepb.GetReplicationPoliciesResponse, error) {
	repCapable, ok := s.storage.(common.ReplicationCapable)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "replication not supported by this storage backend")
	}

	repMgr, err := repCapable.GetReplicationManager()
	if err != nil {
		return nil, mapError(err)
	}

	policies, err := repMgr.GetPolicies()
	if err != nil {
		return nil, mapError(err)
	}

	// Convert to proto
	protoPolicies := make([]*objstorepb.ReplicationPolicy, len(policies))
	for i, p := range policies {
		protoPolicies[i] = replicationPolicyToProto(&p)
	}

	return &objstorepb.GetReplicationPoliciesResponse{
		Policies: protoPolicies,
	}, nil
}

// GetReplicationPolicy retrieves a specific replication policy.
func (s *Server) GetReplicationPolicy(
	ctx context.Context,
	req *objstorepb.GetReplicationPolicyRequest,
) (*objstorepb.GetReplicationPolicyResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "policy ID is required")
	}

	repCapable, ok := s.storage.(common.ReplicationCapable)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "replication not supported by this storage backend")
	}

	repMgr, err := repCapable.GetReplicationManager()
	if err != nil {
		return nil, mapError(err)
	}

	policy, err := repMgr.GetPolicy(req.Id)
	if err != nil {
		return nil, mapError(err)
	}

	return &objstorepb.GetReplicationPolicyResponse{
		Policy: replicationPolicyToProto(policy),
	}, nil
}

// TriggerReplication triggers synchronization for one or all policies.
func (s *Server) TriggerReplication(
	ctx context.Context,
	req *objstorepb.TriggerReplicationRequest,
) (*objstorepb.TriggerReplicationResponse, error) {
	repCapable, ok := s.storage.(common.ReplicationCapable)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "replication not supported by this storage backend")
	}

	repMgr, err := repCapable.GetReplicationManager()
	if err != nil {
		return nil, mapError(err)
	}

	var result *common.SyncResult

	if req.PolicyId == "" {
		// Sync all policies
		result, err = repMgr.SyncAll(ctx)
	} else {
		// Sync specific policy
		result, err = repMgr.SyncPolicy(ctx, req.PolicyId)
	}

	if err != nil {
		logReplicationAudit(ctx, s.opts.AuditLogger, "REPLICATION_SYNC_FAILED", req.PolicyId, err)
		return nil, mapError(err)
	}

	logReplicationAudit(ctx, s.opts.AuditLogger, "REPLICATION_SYNC_TRIGGERED", req.PolicyId, nil)

	return &objstorepb.TriggerReplicationResponse{
		Success: true,
		Result:  syncResultToProto(result),
		Message: fmt.Sprintf("Synced %d objects", result.Synced),
	}, nil
}

// Conversion helpers

// protoToReplicationPolicy converts protobuf ReplicationPolicy to domain model.
func protoToReplicationPolicy(p *objstorepb.ReplicationPolicy) (*common.ReplicationPolicy, error) {
	if p == nil {
		return nil, ErrPolicyCannotBeNil
	}

	policy := &common.ReplicationPolicy{
		ID:                  p.Id,
		SourceBackend:       p.SourceBackend,
		SourceSettings:      p.SourceSettings,
		SourcePrefix:        p.SourcePrefix,
		DestinationBackend:  p.DestinationBackend,
		DestinationSettings: p.DestinationSettings,
		CheckInterval:       time.Duration(p.CheckIntervalSeconds) * time.Second,
		Enabled:             p.Enabled,
	}

	// Convert last sync time
	if p.LastSyncTime != nil {
		policy.LastSyncTime = p.LastSyncTime.AsTime()
	}

	// Convert replication mode
	switch p.ReplicationMode {
	case objstorepb.ReplicationMode_TRANSPARENT:
		policy.ReplicationMode = common.ReplicationModeTransparent
	case objstorepb.ReplicationMode_OPAQUE:
		policy.ReplicationMode = common.ReplicationModeOpaque
	default:
		policy.ReplicationMode = common.ReplicationModeTransparent
	}

	// Convert encryption policy
	if p.Encryption != nil {
		policy.Encryption = protoToEncryptionPolicy(p.Encryption)
	}

	return policy, nil
}

// replicationPolicyToProto converts domain ReplicationPolicy to protobuf.
func replicationPolicyToProto(p *common.ReplicationPolicy) *objstorepb.ReplicationPolicy {
	if p == nil {
		return nil
	}

	proto := &objstorepb.ReplicationPolicy{
		Id:                    p.ID,
		SourceBackend:         p.SourceBackend,
		SourceSettings:        p.SourceSettings,
		SourcePrefix:          p.SourcePrefix,
		DestinationBackend:    p.DestinationBackend,
		DestinationSettings:   p.DestinationSettings,
		CheckIntervalSeconds:  int64(p.CheckInterval.Seconds()),
		LastSyncTime:          timestamppb.New(p.LastSyncTime),
		Enabled:               p.Enabled,
	}

	// Convert replication mode
	switch p.ReplicationMode {
	case common.ReplicationModeTransparent:
		proto.ReplicationMode = objstorepb.ReplicationMode_TRANSPARENT
	case common.ReplicationModeOpaque:
		proto.ReplicationMode = objstorepb.ReplicationMode_OPAQUE
	default:
		proto.ReplicationMode = objstorepb.ReplicationMode_TRANSPARENT
	}

	// Convert encryption policy
	if p.Encryption != nil {
		proto.Encryption = encryptionPolicyToProto(p.Encryption)
	}

	return proto
}

// protoToEncryptionPolicy converts protobuf EncryptionPolicy to domain model.
func protoToEncryptionPolicy(p *objstorepb.EncryptionPolicy) *common.EncryptionPolicy {
	if p == nil {
		return nil
	}

	policy := &common.EncryptionPolicy{}

	if p.Backend != nil {
		policy.Backend = &common.EncryptionConfig{
			Enabled:    p.Backend.Enabled,
			Provider:   p.Backend.Provider,
			DefaultKey: p.Backend.DefaultKey,
		}
	}

	if p.Source != nil {
		policy.Source = &common.EncryptionConfig{
			Enabled:    p.Source.Enabled,
			Provider:   p.Source.Provider,
			DefaultKey: p.Source.DefaultKey,
		}
	}

	if p.Destination != nil {
		policy.Destination = &common.EncryptionConfig{
			Enabled:    p.Destination.Enabled,
			Provider:   p.Destination.Provider,
			DefaultKey: p.Destination.DefaultKey,
		}
	}

	return policy
}

// encryptionPolicyToProto converts domain EncryptionPolicy to protobuf.
func encryptionPolicyToProto(p *common.EncryptionPolicy) *objstorepb.EncryptionPolicy {
	if p == nil {
		return nil
	}

	proto := &objstorepb.EncryptionPolicy{}

	if p.Backend != nil {
		proto.Backend = &objstorepb.EncryptionConfig{
			Enabled:    p.Backend.Enabled,
			Provider:   p.Backend.Provider,
			DefaultKey: p.Backend.DefaultKey,
		}
	}

	if p.Source != nil {
		proto.Source = &objstorepb.EncryptionConfig{
			Enabled:    p.Source.Enabled,
			Provider:   p.Source.Provider,
			DefaultKey: p.Source.DefaultKey,
		}
	}

	if p.Destination != nil {
		proto.Destination = &objstorepb.EncryptionConfig{
			Enabled:    p.Destination.Enabled,
			Provider:   p.Destination.Provider,
			DefaultKey: p.Destination.DefaultKey,
		}
	}

	return proto
}

// syncResultToProto converts domain SyncResult to protobuf.
func syncResultToProto(r *common.SyncResult) *objstorepb.SyncResult {
	if r == nil {
		return nil
	}

	return &objstorepb.SyncResult{
		PolicyId: r.PolicyID,
		Synced:   int32(r.Synced),   // #nosec G115 -- Conversion is safe - replication counts won't exceed int32 range
		Deleted:  int32(r.Deleted),  // #nosec G115 -- Conversion is safe - replication counts won't exceed int32 range
		Failed:   int32(r.Failed),   // #nosec G115 -- Conversion is safe - replication counts won't exceed int32 range
		BytesTotal: r.BytesTotal,
		DurationMs: r.Duration.Milliseconds(),
		Errors:     r.Errors,
	}
}

// GetReplicationStatus retrieves status and metrics for a specific replication policy.
func (s *Server) GetReplicationStatus(
	ctx context.Context,
	req *objstorepb.GetReplicationStatusRequest,
) (*objstorepb.GetReplicationStatusResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "policy ID is required")
	}

	repCapable, ok := s.storage.(common.ReplicationCapable)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "replication not supported by this storage backend")
	}

	repMgr, err := repCapable.GetReplicationManager()
	if err != nil {
		return nil, mapError(err)
	}

	// GetReplicationStatus is defined in replication_persistent.go
	replicationStatus, err := repMgr.(interface {
		GetReplicationStatus(id string) (*replication.ReplicationStatus, error)
	}).GetReplicationStatus(req.Id)
	if err != nil {
		return nil, mapError(err)
	}

	// Convert to proto
	protoStatus := &objstorepb.ReplicationStatus{
		PolicyId:               replicationStatus.PolicyID,
		SourceBackend:          replicationStatus.SourceBackend,
		DestinationBackend:     replicationStatus.DestinationBackend,
		Enabled:                replicationStatus.Enabled,
		TotalObjectsSynced:     replicationStatus.TotalObjectsSynced,
		TotalObjectsDeleted:    replicationStatus.TotalObjectsDeleted,
		TotalBytesSynced:       replicationStatus.TotalBytesSynced,
		TotalErrors:            replicationStatus.TotalErrors,
		LastSyncTime:           timestamppb.New(replicationStatus.LastSyncTime),
		AverageSyncDurationMs:  replicationStatus.AverageSyncDuration.Milliseconds(),
		SyncCount:              replicationStatus.SyncCount,
	}

	return &objstorepb.GetReplicationStatusResponse{
		Success: true,
		Status:  protoStatus,
		Message: "Replication status retrieved successfully",
	}, nil
}

// logReplicationAudit logs replication-related events to the audit log.
func logReplicationAudit(ctx context.Context, auditLogger audit.AuditLogger, eventType string, policyID string, err error) {
	if auditLogger == nil {
		return
	}

	principal, userID := extractGRPCPrincipal(ctx)
	requestID := audit.GetRequestID(ctx)
	ipAddress := extractGRPCClientIP(ctx)

	result := audit.ResultSuccess
	if err != nil {
		result = audit.ResultFailure
	}

	// Log replication event using a custom event type string
	// Cast to EventType to satisfy the interface
	_ = auditLogger.LogObjectMutation(ctx, audit.EventType(eventType),
		userID, principal, "replication", policyID, ipAddress, requestID, 0,
		result, err) // #nosec G104 -- Audit logging errors are logged internally
}
