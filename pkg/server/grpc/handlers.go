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
	"errors"
	"fmt"
	"io"
	"time"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Constants
const (
	principalUnknown = "unknown"
)

// Error variables
var (
	ErrPoliciesCountExceedsRange = fmt.Errorf("policies count exceeds int32 range")
	ErrPolicyCannotBeNil         = fmt.Errorf("policy cannot be nil")
)

// contextKey is a custom type for context keys to avoid collisions
type contextKey string

// principalContextKey is the context key for storing the authenticated principal
const principalContextKey contextKey = "principal"

// Put stores an object in the backend.
func (s *Server) Put(ctx context.Context, req *objstorepb.PutRequest) (*objstorepb.PutResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}

	// Convert protobuf metadata to common.Metadata
	var metadata *common.Metadata
	if req.Metadata != nil {
		metadata = protoToMetadata(req.Metadata)
	}

	// Create a reader from the data
	reader := &bytesReader{data: req.Data}

	// Store the object
	var err error
	if metadata != nil {
		err = s.storage.PutWithMetadata(ctx, req.Key, reader, metadata)
	} else {
		err = s.storage.PutWithContext(ctx, req.Key, reader)
	}

	// Audit logging
	auditLogger := audit.GetAuditLogger(ctx)
	principal, userID := extractGRPCPrincipal(ctx)
	requestID := audit.GetRequestID(ctx)
	ipAddress := extractGRPCClientIP(ctx)

	bytesTransferred := int64(len(req.Data))
	if err != nil {
		_ = auditLogger.LogObjectMutation(ctx, audit.EventObjectCreated,
			userID, principal, "default", req.Key, ipAddress, requestID, 0,
			audit.ResultFailure, err) // #nosec G104 -- Audit logging errors are logged internally, should not block operations
		return nil, mapError(err)
	}

	_ = auditLogger.LogObjectMutation(ctx, audit.EventObjectCreated,
		userID, principal, "default", req.Key, ipAddress, requestID, bytesTransferred,
		audit.ResultSuccess, nil) // #nosec G104 -- Audit logging errors are logged internally, should not block operations

	// Get the ETag from metadata if available
	etag := ""
	if metadata != nil {
		etag = metadata.ETag
	}

	return &objstorepb.PutResponse{
		Success: true,
		Message: "Object stored successfully",
		Etag:    etag,
	}, nil
}

// Get retrieves an object from the backend with streaming support.
func (s *Server) Get(req *objstorepb.GetRequest, stream objstorepb.ObjectStore_GetServer) error {
	if req.Key == "" {
		return status.Error(codes.InvalidArgument, "key is required")
	}

	ctx := stream.Context()

	// Get the object
	reader, err := s.storage.GetWithContext(ctx, req.Key)
	if err != nil {
		return mapError(err)
	}
	defer func() { _ = reader.Close() }()

	// Get metadata
	metadata, err := s.storage.GetMetadata(ctx, req.Key)
	if err != nil {
		return mapError(err)
	}

	// Send metadata in the first response
	firstResponse := true
	buffer := make([]byte, s.opts.ChunkSize)

	for {
		select {
		case <-ctx.Done():
			return status.Error(codes.Canceled, "request canceled")
		default:
		}

		n, err := reader.Read(buffer)
		if n > 0 {
			resp := &objstorepb.GetResponse{
				Data:   buffer[:n],
				IsLast: false,
			}

			if firstResponse {
				resp.Metadata = metadataToProto(metadata)
				firstResponse = false
			}

			if err := stream.Send(resp); err != nil {
				return mapError(err)
			}
		}

		if err == io.EOF {
			// Send final response
			if err := stream.Send(&objstorepb.GetResponse{
				Data:   []byte{},
				IsLast: true,
			}); err != nil {
				return mapError(err)
			}
			break
		}

		if err != nil {
			return mapError(err)
		}
	}

	return nil
}

// Delete removes an object from the backend.
func (s *Server) Delete(ctx context.Context, req *objstorepb.DeleteRequest) (*objstorepb.DeleteResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}

	err := s.storage.DeleteWithContext(ctx, req.Key)

	// Audit logging
	auditLogger := audit.GetAuditLogger(ctx)
	principal, userID := extractGRPCPrincipal(ctx)
	requestID := audit.GetRequestID(ctx)
	ipAddress := extractGRPCClientIP(ctx)

	if err != nil {
		_ = auditLogger.LogObjectMutation(ctx, audit.EventObjectDeleted,
			userID, principal, "default", req.Key, ipAddress, requestID, 0,
			audit.ResultFailure, err) // #nosec G104 -- Audit logging errors are logged internally, should not block operations
		return nil, mapError(err)
	}

	_ = auditLogger.LogObjectMutation(ctx, audit.EventObjectDeleted,
		userID, principal, "default", req.Key, ipAddress, requestID, 0,
		audit.ResultSuccess, nil) // #nosec G104 -- Audit logging errors are logged internally, should not block operations

	return &objstorepb.DeleteResponse{
		Success: true,
		Message: "Object deleted successfully",
	}, nil
}

// List returns a list of objects that match the given criteria.
func (s *Server) List(ctx context.Context, req *objstorepb.ListRequest) (*objstorepb.ListResponse, error) {
	opts := &common.ListOptions{
		Prefix:       req.Prefix,
		Delimiter:    req.Delimiter,
		MaxResults:   int(req.MaxResults),
		ContinueFrom: req.ContinueFrom,
	}

	result, err := s.storage.ListWithOptions(ctx, opts)
	if err != nil {
		return nil, mapError(err)
	}

	// Convert ObjectInfo to proto
	objects := make([]*objstorepb.ObjectInfo, len(result.Objects))
	for i, obj := range result.Objects {
		objects[i] = &objstorepb.ObjectInfo{
			Key:      obj.Key,
			Metadata: metadataToProto(obj.Metadata),
		}
	}

	return &objstorepb.ListResponse{
		Objects:        objects,
		CommonPrefixes: result.CommonPrefixes,
		NextToken:      result.NextToken,
		Truncated:      result.Truncated,
	}, nil
}

// Exists checks if an object exists in the backend.
func (s *Server) Exists(ctx context.Context, req *objstorepb.ExistsRequest) (*objstorepb.ExistsResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}

	exists, err := s.storage.Exists(ctx, req.Key)
	if err != nil {
		return nil, mapError(err)
	}

	return &objstorepb.ExistsResponse{
		Exists: exists,
	}, nil
}

// GetMetadata retrieves only the metadata for an object.
func (s *Server) GetMetadata(ctx context.Context, req *objstorepb.GetMetadataRequest) (*objstorepb.MetadataResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}

	metadata, err := s.storage.GetMetadata(ctx, req.Key)
	if err != nil {
		return nil, mapError(err)
	}

	if metadata == nil {
		return nil, status.Error(codes.NotFound, "object not found")
	}

	return &objstorepb.MetadataResponse{
		Metadata: metadataToProto(metadata),
		Success:  true,
		Message:  "Metadata retrieved successfully",
	}, nil
}

// UpdateMetadata updates the metadata for an existing object.
func (s *Server) UpdateMetadata(ctx context.Context, req *objstorepb.UpdateMetadataRequest) (*objstorepb.UpdateMetadataResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}

	if req.Metadata == nil {
		return nil, status.Error(codes.InvalidArgument, "metadata is required")
	}

	metadata := protoToMetadata(req.Metadata)
	err := s.storage.UpdateMetadata(ctx, req.Key, metadata)
	if err != nil {
		return nil, mapError(err)
	}

	return &objstorepb.UpdateMetadataResponse{
		Success: true,
		Message: "Metadata updated successfully",
	}, nil
}

// Health performs a health check.
func (s *Server) Health(ctx context.Context, req *objstorepb.HealthRequest) (*objstorepb.HealthResponse, error) {
	// Simple health check - can be extended to check storage backend health
	return &objstorepb.HealthResponse{
		Status:  objstorepb.HealthResponse_SERVING,
		Message: "Service is healthy",
	}, nil
}

// Helper functions

// bytesReader wraps a byte slice to implement io.Reader.
type bytesReader struct {
	data   []byte
	offset int
}

func (r *bytesReader) Read(p []byte) (n int, err error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}

// metadataToProto converts common.Metadata to protobuf Metadata.
func metadataToProto(m *common.Metadata) *objstorepb.Metadata {
	if m == nil {
		return nil
	}

	return &objstorepb.Metadata{
		ContentType:     m.ContentType,
		ContentEncoding: m.ContentEncoding,
		Size:            m.Size,
		LastModified:    timestamppb.New(m.LastModified),
		Etag:            m.ETag,
		Custom:          m.Custom,
	}
}

// protoToMetadata converts protobuf Metadata to common.Metadata.
func protoToMetadata(m *objstorepb.Metadata) *common.Metadata {
	if m == nil {
		return nil
	}

	metadata := &common.Metadata{
		ContentType:     m.ContentType,
		ContentEncoding: m.ContentEncoding,
		Size:            m.Size,
		ETag:            m.Etag,
		Custom:          m.Custom,
	}

	if m.LastModified != nil {
		metadata.LastModified = m.LastModified.AsTime()
	}

	return metadata
}

// mapError maps common errors to gRPC status codes.
func mapError(err error) error {
	if err == nil {
		return nil
	}

	// Check for common error patterns
	errStr := err.Error()

	switch errStr {
	case "not found", "key not found":
		return status.Error(codes.NotFound, err.Error())
	case "already exists":
		return status.Error(codes.AlreadyExists, err.Error())
	case "permission denied":
		return status.Error(codes.PermissionDenied, err.Error())
	case "invalid argument", "invalid key":
		return status.Error(codes.InvalidArgument, err.Error())
	case "deadline exceeded", "context deadline exceeded":
		return status.Error(codes.DeadlineExceeded, err.Error())
	case "canceled", "context canceled":
		return status.Error(codes.Canceled, err.Error())
	default:
		// For unknown errors, check if it's a context error
		if ctx := context.Background(); ctx.Err() != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				return status.Error(codes.Canceled, err.Error())
			}
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return status.Error(codes.DeadlineExceeded, err.Error())
			}
		}
		// Default to Internal error
		return status.Error(codes.Internal, err.Error())
	}
}

// extractGRPCPrincipal extracts the principal information from the gRPC context
func extractGRPCPrincipal(ctx context.Context) (principal string, userID string) {
	if principalValue := ctx.Value(principalContextKey); principalValue != nil {
		if p, ok := principalValue.(adapters.Principal); ok {
			return p.Name, p.ID
		}
	}
	return "", ""
}

// extractGRPCClientIP extracts the client IP address from the gRPC context
func extractGRPCClientIP(ctx context.Context) string {
	// Try to get peer info
	if p, ok := peer.FromContext(ctx); ok {
		return p.Addr.String()
	}

	// Try to get from metadata
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if ips := md.Get("x-forwarded-for"); len(ips) > 0 {
			return ips[0]
		}
		if ips := md.Get("x-real-ip"); len(ips) > 0 {
			return ips[0]
		}
	}

	return principalUnknown
}

// Archive copies an object to an archival storage backend.
func (s *Server) Archive(ctx context.Context, req *objstorepb.ArchiveRequest) (*objstorepb.ArchiveResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}

	if req.DestinationType == "" {
		return nil, status.Error(codes.InvalidArgument, "destination_type is required")
	}

	// Create archiver from factory
	archiver, err := createArchiver(req.DestinationType, req.DestinationSettings)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Perform archive operation
	err = s.storage.Archive(req.Key, archiver)

	// Audit logging
	auditLogger := audit.GetAuditLogger(ctx)
	principal, userID := extractGRPCPrincipal(ctx)
	requestID := audit.GetRequestID(ctx)
	ipAddress := extractGRPCClientIP(ctx)

	if err != nil {
		_ = auditLogger.LogObjectMutation(ctx, audit.EventObjectArchived,
			userID, principal, "default", req.Key, ipAddress, requestID, 0,
			audit.ResultFailure, err) // #nosec G104
		return nil, mapError(err)
	}

	_ = auditLogger.LogObjectMutation(ctx, audit.EventObjectArchived,
		userID, principal, "default", req.Key, ipAddress, requestID, 0,
		audit.ResultSuccess, nil) // #nosec G104

	return &objstorepb.ArchiveResponse{
		Success: true,
		Message: "object archived successfully",
	}, nil
}

// AddPolicy adds a new lifecycle policy.
func (s *Server) AddPolicy(ctx context.Context, req *objstorepb.AddPolicyRequest) (*objstorepb.AddPolicyResponse, error) {
	if req.Policy == nil {
		return nil, status.Error(codes.InvalidArgument, "policy is required")
	}

	if req.Policy.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "policy ID is required")
	}

	// Convert proto policy to common.LifecyclePolicy
	policy, err := protoToLifecyclePolicy(req.Policy)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	err = s.storage.AddPolicy(*policy)
	if err != nil {
		return nil, mapError(err)
	}

	return &objstorepb.AddPolicyResponse{
		Success: true,
		Message: "policy added successfully",
	}, nil
}

// RemovePolicy removes an existing lifecycle policy.
func (s *Server) RemovePolicy(ctx context.Context, req *objstorepb.RemovePolicyRequest) (*objstorepb.RemovePolicyResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "policy ID is required")
	}

	err := s.storage.RemovePolicy(req.Id)
	if err != nil {
		return nil, mapError(err)
	}

	return &objstorepb.RemovePolicyResponse{
		Success: true,
		Message: "policy removed successfully",
	}, nil
}

// GetPolicies retrieves all lifecycle policies.
func (s *Server) GetPolicies(ctx context.Context, req *objstorepb.GetPoliciesRequest) (*objstorepb.GetPoliciesResponse, error) {
	policies, err := s.storage.GetPolicies()
	if err != nil {
		return nil, mapError(err)
	}

	// Convert to proto policies
	protoPolicies := make([]*objstorepb.LifecyclePolicy, 0, len(policies))
	for _, policy := range policies {
		// Filter by prefix if specified
		if req.Prefix != "" && policy.Prefix != req.Prefix {
			continue
		}

		protoPolicy := lifecyclePolicyToProto(&policy)
		protoPolicies = append(protoPolicies, protoPolicy)
	}

	return &objstorepb.GetPoliciesResponse{
		Policies: protoPolicies,
		Success:  true,
		Message:  "policies retrieved successfully",
	}, nil
}

// ApplyPolicies executes all lifecycle policies.
func (s *Server) ApplyPolicies(ctx context.Context, req *objstorepb.ApplyPoliciesRequest) (*objstorepb.ApplyPoliciesResponse, error) {
	policies, err := s.storage.GetPolicies()
	if err != nil {
		return nil, mapError(err)
	}

	if len(policies) == 0 {
		return &objstorepb.ApplyPoliciesResponse{
			Success:        true,
			PoliciesCount:  0,
			ObjectsProcessed: 0,
			Message:        "no lifecycle policies to apply",
		}, nil
	}

	// Apply policies by listing objects and checking retention
	objectsProcessed := int32(0)
	opts := &common.ListOptions{
		Prefix: "",
	}
	result, err := s.storage.ListWithOptions(ctx, opts)
	if err != nil {
		return nil, mapError(err)
	}

	for _, policy := range policies {
		for _, obj := range result.Objects {
			// Check if object matches policy prefix
			if policy.Prefix != "" && !hasPrefix(obj.Key, policy.Prefix) {
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
				if err := s.storage.DeleteWithContext(ctx, obj.Key); err != nil {
					s.opts.Logger.Error(ctx, "Failed to delete object during policy application",
						adapters.Field{Key: "key", Value: obj.Key},
						adapters.Field{Key: "error", Value: err.Error()},
					)
					continue
				}
				objectsProcessed++
			case "archive":
				if policy.Destination != nil {
					if err := s.storage.Archive(obj.Key, policy.Destination); err != nil {
						s.opts.Logger.Error(ctx, "Failed to archive object during policy application",
							adapters.Field{Key: "key", Value: obj.Key},
							adapters.Field{Key: "error", Value: err.Error()},
						)
						continue
					}
					objectsProcessed++
				}
			}
		}
	}

	// Safe conversion with overflow check
	policiesCount := len(policies)
	if policiesCount > 2147483647 {
		return nil, ErrPoliciesCountExceedsRange
	}

	return &objstorepb.ApplyPoliciesResponse{
		Success:          true,
		PoliciesCount:    int32(policiesCount),
		ObjectsProcessed: objectsProcessed,
		Message:          "lifecycle policies applied successfully",
	}, nil
}

// hasPrefix checks if a string starts with the given prefix.
func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[0:len(prefix)] == prefix
}

// protoToLifecyclePolicy converts proto LifecyclePolicy to common.LifecyclePolicy.
func protoToLifecyclePolicy(p *objstorepb.LifecyclePolicy) (*common.LifecyclePolicy, error) {
	if p == nil {
		return nil, common.ErrPolicyNil
	}

	policy := &common.LifecyclePolicy{
		ID:        p.Id,
		Prefix:    p.Prefix,
		Retention: time.Duration(p.RetentionSeconds) * time.Second,
		Action:    p.Action,
	}

	// Create archiver if action is "archive"
	if p.Action == "archive" {
		if p.DestinationType == "" {
			return nil, common.ErrDestinationTypeRequired
		}
		archiver, err := createArchiver(p.DestinationType, p.DestinationSettings)
		if err != nil {
			return nil, err
		}
		policy.Destination = archiver
	}

	return policy, nil
}

// lifecyclePolicyToProto converts common.LifecyclePolicy to proto LifecyclePolicy.
func lifecyclePolicyToProto(p *common.LifecyclePolicy) *objstorepb.LifecyclePolicy {
	if p == nil {
		return nil
	}

	proto := &objstorepb.LifecyclePolicy{
		Id:               p.ID,
		Prefix:           p.Prefix,
		RetentionSeconds: int64(p.Retention.Seconds()),
		Action:           p.Action,
	}

	return proto
}

// createArchiver creates an archiver from factory based on destination type.
func createArchiver(destinationType string, settings map[string]string) (common.Archiver, error) {
	// Import factory package
	return factory.NewArchiver(destinationType, settings)
}
