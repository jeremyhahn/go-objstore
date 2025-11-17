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
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
)

// Handler implements HTTP/3 request handlers for object storage operations.
type Handler struct {
	storage            common.Storage
	maxRequestBodySize int64
	readTimeout        time.Duration
	writeTimeout       time.Duration
	logger             adapters.Logger
	authenticator      adapters.Authenticator
}

// NewHandler creates a new HTTP/3 handler.
func NewHandler(storage common.Storage, maxRequestBodySize int64, readTimeout, writeTimeout time.Duration, logger adapters.Logger, authenticator adapters.Authenticator) *Handler {
	return &Handler{
		storage:            storage,
		maxRequestBodySize: maxRequestBodySize,
		readTimeout:        readTimeout,
		writeTimeout:       writeTimeout,
		logger:             logger,
		authenticator:      authenticator,
	}
}

// ServeHTTP handles HTTP/3 requests and routes them to appropriate handlers.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	// Set CORS headers for cross-origin requests
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, DELETE, HEAD, PATCH, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Authorization")

	// Handle preflight OPTIONS request
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Skip authentication for health endpoint
	if r.URL.Path == "/health" {
		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		h.handleHealth(rw, r)
		return
	}

	// Authenticate the request
	principal, err := h.authenticator.AuthenticateHTTP(r.Context(), r)
	if err != nil {
		h.logger.Warn(r.Context(), "QUIC authentication failed",
			adapters.Field{Key: "error", Value: err.Error()},
			adapters.Field{Key: "path", Value: r.URL.Path},
			adapters.Field{Key: "method", Value: r.Method},
		)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Add principal to context and logger
	ctx := context.WithValue(r.Context(), "principal", principal)
	r = r.WithContext(ctx)

	h.logger = h.logger.WithFields(
		adapters.Field{Key: "principal_id", Value: principal.ID},
		adapters.Field{Key: "principal_name", Value: principal.Name},
	)

	// Create a response writer wrapper to capture status code
	rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

	// Route based on path
	if strings.HasPrefix(r.URL.Path, "/objects/") {
		h.handleObject(rw, r)
	} else if r.URL.Path == "/objects" {
		h.handleList(rw, r)
	} else if r.URL.Path == "/archive" {
		h.handleArchive(rw, r)
	} else if r.URL.Path == "/policies/apply" {
		h.handleApplyPolicies(rw, r)
	} else if r.URL.Path == "/policies" {
		h.handlePolicies(rw, r)
	} else if strings.HasPrefix(r.URL.Path, "/policies/") {
		h.handlePolicyByID(rw, r)
	} else if r.URL.Path == "/replication/trigger" {
		h.handleTriggerReplication(rw, r)
	} else if r.URL.Path == "/replication/policies" {
		h.handleReplicationPolicies(rw, r)
	} else if strings.HasPrefix(r.URL.Path, "/replication/policies/") {
		h.handleReplicationPolicyByID(rw, r)
	} else if strings.HasPrefix(r.URL.Path, "/replication/status/") {
		h.handleGetReplicationStatus(rw, r)
	} else {
		http.Error(rw, "not found", http.StatusNotFound)
	}

	// Log the request
	duration := time.Since(start)
	fields := []adapters.Field{
		{Key: "method", Value: r.Method},
		{Key: "path", Value: r.URL.Path},
		{Key: "status", Value: rw.statusCode},
		{Key: "duration", Value: duration.String()},
		{Key: "protocol", Value: "HTTP/3"},
	}

	if rw.statusCode >= 500 {
		h.logger.Error(r.Context(), "QUIC request completed", fields...)
	} else if rw.statusCode >= 400 {
		h.logger.Warn(r.Context(), "QUIC request completed", fields...)
	} else {
		h.logger.Info(r.Context(), "QUIC request completed", fields...)
	}
}

// responseWriter wraps http.ResponseWriter to capture the status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// handleHealth handles health check requests.
func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]string{
		"status":   "healthy",
		"protocol": "HTTP/3",
	}); err != nil {
		h.logger.Error(r.Context(), "failed to encode health response", adapters.Field{Key: "error", Value: err.Error()})
	}
}

// handleObject handles requests for individual objects.
func (h *Handler) handleObject(w http.ResponseWriter, r *http.Request) {
	// Extract key from path
	key := strings.TrimPrefix(r.URL.Path, "/objects/")
	if key == "" {
		http.Error(w, "invalid key", http.StatusBadRequest)
		return
	}

	// Decode URL-encoded key
	key = path.Clean(key)

	// Check for exists query parameter
	if r.Method == http.MethodGet && r.URL.Query().Get("exists") != "" {
		h.handleExists(w, r, key)
		return
	}

	// Check for metadata update via PATCH
	if r.Method == http.MethodPatch {
		h.handleUpdateMetadata(w, r, key)
		return
	}

	switch r.Method {
	case http.MethodPut:
		h.handlePut(w, r, key)
	case http.MethodGet:
		h.handleGet(w, r, key)
	case http.MethodDelete:
		h.handleDelete(w, r, key)
	case http.MethodHead:
		h.handleHead(w, r, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handlePut handles PUT requests to store objects.
func (h *Handler) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	ctx, cancel := context.WithTimeout(r.Context(), h.writeTimeout)
	defer cancel()

	// Check content length
	if r.ContentLength > h.maxRequestBodySize {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}

	// Limit request body size
	limitedReader := io.LimitReader(r.Body, h.maxRequestBodySize)

	// Extract metadata from headers
	metadata := &common.Metadata{
		ContentType:     r.Header.Get("Content-Type"),
		ContentEncoding: r.Header.Get("Content-Encoding"),
		Custom:          make(map[string]string),
	}

	// Extract custom metadata from X-Meta-* headers
	for headerName, values := range r.Header {
		if strings.HasPrefix(headerName, "X-Meta-") && len(values) > 0 {
			metaKey := strings.TrimPrefix(headerName, "X-Meta-")
			metadata.Custom[metaKey] = values[0]
		}
	}

	// Store the object
	err := h.storage.PutWithMetadata(ctx, key, limitedReader, metadata)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			http.Error(w, "request timeout", http.StatusRequestTimeout)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(map[string]string{
		"key":     key,
		"message": "object stored successfully",
	}); err != nil {
		// Log error but response already started
		h.logger.Error(r.Context(), "failed to encode response", adapters.Field{Key: "error", Value: err.Error()})
	}
}

// handleGet handles GET requests to retrieve objects.
func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	ctx, cancel := context.WithTimeout(r.Context(), h.readTimeout)
	defer cancel()

	// Get object metadata first
	info, err := h.storage.GetMetadata(ctx, key)
	if err != nil || info == nil {
		http.Error(w, "object not found", http.StatusNotFound)
		return
	}

	// Get object data
	reader, err := h.storage.GetWithContext(ctx, key)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			http.Error(w, "request timeout", http.StatusRequestTimeout)
			return
		}
		http.Error(w, "object not found", http.StatusNotFound)
		return
	}
	defer reader.Close()

	// Set response headers
	if info.ContentType != "" {
		w.Header().Set("Content-Type", info.ContentType)
	}
	if info.ContentEncoding != "" {
		w.Header().Set("Content-Encoding", info.ContentEncoding)
	}
	if info.ETag != "" {
		w.Header().Set("ETag", info.ETag)
	}
	w.Header().Set("Last-Modified", info.LastModified.Format(http.TimeFormat))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", info.Size))

	// Set custom metadata headers
	if info.Custom != nil {
		for k, v := range info.Custom {
			w.Header().Set("X-Meta-"+k, v)
		}
	}

	// Copy object data to response
	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, reader); err != nil {
		// Cannot send error headers after data has been written
		// Log error or use middleware to handle this
		return
	}
}

// handleDelete handles DELETE requests to remove objects.
func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	ctx, cancel := context.WithTimeout(r.Context(), h.writeTimeout)
	defer cancel()

	// Delete the object
	err := h.storage.DeleteWithContext(ctx, key)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			http.Error(w, "request timeout", http.StatusRequestTimeout)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleHead handles HEAD requests to get object metadata.
func (h *Handler) handleHead(w http.ResponseWriter, r *http.Request, key string) {
	ctx, cancel := context.WithTimeout(r.Context(), h.readTimeout)
	defer cancel()

	// Get object metadata
	info, err := h.storage.GetMetadata(ctx, key)
	if err != nil || info == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// Set response headers
	if info.ContentType != "" {
		w.Header().Set("Content-Type", info.ContentType)
	}
	if info.ContentEncoding != "" {
		w.Header().Set("Content-Encoding", info.ContentEncoding)
	}
	if info.ETag != "" {
		w.Header().Set("ETag", info.ETag)
	}
	w.Header().Set("Last-Modified", info.LastModified.Format(http.TimeFormat))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", info.Size))

	// Set custom metadata headers
	if info.Custom != nil {
		for k, v := range info.Custom {
			w.Header().Set("X-Meta-"+k, v)
		}
	}

	w.WriteHeader(http.StatusOK)
}

// handleList handles GET requests to list objects.
func (h *Handler) handleList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.readTimeout)
	defer cancel()

	// Parse query parameters
	query := r.URL.Query()
	options := &common.ListOptions{
		Prefix:       query.Get("prefix"),
		Delimiter:    query.Get("delimiter"),
		ContinueFrom: query.Get("continue"),
	}

	// Parse max results
	if maxStr := query.Get("max"); maxStr != "" {
		var maxResults int
		if _, err := fmt.Sscanf(maxStr, "%d", &maxResults); err == nil {
			options.MaxResults = maxResults
		}
	}

	// List objects
	result, err := h.storage.ListWithOptions(ctx, options)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			http.Error(w, "request timeout", http.StatusRequestTimeout)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Build response
	response := map[string]any{
		"objects":   result.Objects,
		"truncated": result.Truncated,
	}

	if len(result.CommonPrefixes) > 0 {
		response["prefixes"] = result.CommonPrefixes
	}

	if result.NextToken != "" {
		response["next_token"] = result.NextToken
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		// Log error but response already started
		h.logger.Error(r.Context(), "failed to encode response", adapters.Field{Key: "error", Value: err.Error()})
	}
}

// handleExists handles requests to check if an object exists.
func (h *Handler) handleExists(w http.ResponseWriter, r *http.Request, key string) {
	ctx, cancel := context.WithTimeout(r.Context(), h.readTimeout)
	defer cancel()

	exists, err := h.storage.Exists(ctx, key)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			http.Error(w, "request timeout", http.StatusRequestTimeout)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]bool{
		"exists": exists,
	}); err != nil {
		h.logger.Error(r.Context(), "failed to encode response", adapters.Field{Key: "error", Value: err.Error()})
	}
}

// handleUpdateMetadata handles PATCH requests to update object metadata.
func (h *Handler) handleUpdateMetadata(w http.ResponseWriter, r *http.Request, key string) {
	ctx, cancel := context.WithTimeout(r.Context(), h.writeTimeout)
	defer cancel()

	// Parse request body
	var req struct {
		ContentType     string            `json:"content_type,omitempty"`
		ContentEncoding string            `json:"content_encoding,omitempty"`
		Custom          map[string]string `json:"custom,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Build metadata
	metadata := &common.Metadata{
		ContentType:     req.ContentType,
		ContentEncoding: req.ContentEncoding,
		Custom:          req.Custom,
	}

	// Update metadata
	err := h.storage.UpdateMetadata(ctx, key, metadata)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			http.Error(w, "request timeout", http.StatusRequestTimeout)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]string{
		"message": "metadata updated successfully",
		"key":     key,
	}); err != nil {
		h.logger.Error(r.Context(), "failed to encode response", adapters.Field{Key: "error", Value: err.Error()})
	}
}

// handleArchive handles POST requests to archive objects.
func (h *Handler) handleArchive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.writeTimeout)
	defer cancel()

	// Parse request body
	var req struct {
		Key                 string            `json:"key"`
		DestinationType     string            `json:"destination_type"`
		DestinationSettings map[string]string `json:"destination_settings,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.Key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	if req.DestinationType == "" {
		http.Error(w, "destination_type is required", http.StatusBadRequest)
		return
	}

	// Create archiver from factory
	archiver, err := createArchiver(req.DestinationType, req.DestinationSettings)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Archive the object
	err = h.storage.Archive(req.Key, archiver)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			http.Error(w, "request timeout", http.StatusRequestTimeout)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]string{
		"message":     "object archived successfully",
		"key":         req.Key,
		"destination": req.DestinationType,
	}); err != nil {
		h.logger.Error(r.Context(), "failed to encode response", adapters.Field{Key: "error", Value: err.Error()})
	}
}

// handlePolicies handles GET and POST requests for lifecycle policies.
func (h *Handler) handlePolicies(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.handleGetPolicies(w, r)
	case http.MethodPost:
		h.handleAddPolicy(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleGetPolicies handles GET requests to list lifecycle policies.
func (h *Handler) handleGetPolicies(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), h.readTimeout)
	defer cancel()

	prefix := r.URL.Query().Get("prefix")

	policies, err := h.storage.GetPolicies()
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			http.Error(w, "request timeout", http.StatusRequestTimeout)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
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
			"retention_seconds": int64(policy.Retention.Seconds()),
			"action":            policy.Action,
		}
	}

	response := map[string]any{
		"policies": policyResults,
		"count":    len(policyResults),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		h.logger.Error(r.Context(), "failed to encode response", adapters.Field{Key: "error", Value: err.Error()})
	}
}

// handleAddPolicy handles POST requests to add a lifecycle policy.
func (h *Handler) handleAddPolicy(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), h.writeTimeout)
	defer cancel()

	// Parse request body
	var req struct {
		ID                  string            `json:"id"`
		Prefix              string            `json:"prefix,omitempty"`
		RetentionSeconds    int64             `json:"retention_seconds"`
		Action              string            `json:"action"`
		DestinationType     string            `json:"destination_type,omitempty"`
		DestinationSettings map[string]string `json:"destination_settings,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.ID == "" {
		http.Error(w, "policy ID is required", http.StatusBadRequest)
		return
	}

	if req.Action == "" {
		http.Error(w, "action is required", http.StatusBadRequest)
		return
	}

	if req.Action != "delete" && req.Action != "archive" {
		http.Error(w, "action must be 'delete' or 'archive'", http.StatusBadRequest)
		return
	}

	if req.RetentionSeconds <= 0 {
		http.Error(w, "retention_seconds must be positive", http.StatusBadRequest)
		return
	}

	// Build lifecycle policy
	policy := common.LifecyclePolicy{
		ID:        req.ID,
		Prefix:    req.Prefix,
		Retention: time.Duration(req.RetentionSeconds) * time.Second,
		Action:    req.Action,
	}

	// Create archiver if action is "archive"
	if req.Action == "archive" {
		if req.DestinationType == "" {
			http.Error(w, "destination_type required for archive action", http.StatusBadRequest)
			return
		}

		archiver, err := createArchiver(req.DestinationType, req.DestinationSettings)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		policy.Destination = archiver
	}

	err := h.storage.AddPolicy(policy)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			http.Error(w, "request timeout", http.StatusRequestTimeout)
			return
		}
		if err.Error() == "policy already exists" {
			http.Error(w, "policy already exists", http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(map[string]string{
		"message": "policy added successfully",
		"id":      req.ID,
	}); err != nil {
		h.logger.Error(r.Context(), "failed to encode response", adapters.Field{Key: "error", Value: err.Error()})
	}
}

// handlePolicyByID handles DELETE requests for individual policies.
func (h *Handler) handlePolicyByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.writeTimeout)
	defer cancel()

	// Extract policy ID from path
	id := strings.TrimPrefix(r.URL.Path, "/policies/")
	if id == "" {
		http.Error(w, "policy ID is required", http.StatusBadRequest)
		return
	}

	err := h.storage.RemovePolicy(id)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			http.Error(w, "request timeout", http.StatusRequestTimeout)
			return
		}
		if err == common.ErrPolicyNotFound {
			http.Error(w, "policy not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]string{
		"message": "policy removed successfully",
		"id":      id,
	}); err != nil {
		h.logger.Error(r.Context(), "failed to encode response", adapters.Field{Key: "error", Value: err.Error()})
	}
}

// handleApplyPolicies handles POST requests to apply all lifecycle policies.
func (h *Handler) handleApplyPolicies(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.writeTimeout)
	defer cancel()

	policies, err := h.storage.GetPolicies()
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			http.Error(w, "request timeout", http.StatusRequestTimeout)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if len(policies) == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(map[string]any{
			"message":           "no lifecycle policies to apply",
			"policies_count":    0,
			"objects_processed": 0,
		}); err != nil {
			h.logger.Error(r.Context(), "failed to encode response", adapters.Field{Key: "error", Value: err.Error()})
		}
		return
	}

	// Apply policies by listing objects and checking retention
	objectsProcessed := 0
	opts := &common.ListOptions{
		Prefix: "",
	}
	result, err := h.storage.ListWithOptions(ctx, opts)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			http.Error(w, "request timeout", http.StatusRequestTimeout)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for _, policy := range policies {
		for _, obj := range result.Objects {
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
				if err := h.storage.DeleteWithContext(ctx, obj.Key); err != nil {
					h.logger.Error(ctx, "Failed to delete object during policy application",
						adapters.Field{Key: "key", Value: obj.Key},
						adapters.Field{Key: "error", Value: err.Error()},
					)
					continue
				}
				objectsProcessed++
			case "archive":
				if policy.Destination != nil {
					if err := h.storage.Archive(obj.Key, policy.Destination); err != nil {
						h.logger.Error(ctx, "Failed to archive object during policy application",
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

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]any{
		"message":           "lifecycle policies applied successfully",
		"policies_count":    len(policies),
		"objects_processed": objectsProcessed,
	}); err != nil {
		h.logger.Error(r.Context(), "failed to encode response", adapters.Field{Key: "error", Value: err.Error()})
	}
}

// createArchiver creates an archiver from factory based on destination type
func createArchiver(destinationType string, settings map[string]string) (common.Archiver, error) {
	return factory.NewArchiver(destinationType, settings)
}
