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
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
	"github.com/jeremyhahn/go-objstore/pkg/validation"
	"github.com/jeremyhahn/go-objstore/pkg/version"
)

const (
	// DefaultListLimit is the default number of objects to return in a list operation
	DefaultListLimit = 100

	// MaxListLimit is the maximum number of objects to return in a list operation
	MaxListLimit = 1000
)

// Handler handles REST API requests using the ObjstoreFacade
type Handler struct {
	backend string // Backend name (empty = default)
}

// NewHandler creates a new Handler instance.
// The backend parameter specifies which backend to route to (empty = default).
// The ObjstoreFacade must be initialized before calling NewHandler.
func NewHandler(backend string) (*Handler, error) {
	if !objstore.IsInitialized() {
		return nil, objstore.ErrNotInitialized
	}
	return &Handler{
		backend: backend,
	}, nil
}

// keyRef builds a key reference with optional backend prefix
func (h *Handler) keyRef(key string) string {
	if h.backend == "" {
		return key
	}
	return h.backend + ":" + key
}

// PutObject handles object upload
func (h *Handler) PutObject(c *gin.Context) {
	key := c.Param("key")
	if key == "" {
		RespondWithError(c, http.StatusBadRequest, "key parameter is required")
		return
	}

	// Remove leading slashes if present
	for len(key) > 0 && key[0] == '/' {
		key = key[1:]
	}

	var reader io.Reader
	var metadata *common.Metadata

	// Check if this is a multipart upload
	contentType := c.GetHeader("Content-Type")
	if strings.HasPrefix(contentType, "multipart/form-data") || c.Request.MultipartForm != nil {
		// Handle multipart upload
		file, header, err := c.Request.FormFile("file")
		if err != nil {
			RespondWithError(c, http.StatusBadRequest, "failed to read file from multipart form: "+err.Error())
			return
		}
		defer func() { _ = file.Close() }()

		reader = file

		// Parse metadata if provided
		metadataStr := c.PostForm("metadata")
		if metadataStr != "" {
			metadata = &common.Metadata{}
			if err := json.Unmarshal([]byte(metadataStr), metadata); err != nil {
				RespondWithError(c, http.StatusBadRequest, "invalid metadata JSON: "+err.Error())
				return
			}
		} else {
			metadata = &common.Metadata{
				ContentType: header.Header.Get("Content-Type"),
				Size:        header.Size,
			}
		}
	} else {
		// Handle direct body upload (streaming)
		reader = c.Request.Body

		// Parse metadata from header if provided
		metadataHeader := c.GetHeader("X-Metadata")
		if metadataHeader != "" {
			metadata = &common.Metadata{}
			if err := json.Unmarshal([]byte(metadataHeader), metadata); err != nil {
				RespondWithError(c, http.StatusBadRequest, "invalid metadata JSON in header: "+err.Error())
				return
			}
		} else {
			metadata = &common.Metadata{
				ContentType: c.GetHeader("Content-Type"),
			}
		}
	}

	// Store the object using facade
	err := objstore.PutWithMetadata(c.Request.Context(), h.keyRef(key), reader, metadata)

	// Audit logging
	auditLogger := audit.GetAuditLogger(c.Request.Context())
	principal, userID := extractPrincipal(c)
	requestID := audit.GetRequestID(c.Request.Context())

	if err != nil {
		_ = auditLogger.LogObjectMutation(c.Request.Context(), audit.EventObjectCreated,
			userID, principal, h.backend, key, c.ClientIP(), requestID, 0,
			audit.ResultFailure, err)
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	bytesTransferred := metadata.Size
	_ = auditLogger.LogObjectMutation(c.Request.Context(), audit.EventObjectCreated,
		userID, principal, h.backend, key, c.ClientIP(), requestID, bytesTransferred,
		audit.ResultSuccess, nil)

	// Get the stored metadata to retrieve the ETag
	var etag string
	storedMetadata, metaErr := objstore.GetMetadata(c.Request.Context(), h.keyRef(key))
	if metaErr == nil && storedMetadata != nil && storedMetadata.ETag != "" {
		etag = storedMetadata.ETag
		c.Header("ETag", etag)
	}

	RespondWithSuccess(c, http.StatusCreated, "object uploaded successfully", gin.H{"key": key, "etag": etag})
}

// GetObject handles object download
func (h *Handler) GetObject(c *gin.Context) {
	key := c.Param("key")
	if key == "" {
		RespondWithError(c, http.StatusBadRequest, "key parameter is required")
		return
	}

	// Remove leading slashes if present
	for len(key) > 0 && key[0] == '/' {
		key = key[1:]
	}

	// Get metadata first to set headers
	metadata, err := objstore.GetMetadata(c.Request.Context(), h.keyRef(key))
	if err != nil {
		RespondWithError(c, http.StatusNotFound, common.SanitizeErrorMessage(err))
		return
	}

	// Get the object using facade
	reader, err := objstore.GetWithContext(c.Request.Context(), h.keyRef(key))
	if err != nil {
		RespondWithError(c, http.StatusNotFound, common.SanitizeErrorMessage(err))
		return
	}
	defer func() { _ = reader.Close() }()

	// Set response headers
	if metadata.ContentType != "" {
		c.Header("Content-Type", metadata.ContentType)
	} else {
		c.Header("Content-Type", "application/octet-stream")
	}

	if metadata.ContentEncoding != "" {
		c.Header("Content-Encoding", metadata.ContentEncoding)
	}

	if metadata.ETag != "" {
		c.Header("ETag", metadata.ETag)
	}

	if !metadata.LastModified.IsZero() {
		c.Header("Last-Modified", metadata.LastModified.Format(http.TimeFormat))
	}

	if metadata.Size > 0 {
		c.Header("Content-Length", strconv.FormatInt(metadata.Size, 10))
	}

	// Stream the response
	c.Status(http.StatusOK)
	_, err = io.Copy(c.Writer, reader)
	if err != nil {
		_ = c.Error(err)
	}
}

// DeleteObject handles object deletion
func (h *Handler) DeleteObject(c *gin.Context) {
	key := c.Param("key")
	if key == "" {
		RespondWithError(c, http.StatusBadRequest, "key parameter is required")
		return
	}

	// Remove leading slashes if present
	for len(key) > 0 && key[0] == '/' {
		key = key[1:]
	}

	// Check if object exists using facade
	exists, err := objstore.Exists(c.Request.Context(), h.keyRef(key))
	if err != nil {
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	if !exists {
		RespondWithError(c, http.StatusNotFound, "object not found")
		return
	}

	// Delete the object using facade
	err = objstore.DeleteWithContext(c.Request.Context(), h.keyRef(key))

	// Audit logging
	auditLogger := audit.GetAuditLogger(c.Request.Context())
	principal, userID := extractPrincipal(c)
	requestID := audit.GetRequestID(c.Request.Context())

	if err != nil {
		_ = auditLogger.LogObjectMutation(c.Request.Context(), audit.EventObjectDeleted,
			userID, principal, h.backend, key, c.ClientIP(), requestID, 0,
			audit.ResultFailure, err)
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	_ = auditLogger.LogObjectMutation(c.Request.Context(), audit.EventObjectDeleted,
		userID, principal, h.backend, key, c.ClientIP(), requestID, 0,
		audit.ResultSuccess, nil)

	RespondWithSuccess(c, http.StatusOK, "object deleted successfully", gin.H{"key": key})
}

// HeadObject checks if an object exists
func (h *Handler) HeadObject(c *gin.Context) {
	key := c.Param("key")
	if key == "" {
		RespondWithError(c, http.StatusBadRequest, "key parameter is required")
		return
	}

	// Remove leading slashes if present
	for len(key) > 0 && key[0] == '/' {
		key = key[1:]
	}

	// Check if object exists using facade
	exists, err := objstore.Exists(c.Request.Context(), h.keyRef(key))
	if err != nil {
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	if !exists {
		c.Status(http.StatusNotFound)
		return
	}

	// Get metadata to set headers
	metadata, err := objstore.GetMetadata(c.Request.Context(), h.keyRef(key))
	if err == nil {
		if metadata.ContentType != "" {
			c.Header("Content-Type", metadata.ContentType)
		}
		if metadata.ETag != "" {
			c.Header("ETag", metadata.ETag)
		}
		if !metadata.LastModified.IsZero() {
			c.Header("Last-Modified", metadata.LastModified.Format(http.TimeFormat))
		}
		if metadata.Size > 0 {
			c.Header("Content-Length", strconv.FormatInt(metadata.Size, 10))
		}
	}

	c.Status(http.StatusOK)
}

// ListObjects handles listing objects with pagination
func (h *Handler) ListObjects(c *gin.Context) {
	prefix := c.Query("prefix")
	limitStr := c.Query("limit")
	if limitStr == "" {
		limitStr = c.Query("max_results")
	}
	if limitStr == "" {
		limitStr = strconv.Itoa(DefaultListLimit)
	}
	token := c.Query("token")
	delimiter := c.Query("delimiter")

	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit < 0 {
		RespondWithError(c, http.StatusBadRequest, "invalid limit parameter")
		return
	}

	if limit > MaxListLimit {
		limit = MaxListLimit
	}

	opts := &common.ListOptions{
		Prefix:       prefix,
		MaxResults:   limit,
		ContinueFrom: token,
		Delimiter:    delimiter,
	}

	// List using facade
	result, err := objstore.ListWithOptions(c.Request.Context(), h.backend, opts)
	if err != nil {
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	RespondWithListObjects(c, result)
}

// GetObjectMetadata retrieves object metadata
func (h *Handler) GetObjectMetadata(c *gin.Context) {
	key := c.Param("key")
	if key == "" {
		RespondWithError(c, http.StatusBadRequest, "key parameter is required")
		return
	}

	// Remove leading slashes if present
	for len(key) > 0 && key[0] == '/' {
		key = key[1:]
	}

	metadata, err := objstore.GetMetadata(c.Request.Context(), h.keyRef(key))
	if err != nil {
		RespondWithError(c, http.StatusNotFound, common.SanitizeErrorMessage(err))
		return
	}

	if metadata == nil {
		RespondWithError(c, http.StatusNotFound, "object not found")
		return
	}

	RespondWithObject(c, key, metadata)
}

// UpdateObjectMetadata updates object metadata
func (h *Handler) UpdateObjectMetadata(c *gin.Context) {
	key := c.Param("key")
	if key == "" {
		RespondWithError(c, http.StatusBadRequest, "key parameter is required")
		return
	}

	// Remove leading slashes if present
	for len(key) > 0 && key[0] == '/' {
		key = key[1:]
	}

	// Check if object exists using facade
	exists, err := objstore.Exists(c.Request.Context(), h.keyRef(key))
	if err != nil {
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	if !exists {
		RespondWithError(c, http.StatusNotFound, "object not found")
		return
	}

	// Parse metadata from request body
	var metadata common.Metadata
	if err := c.ShouldBindJSON(&metadata); err != nil {
		RespondWithError(c, http.StatusBadRequest, "invalid metadata JSON: "+err.Error())
		return
	}

	// Update metadata using facade
	err = objstore.UpdateMetadata(c.Request.Context(), h.keyRef(key), &metadata)
	if err != nil {
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	RespondWithSuccess(c, http.StatusOK, "metadata updated successfully", gin.H{"key": key})
}

// HealthCheck handles health check requests
func (h *Handler) HealthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, HealthResponse{
		Status:  "healthy",
		Version: version.Get(),
	})
}

// Archive handles archiving an object to another backend
func (h *Handler) Archive(c *gin.Context) {
	var req ArchiveRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		RespondWithError(c, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.Key == "" {
		RespondWithError(c, http.StatusBadRequest, "key is required")
		return
	}

	// Validate key
	if err := validation.ValidateKey(req.Key); err != nil {
		RespondWithError(c, http.StatusBadRequest, "invalid key: "+common.SanitizeErrorMessage(err))
		return
	}

	if req.DestinationType == "" {
		RespondWithError(c, http.StatusBadRequest, "destination_type is required")
		return
	}

	// Create archiver from factory
	archiver, err := createArchiver(req.DestinationType, req.DestinationSettings)
	if err != nil {
		RespondWithError(c, http.StatusBadRequest, "failed to create archiver: "+common.SanitizeErrorMessage(err))
		return
	}

	// Perform archive operation using facade
	err = objstore.Archive(h.keyRef(req.Key), archiver)

	// Audit logging
	auditLogger := audit.GetAuditLogger(c.Request.Context())
	principal, userID := extractPrincipal(c)
	requestID := audit.GetRequestID(c.Request.Context())

	if err != nil {
		_ = auditLogger.LogObjectMutation(c.Request.Context(), audit.EventObjectArchived,
			userID, principal, h.backend, req.Key, c.ClientIP(), requestID, 0,
			audit.ResultFailure, err)
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	_ = auditLogger.LogObjectMutation(c.Request.Context(), audit.EventObjectArchived,
		userID, principal, h.backend, req.Key, c.ClientIP(), requestID, 0,
		audit.ResultSuccess, nil)

	RespondWithSuccess(c, http.StatusOK, "object archived successfully", gin.H{
		"key":         req.Key,
		"destination": req.DestinationType,
	})
}

// AddPolicy handles adding a new lifecycle policy
func (h *Handler) AddPolicy(c *gin.Context) {
	var req AddPolicyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		RespondWithError(c, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.ID == "" {
		RespondWithError(c, http.StatusBadRequest, "policy ID is required")
		return
	}

	if req.Action == "" {
		RespondWithError(c, http.StatusBadRequest, "action is required")
		return
	}

	if req.Retention <= 0 {
		RespondWithError(c, http.StatusBadRequest, "retention_seconds must be positive")
		return
	}

	// Build lifecycle policy
	policy := common.LifecyclePolicy{
		ID:        req.ID,
		Prefix:    req.Prefix,
		Retention: req.Retention,
		Action:    req.Action,
	}

	// Create archiver if action is "archive"
	if req.Action == "archive" {
		if req.DestinationType == "" {
			RespondWithError(c, http.StatusBadRequest, "destination_type required for archive action")
			return
		}
		archiver, err := createArchiver(req.DestinationType, req.DestinationSettings)
		if err != nil {
			RespondWithError(c, http.StatusBadRequest, "failed to create archiver: "+common.SanitizeErrorMessage(err))
			return
		}
		policy.Destination = archiver
	}

	// Add policy using facade
	err := objstore.AddPolicy(h.backend, policy)
	if err != nil {
		if err.Error() == "policy already exists" {
			RespondWithError(c, http.StatusConflict, common.SanitizeErrorMessage(err))
			return
		}
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	RespondWithSuccess(c, http.StatusCreated, "policy added successfully", gin.H{
		"id": req.ID,
	})
}

// RemovePolicy handles removing a lifecycle policy
func (h *Handler) RemovePolicy(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		RespondWithError(c, http.StatusBadRequest, "policy ID is required")
		return
	}

	// Remove leading slashes if present
	for len(id) > 0 && id[0] == '/' {
		id = id[1:]
	}

	// Remove policy using facade
	err := objstore.RemovePolicy(h.backend, id)
	if err != nil {
		if errors.Is(err, common.ErrPolicyNotFound) {
			RespondWithError(c, http.StatusNotFound, common.SanitizeErrorMessage(err))
			return
		}
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	RespondWithSuccess(c, http.StatusOK, "policy removed successfully", gin.H{
		"id": id,
	})
}

// GetPolicies handles listing all lifecycle policies
func (h *Handler) GetPolicies(c *gin.Context) {
	prefix := c.Query("prefix")

	// Get policies using facade
	policies, err := objstore.GetPolicies(h.backend)
	if err != nil {
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	// Filter by prefix if specified
	var filteredPolicies []common.LifecyclePolicy
	for _, policy := range policies {
		if prefix == "" || policy.Prefix == prefix {
			filteredPolicies = append(filteredPolicies, policy)
		}
	}

	RespondWithPolicies(c, filteredPolicies)
}

// ExistsObject handles GET /api/v1/objects/exists/*key - checks if an object exists.
func (h *Handler) ExistsObject(c *gin.Context) {
	key := c.Param("key")
	if key != "" && key[0] == '/' {
		key = key[1:]
	}

	if key == "" {
		RespondWithError(c, http.StatusBadRequest, "key is required")
		return
	}

	// Check existence using facade
	exists, err := objstore.Exists(c.Request.Context(), h.keyRef(key))
	if err != nil {
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"key":    key,
		"exists": exists,
	})
}

// ApplyPolicies handles POST /api/v1/policies/apply - executes all lifecycle policies.
func (h *Handler) ApplyPolicies(c *gin.Context) {
	ctx := c.Request.Context()

	// Get policies using facade
	policies, err := objstore.GetPolicies(h.backend)
	if err != nil {
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	if len(policies) == 0 {
		c.JSON(http.StatusOK, gin.H{
			"message":           "No lifecycle policies to apply",
			"policies_count":    0,
			"objects_processed": 0,
		})
		return
	}

	// Apply policies by listing objects and checking retention
	objectsProcessed := 0
	opts := &common.ListOptions{
		Prefix: "",
	}
	result, err := objstore.ListWithOptions(ctx, h.backend, opts)
	if err != nil {
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
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

			// Apply action using facade
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

	c.JSON(http.StatusOK, gin.H{
		"message":           "Lifecycle policies applied successfully",
		"policies_count":    len(policies),
		"objects_processed": objectsProcessed,
	})
}

// Helper functions

// extractPrincipal extracts the principal information from the Gin context
func extractPrincipal(c *gin.Context) (principal string, userID string) {
	if principalValue, exists := c.Get("principal"); exists {
		if p, ok := principalValue.(adapters.Principal); ok {
			return p.Name, p.ID
		}
	}
	return "", ""
}

// createArchiver creates an archiver from factory based on destination type
func createArchiver(destinationType string, settings map[string]string) (common.Archiver, error) {
	return factory.NewArchiver(destinationType, settings)
}

// hasPrefix checks if a string starts with the given prefix
func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[0:len(prefix)] == prefix
}
