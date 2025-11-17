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
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
	"github.com/jeremyhahn/go-objstore/pkg/version"
)

const (
	// DefaultListLimit is the default number of objects to return in a list operation
	DefaultListLimit = 100

	// MaxListLimit is the maximum number of objects to return in a list operation
	MaxListLimit = 1000
)

// Handler encapsulates the storage backend for handling requests
type Handler struct {
	storage common.Storage
}

// NewHandler creates a new Handler instance
func NewHandler(storage common.Storage) *Handler {
	return &Handler{
		storage: storage,
	}
}

// PutObject handles object upload
// @Summary Upload an object
// @Description Upload an object to the storage backend with optional metadata
// @Tags objects
// @Accept multipart/form-data,application/octet-stream
// @Produce json
// @Param key path string true "Object key"
// @Param file formData file false "File to upload (multipart)"
// @Param metadata formData string false "JSON metadata"
// @Success 201 {object} SuccessResponse
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /objects/{key} [put]
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

	// Validate key for security
	if err := common.ValidateKey(key); err != nil {
		RespondWithError(c, http.StatusBadRequest, common.SanitizeErrorMessage(err))
		return
	}

	var reader io.Reader
	var metadata *common.Metadata

	// Check if this is a multipart upload
	contentType := c.GetHeader("Content-Type")
	if contentType == "multipart/form-data" || c.Request.MultipartForm != nil {
		// Handle multipart upload
		file, header, err := c.Request.FormFile("file")
		if err != nil {
			RespondWithError(c, http.StatusBadRequest, "failed to read file from multipart form: "+err.Error())
			return
		}
		defer file.Close()

		reader = file

		// Parse metadata if provided
		metadataStr := c.PostForm("metadata")
		if metadataStr != "" {
			metadata = &common.Metadata{}
			if err := json.Unmarshal([]byte(metadataStr), metadata); err != nil {
				RespondWithError(c, http.StatusBadRequest, "invalid metadata JSON: "+err.Error())
				return
			}
			// Validate custom metadata
			if metadata.Custom != nil {
				if err := common.ValidateMetadata(metadata.Custom); err != nil {
					RespondWithError(c, http.StatusBadRequest, err.Error())
					return
				}
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
			// Validate custom metadata
			if metadata.Custom != nil {
				if err := common.ValidateMetadata(metadata.Custom); err != nil {
					RespondWithError(c, http.StatusBadRequest, err.Error())
					return
				}
			}
		} else {
			metadata = &common.Metadata{
				ContentType: c.GetHeader("Content-Type"),
			}
		}
	}

	// Store the object
	err := h.storage.PutWithMetadata(c.Request.Context(), key, reader, metadata)

	// Audit logging
	auditLogger := audit.GetAuditLogger(c.Request.Context())
	principal, userID := extractPrincipal(c)
	requestID := audit.GetRequestID(c.Request.Context())

	if err != nil {
		_ = auditLogger.LogObjectMutation(c.Request.Context(), audit.EventObjectCreated,
			userID, principal, "default", key, c.ClientIP(), requestID, 0,
			audit.ResultFailure, err) // #nosec G104 -- Audit logging errors are logged internally, should not block operations
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	bytesTransferred := int64(0)
	if metadata != nil {
		bytesTransferred = metadata.Size
	}
	_ = auditLogger.LogObjectMutation(c.Request.Context(), audit.EventObjectCreated,
		userID, principal, "default", key, c.ClientIP(), requestID, bytesTransferred,
		audit.ResultSuccess, nil) // #nosec G104 -- Audit logging errors are logged internally, should not block operations

	RespondWithSuccess(c, http.StatusCreated, "object uploaded successfully", gin.H{"key": key})
}

// GetObject handles object download
// @Summary Download an object
// @Description Retrieve an object from the storage backend
// @Tags objects
// @Produce application/octet-stream
// @Param key path string true "Object key"
// @Success 200 {file} binary
// @Failure 404 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /objects/{key} [get]
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

	// Validate key for security
	if err := common.ValidateKey(key); err != nil {
		RespondWithError(c, http.StatusBadRequest, common.SanitizeErrorMessage(err))
		return
	}

	// Get metadata first to set headers
	metadata, err := h.storage.GetMetadata(c.Request.Context(), key)
	if err != nil {
		RespondWithError(c, http.StatusNotFound, common.SanitizeErrorMessage(err))
		return
	}

	// Get the object
	reader, err := h.storage.GetWithContext(c.Request.Context(), key)
	if err != nil {
		RespondWithError(c, http.StatusNotFound, common.SanitizeErrorMessage(err))
		return
	}
	defer reader.Close()

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
		// Log error but can't send error response as headers are already sent
		c.Error(err)
	}
}

// DeleteObject handles object deletion
// @Summary Delete an object
// @Description Remove an object from the storage backend
// @Tags objects
// @Produce json
// @Param key path string true "Object key"
// @Success 200 {object} SuccessResponse
// @Failure 404 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /objects/{key} [delete]
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

	// Validate key for security
	if err := common.ValidateKey(key); err != nil {
		RespondWithError(c, http.StatusBadRequest, common.SanitizeErrorMessage(err))
		return
	}

	// Check if object exists
	exists, err := h.storage.Exists(c.Request.Context(), key)
	if err != nil {
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	if !exists {
		RespondWithError(c, http.StatusNotFound, "object not found")
		return
	}

	// Delete the object
	err = h.storage.DeleteWithContext(c.Request.Context(), key)

	// Audit logging
	auditLogger := audit.GetAuditLogger(c.Request.Context())
	principal, userID := extractPrincipal(c)
	requestID := audit.GetRequestID(c.Request.Context())

	if err != nil {
		_ = auditLogger.LogObjectMutation(c.Request.Context(), audit.EventObjectDeleted,
			userID, principal, "default", key, c.ClientIP(), requestID, 0,
			audit.ResultFailure, err) // #nosec G104 -- Audit logging errors are logged internally, should not block operations
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	_ = auditLogger.LogObjectMutation(c.Request.Context(), audit.EventObjectDeleted,
		userID, principal, "default", key, c.ClientIP(), requestID, 0,
		audit.ResultSuccess, nil) // #nosec G104 -- Audit logging errors are logged internally, should not block operations

	RespondWithSuccess(c, http.StatusOK, "object deleted successfully", gin.H{"key": key})
}

// HeadObject checks if an object exists
// @Summary Check object existence
// @Description Check if an object exists in the storage backend
// @Tags objects
// @Produce json
// @Param key path string true "Object key"
// @Success 200 {object} SuccessResponse
// @Failure 404 {object} ErrorResponse
// @Router /objects/{key} [head]
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

	// Validate key for security
	if err := common.ValidateKey(key); err != nil {
		RespondWithError(c, http.StatusBadRequest, common.SanitizeErrorMessage(err))
		return
	}

	// Check if object exists
	exists, err := h.storage.Exists(c.Request.Context(), key)
	if err != nil {
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	if !exists {
		c.Status(http.StatusNotFound)
		return
	}

	// Get metadata to set headers
	metadata, err := h.storage.GetMetadata(c.Request.Context(), key)
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
// @Summary List objects
// @Description List objects with optional prefix filtering and pagination
// @Tags objects
// @Produce json
// @Param prefix query string false "Prefix filter"
// @Param limit query int false "Maximum number of results" default(100)
// @Param token query string false "Continuation token for pagination"
// @Param delimiter query string false "Delimiter for hierarchical listing"
// @Success 200 {object} ListObjectsResponse
// @Failure 500 {object} ErrorResponse
// @Router /objects [get]
func (h *Handler) ListObjects(c *gin.Context) {
	prefix := c.Query("prefix")
	// Support both "limit" and "max_results" parameters for compatibility
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

	// Cap limit at MaxListLimit
	if limit > MaxListLimit {
		limit = MaxListLimit
	}

	opts := &common.ListOptions{
		Prefix:       prefix,
		MaxResults:   limit,
		ContinueFrom: token,
		Delimiter:    delimiter,
	}

	result, err := h.storage.ListWithOptions(c.Request.Context(), opts)
	if err != nil {
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	RespondWithListObjects(c, result)
}

// GetObjectMetadata retrieves object metadata
// @Summary Get object metadata
// @Description Retrieve metadata for an object without downloading it
// @Tags objects
// @Produce json
// @Param key path string true "Object key"
// @Success 200 {object} ObjectResponse
// @Failure 404 {object} ErrorResponse
// @Router /metadata/{key} [get]
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

	// Validate key for security
	if err := common.ValidateKey(key); err != nil {
		RespondWithError(c, http.StatusBadRequest, common.SanitizeErrorMessage(err))
		return
	}

	metadata, err := h.storage.GetMetadata(c.Request.Context(), key)
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
// @Summary Update object metadata
// @Description Update metadata for an existing object
// @Tags objects
// @Accept json
// @Produce json
// @Param key path string true "Object key"
// @Param metadata body common.Metadata true "Metadata to update"
// @Success 200 {object} SuccessResponse
// @Failure 400 {object} ErrorResponse
// @Failure 404 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /metadata/{key} [put]
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

	// Validate key for security
	if err := common.ValidateKey(key); err != nil {
		RespondWithError(c, http.StatusBadRequest, common.SanitizeErrorMessage(err))
		return
	}

	// Check if object exists
	exists, err := h.storage.Exists(c.Request.Context(), key)
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

	// Validate custom metadata
	if metadata.Custom != nil {
		if err := common.ValidateMetadata(metadata.Custom); err != nil {
			RespondWithError(c, http.StatusBadRequest, err.Error())
			return
		}
	}

	// Update metadata
	err = h.storage.UpdateMetadata(c.Request.Context(), key, &metadata)
	if err != nil {
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	RespondWithSuccess(c, http.StatusOK, "metadata updated successfully", gin.H{"key": key})
}

// HealthCheck handles health check requests
// @Summary Health check
// @Description Check if the server is healthy
// @Tags health
// @Produce json
// @Success 200 {object} HealthResponse
// @Router /health [get]
func (h *Handler) HealthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, HealthResponse{
		Status:  "healthy",
		Version: version.Get(),
	})
}

// Helper functions

// Archive handles archiving an object to another backend
// @Summary Archive an object
// @Description Archive an object to an archival storage backend
// @Tags archive
// @Accept json
// @Produce json
// @Param request body ArchiveRequest true "Archive request"
// @Success 200 {object} SuccessResponse
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /archive [post]
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

	if req.DestinationType == "" {
		RespondWithError(c, http.StatusBadRequest, "destination_type is required")
		return
	}

	// Validate key for security
	if err := common.ValidateKey(req.Key); err != nil {
		RespondWithError(c, http.StatusBadRequest, common.SanitizeErrorMessage(err))
		return
	}

	// Create archiver from factory
	archiver, err := createArchiver(req.DestinationType, req.DestinationSettings)
	if err != nil {
		RespondWithError(c, http.StatusBadRequest, "failed to create archiver: "+common.SanitizeErrorMessage(err))
		return
	}

	// Perform archive operation
	err = h.storage.Archive(req.Key, archiver)

	// Audit logging
	auditLogger := audit.GetAuditLogger(c.Request.Context())
	principal, userID := extractPrincipal(c)
	requestID := audit.GetRequestID(c.Request.Context())

	if err != nil {
		_ = auditLogger.LogObjectMutation(c.Request.Context(), audit.EventObjectArchived,
			userID, principal, "default", req.Key, c.ClientIP(), requestID, 0,
			audit.ResultFailure, err) // #nosec G104 -- Audit logging errors are logged internally, should not block operations
		RespondWithError(c, http.StatusInternalServerError, common.SanitizeErrorMessage(err))
		return
	}

	_ = auditLogger.LogObjectMutation(c.Request.Context(), audit.EventObjectArchived,
		userID, principal, "default", req.Key, c.ClientIP(), requestID, 0,
		audit.ResultSuccess, nil) // #nosec G104 -- Audit logging errors are logged internally, should not block operations

	RespondWithSuccess(c, http.StatusOK, "object archived successfully", gin.H{
		"key":         req.Key,
		"destination": req.DestinationType,
	})
}

// AddPolicy handles adding a new lifecycle policy
// @Summary Add lifecycle policy
// @Description Add a new lifecycle policy for automatic object management
// @Tags policies
// @Accept json
// @Produce json
// @Param request body AddPolicyRequest true "Policy request"
// @Success 200 {object} SuccessResponse
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /policies [post]
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

	if req.RetentionSeconds <= 0 {
		RespondWithError(c, http.StatusBadRequest, "retention_seconds must be positive")
		return
	}

	// Build lifecycle policy
	policy := common.LifecyclePolicy{
		ID:        req.ID,
		Prefix:    req.Prefix,
		Retention: req.RetentionSeconds,
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

	err := h.storage.AddPolicy(policy)
	if err != nil {
		// Check for duplicate policy error
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
// @Summary Remove lifecycle policy
// @Description Remove an existing lifecycle policy by ID
// @Tags policies
// @Produce json
// @Param id path string true "Policy ID"
// @Success 200 {object} SuccessResponse
// @Failure 400 {object} ErrorResponse
// @Failure 404 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /policies/{id} [delete]
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

	err := h.storage.RemovePolicy(id)
	if err != nil {
		if err == common.ErrPolicyNotFound {
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
// @Summary List lifecycle policies
// @Description Retrieve all lifecycle policies with optional prefix filtering
// @Tags policies
// @Produce json
// @Param prefix query string false "Prefix filter"
// @Success 200 {object} GetPoliciesResponse
// @Failure 500 {object} ErrorResponse
// @Router /policies [get]
func (h *Handler) GetPolicies(c *gin.Context) {
	prefix := c.Query("prefix")

	policies, err := h.storage.GetPolicies()
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

	ctx := c.Request.Context()
	exists, err := h.storage.Exists(ctx, key)
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

	policies, err := h.storage.GetPolicies()
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
	result, err := h.storage.ListWithOptions(ctx, opts)
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

			// Apply action
			switch policy.Action {
			case "delete":
				if err := h.storage.DeleteWithContext(ctx, obj.Key); err != nil {
					// Continue processing other objects even if one fails
					continue
				}
				objectsProcessed++
			case "archive":
				if policy.Destination != nil {
					if err := h.storage.Archive(obj.Key, policy.Destination); err != nil {
						// Continue processing other objects even if one fails
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
