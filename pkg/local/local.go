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

package local

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

const metadataSuffix = ".metadata.json"

// Local is a storage backend that stores files on the local disk.
type Local struct {
	path                   string
	lifecycleManager       common.LifecycleManager
	replicationManager     common.ReplicationManager
	atRestEncrypterFactory common.EncrypterFactory
	changeLog              ChangeLog
	logger                 adapters.Logger
	auditLog               audit.AuditLogger
}

// New creates a new Local storage backend.
func New() common.Storage {
	return &Local{
		lifecycleManager: NewLifecycleManager(),
	}
}

// Configure sets up the backend with the necessary settings.
// Settings:
//   - path: The directory path for local storage (required)
//   - runLifecycle: "true" to run lifecycle processing in background (optional)
//   - lifecycleManagerType: "memory" (default) or "persistent" (optional)
//   - lifecyclePolicyFile: Path to policy file when using persistent manager (optional, default: ".lifecycle-policies.json")
//
// Note: Replication is enabled by calling SetReplicationManager() after Configure().
// This allows the caller to configure replication with custom settings and avoids
// import cycles between packages.
func (l *Local) Configure(settings map[string]string) error {
	l.path = settings["path"]
	if l.path == "" {
		return common.ErrPathNotSet
	}

	// Ensure directory exists
	if err := os.MkdirAll(l.path, 0750); err != nil {
		return err
	}

	// Initialize logger and audit log with no-op defaults if not set
	if l.logger == nil {
		l.logger = adapters.NewNoOpLogger()
	}
	if l.auditLog == nil {
		l.auditLog = audit.NewNoOpAuditLogger()
	}

	// Configure lifecycle manager type
	managerType := settings["lifecycleManagerType"]
	if managerType == "" {
		managerType = "memory" // Default to in-memory for backwards compatibility
	}

	switch managerType {
	case "memory":
		// Use in-memory lifecycle manager (default)
		l.lifecycleManager = NewLifecycleManager()
	case "persistent":
		// Use persistent lifecycle manager with storagefs
		policyFile := settings["lifecyclePolicyFile"]
		if policyFile == "" {
			policyFile = ".lifecycle-policies.json"
		}

		fs := &localFileSystem{basePath: l.path}
		persistentManager, err := common.NewPersistentLifecycleManager(fs, policyFile)
		if err != nil {
			return err
		}
		l.lifecycleManager = persistentManager
	default:
		return common.ErrInvalidLifecycleManagerType
	}

	// Start background lifecycle processing if requested
	if settings["runLifecycle"] == "true" {
		// Only in-memory manager supports Run method
		if memManager, ok := l.lifecycleManager.(*LifecycleManager); ok {
			go memManager.Run(l)
		}
	}

	return nil
}

// SetAtRestEncrypterFactory sets the encryption factory for at-rest encryption.
// When set, all data written to disk will be encrypted, and all data read from
// disk will be decrypted. If not set, data is stored in plaintext (noop).
func (l *Local) SetAtRestEncrypterFactory(factory common.EncrypterFactory) {
	l.atRestEncrypterFactory = factory
}

// SetChangeLog sets the change log for tracking Put and Delete operations.
// When set, all mutations will be recorded to the change log for replication.
func (l *Local) SetChangeLog(changeLog ChangeLog) {
	l.changeLog = changeLog
}

// localFileSystem is a simple FileSystem implementation that uses local files
type localFileSystem struct {
	basePath string
}

func (lfs *localFileSystem) OpenFile(name string, flag int, perm os.FileMode) (common.LifecycleFile, error) {
	fullPath := filepath.Join(lfs.basePath, name)
	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(fullPath), 0750); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(fullPath, flag, perm) // #nosec G304 -- Path is controlled by system
	if err != nil {
		return nil, err
	}
	return &localFile{File: file}, nil
}

func (lfs *localFileSystem) Remove(name string) error {
	fullPath := filepath.Join(lfs.basePath, name)
	return os.Remove(fullPath)
}

// localFile wraps os.File to implement common.LifecycleFile
type localFile struct {
	*os.File
}

// Put stores an object in the backend.
func (l *Local) Put(key string, data io.Reader) error {
	return l.PutWithContext(context.Background(), key, data)
}

// PutWithContext stores an object in the backend with context support.
func (l *Local) PutWithContext(ctx context.Context, key string, data io.Reader) error {
	return l.PutWithMetadata(ctx, key, data, nil)
}

// validateKey checks if a key is safe to use (no path traversal attacks)
func (l *Local) validateKey(key string) error {
	return common.ValidateKey(key)
}

// PutWithMetadata stores an object with associated metadata.
func (l *Local) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	if err := l.validateKey(key); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	path := filepath.Join(l.path, key)
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil { // Restrict permissions for security
		return err
	}

	// Get at-rest encrypter if factory is set
	var encrypter common.Encrypter
	if l.atRestEncrypterFactory != nil {
		var err error
		encrypter, err = l.atRestEncrypterFactory.GetEncrypter("")
		if err != nil {
			return fmt.Errorf("failed to get encrypter: %w", err)
		}
	}

	// Encrypt data if encrypter is available
	dataToWrite := data
	if encrypter != nil {
		encryptedData, err := encrypter.Encrypt(ctx, data)
		if err != nil {
			return fmt.Errorf("encryption failed: %w", err)
		}
		defer func() { _ = encryptedData.Close() }()
		dataToWrite = encryptedData
	}

	f, err := os.Create(path) // #nosec G304 -- Path validated by validateKey() to prevent directory traversal
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	size, err := io.Copy(f, dataToWrite)
	if err != nil {
		log.Printf("[LOCAL] ✗ Failed to write object '%s': %v", key, err)
		return err
	}

	// Create or update metadata
	if metadata == nil {
		metadata = &common.Metadata{}
	}
	metadata.Size = size
	metadata.LastModified = time.Now()

	// Get file info for ETag (using modification time as simple ETag)
	info, err := os.Stat(path)
	if err == nil {
		metadata.ETag = fmt.Sprintf("%d-%d", info.ModTime().Unix(), size)
	}

	// Add at-rest encryption metadata if encrypted
	// Use separate field names to avoid conflict with client-side DEK encryption
	if encrypter != nil {
		if metadata.Custom == nil {
			metadata.Custom = make(map[string]string)
		}
		metadata.Custom["at_rest_encryption_algorithm"] = encrypter.Algorithm()
		metadata.Custom["at_rest_encryption_key_id"] = encrypter.KeyID()
	}

	if err := l.saveMetadata(key, metadata); err != nil {
		log.Printf("[LOCAL] ✗ Failed to save metadata for '%s': %v", key, err)
		return err
	}

	// Log successful operation with details
	sizeStr := formatBytes(size)
	log.Printf("[LOCAL] ✓ PUT '%s' → %s (%s)", key, path, sizeStr)
	if metadata.ContentType != "" {
		log.Printf("        Content-Type: %s", metadata.ContentType)
	}
	if encrypter != nil {
		log.Printf("        Encrypted with: %s (key: %s)", encrypter.Algorithm(), encrypter.KeyID())
	}
	if len(metadata.Custom) > 0 {
		log.Printf("        Custom metadata: %d fields", len(metadata.Custom))
	}

	// Record change in changelog if enabled
	if l.changeLog != nil {
		_ = l.changeLog.RecordChange(ChangeEvent{
			Key:       key,
			Operation: "put",
			Timestamp: time.Now(),
			ETag:      metadata.ETag,
			Size:      metadata.Size,
		})
	}

	return nil
}

// Get retrieves an object from the backend.
func (l *Local) Get(key string) (io.ReadCloser, error) {
	return l.GetWithContext(context.Background(), key)
}

// GetWithContext retrieves an object from the backend with context support.
func (l *Local) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := l.validateKey(key); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	path := filepath.Join(l.path, key)
	file, err := os.Open(path) // #nosec G304 -- Path validated by validateKey() to prevent directory traversal
	if err != nil {
		// Don't log "not found" errors - these are expected during initialization
		// and should be handled by the caller. Only return a wrapped error.
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", common.ErrKeyNotFound, key)
		}
		// Log actual unexpected errors
		log.Printf("[LOCAL] ✗ GET '%s' failed: %v", key, err)
		return nil, err
	}

	// Get at-rest encrypter if factory is set
	var encrypter common.Encrypter
	if l.atRestEncrypterFactory != nil {
		var err error
		encrypter, err = l.atRestEncrypterFactory.GetEncrypter("")
		if err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("failed to get encrypter: %w", err)
		}
	}

	// Decrypt data if encrypter is available
	if encrypter != nil {
		decryptedData, err := encrypter.Decrypt(ctx, file)
		if err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("decryption failed: %w", err)
		}
		// Note: file will be closed when decryptedData is closed
		// Get file size for logging
		info, err := os.Stat(path)
		if err == nil {
			sizeStr := formatBytes(info.Size())
			log.Printf("[LOCAL] ✓ GET '%s' ← %s (%s, decrypted)", key, path, sizeStr)
		} else {
			log.Printf("[LOCAL] ✓ GET '%s' ← %s (decrypted)", key, path)
		}
		return decryptedData, nil
	}

	// Get file size for logging
	info, err := file.Stat()
	if err == nil {
		sizeStr := formatBytes(info.Size())
		log.Printf("[LOCAL] ✓ GET '%s' ← %s (%s)", key, path, sizeStr)
	} else {
		log.Printf("[LOCAL] ✓ GET '%s' ← %s", key, path)
	}

	return file, nil
}

// GetMetadata retrieves only the metadata for an object.
func (l *Local) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	if err := l.validateKey(key); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	return l.loadMetadata(key)
}

// UpdateMetadata updates the metadata for an existing object.
func (l *Local) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	if err := l.validateKey(key); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Verify object exists
	path := filepath.Join(l.path, key)
	info, err := os.Stat(path)
	if err != nil {
		return err
	}

	// Update size and last modified from file
	if metadata == nil {
		metadata = &common.Metadata{}
	}
	metadata.Size = info.Size()
	metadata.LastModified = time.Now()
	metadata.ETag = fmt.Sprintf("%d-%d", info.ModTime().Unix(), info.Size())

	return l.saveMetadata(key, metadata)
}

// Delete removes an object from the backend.
func (l *Local) Delete(key string) error {
	return l.DeleteWithContext(context.Background(), key)
}

// DeleteWithContext removes an object from the backend with context support.
func (l *Local) DeleteWithContext(ctx context.Context, key string) error {
	if err := l.validateKey(key); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	path := filepath.Join(l.path, key)

	// Get file size before deletion for logging
	var sizeStr string
	if info, err := os.Stat(path); err == nil {
		sizeStr = formatBytes(info.Size())
	}

	// Delete metadata file if it exists
	metadataPath := path + metadataSuffix
	_ = os.Remove(metadataPath) // Ignore error if metadata doesn't exist

	err := os.Remove(path)
	if err != nil {
		// Don't log "not found" errors - these are expected during cleanup
		// and should be handled by the caller
		if os.IsNotExist(err) {
			return fmt.Errorf("%w: %s", common.ErrKeyNotFound, key)
		}
		// Log actual unexpected errors
		log.Printf("[LOCAL] ✗ DELETE '%s' failed: %v", key, err)
		return err
	}

	if sizeStr != "" {
		log.Printf("[LOCAL] ✓ DELETE '%s' ✗ %s (freed %s)", key, path, sizeStr)
	} else {
		log.Printf("[LOCAL] ✓ DELETE '%s' ✗ %s", key, path)
	}

	// Record change in changelog if enabled
	if l.changeLog != nil {
		_ = l.changeLog.RecordChange(ChangeEvent{
			Key:       key,
			Operation: "delete",
			Timestamp: time.Now(),
		})
	}

	return nil
}

// Exists checks if an object exists in the backend.
func (l *Local) Exists(ctx context.Context, key string) (bool, error) {
	if err := l.validateKey(key); err != nil {
		return false, err
	}

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	path := filepath.Join(l.path, key)
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// List returns a list of keys that start with the given prefix.
func (l *Local) List(prefix string) ([]string, error) {
	return l.ListWithContext(context.Background(), prefix)
}

// ListWithContext returns a list of keys with context support.
func (l *Local) ListWithContext(ctx context.Context, prefix string) ([]string, error) {
	// Validate prefix if not empty (empty prefix is valid for listing all)
	if prefix != "" {
		if err := l.validateKey(prefix); err != nil {
			return nil, err
		}
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	var keys []string
	searchPath := filepath.Join(l.path, prefix)

	// Check if the search path exists
	if _, err := os.Stat(searchPath); os.IsNotExist(err) {
		// If the path doesn't exist, return empty list (not an error)
		return keys, nil
	}

	err := filepath.Walk(l.path, func(path string, info os.FileInfo, err error) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err != nil {
			return err
		}

		// Skip directories and metadata files
		if info.IsDir() || strings.HasSuffix(path, metadataSuffix) {
			return nil
		}

		// Get relative path from basePath
		relPath, err := filepath.Rel(l.path, path)
		if err != nil {
			return err
		}

		// Normalize path separators for comparison (use forward slashes)
		normalizedRel := filepath.ToSlash(relPath)
		normalizedPrefix := filepath.ToSlash(prefix)

		// Check if this path matches the prefix
		if strings.HasPrefix(normalizedRel, normalizedPrefix) {
			keys = append(keys, normalizedRel)
		}

		return nil
	})

	return keys, err
}

// ListWithOptions returns a paginated list of objects with full metadata.
func (l *Local) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	if opts == nil {
		opts = &common.ListOptions{}
	}

	// Validate prefix if not empty (empty prefix is valid for listing all)
	if opts.Prefix != "" {
		if err := l.validateKey(opts.Prefix); err != nil {
			return nil, err
		}
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	result := &common.ListResult{
		Objects:        []*common.ObjectInfo{},
		CommonPrefixes: []string{},
	}

	// Handle delimiter-based hierarchical listing
	prefixMap := make(map[string]bool)
	var allObjects []*common.ObjectInfo

	err := filepath.Walk(l.path, func(path string, info os.FileInfo, err error) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err != nil {
			return err
		}

		// Skip directories and metadata files
		if info.IsDir() || strings.HasSuffix(path, metadataSuffix) {
			return nil
		}

		// Get relative path from basePath
		relPath, err := filepath.Rel(l.path, path)
		if err != nil {
			return err
		}

		// Normalize path separators
		normalizedRel := filepath.ToSlash(relPath)
		normalizedPrefix := filepath.ToSlash(opts.Prefix)

		// Check if this path matches the prefix
		if !strings.HasPrefix(normalizedRel, normalizedPrefix) {
			return nil
		}

		// Handle delimiter
		if opts.Delimiter != "" {
			// Get the remainder after the prefix
			remainder := strings.TrimPrefix(normalizedRel, normalizedPrefix)

			// Check if there's a delimiter in the remainder
			if idx := strings.Index(remainder, opts.Delimiter); idx >= 0 {
				// This is a common prefix (directory)
				commonPrefix := normalizedPrefix + remainder[:idx+len(opts.Delimiter)]
				if !prefixMap[commonPrefix] {
					prefixMap[commonPrefix] = true
					result.CommonPrefixes = append(result.CommonPrefixes, commonPrefix)
				}
				return nil
			}
		}

		// Load metadata
		metadata, err := l.loadMetadata(normalizedRel)
		if err != nil {
			// Create basic metadata if not found
			metadata = &common.Metadata{
				Size:         info.Size(),
				LastModified: info.ModTime(),
				ETag:         fmt.Sprintf("%d-%d", info.ModTime().Unix(), info.Size()),
			}
		}

		objInfo := &common.ObjectInfo{
			Key:      normalizedRel,
			Metadata: metadata,
		}
		allObjects = append(allObjects, objInfo)

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Handle pagination
	startIdx := 0
	if opts.ContinueFrom != "" {
		// Find the starting index based on continuation token
		for i, obj := range allObjects {
			if obj.Key == opts.ContinueFrom {
				startIdx = i + 1
				break
			}
		}
	}

	maxResults := opts.MaxResults
	if maxResults <= 0 {
		maxResults = 1000 // Default max results
	}

	endIdx := startIdx + maxResults
	if endIdx > len(allObjects) {
		endIdx = len(allObjects)
	}

	result.Objects = allObjects[startIdx:endIdx]

	// Set pagination info
	if endIdx < len(allObjects) {
		result.Truncated = true
		result.NextToken = allObjects[endIdx-1].Key
	}

	// Log list operation
	prefixStr := "all objects"
	if opts.Prefix != "" {
		prefixStr = fmt.Sprintf("prefix '%s'", opts.Prefix)
	}
	log.Printf("[LOCAL] ✓ LIST %s: found %d objects, %d common prefixes",
		prefixStr, len(result.Objects), len(result.CommonPrefixes))

	return result, nil
}

// Archive copies an object to another backend for archival.
func (l *Local) Archive(key string, destination common.Archiver) error {
	if err := l.validateKey(key); err != nil {
		return err
	}
	if destination == nil {
		return common.ErrArchiveDestinationNil
	}
	r, err := l.Get(key)
	if err != nil {
		return err
	}
	defer func() { _ = r.Close() }()
	return destination.Put(key, r)
}

// AddPolicy adds a new lifecycle policy.
func (l *Local) AddPolicy(policy common.LifecyclePolicy) error {
	return l.lifecycleManager.AddPolicy(policy)
}

// RemovePolicy removes a lifecycle policy.
func (l *Local) RemovePolicy(id string) error {
	return l.lifecycleManager.RemovePolicy(id)
}

// GetPolicies returns all the lifecycle policies.
func (l *Local) GetPolicies() ([]common.LifecyclePolicy, error) {
	return l.lifecycleManager.GetPolicies()
}

// saveMetadata saves metadata to a sidecar file.
func (l *Local) saveMetadata(key string, metadata *common.Metadata) error {
	if err := l.validateKey(key); err != nil {
		return err
	}

	if metadata == nil {
		return nil
	}

	// Validate custom metadata if present
	if metadata.Custom != nil {
		if err := common.ValidateMetadata(metadata.Custom); err != nil {
			return err
		}
	}

	path := filepath.Join(l.path, key)
	metadataPath := path + metadataSuffix

	data, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	return os.WriteFile(metadataPath, data, 0600) // Restrict file permissions for security
}

// loadMetadata loads metadata from a sidecar file.
func (l *Local) loadMetadata(key string) (*common.Metadata, error) {
	if err := l.validateKey(key); err != nil {
		return nil, err
	}

	path := filepath.Join(l.path, key)
	metadataPath := path + metadataSuffix

	data, err := os.ReadFile(metadataPath) // #nosec G304 -- Path validated by validateKey() to prevent directory traversal
	if err != nil {
		if os.IsNotExist(err) {
			// If metadata file doesn't exist, return error
			return nil, fmt.Errorf("%w: %s", common.ErrMetadataNotFound, key)
		}
		return nil, err
	}

	var metadata common.Metadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}

// formatBytes formats a byte count as a human-readable string
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// GetReplicationManager returns the replication manager for this backend.
// This method implements the common.ReplicationCapable interface.
func (l *Local) GetReplicationManager() (common.ReplicationManager, error) {
	if l.replicationManager == nil {
		return nil, common.ErrReplicationNotSupported
	}
	return l.replicationManager, nil
}

// SetLogger sets the logger for replication operations.
func (l *Local) SetLogger(logger adapters.Logger) {
	l.logger = logger
}

// SetAuditLogger sets the audit logger for replication operations.
func (l *Local) SetAuditLogger(auditLog audit.AuditLogger) {
	l.auditLog = auditLog
}

// SetReplicationManager allows manually setting a replication manager.
// This is useful for testing or when you want to share a replication manager
// across multiple backends.
func (l *Local) SetReplicationManager(rm common.ReplicationManager) {
	l.replicationManager = rm
}

// GetPath returns the base path of the local storage.
// This is useful for creating a replication filesystem that can be passed
// to the replication manager.
func (l *Local) GetPath() string {
	return l.path
}

// GetLogger returns the configured logger.
func (l *Local) GetLogger() adapters.Logger {
	return l.logger
}

// GetAuditLogger returns the configured audit logger.
func (l *Local) GetAuditLogger() audit.AuditLogger {
	return l.auditLog
}
