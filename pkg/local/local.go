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
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeremyhahn/go-objstore/internal/multipart"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/auditlog"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

const metadataSuffix = common.MetadataSuffix

// Local is a storage backend that stores files on the local disk.
type Local struct {
	path                   string
	policyFile             string // a non-default lifecyclePolicyFile, refused as a key
	lifecycleManager       common.LifecycleManager
	replicationManager     common.ReplicationManager
	atRestEncrypterFactory common.EncrypterFactory
	changeLog              ChangeLog
	logger                 adapters.Logger
	auditLog               auditlog.AuditLogger
	lifecycleCancel        context.CancelFunc // stops the background lifecycle goroutine
	lifecycleInterval      time.Duration      // time between scheduled lifecycle passes

	// lifecyclePassHook, when set, is called after each scheduled
	// lifecycle pass completes. It lets tests observe a pass instead of
	// sleeping for one.
	lifecyclePassHook atomic.Pointer[func()]
	// walkVisits and metadataReads count the entries the object walk
	// visits and the metadata objectMetadata reads, so tests can bound the
	// cost of paging through a listing.
	walkVisits      atomic.Int64
	metadataReads   atomic.Int64
	multipartExpiry time.Duration    // age after which an upload in progress is treated as aborted
	now             func() time.Time // time source for upload expiry; nil means time.Now
	buckets         *Buckets         // set when the bucketRoot setting is present

	// pruneMu orders directory pruning against writers. A writer holds the
	// read side from the moment it creates a parent directory until its
	// temporary file exists in it; pruneEmptyDirs holds the write side while
	// it removes directories. Writers never block one another.
	pruneMu sync.RWMutex

	// root is the configured directory opened as an os.Root. Every open,
	// create, rename and remove goes through it, so a symlink planted below
	// the root cannot lead a call outside it. rootMu guards the field; the
	// root itself is safe for concurrent use.
	rootMu sync.Mutex
	root   *os.Root
}

// fsRoot returns the store's root, opening it on first use for a Local built
// without Configure.
func (l *Local) fsRoot() (*os.Root, error) {
	l.rootMu.Lock()
	defer l.rootMu.Unlock()
	if l.root != nil {
		return l.root, nil
	}
	if l.path == "" {
		return nil, common.ErrPathNotSet
	}
	r, err := os.OpenRoot(l.path)
	if err != nil {
		return nil, err
	}
	l.root = r
	return r, nil
}

// setRoot installs r as the store's root and closes the one it replaces.
func (l *Local) setRoot(r *os.Root) {
	l.rootMu.Lock()
	old := l.root
	l.root = r
	l.rootMu.Unlock()
	if old != nil && old != r {
		_ = old.Close()
	}
}

// rootName turns a key, or any other store-relative name, into a name below
// the root. Leading separators are dropped and the name is cleaned, so it can
// never be absolute; the root refuses what still tries to leave it.
func rootName(name string) string {
	sep := string(filepath.Separator)
	n := strings.TrimLeft(filepath.Clean(sep+filepath.FromSlash(name)), sep)
	if n == "" {
		return "."
	}
	return n
}

// New creates a new Local storage backend.
func New() common.Storage {
	return &Local{
		lifecycleManager: NewLifecycleManager(),
	}
}

// Configure sets up the backend with the necessary settings. The local
// filesystem needs no credential, so creds is not read; a credential entry in
// settings is an error.
// Settings:
//   - path: The directory path for local storage (required)
//   - runLifecycle: "true" to run lifecycle processing in background with
//     whichever manager is configured, memory or persistent (optional)
//   - lifecycleInterval: Go duration between scheduled lifecycle passes
//     (optional, default "1h"; zero, negative or unparsable is refused)
//   - lifecycleManagerType: "memory" (default) or "persistent" (optional)
//   - lifecyclePolicyFile: Path to policy file when using persistent manager (optional, default: ".lifecycle-policies.json")
//
// Note: Replication is enabled by calling SetReplicationManager() after Configure().
// This allows the caller to configure replication with custom settings and avoids
// import cycles between packages.
func (l *Local) Configure(settings map[string]string, _ common.Credentials) error {
	return l.configure(settings, nil)
}

// configure is Configure with an optional root already opened on the path,
// which a bucket store gets from its bucket root so that a symlinked bucket
// directory cannot lead it outside. With a nil root the path is created and
// opened here.
func (l *Local) configure(settings map[string]string, root *os.Root) error {
	if err := common.RejectCredentialSettings(settings); err != nil {
		return err
	}
	l.path = settings["path"]
	if l.path == "" {
		return common.ErrPathNotSet
	}

	expiry, err := multipart.ParseExpiry(settings)
	if err != nil {
		return err
	}
	l.multipartExpiry = expiry

	if root == nil {
		// Ensure directory exists
		if err := os.MkdirAll(l.path, 0750); err != nil {
			return err
		}
		root, err = os.OpenRoot(l.path)
		if err != nil {
			return err
		}
	}
	l.setRoot(root)

	l.buckets = nil
	if settings[settingBucketRoot] != "" {
		buckets, err := NewBuckets(settings)
		if err != nil {
			return err
		}
		buckets.SetAtRestEncrypterFactory(l.atRestEncrypterFactory)
		l.buckets = buckets
	}

	// Initialize logger and audit log with no-op defaults if not set
	if l.logger == nil {
		l.logger = adapters.NewNoOpLogger()
	}
	if l.auditLog == nil {
		l.auditLog = auditlog.NewNoOpAuditLogger()
	}

	// Configure lifecycle manager type
	managerType := settings["lifecycleManagerType"]
	if managerType == "" {
		managerType = "memory" // Default to in-memory for backwards compatibility
	}

	l.policyFile = ""
	switch managerType {
	case "memory":
		// Use in-memory lifecycle manager (default)
		l.lifecycleManager = NewLifecycleManager()
	case "persistent":
		// Use persistent lifecycle manager with storagefs
		policyFile := settings["lifecyclePolicyFile"]
		if policyFile == "" {
			policyFile = common.LifecyclePolicyFile
		}
		if !common.SameKey(policyFile, common.LifecyclePolicyFile) {
			// The default name is refused by ValidateKey; a custom one
			// is refused by this store's validateKey.
			l.policyFile = policyFile
		}

		fs := &localFileSystem{root: root, pruneMu: &l.pruneMu}
		persistentManager, err := common.NewPersistentLifecycleManager(fs, policyFile)
		if err != nil {
			return err
		}
		l.lifecycleManager = persistentManager
	default:
		return common.ErrInvalidLifecycleManagerType
	}

	interval, err := parseLifecycleInterval(settings)
	if err != nil {
		return err
	}
	l.lifecycleInterval = interval
	if memManager, ok := l.lifecycleManager.(*LifecycleManager); ok {
		memManager.interval = interval
	}

	// Start background lifecycle processing if requested. The scheduled
	// pass reads the policies of whichever manager was built above.
	if l.lifecycleCancel != nil {
		l.lifecycleCancel()
		l.lifecycleCancel = nil
	}
	if settings["runLifecycle"] == "true" {
		ctx, cancel := context.WithCancel(context.Background())
		l.lifecycleCancel = cancel
		go l.runLifecycle(ctx, interval)
	}

	return nil
}

// SetAtRestEncrypterFactory sets the encryption factory for at-rest encryption.
// When set, all data written to disk will be encrypted, and all data read from
// disk will be decrypted. If not set, data is stored in plaintext (noop).
func (l *Local) SetAtRestEncrypterFactory(factory common.EncrypterFactory) {
	l.atRestEncrypterFactory = factory
	if l.buckets != nil {
		l.buckets.SetAtRestEncrypterFactory(factory)
	}
}

// SetChangeLog sets the change log for tracking Put and Delete operations.
// When set, all mutations will be recorded to the change log for replication.
func (l *Local) SetChangeLog(changeLog ChangeLog) {
	l.changeLog = changeLog
}

// localFileSystem is a simple FileSystem implementation that uses local files
type localFileSystem struct {
	root    *os.Root      // the store's root; every name is resolved below it
	pruneMu *sync.RWMutex // the owning store's pruneMu; nil when no store prunes the tree
}

func (lfs *localFileSystem) OpenFile(name string, flag int, perm os.FileMode) (common.LifecycleFile, error) {
	name = rootName(name)
	// Ensure parent directory exists
	var file *os.File
	for attempt := 1; ; attempt++ {
		release := holdDirs(lfs.pruneMu)
		if err := mkdirAllPruneSafe(lfs.root, filepath.Dir(name)); err != nil {
			release()
			return nil, err
		}
		var err error
		file, err = lfs.root.OpenFile(name, flag, perm)
		release()
		if err == nil {
			break
		}
		if flag&os.O_CREATE == 0 || !errors.Is(err, fs.ErrNotExist) || attempt == pruneRetryAttempts {
			return nil, err
		}
	}
	return &localFile{File: file}, nil
}

func (lfs *localFileSystem) Remove(name string) error {
	return lfs.root.Remove(rootName(name))
}

func (lfs *localFileSystem) Rename(src, dst string) error {
	return lfs.root.Rename(rootName(src), rootName(dst))
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
	if err := common.ValidateKey(key); err != nil {
		return err
	}
	if l.policyFile != "" && common.SameKey(key, l.policyFile) {
		return &common.ValidationError{
			Field:   "key",
			Message: fmt.Sprintf("key %q is the store's lifecycle policy file", key),
		}
	}
	return nil
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

	root, err := l.fsRoot()
	if err != nil {
		return err
	}
	name := rootName(key)
	path := filepath.Join(l.path, name)

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

	var size int64
	if err := writeObjectAtomic(root, &l.pruneMu, name, 0644, func(w io.Writer) error {
		n, werr := io.Copy(w, dataToWrite)
		size = n
		return werr
	}); err != nil {
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
	info, err := root.Stat(name)
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

	root, err := l.fsRoot()
	if err != nil {
		return nil, err
	}
	name := rootName(key)
	path := filepath.Join(l.path, name)
	file, err := root.Open(name)
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
		info, err := root.Stat(name)
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
	root, err := l.fsRoot()
	if err != nil {
		return err
	}
	info, err := root.Stat(rootName(key))
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("%w: %s", common.ErrKeyNotFound, key)
		}
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

	root, err := l.fsRoot()
	if err != nil {
		return err
	}
	name := rootName(key)
	path := filepath.Join(l.path, name)

	// Get file size before deletion for logging. A key that resolves
	// outside the root is refused here rather than unlinked.
	var sizeStr string
	info, err := root.Stat(name)
	switch {
	case err == nil:
		sizeStr = formatBytes(info.Size())
	case !errors.Is(err, fs.ErrNotExist):
		log.Printf("[LOCAL] ✗ DELETE '%s' failed: %v", key, err)
		return err
	}

	// Delete metadata file if it exists
	_ = root.Remove(name + metadataSuffix) // Ignore error if metadata doesn't exist

	err = root.Remove(name)
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

	l.pruneEmptyDirs(filepath.Dir(name))

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

// pruneEmptyDirs removes dir and then its parents for as long as they are
// empty, stopping below the store's own directory. A directory that still holds
// an object, a sidecar or an upload is kept, since os.Remove refuses it.
//
// It holds the write side of pruneMu, so it waits for the writers that are
// between creating a directory and creating a file in it.
func (l *Local) pruneEmptyDirs(dir string) {
	root, err := l.fsRoot()
	if err != nil {
		return
	}
	l.pruneMu.Lock()
	defer l.pruneMu.Unlock()
	for dir = rootName(dir); dir != "." && dir != string(filepath.Separator); dir = filepath.Dir(dir) {
		if err := root.Remove(dir); err != nil {
			return
		}
	}
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

	root, err := l.fsRoot()
	if err != nil {
		return false, err
	}
	_, err = root.Stat(rootName(key))
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

	// Prefixes are matched byte-wise against keys below, not resolved as
	// filesystem paths, so a prefix ending mid key-component (e.g. "k0" for
	// keys "k01", "k02") must not be short-circuited here. Only guard against
	// a missing base directory, which yields an empty list rather than an
	// error.
	root, err := l.fsRoot()
	if errors.Is(err, fs.ErrNotExist) {
		return keys, nil
	}
	if err != nil {
		return nil, err
	}

	err = l.walkObjects(ctx, root, func(normalizedRel string, _ fs.DirEntry) error {
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

	root, err := l.fsRoot()
	if errors.Is(err, fs.ErrNotExist) {
		return result, nil
	}
	if err != nil {
		return nil, err
	}

	maxResults := opts.MaxResults
	if maxResults <= 0 {
		maxResults = 1000 // Default max results
	}

	// Without a delimiter the walk resumes after the continuation key and
	// stops as soon as the page is full, so paging through N objects visits
	// and reads each about once instead of the whole tree per page. With a
	// delimiter, CommonPrefixes covers the whole listing, so the walk still
	// sees every key and the page is sliced from it below.
	paged := opts.Delimiter == ""
	after := ""
	if paged {
		after = filepath.ToSlash(opts.ContinueFrom)
	}
	more := false

	err = l.walkObjectsAfter(ctx, root, after, func(normalizedRel string, d fs.DirEntry) error {
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

		if paged && len(allObjects) == maxResults {
			more = true
			return fs.SkipAll
		}

		metadata, err := l.objectMetadata(normalizedRel, d)
		if err != nil {
			return err
		}
		if metadata == nil {
			return nil
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

	if paged {
		result.Objects = allObjects
		if more {
			result.Truncated = true
			result.NextToken = allObjects[len(allObjects)-1].Key
		}
	} else {
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

	root, err := l.fsRoot()
	if err != nil {
		return err
	}
	metadataName := rootName(key) + metadataSuffix

	data, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	// Write the sidecar atomically so a crash mid-write cannot leave a
	// truncated or partial metadata file alongside the object. The parent
	// directory is created under the prune lock.
	return writeObjectAtomic(root, &l.pruneMu, metadataName, 0600, func(w io.Writer) error {
		_, werr := w.Write(data)
		return werr
	})
}

// loadMetadata loads metadata from a sidecar file.
func (l *Local) loadMetadata(key string) (*common.Metadata, error) {
	if err := l.validateKey(key); err != nil {
		return nil, err
	}

	root, err := l.fsRoot()
	if err != nil {
		return nil, err
	}
	data, err := root.ReadFile(rootName(key) + metadataSuffix)
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

// tmpFilePrefix is the name prefix of the in-flight files writeFileAtomic
// creates before renaming them into place. They are never objects, and
// ValidateKey refuses keys that would name one.
const tmpFilePrefix = common.TempFilePrefix

// walkObjects walks root and calls fn with the slash-separated name of every
// object file, skipping directories, metadata sidecars and in-flight
// temporary files. The walk reads the tree through the root and never
// follows a symlink. Entries that vanish while the walk is in progress (a
// concurrent Put renaming its temp file, a concurrent Delete) are treated as
// absent rather than failing the whole walk; any other error aborts it.
// objectMetadata returns the metadata of the object the walk found at name:
// its sidecar, or, when it has none, one built from the data file. A nil
// result with no error means the object was removed since its directory was
// read. Listing and the scheduled lifecycle pass both age an object by the
// LastModified this returns, so they agree on which objects are expired.
func (l *Local) objectMetadata(name string, d fs.DirEntry) (*common.Metadata, error) {
	l.metadataReads.Add(1)
	metadata, err := l.loadMetadata(name)
	if err == nil {
		return metadata, nil
	}
	info, ierr := d.Info()
	if ierr != nil {
		if errors.Is(ierr, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, ierr
	}
	return &common.Metadata{
		Size:         info.Size(),
		LastModified: info.ModTime(),
		ETag:         fmt.Sprintf("%d-%d", info.ModTime().Unix(), info.Size()),
	}, nil
}

func (l *Local) walkObjects(ctx context.Context, root *os.Root, fn func(name string, d fs.DirEntry) error) error {
	return l.walkObjectsAfter(ctx, root, "", fn)
}

// walkObjectsAfter is walkObjects starting after the key after (all keys
// when after is empty). The walk visits keys in walk order (see
// compareWalkOrder); a directory whose whole subtree comes before after is
// skipped without being read, and so is every object up to and including
// after, so resuming a paged listing costs the page, not the keys before
// it. A key that was deleted since it was handed out still resumes at the
// first key after it.
func (l *Local) walkObjectsAfter(ctx context.Context, root *os.Root, after string, fn func(name string, d fs.DirEntry) error) error {
	return fs.WalkDir(root.FS(), ".", func(name string, d fs.DirEntry, err error) error {
		l.walkVisits.Add(1)
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}

		// Staged multipart state is never an object.
		if d.IsDir() && name == stagingDir {
			return fs.SkipDir
		}

		if after != "" && name != "." {
			if d.IsDir() {
				if !strings.HasPrefix(after, name+"/") && compareWalkOrder(name, after) < 0 {
					return fs.SkipDir
				}
			} else if compareWalkOrder(name, after) <= 0 {
				return nil
			}
		}

		if d.IsDir() || strings.HasSuffix(name, metadataSuffix) || strings.HasPrefix(d.Name(), tmpFilePrefix) {
			return nil
		}

		// A name Get, Put and Delete refuse (a reserved policy file, the
		// store's custom lifecycle policy file) is never listed either.
		if l.validateKey(filepath.ToSlash(name)) != nil {
			return nil
		}

		return fn(name, d)
	})
}

// compareWalkOrder compares two slash-separated keys in the order
// fs.WalkDir visits them: segment by segment, each segment by bytes, a
// directory before its contents. It differs from plain string order
// ("a-b" sorts before "a/b" as strings but after it in the walk).
func compareWalkOrder(a, b string) int {
	sa, sb := strings.Split(a, "/"), strings.Split(b, "/")
	for i := 0; i < len(sa) && i < len(sb); i++ {
		if c := strings.Compare(sa[i], sb[i]); c != 0 {
			return c
		}
	}
	return len(sa) - len(sb)
}

// writeFileAtomic writes the file name below root durably and atomically. It
// streams the payload into a temporary file created in filepath.Dir(name) —
// the same directory as name, so the final rename stays on a single
// filesystem and the temp location can never diverge from the rename target
// — fsyncs it, then renames it over name. A crash or concurrent reader
// therefore never observes a truncated or partial file: the target either
// contains the previous contents or the fully written new ones. On any error
// the temporary file is removed and name is left untouched.
//
// write is invoked with the open temporary file. The final file is given the
// supplied mode.
func writeFileAtomic(root *os.Root, name string, mode os.FileMode, write func(io.Writer) error) error {
	return writeAtomic(root, nil, name, mode, false, write)
}

// writeObjectAtomic is writeFileAtomic for a file whose directory may not exist
// yet. Deleting an object prunes the directories it leaves empty, so the
// directory is created, and the temp file made in it, while holding off the
// prune of mu's store (see createObjectTemp).
func writeObjectAtomic(root *os.Root, mu *sync.RWMutex, name string, mode os.FileMode, write func(io.Writer) error) error {
	return writeAtomic(root, mu, name, mode, true, write)
}

// holdDirs takes the read side of mu, which keeps pruneEmptyDirs from removing
// directories until the returned function is called. A nil mu holds nothing.
// Callers hold it only across a mkdir and the create that follows it, never
// across a read of a caller-supplied io.Reader: a Delete queued behind the
// read lock would otherwise block every later writer, and a reader that
// itself waits on that Delete would deadlock.
func holdDirs(mu *sync.RWMutex) (release func()) {
	if mu == nil {
		return func() {}
	}
	mu.RLock()
	return mu.RUnlock
}

// tempNameAttempts bounds the retries of a temp name that already exists.
const tempNameAttempts = 100

// tempName returns a fresh, unpredictable name for an in-flight file or
// directory in dir.
func tempName(dir string) string {
	return filepath.Join(dir, tmpFilePrefix+rand.Text())
}

// createTemp is os.CreateTemp below root: os.Root has none, so the file is
// made with O_EXCL under a random name, and a name that is taken is retried.
func createTemp(root *os.Root, dir string) (*os.File, string, error) {
	for attempt := 1; ; attempt++ {
		name := tempName(dir)
		f, err := root.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
		if err == nil {
			return f, name, nil
		}
		if !errors.Is(err, fs.ErrExist) || attempt == tempNameAttempts {
			return nil, "", err
		}
	}
}

// mkdirTemp is os.MkdirTemp below root.
func mkdirTemp(root *os.Root, dir string) (string, error) {
	for attempt := 1; ; attempt++ {
		name := tempName(dir)
		err := root.Mkdir(name, 0700)
		if err == nil {
			return name, nil
		}
		if !errors.Is(err, fs.ErrExist) || attempt == tempNameAttempts {
			return "", err
		}
	}
}

// createObjectTemp creates the temp file for a write to dir and returns it
// with its name. With makeDirs it also creates dir. mu is held from the mkdir
// until the temp file exists, and a file in dir keeps a remove from pruning
// it, so within one store no prune can land in that window.
//
// Pruning by another process sharing the root takes no part in mu, so the
// ENOENT retry stays as a second line of defense. It is bounded by
// pruneRetryAttempts and so only makes losing the race unlikely: a writer
// that loses every round still fails. The store-level lock, not the retry, is
// what makes writers within one process safe.
func createObjectTemp(root *os.Root, mu *sync.RWMutex, dir string, makeDirs bool) (*os.File, string, error) {
	for attempt := 1; ; attempt++ {
		release := holdDirs(mu)
		if makeDirs {
			if err := mkdirAllPruneSafe(root, dir); err != nil {
				release()
				return nil, "", err
			}
		}
		tmp, name, err := createTemp(root, dir)
		release()
		if err == nil || !makeDirs || !errors.Is(err, fs.ErrNotExist) || attempt == pruneRetryAttempts {
			return tmp, name, err
		}
	}
}

// pruneRetryAttempts bounds how often a writer repeats a mkdir-then-create
// sequence that a prune by another process interrupted.
const pruneRetryAttempts = 100

// mkdirAllPruneSafe is Root.MkdirAll that survives a prune removing the
// directory, or one of its parents, while the path is being created. Within
// a store the caller excludes pruneEmptyDirs with holdDirs; this retry covers
// a prune by another process sharing the same root, which no in-process lock
// can order. Each retry follows a prune that has already completed, and the
// number of prunes is bounded by the deletes in flight, but the bound is not
// a guarantee. EEXIST is retried too: MkdirAll reports it when the directory
// it found was pruned again before its follow-up stat. A caller still has to
// retry its own create step after this returns, since a prune can land
// between the two.
func mkdirAllPruneSafe(root *os.Root, dir string) error {
	if dir == "." || dir == "" {
		return nil
	}
	for attempt := 1; ; attempt++ {
		err := root.MkdirAll(dir, 0750)
		if err == nil || !(errors.Is(err, fs.ErrNotExist) || errors.Is(err, fs.ErrExist)) || attempt == pruneRetryAttempts {
			return err
		}
	}
}

func writeAtomic(root *os.Root, mu *sync.RWMutex, name string, mode os.FileMode, makeDirs bool, write func(io.Writer) error) error {
	// A target that is a symlink leading out of the root is refused. The
	// rename below would only replace the link, but the caller asked to
	// write through it, which the root forbids.
	if _, err := root.Stat(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}

	dir := filepath.Dir(name)
	tmp, tmpName, err := createObjectTemp(root, mu, dir, makeDirs)
	if err != nil {
		return err
	}

	// Ensure the temp file is cleaned up unless it was successfully renamed.
	committed := false
	defer func() {
		if !committed {
			_ = tmp.Close()
			_ = root.Remove(tmpName)
		}
	}()

	if err := write(tmp); err != nil {
		return err
	}

	// Match the requested final permissions before publishing the file.
	if err := tmp.Chmod(mode); err != nil {
		return err
	}

	// Flush file contents to stable storage before the rename so the data is
	// durable, not just the directory entry.
	if err := tmp.Sync(); err != nil {
		return err
	}

	if err := tmp.Close(); err != nil {
		return err
	}

	if err := root.Rename(tmpName, name); err != nil {
		return err
	}
	committed = true

	// Fsync the parent directory so the rename is durable on the inode level.
	d, err := root.Open(dir)
	if errors.Is(err, fs.ErrNotExist) {
		// A concurrent delete removed the renamed file and pruned the now
		// empty directory; there is nothing left to make durable.
		return nil
	}
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
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
func (l *Local) SetAuditLogger(auditLog auditlog.AuditLogger) {
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
func (l *Local) GetAuditLogger() auditlog.AuditLogger {
	return l.auditLog
}

// Close stops any background goroutines started by Configure (e.g. the lifecycle
// ticker). It is safe to call multiple times.
func (l *Local) Close() {
	if l.lifecycleCancel != nil {
		l.lifecycleCancel()
	}
	// A later call reopens the root on the configured path.
	l.setRoot(nil)
}
