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

//nolint:err113 // Configuration and initialization errors are intentionally dynamic
package objstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
	"github.com/jeremyhahn/go-objstore/pkg/replication"
	"github.com/jeremyhahn/go-objstore/pkg/validation"
)

var (
	// ErrNotInitialized is returned when the facade is not initialized
	ErrNotInitialized = errors.New("objstore facade not initialized")

	// ErrNoDefaultBackend is returned when no default backend is set
	ErrNoDefaultBackend = errors.New("no default backend configured")

	// ErrBackendNotFound is returned when a backend is not found
	ErrBackendNotFound = errors.New("backend not found")
)

// Facade singleton instance
var (
	facade   *ObjstoreFacade
	initOnce sync.Once
	initMu   sync.RWMutex
)

// ObjstoreFacade provides a simplified API for object storage operations
// across multiple backends. Applications and services use this instead of managing
// Storage instances directly, preventing leaky abstractions.
type ObjstoreFacade struct {
	backends       map[string]common.Storage // backend name -> Storage
	defaultBackend string                    // default backend to use
	mu             sync.RWMutex
}

// BackendConfig defines configuration for a storage backend
// Used with the simplified initialization API
type BackendConfig struct {
	// Type is the backend type (local, s3, gcs, azure)
	Type string

	// Settings contains backend-specific configuration
	Settings map[string]string
}

// FacadeConfig configures the objstore facade
type FacadeConfig struct {
	// Backends is a map of backend name to Storage (legacy approach)
	// Use this when you need to pass pre-configured Storage instances
	Backends map[string]common.Storage

	// BackendConfigs is a map of backend name to BackendConfig (simplified approach)
	// Use this to let the facade create storage instances via the factory
	// If both Backends and BackendConfigs are set, they are merged (Backends takes precedence)
	BackendConfigs map[string]BackendConfig

	// DefaultBackend is the name of the default backend to use
	// when no backend is specified in the key reference
	DefaultBackend string
}

// Initialize sets up the objstore facade
// This should be called once at application startup
//
// Two initialization patterns are supported:
//
// 1. Simplified (recommended) - let facade create backends via factory:
//
//	objstore.Initialize(&objstore.FacadeConfig{
//	    BackendConfigs: map[string]objstore.BackendConfig{
//	        "default": {Type: "local", Settings: map[string]string{"path": "/data"}},
//	    },
//	    DefaultBackend: "default",
//	})
//
// 2. Legacy - pass pre-configured Storage instances:
//
//	storage, _ := factory.NewStorage("local", settings)
//	objstore.Initialize(&objstore.FacadeConfig{
//	    Backends: map[string]common.Storage{"default": storage},
//	    DefaultBackend: "default",
//	})
func Initialize(config *FacadeConfig) error {
	var initErr error

	initOnce.Do(func() {
		if config == nil {
			initErr = errors.New("config cannot be nil")
			return
		}

		// Build backends map from both sources
		backends := make(map[string]common.Storage)

		// First, create backends from BackendConfigs (simplified API)
		for name, cfg := range config.BackendConfigs {
			storage, err := factory.NewStorage(cfg.Type, cfg.Settings)
			if err != nil {
				initErr = fmt.Errorf("failed to create backend %q: %w", name, err)
				return
			}
			backends[name] = storage
		}

		// Then, merge in pre-configured Backends (legacy API takes precedence)
		for name, storage := range config.Backends {
			backends[name] = storage
		}

		if len(backends) == 0 {
			initErr = errors.New("at least one backend must be configured")
			return
		}

		// If no default specified, use first backend
		defaultBackend := config.DefaultBackend
		if defaultBackend == "" {
			for name := range backends {
				defaultBackend = name
				break
			}
		}

		// Verify default backend exists
		if _, ok := backends[defaultBackend]; !ok {
			initErr = fmt.Errorf("default backend %s not found in configured backends", defaultBackend)
			return
		}

		facade = &ObjstoreFacade{
			backends:       backends,
			defaultBackend: defaultBackend,
		}
	})

	return initErr
}

// Reset clears the facade (useful for testing)
func Reset() {
	initMu.Lock()
	defer initMu.Unlock()

	if facade != nil {
		facade.mu.Lock()
		facade.backends = nil
		facade.mu.Unlock()
	}

	facade = nil
	initOnce = sync.Once{}
}

// IsInitialized returns whether the facade has been initialized
func IsInitialized() bool {
	initMu.RLock()
	defer initMu.RUnlock()
	return facade != nil
}

// Backend returns a specific backend by name
func Backend(name string) (common.Storage, error) {
	// Validate backend name to prevent injection attacks
	if err := validation.ValidateBackendName(name); err != nil {
		return nil, fmt.Errorf("invalid backend name: %w", err)
	}

	if !IsInitialized() {
		return nil, ErrNotInitialized
	}

	facade.mu.RLock()
	defer facade.mu.RUnlock()

	storage, ok := facade.backends[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrBackendNotFound, validation.SanitizeForLog(name))
	}

	return storage, nil
}

// DefaultBackend returns the default backend
func DefaultBackend() (common.Storage, error) {
	if !IsInitialized() {
		return nil, ErrNotInitialized
	}

	facade.mu.RLock()
	defer facade.mu.RUnlock()

	if facade.defaultBackend == "" {
		return nil, ErrNoDefaultBackend
	}

	storage, ok := facade.backends[facade.defaultBackend]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrBackendNotFound, facade.defaultBackend)
	}

	return storage, nil
}

// Backends returns the names of all registered backends
func Backends() []string {
	if !IsInitialized() {
		return nil
	}

	facade.mu.RLock()
	defer facade.mu.RUnlock()

	backends := make([]string, 0, len(facade.backends))
	for name := range facade.backends {
		backends = append(backends, name)
	}

	return backends
}

// parseKeyReference parses a key reference in the format:
// - "backend:key" - use specific backend
// - "key" - use default backend
func parseKeyReference(keyRef string) (backend, key string) {
	// Split on first colon only
	parts := strings.SplitN(keyRef, ":", 2)
	if len(parts) == 2 {
		// Format: "backend:key"
		return parts[0], parts[1]
	}
	// Format: "key" (use default backend)
	return "", keyRef
}

// getStorageForKey determines which storage backend to use for a given key reference
func getStorageForKey(keyRef string) (common.Storage, string, error) {
	if !IsInitialized() {
		return nil, "", ErrNotInitialized
	}

	backend, key := parseKeyReference(keyRef)

	var storage common.Storage
	var err error

	if backend == "" {
		// Use default backend
		storage, err = DefaultBackend()
	} else {
		// Use specified backend
		storage, err = Backend(backend)
	}

	if err != nil {
		return nil, "", err
	}

	return storage, key, nil
}

// Simplified API - applications use these functions directly

// Put stores an object in the default backend
func Put(key string, data io.Reader) error {
	// Validate key to prevent injection attacks
	if err := validation.ValidateKey(key); err != nil {
		return fmt.Errorf("invalid key: %w", err)
	}

	storage, err := DefaultBackend()
	if err != nil {
		return err
	}

	return storage.Put(key, data)
}

// PutWithContext stores an object with context support
func PutWithContext(ctx context.Context, keyRef string, data io.Reader) error {
	// Validate key reference to prevent injection attacks
	if err := validation.ValidateKeyReference(keyRef); err != nil {
		return fmt.Errorf("invalid key reference: %w", err)
	}

	storage, key, err := getStorageForKey(keyRef)
	if err != nil {
		return err
	}

	return storage.PutWithContext(ctx, key, data)
}

// PutWithMetadata stores an object with metadata
func PutWithMetadata(ctx context.Context, keyRef string, data io.Reader, metadata *common.Metadata) error {
	// Validate key reference to prevent injection attacks
	if err := validation.ValidateKeyReference(keyRef); err != nil {
		return fmt.Errorf("invalid key reference: %w", err)
	}

	// Validate metadata
	if metadata != nil && metadata.Custom != nil {
		if err := common.ValidateMetadata(metadata.Custom); err != nil {
			return fmt.Errorf("invalid metadata: %w", err)
		}
	}

	storage, key, err := getStorageForKey(keyRef)
	if err != nil {
		return err
	}

	return storage.PutWithMetadata(ctx, key, data, metadata)
}

// Get retrieves an object from the default backend
func Get(key string) (io.ReadCloser, error) {
	// Validate key to prevent injection attacks
	if err := validation.ValidateKey(key); err != nil {
		return nil, fmt.Errorf("invalid key: %w", err)
	}

	storage, err := DefaultBackend()
	if err != nil {
		return nil, err
	}

	return storage.Get(key)
}

// GetWithContext retrieves an object with context support
// Supports format: "backend:key" or just "key" (uses default backend)
func GetWithContext(ctx context.Context, keyRef string) (io.ReadCloser, error) {
	// Validate key reference to prevent injection attacks
	if err := validation.ValidateKeyReference(keyRef); err != nil {
		return nil, fmt.Errorf("invalid key reference: %w", err)
	}

	storage, key, err := getStorageForKey(keyRef)
	if err != nil {
		return nil, err
	}

	return storage.GetWithContext(ctx, key)
}

// GetMetadata retrieves metadata for an object
func GetMetadata(ctx context.Context, keyRef string) (*common.Metadata, error) {
	// Validate key reference to prevent injection attacks
	if err := validation.ValidateKeyReference(keyRef); err != nil {
		return nil, fmt.Errorf("invalid key reference: %w", err)
	}

	storage, key, err := getStorageForKey(keyRef)
	if err != nil {
		return nil, err
	}

	return storage.GetMetadata(ctx, key)
}

// UpdateMetadata updates metadata for an object
func UpdateMetadata(ctx context.Context, keyRef string, metadata *common.Metadata) error {
	// Validate key reference to prevent injection attacks
	if err := validation.ValidateKeyReference(keyRef); err != nil {
		return fmt.Errorf("invalid key reference: %w", err)
	}

	// Validate metadata
	if metadata != nil && metadata.Custom != nil {
		if err := common.ValidateMetadata(metadata.Custom); err != nil {
			return fmt.Errorf("invalid metadata: %w", err)
		}
	}

	storage, key, err := getStorageForKey(keyRef)
	if err != nil {
		return err
	}

	return storage.UpdateMetadata(ctx, key, metadata)
}

// Delete removes an object
func Delete(key string) error {
	// Validate key to prevent injection attacks
	if err := validation.ValidateKey(key); err != nil {
		return fmt.Errorf("invalid key: %w", err)
	}

	storage, err := DefaultBackend()
	if err != nil {
		return err
	}

	return storage.Delete(key)
}

// DeleteWithContext removes an object with context support
func DeleteWithContext(ctx context.Context, keyRef string) error {
	// Validate key reference to prevent injection attacks
	if err := validation.ValidateKeyReference(keyRef); err != nil {
		return fmt.Errorf("invalid key reference: %w", err)
	}

	storage, key, err := getStorageForKey(keyRef)
	if err != nil {
		return err
	}

	return storage.DeleteWithContext(ctx, key)
}

// Exists checks if an object exists
func Exists(ctx context.Context, keyRef string) (bool, error) {
	// Validate key reference to prevent injection attacks
	if err := validation.ValidateKeyReference(keyRef); err != nil {
		return false, fmt.Errorf("invalid key reference: %w", err)
	}

	storage, key, err := getStorageForKey(keyRef)
	if err != nil {
		return false, err
	}

	return storage.Exists(ctx, key)
}

// List returns a list of keys with the given prefix
func List(prefix string) ([]string, error) {
	// Validate prefix to prevent injection attacks
	if err := validation.ValidatePrefix(prefix); err != nil {
		return nil, fmt.Errorf("invalid prefix: %w", err)
	}

	storage, err := DefaultBackend()
	if err != nil {
		return nil, err
	}

	return storage.List(prefix)
}

// ListWithContext returns a list of keys with context support
func ListWithContext(ctx context.Context, keyRef string) ([]string, error) {
	// Parse key reference to extract backend and prefix
	backend, prefix := parseKeyReference(keyRef)

	// Validate prefix
	if err := validation.ValidatePrefix(prefix); err != nil {
		return nil, fmt.Errorf("invalid prefix: %w", err)
	}

	var storage common.Storage
	var err error

	if backend == "" {
		storage, err = DefaultBackend()
	} else {
		storage, err = Backend(backend)
	}

	if err != nil {
		return nil, err
	}

	return storage.ListWithContext(ctx, prefix)
}

// ListWithOptions returns a paginated list of objects with full metadata
func ListWithOptions(ctx context.Context, backendName string, opts *common.ListOptions) (*common.ListResult, error) {
	// Validate backend name if provided
	var storage common.Storage
	var err error

	if backendName == "" {
		storage, err = DefaultBackend()
	} else {
		if err := validation.ValidateBackendName(backendName); err != nil {
			return nil, fmt.Errorf("invalid backend name: %w", err)
		}
		storage, err = Backend(backendName)
	}

	if err != nil {
		return nil, err
	}

	// Validate prefix in options
	if opts != nil && opts.Prefix != "" {
		if err := validation.ValidatePrefix(opts.Prefix); err != nil {
			return nil, fmt.Errorf("invalid prefix in options: %w", err)
		}
	}

	return storage.ListWithOptions(ctx, opts)
}

// Archive copies an object to an archiver
func Archive(keyRef string, destination common.Archiver) error {
	// Validate key reference to prevent injection attacks
	if err := validation.ValidateKeyReference(keyRef); err != nil {
		return fmt.Errorf("invalid key reference: %w", err)
	}

	storage, key, err := getStorageForKey(keyRef)
	if err != nil {
		return err
	}

	return storage.Archive(key, destination)
}

// AddPolicy adds a lifecycle policy to a backend
func AddPolicy(backendName string, policy common.LifecyclePolicy) error {
	// Validate backend name if provided
	var storage common.Storage
	var err error

	if backendName == "" {
		storage, err = DefaultBackend()
	} else {
		if err := validation.ValidateBackendName(backendName); err != nil {
			return fmt.Errorf("invalid backend name: %w", err)
		}
		storage, err = Backend(backendName)
	}

	if err != nil {
		return err
	}

	// Validate policy prefix
	if policy.Prefix != "" {
		if err := validation.ValidatePrefix(policy.Prefix); err != nil {
			return fmt.Errorf("invalid policy prefix: %w", err)
		}
	}

	return storage.AddPolicy(policy)
}

// RemovePolicy removes a lifecycle policy from a backend
func RemovePolicy(backendName string, policyID string) error {
	// Validate backend name if provided
	var storage common.Storage
	var err error

	if backendName == "" {
		storage, err = DefaultBackend()
	} else {
		if err := validation.ValidateBackendName(backendName); err != nil {
			return fmt.Errorf("invalid backend name: %w", err)
		}
		storage, err = Backend(backendName)
	}

	if err != nil {
		return err
	}

	return storage.RemovePolicy(policyID)
}

// GetPolicies retrieves all lifecycle policies from a backend
func GetPolicies(backendName string) ([]common.LifecyclePolicy, error) {
	// Validate backend name if provided
	var storage common.Storage
	var err error

	if backendName == "" {
		storage, err = DefaultBackend()
	} else {
		if err := validation.ValidateBackendName(backendName); err != nil {
			return nil, fmt.Errorf("invalid backend name: %w", err)
		}
		storage, err = Backend(backendName)
	}

	if err != nil {
		return nil, err
	}

	return storage.GetPolicies()
}

// GetReplicationManager returns the replication manager for a backend if supported
func GetReplicationManager(backendName string) (common.ReplicationManager, error) {
	// Validate backend name if provided
	var storage common.Storage
	var err error

	if backendName == "" {
		storage, err = DefaultBackend()
	} else {
		if err := validation.ValidateBackendName(backendName); err != nil {
			return nil, fmt.Errorf("invalid backend name: %w", err)
		}
		storage, err = Backend(backendName)
	}

	if err != nil {
		return nil, err
	}

	// Check if backend supports replication
	replicable, ok := storage.(common.ReplicationCapable)
	if !ok {
		return nil, common.ErrReplicationNotSupported
	}

	return replicable.GetReplicationManager()
}

// ReplicationConfig contains configuration for enabling replication on a backend
type ReplicationConfig struct {
	// PolicyFilePath is the path to the replication policy file.
	// If empty, defaults to ".replication-policies.json" in the current directory.
	PolicyFilePath string

	// Interval is the interval between automatic sync operations.
	// If zero, defaults to 5 minutes.
	Interval time.Duration

	// Logger is the logger to use for replication operations.
	// If nil, a no-op logger is used.
	Logger adapters.Logger

	// AuditLog is the audit logger to use for replication operations.
	// If nil, a no-op audit logger is used.
	AuditLog audit.AuditLogger

	// RunInBackground starts a background goroutine to run periodic syncs.
	// If false, syncs must be triggered manually.
	RunInBackground bool
}

// ReplicationManagerSetter is an interface for backends that can have a replication manager set
type ReplicationManagerSetter interface {
	SetReplicationManager(rm common.ReplicationManager)
}

// EnableReplication creates and configures a replication manager for a backend.
// The backend must implement both ReplicationCapable and ReplicationManagerSetter interfaces.
// This function creates a PersistentReplicationManager using the OSFileSystem and
// attaches it to the backend.
//
// Example usage:
//
//	objstore.EnableReplication("local", &objstore.ReplicationConfig{
//	    PolicyFilePath: "/data/replication-policies.json",
//	    Interval:       10 * time.Minute,
//	    RunInBackground: true,
//	})
func EnableReplication(backendName string, config *ReplicationConfig) error {
	if config == nil {
		config = &ReplicationConfig{}
	}

	// Get the backend
	var storage common.Storage
	var err error

	if backendName == "" {
		storage, err = DefaultBackend()
	} else {
		if err := validation.ValidateBackendName(backendName); err != nil {
			return fmt.Errorf("invalid backend name: %w", err)
		}
		storage, err = Backend(backendName)
	}

	if err != nil {
		return err
	}

	// Check if backend supports setting replication manager
	setter, ok := storage.(ReplicationManagerSetter)
	if !ok {
		return fmt.Errorf("backend does not support setting replication manager")
	}

	// Set defaults
	policyFile := config.PolicyFilePath
	if policyFile == "" {
		policyFile = ".replication-policies.json"
	}

	interval := config.Interval
	if interval == 0 {
		interval = 5 * time.Minute
	}

	logger := config.Logger
	if logger == nil {
		logger = adapters.NewNoOpLogger()
	}

	auditLog := config.AuditLog
	if auditLog == nil {
		auditLog = audit.NewNoOpAuditLogger()
	}

	// Create the replication manager with OSFileSystem
	rm, err := replication.NewPersistentReplicationManager(
		&replication.OSFileSystem{},
		policyFile,
		interval,
		logger,
		auditLog,
	)
	if err != nil {
		return fmt.Errorf("failed to create replication manager: %w", err)
	}

	// Set the replication manager on the backend
	setter.SetReplicationManager(rm)

	// Start background processing if requested
	if config.RunInBackground {
		go rm.Run(context.Background())
	}

	return nil
}
