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

package replication

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"sync"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// FileSystem interface for testability (matches lifecycle_persistent.go pattern).
type FileSystem interface {
	OpenFile(name string, flag int, perm os.FileMode) (ReplicationFile, error)
	Remove(name string) error
}

// ReplicationFile represents a file in the replication storage filesystem.
type ReplicationFile interface {
	io.ReadWriteCloser
	io.Seeker
	Truncate(size int64) error
	Sync() error
}

// OSFileSystem is the default OS filesystem implementation.
type OSFileSystem struct{}

// OpenFile opens a file using os.OpenFile.
func (fs *OSFileSystem) OpenFile(name string, flag int, perm os.FileMode) (ReplicationFile, error) {
	return os.OpenFile(name, flag, perm) // #nosec G304 -- Internal filesystem abstraction, paths controlled by application
}

// Remove removes a file using os.Remove.
func (fs *OSFileSystem) Remove(name string) error {
	return os.Remove(name)
}

// PersistentReplicationManager manages replication policies with JSON persistence.
type PersistentReplicationManager struct {
	fs         FileSystem
	policyFile string
	policies   map[string]common.ReplicationPolicy
	mutex      sync.RWMutex
	interval   time.Duration
	logger     adapters.Logger
	auditLog   audit.AuditLogger

	// Encryption factories per policy
	backendFactories map[string]common.EncrypterFactory
	sourceFactories  map[string]common.EncrypterFactory
	destFactories    map[string]common.EncrypterFactory

	// Metrics per policy
	metrics map[string]*ReplicationMetrics

	// Background processing control
	stopChan chan struct{}
}

// persistedPolicies is the structure used for JSON serialization.
type persistedPolicies struct {
	Policies map[string]common.ReplicationPolicy `json:"policies"`
}

// NewPersistentReplicationManager creates a new persistent replication manager.
// The manager automatically loads existing policies from the policy file.
// If policyFile is empty, it defaults to ".replication-policies.json".
func NewPersistentReplicationManager(
	fs FileSystem,
	policyFile string,
	interval time.Duration,
	logger adapters.Logger,
	auditLog audit.AuditLogger,
) (*PersistentReplicationManager, error) {
	if fs == nil {
		fs = &OSFileSystem{}
	}

	if policyFile == "" {
		policyFile = ".replication-policies.json"
	}

	if logger == nil {
		logger = adapters.NewNoOpLogger()
	}

	if auditLog == nil {
		auditLog = audit.NewNoOpAuditLogger()
	}

	if interval <= 0 {
		interval = 5 * time.Minute // Default interval
	}

	prm := &PersistentReplicationManager{
		fs:               fs,
		policyFile:       policyFile,
		policies:         make(map[string]common.ReplicationPolicy),
		backendFactories: make(map[string]common.EncrypterFactory),
		sourceFactories:  make(map[string]common.EncrypterFactory),
		destFactories:    make(map[string]common.EncrypterFactory),
		metrics:          make(map[string]*ReplicationMetrics),
		interval:         interval,
		logger:           logger,
		auditLog:         auditLog,
		stopChan:         make(chan struct{}),
	}

	// Load existing policies
	if err := prm.load(); err != nil {
		// If the file doesn't exist, that's okay - we'll create it on first save
		if !os.IsNotExist(err) {
			return nil, err
		}
		logger.Info(context.Background(), "No existing policy file found, starting fresh",
			adapters.Field{Key: "policy_file", Value: policyFile})
	}

	return prm, nil
}

// AddPolicy adds a new replication policy and persists it to storage.
func (prm *PersistentReplicationManager) AddPolicy(policy common.ReplicationPolicy) error {
	if policy.ID == "" {
		return common.ErrInvalidPolicy
	}

	prm.mutex.Lock()
	defer prm.mutex.Unlock()

	prm.policies[policy.ID] = policy
	// Initialize metrics for the new policy
	if _, exists := prm.metrics[policy.ID]; !exists {
		prm.metrics[policy.ID] = NewReplicationMetrics()
	}

	if err := prm.save(); err != nil {
		return err
	}

	prm.logger.Info(context.Background(), "Replication policy added",
		adapters.Field{Key: "policy_id", Value: policy.ID})

	return nil
}

// RemovePolicy removes a replication policy and persists the change.
func (prm *PersistentReplicationManager) RemovePolicy(id string) error {
	prm.mutex.Lock()
	defer prm.mutex.Unlock()

	if _, exists := prm.policies[id]; !exists {
		return common.ErrPolicyNotFound
	}

	delete(prm.policies, id)
	delete(prm.backendFactories, id)
	delete(prm.sourceFactories, id)
	delete(prm.destFactories, id)
	delete(prm.metrics, id)

	if err := prm.save(); err != nil {
		return err
	}

	prm.logger.Info(context.Background(), "Replication policy removed",
		adapters.Field{Key: "policy_id", Value: id})

	return nil
}

// GetPolicy retrieves a replication policy by ID.
func (prm *PersistentReplicationManager) GetPolicy(id string) (*common.ReplicationPolicy, error) {
	prm.mutex.RLock()
	defer prm.mutex.RUnlock()

	policy, exists := prm.policies[id]
	if !exists {
		return nil, common.ErrPolicyNotFound
	}

	return &policy, nil
}

// GetPolicies retrieves all replication policies.
func (prm *PersistentReplicationManager) GetPolicies() ([]common.ReplicationPolicy, error) {
	prm.mutex.RLock()
	defer prm.mutex.RUnlock()

	policies := make([]common.ReplicationPolicy, 0, len(prm.policies))
	for _, p := range prm.policies {
		policies = append(policies, p)
	}

	return policies, nil
}

// GetReplicationStatus retrieves the status and metrics for a specific policy.
func (prm *PersistentReplicationManager) GetReplicationStatus(id string) (*ReplicationStatus, error) {
	prm.mutex.RLock()
	defer prm.mutex.RUnlock()

	policy, exists := prm.policies[id]
	if !exists {
		return nil, common.ErrPolicyNotFound
	}

	metrics, hasMetrics := prm.metrics[id]
	if !hasMetrics {
		metrics = NewReplicationMetrics()
	}

	snapshot := metrics.GetMetricsSnapshot()

	status := &ReplicationStatus{
		PolicyID:            policy.ID,
		SourceBackend:       policy.SourceBackend,
		DestinationBackend:  policy.DestinationBackend,
		Enabled:             policy.Enabled,
		TotalObjectsSynced:  snapshot.TotalObjectsSynced,
		TotalObjectsDeleted: snapshot.TotalObjectsDeleted,
		TotalBytesSynced:    snapshot.TotalBytesSynced,
		TotalErrors:         snapshot.TotalErrors,
		LastSyncTime:        snapshot.LastSyncTime,
		AverageSyncDuration: snapshot.AverageSyncDuration,
		SyncCount:           snapshot.SyncCount,
	}

	return status, nil
}

// SetBackendEncrypterFactory sets the backend at-rest encrypter factory for a policy.
func (prm *PersistentReplicationManager) SetBackendEncrypterFactory(policyID string, factory common.EncrypterFactory) error {
	prm.mutex.Lock()
	defer prm.mutex.Unlock()

	if _, exists := prm.policies[policyID]; !exists {
		return common.ErrPolicyNotFound
	}

	prm.backendFactories[policyID] = factory

	prm.logger.Debug(context.Background(), "Backend encrypter factory set",
		adapters.Field{Key: "policy_id", Value: policyID})

	return nil
}

// SetSourceEncrypterFactory sets the source DEK encrypter factory for a policy.
func (prm *PersistentReplicationManager) SetSourceEncrypterFactory(policyID string, factory common.EncrypterFactory) error {
	prm.mutex.Lock()
	defer prm.mutex.Unlock()

	if _, exists := prm.policies[policyID]; !exists {
		return common.ErrPolicyNotFound
	}

	prm.sourceFactories[policyID] = factory

	prm.logger.Debug(context.Background(), "Source encrypter factory set",
		adapters.Field{Key: "policy_id", Value: policyID})

	return nil
}

// SetDestinationEncrypterFactory sets the destination DEK encrypter factory for a policy.
func (prm *PersistentReplicationManager) SetDestinationEncrypterFactory(policyID string, factory common.EncrypterFactory) error {
	prm.mutex.Lock()
	defer prm.mutex.Unlock()

	if _, exists := prm.policies[policyID]; !exists {
		return common.ErrPolicyNotFound
	}

	prm.destFactories[policyID] = factory

	prm.logger.Debug(context.Background(), "Destination encrypter factory set",
		adapters.Field{Key: "policy_id", Value: policyID})

	return nil
}

// getFactories retrieves encrypter factories for a policy, returning noop factories as defaults.
func (prm *PersistentReplicationManager) getFactories(policyID string) (backend, source, dest common.EncrypterFactory) {
	prm.mutex.RLock()
	defer prm.mutex.RUnlock()

	backend = prm.backendFactories[policyID]
	if backend == nil {
		backend = NewNoopEncrypterFactory()
	}

	source = prm.sourceFactories[policyID]
	if source == nil {
		source = NewNoopEncrypterFactory()
	}

	dest = prm.destFactories[policyID]
	if dest == nil {
		dest = NewNoopEncrypterFactory()
	}

	return
}

// getOrCreateMetrics returns existing metrics or creates new ones for a policy.
func (prm *PersistentReplicationManager) getOrCreateMetrics(policyID string) *ReplicationMetrics {
	prm.mutex.Lock()
	defer prm.mutex.Unlock()

	if metrics, exists := prm.metrics[policyID]; exists {
		return metrics
	}

	metrics := NewReplicationMetrics()
	prm.metrics[policyID] = metrics
	return metrics
}

// SyncAll synchronizes all enabled policies.
func (prm *PersistentReplicationManager) SyncAll(ctx context.Context) (*common.SyncResult, error) {
	policies, err := prm.GetPolicies()
	if err != nil {
		return nil, err
	}

	totalResult := &common.SyncResult{
		PolicyID: "all",
	}

	for _, policy := range policies {
		if !policy.Enabled {
			prm.logger.Debug(ctx, "Skipping disabled policy",
				adapters.Field{Key: "policy_id", Value: policy.ID})
			continue
		}

		result, err := prm.SyncPolicy(ctx, policy.ID)
		if err != nil {
			prm.logger.Error(ctx, "Policy sync failed",
				adapters.Field{Key: "policy_id", Value: policy.ID},
				adapters.Field{Key: "error", Value: err.Error()})
			totalResult.Failed++
			if totalResult.Errors == nil {
				totalResult.Errors = make([]string, 0)
			}
			totalResult.Errors = append(totalResult.Errors, policy.ID+": "+err.Error())
			continue
		}

		totalResult.Synced += result.Synced
		totalResult.Deleted += result.Deleted
		totalResult.Failed += result.Failed
		totalResult.BytesTotal += result.BytesTotal
		if result.Errors != nil {
			if totalResult.Errors == nil {
				totalResult.Errors = make([]string, 0)
			}
			totalResult.Errors = append(totalResult.Errors, result.Errors...)
		}
	}

	return totalResult, nil
}

// SyncPolicy synchronizes a specific policy.
func (prm *PersistentReplicationManager) SyncPolicy(ctx context.Context, policyID string) (*common.SyncResult, error) {
	policy, err := prm.GetPolicy(policyID)
	if err != nil {
		return nil, err
	}

	backendFactory, sourceFactory, destFactory := prm.getFactories(policyID)
	policyMetrics := prm.getOrCreateMetrics(policyID)

	syncer, err := NewSyncer(*policy, backendFactory, sourceFactory, destFactory, prm.logger, prm.auditLog)
	if err != nil {
		return nil, err
	}

	result, err := syncer.SyncAll(ctx)

	// Update policy-level metrics
	if result != nil {
		policyMetrics.IncrementObjectsSynced(int64(result.Synced))
		policyMetrics.IncrementObjectsDeleted(int64(result.Deleted))
		policyMetrics.IncrementBytesSynced(result.BytesTotal)
		policyMetrics.IncrementErrors(int64(result.Failed))
		policyMetrics.RecordSync(result.Duration)
	}

	// Update last sync time on success
	if err == nil {
		prm.mutex.Lock()
		p := prm.policies[policyID]
		p.LastSyncTime = time.Now()
		prm.policies[policyID] = p
		_ = prm.save() // Best effort - don't fail the sync if save fails
		prm.mutex.Unlock()
	}

	return result, err
}

// SyncPolicyParallel synchronizes a specific policy using parallel workers.
func (prm *PersistentReplicationManager) SyncPolicyParallel(ctx context.Context, policyID string, workerCount int) (*common.SyncResult, error) {
	policy, err := prm.GetPolicy(policyID)
	if err != nil {
		return nil, err
	}

	backendFactory, sourceFactory, destFactory := prm.getFactories(policyID)
	policyMetrics := prm.getOrCreateMetrics(policyID)

	syncer, err := NewSyncer(*policy, backendFactory, sourceFactory, destFactory, prm.logger, prm.auditLog)
	if err != nil {
		return nil, err
	}

	result, err := syncer.SyncAllParallel(ctx, workerCount)

	// Update policy-level metrics
	if result != nil {
		policyMetrics.IncrementObjectsSynced(int64(result.Synced))
		policyMetrics.IncrementObjectsDeleted(int64(result.Deleted))
		policyMetrics.IncrementBytesSynced(result.BytesTotal)
		policyMetrics.IncrementErrors(int64(result.Failed))
		policyMetrics.RecordSync(result.Duration)
	}

	// Update last sync time on success
	if err == nil {
		prm.mutex.Lock()
		p := prm.policies[policyID]
		p.LastSyncTime = time.Now()
		prm.policies[policyID] = p
		_ = prm.save() // Best effort - don't fail the sync if save fails
		prm.mutex.Unlock()
	}

	return result, err
}

// Run starts the background sync ticker (blocking).
// This should be run in a goroutine.
func (prm *PersistentReplicationManager) Run(ctx context.Context) {
	ticker := time.NewTicker(prm.interval)
	defer ticker.Stop()

	prm.logger.Info(ctx, "Replication manager started",
		adapters.Field{Key: "interval", Value: prm.interval.String()})

	for {
		select {
		case <-ticker.C:
			prm.logger.Debug(ctx, "Running scheduled sync")
			result, err := prm.SyncAll(ctx)
			if err != nil {
				prm.logger.Error(ctx, "Scheduled sync failed",
					adapters.Field{Key: "error", Value: err.Error()})
			} else {
				prm.logger.Info(ctx, "Scheduled sync completed",
					adapters.Field{Key: "synced", Value: result.Synced},
					adapters.Field{Key: "failed", Value: result.Failed},
					adapters.Field{Key: "bytes", Value: result.BytesTotal})
			}

		case <-ctx.Done():
			prm.logger.Info(ctx, "Replication manager stopping (context done)")
			return

		case <-prm.stopChan:
			prm.logger.Info(ctx, "Replication manager stopped")
			return
		}
	}
}

// Stop stops the background sync process.
func (prm *PersistentReplicationManager) Stop() {
	close(prm.stopChan)
}

// save persists the current policies to storage.
// Must be called with mutex locked.
func (prm *PersistentReplicationManager) save() error {
	data := persistedPolicies{
		Policies: prm.policies,
	}

	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}

	file, err := prm.fs.OpenFile(prm.policyFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	if _, err := file.Write(jsonData); err != nil {
		return err
	}

	return file.Sync()
}

// load reads policies from storage.
func (prm *PersistentReplicationManager) load() error {
	file, err := prm.fs.OpenFile(prm.policyFile, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	jsonData, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	var data persistedPolicies
	if err := json.Unmarshal(jsonData, &data); err != nil {
		return err
	}

	// Load policies into the map
	prm.policies = data.Policies
	if prm.policies == nil {
		prm.policies = make(map[string]common.ReplicationPolicy)
	}

	// Initialize metrics for all loaded policies
	for policyID := range prm.policies {
		if _, exists := prm.metrics[policyID]; !exists {
			prm.metrics[policyID] = NewReplicationMetrics()
		}
	}

	prm.logger.Info(context.Background(), "Loaded replication policies",
		adapters.Field{Key: "count", Value: len(prm.policies)})

	return nil
}
