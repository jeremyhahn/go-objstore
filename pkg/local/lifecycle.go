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
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

const (
	actionDelete  = "delete"
	actionArchive = "archive"
)

// LifecycleManager is an in-memory lifecycle manager for the local storage backend.
type LifecycleManager struct {
	policies map[string]common.LifecyclePolicy
	mutex    sync.RWMutex
	interval time.Duration

	// onProcessed, when set before Run starts, is called after each
	// scheduled Process pass completes. It lets tests observe a pass
	// instead of sleeping for one.
	onProcessed func()
}

// NewLifecycleManager creates a new in-memory lifecycle manager.
func NewLifecycleManager() *LifecycleManager {
	return &LifecycleManager{
		policies: make(map[string]common.LifecyclePolicy),
		interval: time.Hour,
	}
}

// AddPolicy adds a new lifecycle policy.
func (lm *LifecycleManager) AddPolicy(policy common.LifecyclePolicy) error {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()
	lm.policies[policy.ID] = policy
	return nil
}

// RemovePolicy removes a lifecycle policy.
func (lm *LifecycleManager) RemovePolicy(id string) error {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()
	delete(lm.policies, id)
	return nil
}

// GetPolicies returns all the lifecycle policies.
func (lm *LifecycleManager) GetPolicies() ([]common.LifecyclePolicy, error) {
	lm.mutex.RLock()
	defer lm.mutex.RUnlock()
	policies := make([]common.LifecyclePolicy, 0, len(lm.policies))
	for _, policy := range lm.policies {
		policies = append(policies, policy)
	}
	return policies, nil
}

// Run runs the lifecycle manager until ctx is cancelled.
// Process is invoked once per interval tick; the goroutine exits cleanly when
// the context is done.
func (lm *LifecycleManager) Run(ctx context.Context, storage *Local) {
	ticker := time.NewTicker(lm.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lm.Process(storage)
			if lm.onProcessed != nil {
				lm.onProcessed()
			}
		}
	}
}

// Process runs a single pass applying lifecycle policies to the storage.
func (lm *LifecycleManager) Process(storage *Local) {
	// GetPolicies acquires RLock internally and returns a copy; no outer lock needed.
	policies, _ := lm.GetPolicies()
	storage.applyLifecycle(policies)
}

// lifecycleIntervalSetting names the setting that sets the time between
// scheduled lifecycle passes.
const lifecycleIntervalSetting = "lifecycleInterval"

// defaultLifecycleInterval is the time between scheduled lifecycle passes
// when lifecycleInterval is not set.
const defaultLifecycleInterval = time.Hour

// parseLifecycleInterval reads lifecycleInterval from settings. An absent
// or empty value gives the default; a value that does not parse as a Go
// duration, or is zero or negative, is refused.
func parseLifecycleInterval(settings map[string]string) (time.Duration, error) {
	raw := settings[lifecycleIntervalSetting]
	if raw == "" {
		return defaultLifecycleInterval, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%w: %s must be a positive duration, got %q", common.ErrInvalidArgument, lifecycleIntervalSetting, raw)
	}
	return d, nil
}

// runLifecycle runs a lifecycle pass every interval until ctx is done. Each
// pass reads the policies of the store's lifecycle manager, whatever its
// type, so persistent policies loaded at Configure fire on the schedule.
func (l *Local) runLifecycle(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.processLifecycle()
			if hook := l.lifecyclePassHook.Load(); hook != nil {
				(*hook)()
			}
		}
	}
}

// processLifecycle runs one lifecycle pass with the policies of the store's
// lifecycle manager.
func (l *Local) processLifecycle() {
	if l.lifecycleManager == nil {
		l.expireUploads()
		return
	}
	policies, err := l.lifecycleManager.GetPolicies()
	if err != nil {
		l.logger.Warn(context.Background(), "lifecycle: reading policies failed",
			adapters.Field{Key: "error", Value: err.Error()})
		policies = nil
	}
	l.applyLifecycle(policies)
}

// applyLifecycle drops uploads past their expiry and applies every policy
// to the objects in the store.
func (l *Local) applyLifecycle(policies []common.LifecyclePolicy) {
	// Uploads past their expiry are dropped on the same schedule as the
	// retention policies.
	l.expireUploads()

	// The walk reads the tree through the store's root, so a symlinked
	// directory below it is never followed.
	root, err := l.fsRoot()
	if err != nil {
		return
	}

	for _, policy := range policies {
		// walkObjects visits only objects (no sidecars, temp files, staged
		// uploads or reserved names), and an object's age is its metadata
		// LastModified, the same age ApplyPolicies uses, so the schedule and
		// a manual pass expire the same objects.
		walkFn := func(relPath string, d fs.DirEntry) error {
			key := filepath.ToSlash(relPath)
			if !strings.HasPrefix(key, policy.Prefix) {
				return nil
			}
			metadata, err := l.objectMetadata(relPath, d)
			if err != nil || metadata == nil {
				return nil
			}
			if time.Since(metadata.LastModified) <= policy.Retention {
				return nil
			}
			l.expireObject(root, key, metadata.LastModified, policy)
			return nil
		}
		_ = l.walkObjects(context.Background(), root, walkFn)
	}
}

// expireObject applies policy's action to key, judged expired at age
// judged. It holds the key's write lock and re-reads the object's age
// under it, and does nothing when the object changed since the judgement:
// a replacement written after the decision is never deleted or archived.
func (l *Local) expireObject(root *os.Root, key string, judged time.Time, policy common.LifecyclePolicy) {
	if l.expiryHook != nil {
		l.expiryHook(key)
	}
	unlock := l.keyMu.lock(key)
	defer unlock()

	info, err := root.Stat(rootName(key))
	if err != nil || !info.Mode().IsRegular() {
		return
	}
	current, err := l.objectMetadata(filepath.FromSlash(key), fs.FileInfoToDirEntry(info))
	if err != nil || current == nil || !current.LastModified.Equal(judged) {
		return
	}
	switch policy.Action {
	case actionDelete:
		_ = l.Delete(key)
	case actionArchive:
		if policy.Destination != nil {
			_ = l.Archive(key, policy.Destination)
		}
	}
}
