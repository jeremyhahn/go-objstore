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
	"io/fs"
	"strings"
	"sync"
	"time"

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
	// Uploads past their expiry are dropped on the same schedule as the
	// retention policies.
	storage.expireUploads()

	// GetPolicies acquires RLock internally and returns a copy; no outer lock needed.
	policies, _ := lm.GetPolicies()

	// The walk reads the tree through the store's root, so a symlinked
	// directory below it is never followed.
	root, err := storage.fsRoot()
	if err != nil {
		return
	}

	for _, policy := range policies {
		walkFn := func(relPath string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if relPath == stagingDir {
					return fs.SkipDir
				}
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return nil
			}

			if strings.HasPrefix(relPath, policy.Prefix) {
				if time.Since(info.ModTime()) > policy.Retention {
					switch policy.Action {
					case actionDelete:
						_ = storage.Delete(relPath)
					case actionArchive:
						if policy.Destination != nil {
							_ = storage.Archive(relPath, policy.Destination)
						}
					}
				}
			}
			return nil
		}
		_ = fs.WalkDir(root.FS(), ".", walkFn)
	}
}
