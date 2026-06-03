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
	"os"
	"path/filepath"
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
		}
	}
}

// Process runs a single pass applying lifecycle policies to the storage.
func (lm *LifecycleManager) Process(storage *Local) {
	// GetPolicies acquires RLock internally and returns a copy; no outer lock needed.
	policies, _ := lm.GetPolicies()

	for _, policy := range policies {
		walkFn := func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}

			relPath, err := filepath.Rel(storage.path, path)
			if err != nil {
				return err
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
		_ = filepath.Walk(storage.path, walkFn)
	}
}
