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

package memory

import (
	"strings"
	"sync"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

const (
	actionDelete  = "delete"
	actionArchive = "archive"
)

// LifecycleManager is an in-memory lifecycle manager for the memory storage backend.
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

// Run runs the lifecycle manager in a background loop.
func (lm *LifecycleManager) Run(storage *Memory) {
	for {
		lm.Process(storage)
		time.Sleep(lm.interval)
	}
}

// Process runs a single pass applying lifecycle policies to the storage.
func (lm *LifecycleManager) Process(storage *Memory) {
	lm.mutex.RLock()
	policies := make([]common.LifecyclePolicy, 0, len(lm.policies))
	for _, policy := range lm.policies {
		policies = append(policies, policy)
	}
	lm.mutex.RUnlock()

	for _, policy := range policies {
		// Get all keys matching the prefix
		storage.mu.RLock()
		var keysToProcess []string
		for key, obj := range storage.objects {
			if strings.HasPrefix(key, policy.Prefix) {
				if time.Since(obj.metadata.LastModified) > policy.Retention {
					keysToProcess = append(keysToProcess, key)
				}
			}
		}
		storage.mu.RUnlock()

		// Process each key outside of the read lock
		for _, key := range keysToProcess {
			switch policy.Action {
			case actionDelete:
				_ = storage.Delete(key)
			case actionArchive:
				if policy.Destination != nil {
					_ = storage.Archive(key, policy.Destination)
				}
			}
		}
	}
}
