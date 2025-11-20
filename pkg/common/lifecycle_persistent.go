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

package common

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
)

var (
	// ErrFileSystemNil is returned when filesystem is nil
	ErrFileSystemNil = errors.New("storage filesystem cannot be nil")
	// ErrNoRead is returned when file doesn't support Read
	ErrNoRead = errors.New("file does not support Read")
	// ErrNoWrite is returned when file doesn't support Write
	ErrNoWrite = errors.New("file does not support Write")
	// ErrNoClose is returned when file doesn't support Close
	ErrNoClose = errors.New("file does not support Close")
	// ErrNoSeek is returned when file doesn't support Seek
	ErrNoSeek = errors.New("file does not support Seek")
	// ErrNoTruncate is returned when file doesn't support Truncate
	ErrNoTruncate = errors.New("file does not support Truncate")
	// ErrNoSync is returned when file doesn't support Sync
	ErrNoSync = errors.New("file does not support Sync")
)

// FileSystem defines the minimal interface needed for persistent lifecycle storage.
// This allows users to provide any filesystem-like implementation, including storagefs.StorageFS.
type FileSystem interface {
	// OpenFile opens a file with specified flags and permissions
	OpenFile(name string, flag int, perm os.FileMode) (LifecycleFile, error)
	// Remove removes a file or empty directory
	Remove(name string) error
}

// LifecycleFile represents a file in the storage filesystem.
// This interface is compatible with storagefs.File.
type LifecycleFile interface {
	io.ReadWriteCloser
	io.Seeker
	// Truncate changes the size of the file
	Truncate(size int64) error
	// Sync commits the current contents of the file to storage
	Sync() error
}

// PersistentLifecycleManager is a persistent lifecycle manager that stores policies
// using a FileSystem interface, allowing policies to survive process restarts.
type PersistentLifecycleManager struct {
	fs         FileSystem
	policyFile string
	policies   map[string]LifecyclePolicy
	mutex      sync.RWMutex
}

// persistedPolicies is the structure used for JSON serialization
type persistedPolicies struct {
	Policies []LifecyclePolicy `json:"policies"`
}

// NewPersistentLifecycleManager creates a new persistent lifecycle manager.
// It uses the provided FileSystem to save and load policies from the specified file.
// If policyFile is empty, it defaults to ".lifecycle-policies.json".
//
// To use with storagefs.StorageFS, wrap it using NewFileSystemAdapter:
//
//	storage := factory.NewStorage("local", config)
//	fs := storagefs.New(storage)
//	adapter := common.NewFileSystemAdapter(fs)
//	manager := common.NewPersistentLifecycleManager(adapter, "")
func NewPersistentLifecycleManager(fs FileSystem, policyFile string) (*PersistentLifecycleManager, error) {
	if fs == nil {
		return nil, ErrFileSystemNil
	}

	if policyFile == "" {
		policyFile = ".lifecycle-policies.json"
	}

	lm := &PersistentLifecycleManager{
		fs:         fs,
		policyFile: policyFile,
		policies:   make(map[string]LifecyclePolicy),
	}

	// Load existing policies from storage
	if err := lm.load(); err != nil {
		// If the file doesn't exist, that's okay - we'll create it on first save
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}

	return lm, nil
}

// fileSystemAdapter adapts any filesystem interface with standard OpenFile/Remove methods
// to the FileSystem interface used by PersistentLifecycleManager.
type fileSystemAdapter struct {
	openFile func(name string, flag int, perm os.FileMode) (LifecycleFile, error)
	remove   func(name string) error
}

func (a *fileSystemAdapter) OpenFile(name string, flag int, perm os.FileMode) (LifecycleFile, error) {
	return a.openFile(name, flag, perm)
}

func (a *fileSystemAdapter) Remove(name string) error {
	return a.remove(name)
}

// NewFileSystemAdapter creates a FileSystem adapter from any object that has
// OpenFile and Remove methods. This is useful for adapting storagefs.StorageFS
// or other filesystem implementations.
func NewFileSystemAdapter(fs interface {
	OpenFile(name string, flag int, perm os.FileMode) (any, error)
	Remove(name string) error
}) FileSystem {
	return &fileSystemAdapter{
		openFile: func(name string, flag int, perm os.FileMode) (LifecycleFile, error) {
			file, err := fs.OpenFile(name, flag, perm)
			if err != nil {
				return nil, err
			}
			// Type assert to LifecycleFile (should work if the file implements the interface)
			if lf, ok := file.(LifecycleFile); ok {
				return lf, nil
			}
			// If not, try to wrap it
			return &fileAdapter{file: file}, nil
		},
		remove: fs.Remove,
	}
}

// fileAdapter wraps any object that has Read/Write/Close/Seek/Truncate/Sync methods
type fileAdapter struct {
	file any
}

func (f *fileAdapter) Read(p []byte) (n int, err error) {
	if r, ok := f.file.(io.Reader); ok {
		return r.Read(p)
	}
	return 0, ErrNoRead
}

func (f *fileAdapter) Write(p []byte) (n int, err error) {
	if w, ok := f.file.(io.Writer); ok {
		return w.Write(p)
	}
	return 0, ErrNoWrite
}

func (f *fileAdapter) Close() error {
	if c, ok := f.file.(io.Closer); ok {
		return c.Close()
	}
	return ErrNoClose
}

func (f *fileAdapter) Seek(offset int64, whence int) (int64, error) {
	if s, ok := f.file.(io.Seeker); ok {
		return s.Seek(offset, whence)
	}
	return 0, ErrNoSeek
}

func (f *fileAdapter) Truncate(size int64) error {
	if t, ok := f.file.(interface{ Truncate(int64) error }); ok {
		return t.Truncate(size)
	}
	return ErrNoTruncate
}

func (f *fileAdapter) Sync() error {
	if s, ok := f.file.(interface{ Sync() error }); ok {
		return s.Sync()
	}
	return ErrNoSync
}

// AddPolicy adds a new lifecycle policy and persists it to storage.
func (lm *PersistentLifecycleManager) AddPolicy(policy LifecyclePolicy) error {
	if policy.ID == "" {
		return ErrInvalidPolicy
	}

	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	lm.policies[policy.ID] = policy

	return lm.save()
}

// RemovePolicy removes a lifecycle policy and persists the change to storage.
func (lm *PersistentLifecycleManager) RemovePolicy(id string) error {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	delete(lm.policies, id)

	return lm.save()
}

// GetPolicies returns all the lifecycle policies.
func (lm *PersistentLifecycleManager) GetPolicies() ([]LifecyclePolicy, error) {
	lm.mutex.RLock()
	defer lm.mutex.RUnlock()

	policies := make([]LifecyclePolicy, 0, len(lm.policies))
	for _, policy := range lm.policies {
		policies = append(policies, policy)
	}

	return policies, nil
}

// save persists the current policies to storage.
// Must be called with mutex locked.
func (lm *PersistentLifecycleManager) save() error {
	// Convert policies map to slice for serialization
	policies := make([]LifecyclePolicy, 0, len(lm.policies))
	for _, policy := range lm.policies {
		// Don't serialize the Destination Archiver since it's not serializable
		// Users will need to re-register archive policies after restart
		policyCopy := policy
		policyCopy.Destination = nil
		policies = append(policies, policyCopy)
	}

	data := persistedPolicies{
		Policies: policies,
	}

	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}

	// Open or create the file for writing
	file, err := lm.fs.OpenFile(lm.policyFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	// Write the data
	if _, err := file.Write(jsonData); err != nil {
		return err
	}

	// Sync to ensure it's written to storage
	return file.Sync()
}

// load reads policies from storage.
func (lm *PersistentLifecycleManager) load() error {
	file, err := lm.fs.OpenFile(lm.policyFile, os.O_RDONLY, 0)
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
	for _, policy := range data.Policies {
		lm.policies[policy.ID] = policy
	}

	return nil
}
