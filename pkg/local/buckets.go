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
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/jeremyhahn/go-objstore/internal/multipart"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// Compile-time check that Buckets satisfies the bucket capability.
var _ common.BucketManager = (*Buckets)(nil)

// settingBucketRoot names the directory whose subdirectories are the buckets.
const settingBucketRoot = "bucketRoot"

// Buckets emulates buckets over the filesystem: a bucket is a directory
// directly under the bucket root, and Bucket returns a Local rooted there.
//
// The root is explicit because its parent, the alternative, could hold
// unrelated directories that would then be listed as buckets.
type Buckets struct {
	root     string
	dirMu    sync.Mutex
	dir      *os.Root          // root opened as an os.Root; bucket directories are reached only through it (see openDir)
	settings map[string]string // settings handed to each bucket's Local
	now      func() time.Time  // handed to each bucket; nil means time.Now

	encrypters common.EncrypterFactory // handed to each bucket; nil means no encryption at rest
}

// SetAtRestEncrypterFactory sets the factory that encrypts the objects and
// multipart parts of every bucket handed out afterwards, as it does for a
// single Local.
func (b *Buckets) SetAtRestEncrypterFactory(factory common.EncrypterFactory) {
	b.encrypters = factory
}

// NewBuckets returns a bucket manager for the settings. The bucketRoot
// setting is required; the multipartExpiry setting and the settings that
// configure a Local, other than path, apply to every bucket.
func NewBuckets(settings map[string]string) (*Buckets, error) {
	root := settings[settingBucketRoot]
	if root == "" {
		return nil, common.ErrBucketRootNotSet
	}
	if _, err := multipart.ParseExpiry(settings); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(root, 0750); err != nil {
		return nil, err
	}
	dir, err := os.OpenRoot(root)
	if err != nil {
		return nil, err
	}

	child := make(map[string]string, len(settings))
	for k, v := range settings {
		child[k] = v
	}
	delete(child, settingBucketRoot)
	delete(child, "path")
	// Each Bucket call returns a new Local; none of them runs a background
	// goroutine of its own.
	delete(child, "runLifecycle")
	return &Buckets{root: root, dir: dir, settings: child}, nil
}

func (b *Buckets) bucketPath(name string) string {
	return filepath.Join(b.root, name)
}

// CreateBucket creates an empty bucket.
func (b *Buckets) CreateBucket(ctx context.Context, name string) error {
	if err := common.ValidateBucketName(name); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	dir, err := b.openDir(true)
	if err != nil {
		return err
	}
	if err := dir.Mkdir(name, 0750); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("%w: %s", common.ErrBucketAlreadyExists, name)
		}
		return err
	}
	return nil
}

// DeleteBucket removes an empty bucket. A bucket that holds objects or
// uploads in progress is not empty.
func (b *Buckets) DeleteBucket(ctx context.Context, name string) error {
	if err := common.ValidateBucketName(name); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	dir, err := b.openDir(false)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("%w: %s", common.ErrBucketNotFound, name)
		}
		return err
	}
	if err := requireDir(dir, name); err != nil {
		return err
	}

	// Directories that held objects, and the staging directory once its
	// uploads are gone, are left behind empty. Only those go: os.Remove
	// refuses a directory that still holds an object, a sidecar or an upload.
	removeEmptyDirs(dir, name)
	if err := dir.Remove(name); err != nil {
		if entries, rerr := fs.ReadDir(dir.FS(), name); rerr == nil && len(entries) > 0 {
			return fmt.Errorf("%w: %s", common.ErrBucketNotEmpty, name)
		}
		return err
	}
	return nil
}

// removeEmptyDirs removes the directories below dir that hold nothing, deepest
// first. dir itself stays.
func removeEmptyDirs(root *os.Root, dir string) {
	entries, err := fs.ReadDir(root.FS(), filepath.ToSlash(dir))
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		sub := filepath.Join(dir, e.Name())
		removeEmptyDirs(root, sub)
		_ = root.Remove(sub) // fails, as wanted, while the directory holds anything
	}
}

// ListBuckets returns the buckets under the root, sorted by name. The
// creation time is the modification time of the bucket's directory, because
// the filesystem interface offers no portable birth time.
func (b *Buckets) ListBuckets(ctx context.Context) ([]common.BucketInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dir, err := b.openDir(false)
	if errors.Is(err, fs.ErrNotExist) {
		return []common.BucketInfo{}, nil
	}
	if err != nil {
		return nil, err
	}
	entries, err := fs.ReadDir(dir.FS(), ".")
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return []common.BucketInfo{}, nil
		}
		return nil, err
	}
	out := make([]common.BucketInfo, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() || common.ValidateBucketName(e.Name()) != nil {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		out = append(out, common.BucketInfo{Name: e.Name(), Created: info.ModTime()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Bucket returns the Storage bound to the named bucket.
func (b *Buckets) Bucket(ctx context.Context, name string) (common.Storage, error) {
	if err := common.ValidateBucketName(name); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root, err := b.openDir(false)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", common.ErrBucketNotFound, name)
		}
		return nil, err
	}
	if err := requireDir(root, name); err != nil {
		return nil, err
	}

	settings := make(map[string]string, len(b.settings)+1)
	for k, v := range b.settings {
		settings[k] = v
	}
	settings["path"] = b.bucketPath(name)
	// The bucket's store is rooted in the bucket directory opened through
	// the bucket root, so a bucket directory swapped for a symlink cannot
	// lead the store outside the bucket root.
	dir, err := root.OpenRoot(name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", common.ErrBucketNotFound, name)
		}
		return nil, err
	}
	l := &Local{lifecycleManager: NewLifecycleManager()}
	if err := l.configure(settings, dir); err != nil {
		_ = dir.Close()
		return nil, err
	}
	l.now = b.now
	l.atRestEncrypterFactory = b.encrypters
	return l, nil
}

// requireDir reports ErrBucketNotFound unless the bucket is a directory.
// openDir returns the bucket root as an os.Root. The handle held in b.dir is
// reused while it still names the directory at b.root. Once that directory
// has been removed or replaced, the path is opened again, after creating it
// when create is set, so the buckets follow the root rather than a directory
// that is gone. A handle that is replaced stays open: a call still using it
// must not see it closed under it.
func (b *Buckets) openDir(create bool) (*os.Root, error) {
	b.dirMu.Lock()
	defer b.dirMu.Unlock()
	if b.dir != nil {
		held, herr := b.dir.Stat(".")
		cur, cerr := os.Stat(b.root)
		if herr == nil && cerr == nil && os.SameFile(held, cur) {
			return b.dir, nil
		}
	}
	if create {
		if err := os.MkdirAll(b.root, 0750); err != nil {
			return nil, err
		}
	}
	dir, err := os.OpenRoot(b.root)
	if err != nil {
		return nil, err
	}
	b.dir = dir
	return dir, nil
}

func requireDir(root *os.Root, name string) error {
	info, err := root.Lstat(name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("%w: %s", common.ErrBucketNotFound, name)
		}
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%w: %s", common.ErrBucketNotFound, name)
	}
	return nil
}

// Compile-time check that Local satisfies the bucket capability.
var _ common.BucketManager = (*Local)(nil)

// bucketManager returns the buckets configured by the bucketRoot setting.
func (l *Local) bucketManager() (*Buckets, error) {
	if l.buckets == nil {
		return nil, common.ErrBucketRootNotSet
	}
	return l.buckets, nil
}

// CreateBucket creates a bucket under the bucketRoot setting.
func (l *Local) CreateBucket(ctx context.Context, name string) error {
	b, err := l.bucketManager()
	if err != nil {
		return err
	}
	return b.CreateBucket(ctx, name)
}

// DeleteBucket removes an empty bucket under the bucketRoot setting.
func (l *Local) DeleteBucket(ctx context.Context, name string) error {
	b, err := l.bucketManager()
	if err != nil {
		return err
	}
	return b.DeleteBucket(ctx, name)
}

// ListBuckets returns the buckets under the bucketRoot setting.
func (l *Local) ListBuckets(ctx context.Context) ([]common.BucketInfo, error) {
	b, err := l.bucketManager()
	if err != nil {
		return nil, err
	}
	return b.ListBuckets(ctx)
}

// Bucket returns the Storage bound to a bucket under the bucketRoot setting.
func (l *Local) Bucket(ctx context.Context, name string) (common.Storage, error) {
	b, err := l.bucketManager()
	if err != nil {
		return nil, err
	}
	return b.Bucket(ctx, name)
}
