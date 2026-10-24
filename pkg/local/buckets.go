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

	child := make(map[string]string, len(settings))
	for k, v := range settings {
		child[k] = v
	}
	delete(child, settingBucketRoot)
	delete(child, "path")
	// Each Bucket call returns a new Local; none of them runs a background
	// goroutine of its own.
	delete(child, "runLifecycle")
	return &Buckets{root: root, settings: child}, nil
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
	if err := os.MkdirAll(b.root, 0750); err != nil {
		return err
	}
	if err := os.Mkdir(b.bucketPath(name), 0750); err != nil {
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
	dir := b.bucketPath(name)
	if err := b.requireDir(name); err != nil {
		return err
	}

	// Directories that held objects, and the staging directory once its
	// uploads are gone, are left behind empty. Only those go: os.Remove
	// refuses a directory that still holds an object, a sidecar or an upload.
	removeEmptyDirs(dir)
	if err := os.Remove(dir); err != nil {
		if entries, rerr := os.ReadDir(dir); rerr == nil && len(entries) > 0 {
			return fmt.Errorf("%w: %s", common.ErrBucketNotEmpty, name)
		}
		return err
	}
	return nil
}

// removeEmptyDirs removes the directories below dir that hold nothing, deepest
// first. dir itself stays.
func removeEmptyDirs(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		sub := filepath.Join(dir, e.Name())
		removeEmptyDirs(sub)
		_ = os.Remove(sub) // fails, as wanted, while the directory holds anything
	}
}

// ListBuckets returns the buckets under the root, sorted by name. The
// creation time is the modification time of the bucket's directory, because
// the filesystem interface offers no portable birth time.
func (b *Buckets) ListBuckets(ctx context.Context) ([]common.BucketInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(b.root)
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
	if err := b.requireDir(name); err != nil {
		return nil, err
	}

	settings := make(map[string]string, len(b.settings)+1)
	for k, v := range b.settings {
		settings[k] = v
	}
	settings["path"] = b.bucketPath(name)
	l := &Local{lifecycleManager: NewLifecycleManager()}
	if err := l.Configure(settings); err != nil {
		return nil, err
	}
	l.now = b.now
	l.atRestEncrypterFactory = b.encrypters
	return l, nil
}

// requireDir reports ErrBucketNotFound unless the bucket is a directory.
func (b *Buckets) requireDir(name string) error {
	info, err := os.Lstat(b.bucketPath(name))
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
