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

//go:build gcpstorage

package gcs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/jeremyhahn/go-objstore/pkg/common"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

// Compile-time checks that GCS satisfies the optional capabilities.
var (
	_ common.BucketManager     = (*GCS)(nil)
	_ common.MultipartUploader = (*GCS)(nil)
)

// gcsBucketAdmin is implemented by a client that can manage buckets. It is
// kept apart from gcsClient so that a client without it still serves objects.
type gcsBucketAdmin interface {
	CreateBucket(ctx context.Context, name, project string) error
	DeleteBucket(ctx context.Context, name string) error
	ListBuckets(ctx context.Context, project string) gcsBucketIterator
}

// gcsBucketIterator iterates over the buckets of a project.
type gcsBucketIterator interface {
	Next() (*storage.BucketAttrs, error)
}

// CreateBucket creates a bucket in the project.
func (c clientWrapper) CreateBucket(ctx context.Context, name, project string) error {
	return c.Client.Bucket(name).Create(ctx, project, nil)
}

// DeleteBucket deletes an empty bucket.
func (c clientWrapper) DeleteBucket(ctx context.Context, name string) error {
	return c.Client.Bucket(name).Delete(ctx)
}

// ListBuckets iterates over the buckets of the project.
func (c clientWrapper) ListBuckets(ctx context.Context, project string) gcsBucketIterator {
	return c.Client.Buckets(ctx, project)
}

// staged reports whether name lies under the prefix that holds staged
// multipart state, which is never an object.
func staged(name string) bool {
	return strings.HasPrefix(name, common.MultipartPrefix)
}

// admin returns the client's bucket management, or ErrNotSupported.
func (g *GCS) admin() (gcsBucketAdmin, error) {
	a, ok := g.client.(gcsBucketAdmin)
	if !ok {
		return nil, fmt.Errorf("%w: the gcs client cannot manage buckets", common.ErrNotSupported)
	}
	return a, nil
}

// statusCode returns the HTTP status of a service error, or 0.
func statusCode(err error) int {
	var gerr *googleapi.Error
	if errors.As(err, &gerr) {
		return gerr.Code
	}
	return 0
}

// CreateBucket creates a bucket in the configured project.
func (g *GCS) CreateBucket(ctx context.Context, name string) error {
	if err := common.ValidateBucketName(name); err != nil {
		return err
	}
	if g.project == "" {
		return common.ErrProjectNotSet
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	a, err := g.admin()
	if err != nil {
		return err
	}
	err = a.CreateBucket(ctx, name, g.project)
	switch statusCode(err) {
	case 0:
		return err
	case http.StatusConflict:
		return fmt.Errorf("%w: %s", common.ErrBucketAlreadyExists, name)
	case http.StatusBadRequest:
		return fmt.Errorf("%w: %s: %v", common.ErrInvalidBucketName, name, err)
	}
	return err
}

// DeleteBucket removes an empty bucket. The service refuses a bucket that
// holds an object, and an upload in progress holds staged objects, so it
// counts as content.
func (g *GCS) DeleteBucket(ctx context.Context, name string) error {
	if err := common.ValidateBucketName(name); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	a, err := g.admin()
	if err != nil {
		return err
	}
	return bucketError(a.DeleteBucket(ctx, name), name)
}

// bucketError maps the service errors of a call on a named bucket.
func bucketError(err error, name string) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, storage.ErrBucketNotExist), statusCode(err) == http.StatusNotFound:
		return fmt.Errorf("%w: %s", common.ErrBucketNotFound, name)
	case statusCode(err) == http.StatusConflict:
		return fmt.Errorf("%w: %s", common.ErrBucketNotEmpty, name)
	}
	return err
}

// ListBuckets returns the buckets of the configured project, sorted by name.
func (g *GCS) ListBuckets(ctx context.Context) ([]common.BucketInfo, error) {
	if g.project == "" {
		return nil, common.ErrProjectNotSet
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	a, err := g.admin()
	if err != nil {
		return nil, err
	}
	infos := []common.BucketInfo{}
	it := a.ListBuckets(ctx, g.project)
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}
		infos = append(infos, common.BucketInfo{Name: attrs.Name, Created: attrs.Created})
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	return infos, nil
}

// Bucket returns the Storage bound to the named bucket. It shares this client
// and its credentials.
func (g *GCS) Bucket(ctx context.Context, name string) (common.Storage, error) {
	if err := common.ValidateBucketName(name); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if g.client == nil {
		return nil, common.ErrNotSupported
	}
	if _, err := g.client.Bucket(name).Attrs(ctx); err != nil {
		return nil, bucketError(err, name)
	}
	return &GCS{
		client:          g.client,
		bucket:          name,
		project:         g.project,
		multipartExpiry: g.multipartExpiry,
	}, nil
}
