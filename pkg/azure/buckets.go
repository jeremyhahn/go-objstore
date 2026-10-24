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

//go:build azureblob

package azure

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

// Compile-time checks that Azure satisfies the optional capabilities.
var (
	_ common.BucketManager     = (*Azure)(nil)
	_ common.MultipartUploader = (*Azure)(nil)
)

// containerInfo describes a container of the account.
type containerInfo struct {
	Name    string
	Created time.Time
}

// accountAPI manages the containers of a storage account.
type accountAPI interface {
	CreateContainer(ctx context.Context, name string) error
	DeleteContainer(ctx context.Context, name string) error
	// ContainerExists reports whether the container exists.
	ContainerExists(ctx context.Context, name string) (bool, error)
	// ContainerHasBlobs reports whether the container holds any blob,
	// including staged multipart state. A missing container is an error.
	ContainerHasBlobs(ctx context.Context, name string) (bool, error)
	ListContainers(ctx context.Context) ([]containerInfo, error)
	// Container returns the blob operations of one container.
	Container(name string) ContainerAPI
}

// serviceWrapper is the account-level client, built from the same account
// name and endpoint as the container-level client.
type serviceWrapper struct{ azblob.ServiceURL }

func (s serviceWrapper) Container(name string) ContainerAPI {
	return containerWrapper{s.ServiceURL.NewContainerURL(name)}
}

func (s serviceWrapper) CreateContainer(ctx context.Context, name string) error {
	_, err := s.ServiceURL.NewContainerURL(name).Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	return err
}

func (s serviceWrapper) DeleteContainer(ctx context.Context, name string) error {
	_, err := s.ServiceURL.NewContainerURL(name).Delete(ctx, azblob.ContainerAccessConditions{})
	return err
}

func (s serviceWrapper) ContainerExists(ctx context.Context, name string) (bool, error) {
	_, err := s.ServiceURL.NewContainerURL(name).GetProperties(ctx, azblob.LeaseAccessConditions{})
	if hasServiceCode(err, azblob.ServiceCodeContainerNotFound) {
		return false, nil
	}
	return err == nil, err
}

func (s serviceWrapper) ContainerHasBlobs(ctx context.Context, name string) (bool, error) {
	c := s.ServiceURL.NewContainerURL(name)
	// A page can be empty while more follow, so go on until a blob turns up
	// or the listing ends.
	for marker := (azblob.Marker{}); marker.NotDone(); {
		page, err := c.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{MaxResults: 1})
		if err != nil {
			return false, err
		}
		if len(page.Segment.BlobItems) > 0 {
			return true, nil
		}
		marker = page.NextMarker
	}
	return false, nil
}

func (s serviceWrapper) ListContainers(ctx context.Context) ([]containerInfo, error) {
	var out []containerInfo
	for marker := (azblob.Marker{}); marker.NotDone(); {
		page, err := s.ServiceURL.ListContainersSegment(ctx, marker, azblob.ListContainersSegmentOptions{})
		if err != nil {
			return nil, err
		}
		for _, item := range page.ContainerItems {
			// The service reports when a container last changed, not when it
			// was created.
			out = append(out, containerInfo{Name: item.Name, Created: item.Properties.LastModified})
		}
		marker = page.NextMarker
	}
	return out, nil
}

// hasServiceCode reports whether err is a service error with the given code.
func hasServiceCode(err error, code azblob.ServiceCodeType) bool {
	var stgErr azblob.StorageError
	return errors.As(err, &stgErr) && stgErr.ServiceCode() == code
}

// staged reports whether name lies under the prefix that holds staged
// multipart state, which is never an object.
func staged(name string) bool {
	return strings.HasPrefix(name, common.MultipartPrefix)
}

// listKeys lists the keys under prefix, without staged multipart state.
func (a *Azure) listKeys(ctx context.Context, prefix string) ([]string, error) {
	keys, err := a.container.ListBlobsFlat(ctx, prefix)
	if err != nil {
		return nil, err
	}
	hidden := 0
	for _, k := range keys {
		if staged(k) {
			hidden++
		}
	}
	if hidden == 0 {
		return keys, nil
	}
	out := make([]string, 0, len(keys)-hidden)
	for _, k := range keys {
		if !staged(k) {
			out = append(out, k)
		}
	}
	return out, nil
}

// accounts returns the account client, or ErrNotSupported when the backend
// was given a container and cannot reach the account.
func (a *Azure) accounts() (accountAPI, error) {
	if a.account == nil {
		return nil, fmt.Errorf("%w: the azure backend has no account client", common.ErrNotSupported)
	}
	return a.account, nil
}

// bucketError maps the service errors of a call on a named container.
func bucketError(err error, name string) error {
	switch {
	case err == nil:
		return nil
	case hasServiceCode(err, azblob.ServiceCodeContainerNotFound):
		return fmt.Errorf("%w: %s", common.ErrBucketNotFound, name)
	case hasServiceCode(err, azblob.ServiceCodeContainerAlreadyExists):
		return fmt.Errorf("%w: %s", common.ErrBucketAlreadyExists, name)
	case hasServiceCode(err, azblob.ServiceCodeInvalidResourceName), hasServiceCode(err, azblob.ServiceCodeOutOfRangeInput):
		return fmt.Errorf("%w: %s: %v", common.ErrInvalidBucketName, name, err)
	}
	return err
}

// CreateBucket creates a container in the account.
func (a *Azure) CreateBucket(ctx context.Context, name string) error {
	if err := common.ValidateBucketName(name); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	acc, err := a.accounts()
	if err != nil {
		return err
	}
	return bucketError(acc.CreateContainer(ctx, name), name)
}

// DeleteBucket removes an empty container. The service itself deletes a
// container together with its blobs, so the container is listed first and
// refused when it holds one; staged multipart state counts as content.
//
// The check and the delete are two calls: a blob written in between is
// deleted with the container. A caller that cannot allow that must stop its
// writers before it deletes the bucket.
func (a *Azure) DeleteBucket(ctx context.Context, name string) error {
	if err := common.ValidateBucketName(name); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	acc, err := a.accounts()
	if err != nil {
		return err
	}
	has, err := acc.ContainerHasBlobs(ctx, name)
	if err != nil {
		return bucketError(err, name)
	}
	if has {
		return fmt.Errorf("%w: %s", common.ErrBucketNotEmpty, name)
	}
	return bucketError(acc.DeleteContainer(ctx, name), name)
}

// ListBuckets returns the containers of the account, sorted by name.
func (a *Azure) ListBuckets(ctx context.Context) ([]common.BucketInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	acc, err := a.accounts()
	if err != nil {
		return nil, err
	}
	containers, err := acc.ListContainers(ctx)
	if err != nil {
		return nil, err
	}
	infos := make([]common.BucketInfo, 0, len(containers))
	for _, c := range containers {
		infos = append(infos, common.BucketInfo{Name: c.Name, Created: c.Created})
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	return infos, nil
}

// Bucket returns the Storage bound to the named container. It shares this
// backend's account client and credentials.
func (a *Azure) Bucket(ctx context.Context, name string) (common.Storage, error) {
	if err := common.ValidateBucketName(name); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	acc, err := a.accounts()
	if err != nil {
		return nil, err
	}
	exists, err := acc.ContainerExists(ctx, name)
	if err != nil {
		return nil, bucketError(err, name)
	}
	if !exists {
		return nil, fmt.Errorf("%w: %s", common.ErrBucketNotFound, name)
	}
	return &Azure{
		account:         acc,
		container:       acc.Container(name),
		accountName:     a.accountName,
		containerName:   name,
		sharedKey:       a.sharedKey,
		subscriptionID:  a.subscriptionID,
		resourceGroup:   a.resourceGroup,
		mgmtClient:      a.mgmtClient,
		multipartExpiry: a.multipartExpiry,
	}, nil
}
