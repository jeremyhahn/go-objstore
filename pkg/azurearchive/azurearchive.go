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

//go:build azurearchive

package azurearchive

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"

	"github.com/jeremyhahn/go-objstore/pkg/common"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

// Internal small interfaces to enable unit testing without network calls.
type blobUploader interface {
	UploadFromReader(ctx context.Context, r io.Reader) error
}

type containerAPI interface {
	NewBlockBlob(name string) blobUploader
}

type containerWrapper struct{ azblob.ContainerURL }

type blobWrapper struct{ azblob.BlockBlobURL }

func (b blobWrapper) UploadFromReader(ctx context.Context, r io.Reader) error {
	return azureArchUploadFn(ctx, r, b.BlockBlobURL)
}

var azureArchUploadFn = func(ctx context.Context, r io.Reader, b azblob.BlockBlobURL) error {
	_, err := azblob.UploadStreamToBlockBlob(ctx, r, b, azblob.UploadStreamToBlockBlobOptions{})
	return err
}

func (c containerWrapper) NewBlockBlob(name string) blobUploader {
	return blobWrapper{c.NewBlockBlobURL(name)}
}

// AzureArchive is an archive-only storage backend for Azure Archive.
type AzureArchive struct {
	container containerAPI
}

// New creates a new AzureArchive storage backend.
func New() common.ArchiveOnlyStorage {
	return &AzureArchive{}
}

// Configure sets up the backend with the necessary settings.
func (a *AzureArchive) Configure(settings map[string]string) error {
	accountName := settings["accountName"]
	accountKey := settings["accountKey"]
	containerName := settings["containerName"]

	if accountName == "" || accountKey == "" || containerName == "" {
		return common.ErrAccountNotSet
	}

	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return err
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	var u *url.URL
	if ep := settings["endpoint"]; ep != "" {
		u, err = url.Parse(fmt.Sprintf("%s/%s", ep, containerName))
	} else {
		u, err = url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, containerName))
	}
	if err != nil {
		return err
	}

	a.container = containerWrapper{azblob.NewContainerURL(*u, p)}

	return nil
}

// Put stores an object in the archive.
func (a *AzureArchive) Put(key string, data io.Reader) error {
	if a.container == nil {
		return common.ErrNotConfigured
	}
	blob := a.container.NewBlockBlob(key)
	// Upload requires a ReadSeeker. Buffer to memory for simplicity in archive-only usage.
	buf, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	return blob.UploadFromReader(context.Background(), bytes.NewReader(buf))
}
