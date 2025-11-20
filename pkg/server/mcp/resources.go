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

package mcp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// Resource represents an MCP resource
type Resource struct {
	URI         string `json:"uri"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	MIMEType    string `json:"mimeType,omitempty"`
}

// ResourceManager manages MCP resources
type ResourceManager struct {
	storage common.Storage
	prefix  string
}

// NewResourceManager creates a new resource manager
func NewResourceManager(storage common.Storage, prefix string) *ResourceManager {
	return &ResourceManager{
		storage: storage,
		prefix:  prefix,
	}
}

// ListResources lists available resources
func (m *ResourceManager) ListResources(ctx context.Context, cursor string) ([]Resource, error) {
	// Use ListWithOptions for pagination support
	opts := &common.ListOptions{
		Prefix:       m.prefix,
		MaxResults:   100, // Limit per page
		ContinueFrom: cursor,
	}

	result, err := m.storage.ListWithOptions(ctx, opts)
	if err != nil {
		return nil, err
	}

	resources := make([]Resource, 0, len(result.Objects))
	for _, obj := range result.Objects {
		resource := Resource{
			URI:  m.objectKeyToURI(obj.Key),
			Name: m.extractName(obj.Key),
		}

		// Add metadata if available
		if obj.Metadata != nil {
			resource.MIMEType = obj.Metadata.ContentType
			resource.Description = fmt.Sprintf("Size: %d bytes, Last Modified: %s",
				obj.Metadata.Size,
				obj.Metadata.LastModified.Format("2006-01-02 15:04:05"))
		}

		resources = append(resources, resource)
	}

	return resources, nil
}

// ReadResource reads a resource's content
func (m *ResourceManager) ReadResource(ctx context.Context, uri string) (string, string, error) {
	key := m.uriToObjectKey(uri)

	// Get metadata first to determine MIME type
	metadata, err := m.storage.GetMetadata(ctx, key)
	if err != nil {
		return "", "", err
	}

	mimeType := metadata.ContentType
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	// Get the actual content
	reader, err := m.storage.GetWithContext(ctx, key)
	if err != nil {
		return "", "", err
	}
	defer func() { _ = reader.Close() }()

	var buf bytes.Buffer
	_, err = io.Copy(&buf, reader)
	if err != nil {
		return "", "", err
	}

	return buf.String(), mimeType, nil
}

// objectKeyToURI converts an object key to a resource URI
func (m *ResourceManager) objectKeyToURI(key string) string {
	return fmt.Sprintf("objstore://%s", key)
}

// uriToObjectKey converts a resource URI to an object key
func (m *ResourceManager) uriToObjectKey(uri string) string {
	// Handle both "objstore://key" and just "key"
	key := strings.TrimPrefix(uri, "objstore://")
	return key
}

// extractName extracts a display name from an object key
func (m *ResourceManager) extractName(key string) string {
	// Get the last component of the key path
	parts := strings.Split(key, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return key
}

// SubscribeToResource subscribes to resource changes (not implemented)
func (m *ResourceManager) SubscribeToResource(ctx context.Context, uri string) error {
	// MCP specification allows for resource subscriptions
	// This is a placeholder for future implementation
	return ErrResourceSubscriptionsNotImplemented
}

// UnsubscribeFromResource unsubscribes from resource changes (not implemented)
func (m *ResourceManager) UnsubscribeFromResource(ctx context.Context, uri string) error {
	// MCP specification allows for resource subscriptions
	// This is a placeholder for future implementation
	return ErrResourceSubscriptionsNotImplemented
}
