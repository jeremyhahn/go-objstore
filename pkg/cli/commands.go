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

package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/cli/client"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
	"github.com/jeremyhahn/go-objstore/pkg/version"
)

const (
	// BackendLocal represents the local filesystem backend type
	BackendLocal = "local"
)

// CommandContext holds the context for executing commands.
type CommandContext struct {
	Storage common.Storage
	Client  client.Client
	Config  *Config
}

// NewCommandContext creates a new command context from the configuration.
func NewCommandContext(cfg *Config) (*CommandContext, error) {
	// Validate configuration
	if err := ValidateConfig(cfg); err != nil {
		return nil, err
	}

	ctx := &CommandContext{
		Config: cfg,
	}

	// Check if using remote server
	if cfg.Server != "" {
		// Create remote client
		clientConfig := &client.Config{
			ServerURL: cfg.Server,
			Protocol:  cfg.ServerProtocol,
		}
		remoteClient, err := client.NewClient(clientConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create remote client: %w", err)
		}
		ctx.Client = remoteClient
	} else {
		// Create local storage backend
		settings := cfg.GetStorageSettings()
		storage, err := factory.NewStorage(cfg.Backend, settings)
		if err != nil {
			return nil, err
		}
		ctx.Storage = storage
	}

	return ctx, nil
}

// Close closes the command context and cleans up resources.
func (ctx *CommandContext) Close() error {
	if ctx.Client != nil {
		return ctx.Client.Close()
	}
	// Storage backends don't currently have a Close method
	// but we provide this for future extensibility
	return nil
}

// PutCommand uploads a file to the object store.
// If filePath is empty or "-", reads from stdin.
func (ctx *CommandContext) PutCommand(key, filePath string) error {
	return ctx.PutCommandWithMetadata(key, filePath, "", "", nil)
}

// PutCommandWithMetadata uploads a file to the object store with custom metadata.
// If filePath is empty or "-", reads from stdin.
func (ctx *CommandContext) PutCommandWithMetadata(key, filePath, contentType, contentEncoding string, customFields map[string]string) error {
	var reader io.Reader
	var metadata *common.Metadata

	// Determine input source
	if filePath == "" || filePath == "-" {
		// Read from stdin
		reader = os.Stdin
		metadata = &common.Metadata{
			Size: 0, // Size unknown when reading from stdin
		}
	} else {
		// Open the file
		file, err := os.Open(filePath) // #nosec G304 -- User-provided path for CLI file operations, intended behavior
		if err != nil {
			return err
		}
		defer func() { _ = file.Close() }()

		// Get file info for metadata
		fileInfo, err := file.Stat()
		if err != nil {
			return err
		}

		reader = file
		metadata = &common.Metadata{
			Size: fileInfo.Size(),
		}
	}

	// Add custom metadata if provided
	if contentType != "" {
		metadata.ContentType = contentType
	}
	if contentEncoding != "" {
		metadata.ContentEncoding = contentEncoding
	}
	if len(customFields) > 0 {
		if metadata.Custom == nil {
			metadata.Custom = make(map[string]string)
		}
		for k, v := range customFields {
			metadata.Custom[k] = v
		}
	}

	// Upload the data
	ctxBg := context.Background()

	if ctx.Client != nil {
		// Use remote client
		return ctx.Client.Put(ctxBg, key, reader, metadata)
	}

	// Use local storage
	if err := ctx.Storage.PutWithMetadata(ctxBg, key, reader, metadata); err != nil {
		return err
	}

	return nil
}

// GetCommand downloads a file from the object store.
func (ctx *CommandContext) GetCommand(key, outputPath string) error {
	ctxBg := context.Background()

	var reader io.ReadCloser
	var err error

	// Retrieve the object
	if ctx.Client != nil {
		// Use remote client
		reader, _, err = ctx.Client.Get(ctxBg, key)
		if err != nil {
			return err
		}
	} else {
		// Use local storage
		reader, err = ctx.Storage.GetWithContext(ctxBg, key)
		if err != nil {
			return err
		}
	}
	defer func() { _ = reader.Close() }()

	// Determine output destination
	var writer io.Writer
	if outputPath == "" || outputPath == "-" {
		// Write to stdout
		writer = os.Stdout
	} else {
		// Write to file
		file, err := os.Create(outputPath) // #nosec G304 -- User-provided path for CLI file operations, intended behavior
		if err != nil {
			return err
		}
		defer func() { _ = file.Close() }()
		writer = file
	}

	// Copy the data
	if _, err := io.Copy(writer, reader); err != nil {
		return err
	}

	return nil
}

// DeleteCommand deletes an object from the object store.
func (ctx *CommandContext) DeleteCommand(key string) error {
	ctxBg := context.Background()

	if ctx.Client != nil {
		// Use remote client
		return ctx.Client.Delete(ctxBg, key)
	}

	// Delete the object using local storage
	if err := ctx.Storage.DeleteWithContext(ctxBg, key); err != nil {
		return err
	}

	return nil
}

// ListCommand lists objects in the object store with the given prefix.
func (ctx *CommandContext) ListCommand(prefix string) ([]ObjectInfo, error) {
	ctxBg := context.Background()

	// List objects
	opts := &common.ListOptions{
		Prefix: prefix,
	}

	var result *common.ListResult
	var err error

	if ctx.Client != nil {
		// Use remote client
		result, err = ctx.Client.List(ctxBg, opts)
	} else {
		// Use local storage
		result, err = ctx.Storage.ListWithOptions(ctxBg, opts)
	}

	if err != nil {
		return nil, err
	}

	return ConvertListResultToObjectInfo(result), nil
}

// ExistsCommand checks if an object exists in the object store.
func (ctx *CommandContext) ExistsCommand(key string) (bool, error) {
	ctxBg := context.Background()

	if ctx.Client != nil {
		// Use remote client
		return ctx.Client.Exists(ctxBg, key)
	}

	// Check if object exists using local storage
	exists, err := ctx.Storage.Exists(ctxBg, key)
	if err != nil {
		return false, err
	}

	return exists, nil
}

// ConfigCommand returns the current configuration.
func (ctx *CommandContext) ConfigCommand() *Config {
	return ctx.Config
}

// ArchiveCommand archives an object to archival storage.
// Uses the current backend settings for the destination.
func (ctx *CommandContext) ArchiveCommand(key, destinationBackend string) error {
	settings := ctx.Config.GetStorageSettings()
	return ctx.ArchiveCommandWithSettings(key, destinationBackend, settings)
}

// ArchiveCommandWithSettings archives an object to archival storage with custom settings.
// This allows specifying different paths/buckets for the archive destination.
func (ctx *CommandContext) ArchiveCommandWithSettings(key, destinationBackend string, destinationSettings map[string]string) error {
	// If no custom settings provided, use backend settings
	if len(destinationSettings) == 0 {
		destinationSettings = ctx.Config.GetStorageSettings()
	}

	ctxBg := context.Background()

	if ctx.Client != nil {
		// Use remote client
		return ctx.Client.Archive(ctxBg, key, destinationBackend, destinationSettings)
	}

	// Create archiver with custom settings
	archiver, err := factory.NewArchiver(destinationBackend, destinationSettings)
	if err != nil {
		return err
	}

	// Archive the object using local storage
	if err := ctx.Storage.Archive(key, archiver); err != nil {
		return err
	}

	return nil
}

// AddPolicyCommand adds a lifecycle policy.
func (ctx *CommandContext) AddPolicyCommand(id, prefix, retentionDays, action string) error {
	// Parse retention days
	var retentionSeconds int64
	if _, err := fmt.Sscanf(retentionDays, "%d", &retentionSeconds); err != nil {
		return err
	}

	// Convert days to seconds
	retentionSeconds = retentionSeconds * 24 * 60 * 60

	policy := common.LifecyclePolicy{
		ID:        id,
		Prefix:    prefix,
		Retention: time.Duration(retentionSeconds) * time.Second,
		Action:    action,
	}

	// If action is archive, we need a destination
	if action == "archive" {
		// For CLI, we'll need to extend this to support destination configuration
		// For now, we'll use the backend settings
		settings := ctx.Config.GetStorageSettings()
		archiver, err := factory.NewArchiver("glacier", settings)
		if err != nil {
			return err
		}
		policy.Destination = archiver
	}

	ctxBg := context.Background()

	if ctx.Client != nil {
		// Use remote client
		return ctx.Client.AddPolicy(ctxBg, policy)
	}

	// Add the policy using local storage
	if err := ctx.Storage.AddPolicy(policy); err != nil {
		return err
	}

	return nil
}

// RemovePolicyCommand removes a lifecycle policy.
func (ctx *CommandContext) RemovePolicyCommand(id string) error {
	ctxBg := context.Background()

	if ctx.Client != nil {
		// Use remote client
		return ctx.Client.RemovePolicy(ctxBg, id)
	}

	// Remove policy using local storage
	if err := ctx.Storage.RemovePolicy(id); err != nil {
		return err
	}

	return nil
}

// ListPoliciesCommand lists all lifecycle policies.
func (ctx *CommandContext) ListPoliciesCommand() ([]common.LifecyclePolicy, error) {
	ctxBg := context.Background()

	if ctx.Client != nil {
		// Use remote client
		return ctx.Client.GetPolicies(ctxBg)
	}

	// List policies using local storage
	policies, err := ctx.Storage.GetPolicies()
	if err != nil {
		return nil, err
	}

	return policies, nil
}

// ApplyPoliciesCommand applies all lifecycle policies now.
func (ctx *CommandContext) ApplyPoliciesCommand() error {
	ctxBg := context.Background()

	if ctx.Client != nil {
		// Use remote client
		_, _, err := ctx.Client.ApplyPolicies(ctxBg)
		return err
	}

	// Get all policies
	policies, err := ctx.Storage.GetPolicies()
	if err != nil {
		return err
	}

	if len(policies) == 0 {
		return nil // No policies to apply
	}

	// Apply policies based on backend type
	switch ctx.Config.Backend {
	case BackendLocal:
		// For local backend, we can apply policies directly
		return ctx.applyLocalPolicies(policies)
	default:
		// For cloud backends, policies are managed by the cloud provider
		return fmt.Errorf("%w: %s", ErrPolicyManagedByProvider, ctx.Config.Backend)
	}
}

// applyLocalPolicies applies lifecycle policies to local storage.
func (ctx *CommandContext) applyLocalPolicies(policies []common.LifecyclePolicy) error {
	ctxBg := context.Background()

	// List all objects
	opts := &common.ListOptions{
		Prefix: "",
	}
	result, err := ctx.Storage.ListWithOptions(ctxBg, opts)
	if err != nil {
		return err
	}

	// Apply each policy
	for _, policy := range policies {
		for _, obj := range result.Objects {
			// Check if object matches policy prefix
			if !strings.HasPrefix(obj.Key, policy.Prefix) {
				continue
			}

			// Get metadata to check last modified time
			if obj.Metadata == nil {
				continue // Skip objects without metadata
			}

			// Check if object is older than retention period
			age := time.Since(obj.Metadata.LastModified)
			if age <= policy.Retention {
				continue
			}

			// Apply action
			switch policy.Action {
			case "delete":
				if err := ctx.Storage.DeleteWithContext(ctxBg, obj.Key); err != nil {
					// Log error but continue with other objects
					fmt.Fprintf(os.Stderr, "Error deleting %s: %v\n", obj.Key, err)
				}
			case "archive":
				if policy.Destination != nil {
					if err := ctx.Storage.Archive(obj.Key, policy.Destination); err != nil {
						fmt.Fprintf(os.Stderr, "Error archiving %s: %v\n", obj.Key, err)
					}
				}
			}
		}
	}

	return nil
}

// GetMetadataCommand retrieves metadata for an object.
func (ctx *CommandContext) GetMetadataCommand(key string) (*common.Metadata, error) {
	ctxBg := context.Background()

	if ctx.Client != nil {
		// Use remote client
		return ctx.Client.GetMetadata(ctxBg, key)
	}

	// Get metadata using local storage
	metadata, err := ctx.Storage.GetMetadata(ctxBg, key)
	if err != nil {
		return nil, err
	}

	return metadata, nil
}

// UpdateMetadataCommand updates metadata for an existing object.
func (ctx *CommandContext) UpdateMetadataCommand(key, contentType, contentEncoding string, custom map[string]string) error {
	ctxBg := context.Background()

	// Build metadata object
	metadata := &common.Metadata{
		ContentType:     contentType,
		ContentEncoding: contentEncoding,
		Custom:          custom,
	}

	if ctx.Client != nil {
		// Use remote client
		return ctx.Client.UpdateMetadata(ctxBg, key, metadata)
	}

	// Update metadata using local storage
	if err := ctx.Storage.UpdateMetadata(ctxBg, key, metadata); err != nil {
		return err
	}

	return nil
}

// HealthCommand performs a health check on the storage backend.
func (ctx *CommandContext) HealthCommand() (map[string]any, error) {
	ctxBg := context.Background()

	if ctx.Client != nil {
		// Use remote client
		if err := ctx.Client.Health(ctxBg); err != nil {
			return map[string]any{
				"status": "unhealthy",
				"error":  err.Error(),
			}, err
		}
		return map[string]any{
			"status":   "healthy",
			"version":  version.Get(),
			"mode":     "remote",
			"server":   ctx.Config.Server,
			"protocol": ctx.Config.ServerProtocol,
		}, nil
	}

	// Local health check
	result := map[string]any{
		"status":  "healthy",
		"version": version.Get(),
		"mode":    BackendLocal,
		"backend": ctx.Config.Backend,
	}

	return result, nil
}
