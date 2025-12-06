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

//nolint:gocritic,staticcheck // Style suggestions not critical for Azure storage implementation

package azure

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"sync"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/storage/armstorage"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

// ManagementPoliciesClient is an interface for Azure lifecycle management operations
type ManagementPoliciesClient interface {
	Get(ctx context.Context, resourceGroupName string, accountName string, managementPolicyName armstorage.ManagementPolicyName, options *armstorage.ManagementPoliciesClientGetOptions) (armstorage.ManagementPoliciesClientGetResponse, error)
	CreateOrUpdate(ctx context.Context, resourceGroupName string, accountName string, managementPolicyName armstorage.ManagementPolicyName, properties armstorage.ManagementPolicy, options *armstorage.ManagementPoliciesClientCreateOrUpdateOptions) (armstorage.ManagementPoliciesClientCreateOrUpdateResponse, error)
	Delete(ctx context.Context, resourceGroupName string, accountName string, managementPolicyName armstorage.ManagementPolicyName, options *armstorage.ManagementPoliciesClientDeleteOptions) (armstorage.ManagementPoliciesClientDeleteResponse, error)
}

// Small internal interfaces for testability without network.
type BlobAPI interface {
	UploadFromReader(ctx context.Context, r io.Reader) error
	NewReader(ctx context.Context) (io.ReadCloser, error)
	Delete(ctx context.Context) error
	GetProperties(ctx context.Context) error
}

type ContainerAPI interface {
	NewBlockBlob(name string) BlobAPI
	ListBlobsFlat(ctx context.Context, prefix string) ([]string, error)
}

type containerWrapper struct{ azblob.ContainerURL }
type blobWrapper struct{ azblob.BlockBlobURL }

// Constants
const (
	actionDelete  = "delete"
	actionArchive = "archive"
)

// Error variables
var (
	ErrLifecycleNotAvailable = fmt.Errorf("lifecycle management not available: subscriptionID and resourceGroup required in configuration")
)

// Function variables to enable unit testing without real network I/O.
var (
	azureUploadFn = func(ctx context.Context, r io.Reader, b azblob.BlockBlobURL) error {
		_, err := azblob.UploadStreamToBlockBlob(ctx, r, b, azblob.UploadStreamToBlockBlobOptions{})
		return err
	}
	azureDownloadFn = func(ctx context.Context, b azblob.BlockBlobURL) (io.ReadCloser, error) {
		resp, err := b.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
		if err != nil {
			return nil, err
		}
		return resp.Body(azblob.RetryReaderOptions{}), nil
	}
	azureDeleteFn = func(ctx context.Context, b azblob.BlockBlobURL) error {
		_, err := b.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
		return err
	}
	azureGetPropertiesFn = func(ctx context.Context, b azblob.BlockBlobURL) error {
		_, err := b.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
		return err
	}
	azureListFn = func(ctx context.Context, c azblob.ContainerURL, prefix string) ([]string, error) {
		// Pre-allocate with reasonable capacity to reduce allocations
		keys := make([]string, 0, 100)
		marker := azblob.Marker{}

		for marker.NotDone() {
			listBlob, err := c.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{
				Prefix: prefix,
			})
			if err != nil {
				return nil, err
			}

			for _, blob := range listBlob.Segment.BlobItems {
				keys = append(keys, blob.Name)
			}

			marker = listBlob.NextMarker
		}

		return keys, nil
	}
)

func (c containerWrapper) NewBlockBlob(name string) BlobAPI {
	return blobWrapper{c.ContainerURL.NewBlockBlobURL(name)}
}

func (c containerWrapper) ListBlobsFlat(ctx context.Context, prefix string) ([]string, error) {
	return azureListFn(ctx, c.ContainerURL, prefix)
}

func (b blobWrapper) UploadFromReader(ctx context.Context, r io.Reader) error {
	return azureUploadFn(ctx, r, b.BlockBlobURL)
}
func (b blobWrapper) NewReader(ctx context.Context) (io.ReadCloser, error) {
	return azureDownloadFn(ctx, b.BlockBlobURL)
}
func (b blobWrapper) Delete(ctx context.Context) error {
	return azureDeleteFn(ctx, b.BlockBlobURL)
}
func (b blobWrapper) GetProperties(ctx context.Context) error {
	return azureGetPropertiesFn(ctx, b.BlockBlobURL)
}

// Azure is a storage backend that stores files in Azure Blob Storage.
type Azure struct {
	container ContainerAPI
	// For testing purposes, allow injecting a pre-configured ContainerURL
	TestContainerURL azblob.ContainerURL
	// Management plane client for lifecycle policies (optional)
	mgmtClient         ManagementPoliciesClient
	subscriptionID     string
	resourceGroup      string
	accountName        string
	containerName      string
	policiesMutex      sync.RWMutex
	replicationManager common.ReplicationManager
}

// New creates a new Azure storage backend.
func New() common.Storage {
	return &Azure{}
}

// Configure sets up the backend with the necessary settings.
// Required settings for blob operations:
//   - accountName: Azure storage account name
//   - accountKey: Azure storage account key
//   - containerName: Azure blob container name
//
// Optional settings for lifecycle management:
//   - subscriptionID: Azure subscription ID (required for lifecycle policies)
//   - resourceGroup: Azure resource group name (required for lifecycle policies)
//
// Optional settings:
//   - endpoint: Custom endpoint URL (for Azurite, etc.)
func (a *Azure) Configure(settings map[string]string) error {
	if a.TestContainerURL.URL().Host != "" { // If TestContainerURL is set, use it
		a.container = containerWrapper{a.TestContainerURL}
		return nil
	}

	accountName := settings["accountName"]
	accountKey := settings["accountKey"]
	containerName := settings["containerName"]

	if accountName == "" || accountKey == "" || containerName == "" {
		return common.ErrAccountNotSet
	}

	// Store for lifecycle operations
	a.accountName = accountName
	a.containerName = containerName
	a.subscriptionID = settings["subscriptionID"]
	a.resourceGroup = settings["resourceGroup"]

	// Set up blob operations client
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return err
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	var u *url.URL
	var parseErr error
	if ep := settings["endpoint"]; ep != "" {
		u, parseErr = url.Parse(fmt.Sprintf("%s/%s", ep, containerName))
	} else {
		u, parseErr = url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, containerName))
	}
	if parseErr != nil {
		return parseErr
	}

	a.container = containerWrapper{azblob.NewContainerURL(*u, p)}

	// Optionally set up management client for lifecycle policies
	// This requires Azure AD authentication and subscription/resource group info
	if a.subscriptionID != "" && a.resourceGroup != "" {
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			// Don't fail configuration if management client setup fails
			// Lifecycle operations just won't be available
			return nil
		}

		clientFactory, err := armstorage.NewClientFactory(a.subscriptionID, cred, nil)
		if err != nil {
			return nil
		}

		a.mgmtClient = clientFactory.NewManagementPoliciesClient()
	}

	return nil
}

// Put stores an object in the backend.
func (a *Azure) Put(key string, data io.Reader) error {
	if a.container == nil {
		return common.ErrNotConfigured
	}
	blob := a.container.NewBlockBlob(key)
	return blob.UploadFromReader(context.Background(), data)
}

// Get retrieves an object from the backend.
func (a *Azure) Get(key string) (io.ReadCloser, error) {
	if a.container == nil {
		return nil, common.ErrNotConfigured
	}
	blob := a.container.NewBlockBlob(key)
	return blob.NewReader(context.Background())
}

// Delete removes an object from the backend.
func (a *Azure) Delete(key string) error {
	if a.container == nil {
		return common.ErrNotConfigured
	}
	blob := a.container.NewBlockBlob(key)
	return blob.Delete(context.Background())
}

// List returns a list of keys that start with the given prefix.
func (a *Azure) List(prefix string) ([]string, error) {
	if a.container == nil {
		return nil, common.ErrNotConfigured
	}
	return a.container.ListBlobsFlat(context.Background(), prefix)
}

func (a *Azure) Archive(key string, destination common.Archiver) error {
	rc, err := a.Get(key)
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()

	return destination.Put(key, rc)
}

// AddPolicy adds a new lifecycle policy by configuring Azure Blob lifecycle management.
func (a *Azure) AddPolicy(policy common.LifecyclePolicy) error {
	if policy.ID == "" {
		return common.ErrInvalidPolicy
	}
	if policy.Action != actionDelete && policy.Action != actionArchive {
		return common.ErrInvalidPolicy
	}

	// Check if management client is available
	if a.mgmtClient == nil {
		return ErrLifecycleNotAvailable
	}

	a.policiesMutex.Lock()
	defer a.policiesMutex.Unlock()

	ctx := context.Background()

	// Get existing management policy
	existing, err := a.mgmtClient.Get(ctx, a.resourceGroup, a.accountName, armstorage.ManagementPolicyNameDefault, nil)

	var rules []*armstorage.ManagementPolicyRule
	if err == nil && existing.ManagementPolicy.Properties != nil && existing.ManagementPolicy.Properties.Policy != nil && existing.ManagementPolicy.Properties.Policy.Rules != nil {
		// Remove existing rule with same name
		for _, rule := range existing.ManagementPolicy.Properties.Policy.Rules {
			if rule.Name != nil && *rule.Name != policy.ID {
				rules = append(rules, rule)
			}
		}
	}

	// Convert retention duration to days (minimum 1 day)
	days := float64(policy.Retention.Hours() / 24)
	if days < 1 {
		days = 1
	}
	daysAfterModification := float32(days)

	// Create new lifecycle rule
	enabled := true
	ruleType := armstorage.RuleTypeLifecycle
	blobType := "blockBlob"
	prefixMatch := fmt.Sprintf("%s/%s", a.containerName, policy.Prefix)

	newRule := &armstorage.ManagementPolicyRule{
		Name:    &policy.ID,
		Enabled: &enabled,
		Type:    &ruleType,
		Definition: &armstorage.ManagementPolicyDefinition{
			Filters: &armstorage.ManagementPolicyFilter{
				BlobTypes:   []*string{&blobType},
				PrefixMatch: []*string{&prefixMatch},
			},
		},
	}

	if policy.Action == "delete" {
		newRule.Definition.Actions = &armstorage.ManagementPolicyAction{
			BaseBlob: &armstorage.ManagementPolicyBaseBlob{
				Delete: &armstorage.DateAfterModification{
					DaysAfterModificationGreaterThan: &daysAfterModification,
				},
			},
		}
	} else if policy.Action == "archive" {
		newRule.Definition.Actions = &armstorage.ManagementPolicyAction{
			BaseBlob: &armstorage.ManagementPolicyBaseBlob{
				TierToArchive: &armstorage.DateAfterModification{
					DaysAfterModificationGreaterThan: &daysAfterModification,
				},
			},
		}
	}

	rules = append(rules, newRule)

	// Create or update the management policy
	managementPolicy := armstorage.ManagementPolicy{
		Properties: &armstorage.ManagementPolicyProperties{
			Policy: &armstorage.ManagementPolicySchema{
				Rules: rules,
			},
		},
	}

	_, err = a.mgmtClient.CreateOrUpdate(ctx, a.resourceGroup, a.accountName, armstorage.ManagementPolicyNameDefault, managementPolicy, nil)
	return err
}

// RemovePolicy removes a lifecycle policy by updating Azure Blob lifecycle management.
func (a *Azure) RemovePolicy(id string) error {
	// Check if management client is available
	if a.mgmtClient == nil {
		return ErrLifecycleNotAvailable
	}

	a.policiesMutex.Lock()
	defer a.policiesMutex.Unlock()

	ctx := context.Background()

	// Get existing management policy
	existing, err := a.mgmtClient.Get(ctx, a.resourceGroup, a.accountName, armstorage.ManagementPolicyNameDefault, nil)
	if err != nil {
		// If policy doesn't exist, nothing to remove
		return nil
	}

	if existing.ManagementPolicy.Properties == nil || existing.ManagementPolicy.Properties.Policy == nil || existing.ManagementPolicy.Properties.Policy.Rules == nil {
		return nil
	}

	// Filter out the rule with the given ID
	var rules []*armstorage.ManagementPolicyRule
	for _, rule := range existing.ManagementPolicy.Properties.Policy.Rules {
		if rule.Name != nil && *rule.Name != id {
			rules = append(rules, rule)
		}
	}

	// If no rules left, delete the management policy
	if len(rules) == 0 {
		_, err = a.mgmtClient.Delete(ctx, a.resourceGroup, a.accountName, armstorage.ManagementPolicyNameDefault, nil)
		return err
	}

	// Otherwise, update with remaining rules
	managementPolicy := armstorage.ManagementPolicy{
		Properties: &armstorage.ManagementPolicyProperties{
			Policy: &armstorage.ManagementPolicySchema{
				Rules: rules,
			},
		},
	}

	_, err = a.mgmtClient.CreateOrUpdate(ctx, a.resourceGroup, a.accountName, armstorage.ManagementPolicyNameDefault, managementPolicy, nil)
	return err
}

// GetPolicies returns all lifecycle policies by fetching Azure Blob lifecycle management rules.
func (a *Azure) GetPolicies() ([]common.LifecyclePolicy, error) {
	// Check if management client is available
	if a.mgmtClient == nil {
		return []common.LifecyclePolicy{}, nil
	}

	a.policiesMutex.RLock()
	defer a.policiesMutex.RUnlock()

	ctx := context.Background()

	// Get management policy
	result, err := a.mgmtClient.Get(ctx, a.resourceGroup, a.accountName, armstorage.ManagementPolicyNameDefault, nil)
	if err != nil {
		// If no policy exists, return empty list
		return []common.LifecyclePolicy{}, nil
	}

	if result.ManagementPolicy.Properties == nil || result.ManagementPolicy.Properties.Policy == nil || result.ManagementPolicy.Properties.Policy.Rules == nil {
		return []common.LifecyclePolicy{}, nil
	}

	// Convert Azure management policy rules to common.LifecyclePolicy
	// Only include rules that match our container
	containerPrefix := a.containerName + "/"
	policies := make([]common.LifecyclePolicy, 0)

	for _, rule := range result.ManagementPolicy.Properties.Policy.Rules {
		if rule.Name == nil || rule.Definition == nil {
			continue
		}

		policy := common.LifecyclePolicy{
			ID: *rule.Name,
		}

		// Extract prefix from filters
		if rule.Definition.Filters != nil && rule.Definition.Filters.PrefixMatch != nil && len(rule.Definition.Filters.PrefixMatch) > 0 {
			prefix := *rule.Definition.Filters.PrefixMatch[0]
			// Only include rules for our container
			if len(prefix) >= len(containerPrefix) && prefix[:len(containerPrefix)] == containerPrefix {
				policy.Prefix = prefix[len(containerPrefix):]
			} else {
				continue // Skip rules for other containers
			}
		}

		// Determine action and retention
		if rule.Definition.Actions != nil && rule.Definition.Actions.BaseBlob != nil {
			if rule.Definition.Actions.BaseBlob.Delete != nil && rule.Definition.Actions.BaseBlob.Delete.DaysAfterModificationGreaterThan != nil { //nolint:gocritic // Nested conditions intentional for clarity

				policy.Action = "delete"
				policy.Retention = time.Duration(*rule.Definition.Actions.BaseBlob.Delete.DaysAfterModificationGreaterThan) * 24 * time.Hour
			} else if rule.Definition.Actions.BaseBlob.TierToArchive != nil && rule.Definition.Actions.BaseBlob.TierToArchive.DaysAfterModificationGreaterThan != nil {
				policy.Action = "archive"
				policy.Retention = time.Duration(*rule.Definition.Actions.BaseBlob.TierToArchive.DaysAfterModificationGreaterThan) * 24 * time.Hour
			} else {
				continue // Skip rules we don't understand
			}
		} else {
			continue
		}

		policies = append(policies, policy)
	}

	return policies, nil
}

// GetReplicationManager returns the replication manager for this backend.
// This method implements the common.ReplicationCapable interface.
func (a *Azure) GetReplicationManager() (common.ReplicationManager, error) {
	if a.replicationManager == nil {
		return nil, common.ErrReplicationNotSupported
	}
	return a.replicationManager, nil
}

// SetReplicationManager allows manually setting a replication manager.
// This is useful for testing or when you want to share a replication manager
// across multiple backends.
func (a *Azure) SetReplicationManager(rm common.ReplicationManager) {
	a.replicationManager = rm
}
