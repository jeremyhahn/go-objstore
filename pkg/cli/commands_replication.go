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
	"strings"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/replication"
)

// AddReplicationPolicyCommand adds a new replication policy
func (ctx *CommandContext) AddReplicationPolicyCommand(
	id, sourceBackend, destBackend string,
	sourceSettings, destSettings map[string]string,
	prefix string,
	interval time.Duration,
	mode string,
	backendKey, sourceDEK, destDEK string,
) error {
	// Build the policy
	policy := common.ReplicationPolicy{
		ID:                  id,
		SourceBackend:       sourceBackend,
		SourceSettings:      sourceSettings,
		SourcePrefix:        prefix,
		DestinationBackend:  destBackend,
		DestinationSettings: destSettings,
		CheckInterval:       interval,
		Enabled:             true,
		ReplicationMode:     common.ReplicationMode(mode),
	}

	// Add encryption config if any keys are specified
	if backendKey != "" || sourceDEK != "" || destDEK != "" {
		policy.Encryption = &common.EncryptionPolicy{}

		if backendKey != "" {
			policy.Encryption.Backend = &common.EncryptionConfig{
				Enabled:    true,
				Provider:   "custom",
				DefaultKey: backendKey,
			}
		}

		if sourceDEK != "" {
			policy.Encryption.Source = &common.EncryptionConfig{
				Enabled:    true,
				Provider:   "custom",
				DefaultKey: sourceDEK,
			}
		}

		if destDEK != "" {
			policy.Encryption.Destination = &common.EncryptionConfig{
				Enabled:    true,
				Provider:   "custom",
				DefaultKey: destDEK,
			}
		}
	}

	ctxBg := context.Background()

	if ctx.Client != nil {
		// Use remote client
		return ctx.Client.AddReplicationPolicy(ctxBg, policy)
	}

	// Use local storage - this requires the storage to support replication
	// For now, return an error indicating this needs to be implemented
	return common.ErrReplicationNotSupported
}

// RemoveReplicationPolicyCommand removes a replication policy
func (ctx *CommandContext) RemoveReplicationPolicyCommand(id string) error {
	ctxBg := context.Background()

	if ctx.Client != nil {
		// Use remote client
		return ctx.Client.RemoveReplicationPolicy(ctxBg, id)
	}

	// Use local storage
	return common.ErrReplicationNotSupported
}

// GetReplicationPolicyCommand retrieves a specific replication policy
func (ctx *CommandContext) GetReplicationPolicyCommand(id string) (*common.ReplicationPolicy, error) {
	ctxBg := context.Background()

	if ctx.Client != nil {
		// Use remote client
		return ctx.Client.GetReplicationPolicy(ctxBg, id)
	}

	// Use local storage
	return nil, common.ErrReplicationNotSupported
}

// ListReplicationPoliciesCommand lists all replication policies
func (ctx *CommandContext) ListReplicationPoliciesCommand() ([]common.ReplicationPolicy, error) {
	ctxBg := context.Background()

	if ctx.Client != nil {
		// Use remote client
		return ctx.Client.GetReplicationPolicies(ctxBg)
	}

	// Use local storage
	return nil, common.ErrReplicationNotSupported
}

// TriggerReplicationCommand triggers replication sync
func (ctx *CommandContext) TriggerReplicationCommand(policyID string) (*common.SyncResult, error) {
	ctxBg := context.Background()

	if ctx.Client != nil {
		// Use remote client
		return ctx.Client.TriggerReplication(ctxBg, policyID)
	}

	// Use local storage
	return nil, common.ErrReplicationNotSupported
}

// GetReplicationStatusCommand retrieves replication status for a specific policy
func (ctx *CommandContext) GetReplicationStatusCommand(policyID string) (*replication.ReplicationStatus, error) {
	ctxBg := context.Background()

	if ctx.Client != nil {
		// Use remote client
		return ctx.Client.GetReplicationStatus(ctxBg, policyID)
	}

	// Use local storage
	return nil, common.ErrReplicationNotSupported
}

// parseSettings converts a slice of "key=value" strings to a map
func parseSettings(settings []string) map[string]string {
	result := make(map[string]string)
	for _, s := range settings {
		parts := strings.SplitN(s, "=", 2)
		if len(parts) == 2 {
			result[parts[0]] = parts[1]
		}
	}
	return result
}

// FormatReplicationPoliciesResult formats replication policies for output
func FormatReplicationPoliciesResult(policies []common.ReplicationPolicy, format OutputFormat) string {
	switch format {
	case FormatJSON:
		return formatJSON(policies)
	case FormatTable:
		return formatReplicationPoliciesTable(policies)
	default:
		return formatReplicationPoliciesText(policies)
	}
}

// FormatSyncResult formats a sync result for output
func FormatSyncResult(result *common.SyncResult, format OutputFormat) string {
	switch format {
	case FormatJSON:
		return formatJSON(result)
	case FormatTable:
		return formatSyncResultTable(result)
	default:
		return formatSyncResultText(result)
	}
}

func formatReplicationPoliciesText(policies []common.ReplicationPolicy) string {
	if len(policies) == 0 {
		return "No replication policies configured\n"
	}

	var output strings.Builder
	output.WriteString(fmt.Sprintf("Total policies: %d\n\n", len(policies)))

	for i := range policies {
		p := &policies[i]
		output.WriteString(fmt.Sprintf("ID: %s\n", p.ID))
		output.WriteString(fmt.Sprintf("  Source: %s\n", p.SourceBackend))
		if p.SourcePrefix != "" {
			output.WriteString(fmt.Sprintf("  Source Prefix: %s\n", p.SourcePrefix))
		}
		output.WriteString(fmt.Sprintf("  Destination: %s\n", p.DestinationBackend))
		output.WriteString(fmt.Sprintf("  Mode: %s\n", p.ReplicationMode))
		output.WriteString(fmt.Sprintf("  Enabled: %v\n", p.Enabled))
		output.WriteString(fmt.Sprintf("  Check Interval: %s\n", p.CheckInterval))
		if !p.LastSyncTime.IsZero() {
			output.WriteString(fmt.Sprintf("  Last Sync: %s\n", p.LastSyncTime.Format(time.RFC3339)))
		}
		if p.Encryption != nil {
			if p.Encryption.Backend != nil && p.Encryption.Backend.Enabled {
				output.WriteString(fmt.Sprintf("  Backend Encryption: %s (key: %s)\n",
					p.Encryption.Backend.Provider, p.Encryption.Backend.DefaultKey))
			}
			if p.Encryption.Source != nil && p.Encryption.Source.Enabled {
				output.WriteString(fmt.Sprintf("  Source DEK: %s (key: %s)\n",
					p.Encryption.Source.Provider, p.Encryption.Source.DefaultKey))
			}
			if p.Encryption.Destination != nil && p.Encryption.Destination.Enabled {
				output.WriteString(fmt.Sprintf("  Destination DEK: %s (key: %s)\n",
					p.Encryption.Destination.Provider, p.Encryption.Destination.DefaultKey))
			}
		}
		output.WriteString("\n")
	}

	return output.String()
}

func formatReplicationPoliciesTable(policies []common.ReplicationPolicy) string {
	if len(policies) == 0 {
		return "No replication policies configured\n"
	}

	var output strings.Builder
	output.WriteString("┌────────────────────────────────────────────────────────────────────────────────────────┐\n")
	output.WriteString("│ Replication Policies                                                                   │\n")
	output.WriteString("├──────────────┬─────────────┬─────────────┬────────────┬─────────┬──────────────────────┤\n")
	output.WriteString("│ ID           │ Source      │ Destination │ Mode       │ Enabled │ Last Sync            │\n")
	output.WriteString("├──────────────┼─────────────┼─────────────┼────────────┼─────────┼──────────────────────┤\n")

	for i := range policies {
		p := &policies[i]
		lastSync := "Never"
		if !p.LastSyncTime.IsZero() {
			lastSync = p.LastSyncTime.Format("2006-01-02 15:04")
		}
		enabled := "Yes"
		if !p.Enabled {
			enabled = "No"
		}

		output.WriteString(fmt.Sprintf("│ %-12s │ %-11s │ %-11s │ %-10s │ %-7s │ %-20s │\n",
			truncateString(p.ID, 12),
			truncateString(p.SourceBackend, 11),
			truncateString(p.DestinationBackend, 11),
			truncateString(string(p.ReplicationMode), 10),
			enabled,
			lastSync))
	}

	output.WriteString("└──────────────┴─────────────┴─────────────┴────────────┴─────────┴──────────────────────┘\n")
	return output.String()
}

func formatSyncResultText(result *common.SyncResult) string {
	var output strings.Builder

	if result.PolicyID != "" {
		output.WriteString(fmt.Sprintf("Policy: %s\n", result.PolicyID))
	}
	output.WriteString(fmt.Sprintf("Synced: %d objects\n", result.Synced))
	output.WriteString(fmt.Sprintf("Deleted: %d objects\n", result.Deleted))
	output.WriteString(fmt.Sprintf("Failed: %d objects\n", result.Failed))
	output.WriteString(fmt.Sprintf("Total Bytes: %d\n", result.BytesTotal))
	output.WriteString(fmt.Sprintf("Duration: %s\n", result.Duration))

	if len(result.Errors) > 0 {
		output.WriteString("\nErrors:\n")
		for _, err := range result.Errors {
			output.WriteString(fmt.Sprintf("  - %s\n", err))
		}
	}

	return output.String()
}

func formatSyncResultTable(result *common.SyncResult) string {
	var output strings.Builder

	output.WriteString("┌────────────────────────────────────────────────────────┐\n")
	output.WriteString("│ Sync Result                                            │\n")
	output.WriteString("├────────────────────────────────────────────────────────┤\n")
	if result.PolicyID != "" {
		output.WriteString(fmt.Sprintf("│ Policy: %-47s │\n", truncateString(result.PolicyID, 47)))
	}
	output.WriteString(fmt.Sprintf("│ Synced: %-47d │\n", result.Synced))
	output.WriteString(fmt.Sprintf("│ Deleted: %-46d │\n", result.Deleted))
	output.WriteString(fmt.Sprintf("│ Failed: %-47d │\n", result.Failed))
	output.WriteString(fmt.Sprintf("│ Total Bytes: %-42d │\n", result.BytesTotal))
	output.WriteString(fmt.Sprintf("│ Duration: %-45s │\n", result.Duration.String()))
	output.WriteString("└────────────────────────────────────────────────────────┘\n")

	if len(result.Errors) > 0 {
		output.WriteString("\nErrors:\n")
		for _, err := range result.Errors {
			output.WriteString(fmt.Sprintf("  - %s\n", err))
		}
	}

	return output.String()
}

// FormatReplicationStatus formats a replication status for output
func FormatReplicationStatus(status *replication.ReplicationStatus, format OutputFormat) string {
	switch format {
	case FormatJSON:
		return formatJSON(status)
	case FormatTable:
		return formatReplicationStatusTable(status)
	default:
		return formatReplicationStatusText(status)
	}
}

func formatReplicationStatusText(status *replication.ReplicationStatus) string {
	var output strings.Builder

	output.WriteString(fmt.Sprintf("Policy ID: %s\n", status.PolicyID))
	output.WriteString(fmt.Sprintf("Source Backend: %s\n", status.SourceBackend))
	output.WriteString(fmt.Sprintf("Destination Backend: %s\n", status.DestinationBackend))
	output.WriteString(fmt.Sprintf("Enabled: %v\n\n", status.Enabled))

	output.WriteString("Statistics:\n")
	output.WriteString(fmt.Sprintf("  Total Objects Synced: %d\n", status.TotalObjectsSynced))
	output.WriteString(fmt.Sprintf("  Total Objects Deleted: %d\n", status.TotalObjectsDeleted))
	output.WriteString(fmt.Sprintf("  Total Bytes Synced: %d\n", status.TotalBytesSynced))
	output.WriteString(fmt.Sprintf("  Total Errors: %d\n", status.TotalErrors))
	output.WriteString(fmt.Sprintf("  Sync Count: %d\n", status.SyncCount))

	if !status.LastSyncTime.IsZero() {
		output.WriteString(fmt.Sprintf("  Last Sync: %s\n", status.LastSyncTime.Format(time.RFC3339)))
	}

	if status.AverageSyncDuration > 0 {
		output.WriteString(fmt.Sprintf("  Average Sync Duration: %s\n", status.AverageSyncDuration))
	}

	return output.String()
}

func formatReplicationStatusTable(status *replication.ReplicationStatus) string {
	var output strings.Builder

	output.WriteString("┌────────────────────────────────────────────────────────────────────────────────┐\n")
	output.WriteString("│ Replication Status                                                             │\n")
	output.WriteString("├────────────────────────────────────────────────────────────────────────────────┤\n")
	output.WriteString(fmt.Sprintf("│ Policy ID: %-68s │\n", truncateString(status.PolicyID, 68)))
	output.WriteString(fmt.Sprintf("│ Source: %-71s │\n", truncateString(status.SourceBackend, 71)))
	output.WriteString(fmt.Sprintf("│ Destination: %-67s │\n", truncateString(status.DestinationBackend, 67)))
	enabled := "Yes"
	if !status.Enabled {
		enabled = "No"
	}
	output.WriteString(fmt.Sprintf("│ Enabled: %-70s │\n", enabled))
	output.WriteString("├────────────────────────────────────────────────────────────────────────────────┤\n")
	output.WriteString(fmt.Sprintf("│ Objects Synced: %-63d │\n", status.TotalObjectsSynced))
	output.WriteString(fmt.Sprintf("│ Objects Deleted: %-62d │\n", status.TotalObjectsDeleted))
	output.WriteString(fmt.Sprintf("│ Bytes Synced: %-65d │\n", status.TotalBytesSynced))
	output.WriteString(fmt.Sprintf("│ Total Errors: %-65d │\n", status.TotalErrors))
	output.WriteString(fmt.Sprintf("│ Sync Count: %-67d │\n", status.SyncCount))

	if !status.LastSyncTime.IsZero() {
		output.WriteString(fmt.Sprintf("│ Last Sync: %-68s │\n", status.LastSyncTime.Format("2006-01-02 15:04:05")))
	}

	if status.AverageSyncDuration > 0 {
		output.WriteString(fmt.Sprintf("│ Avg Duration: %-65s │\n", status.AverageSyncDuration.String()))
	}

	output.WriteString("└────────────────────────────────────────────────────────────────────────────────┘\n")
	return output.String()
}

// truncate truncates a string to the specified length, adding "..." if truncated
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}
