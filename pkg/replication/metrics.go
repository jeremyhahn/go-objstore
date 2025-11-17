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

package replication

import (
	"sync/atomic"
	"time"
)

// ReplicationMetrics tracks replication performance and status metrics.
// All fields use atomic operations for thread-safe updates.
type ReplicationMetrics struct {
	// Counters
	totalObjectsSynced  atomic.Int64
	totalObjectsDeleted atomic.Int64
	totalBytesSynced    atomic.Int64
	totalErrors         atomic.Int64

	// Timing
	lastSyncTime        atomic.Int64 // Unix timestamp in nanoseconds
	totalSyncDuration   atomic.Int64 // Total duration in nanoseconds
	syncCount           atomic.Int64 // Number of syncs performed
}

// NewReplicationMetrics creates a new metrics instance.
func NewReplicationMetrics() *ReplicationMetrics {
	return &ReplicationMetrics{}
}

// IncrementObjectsSynced increments the synced objects counter.
func (m *ReplicationMetrics) IncrementObjectsSynced(count int64) {
	m.totalObjectsSynced.Add(count)
}

// IncrementObjectsDeleted increments the deleted objects counter.
func (m *ReplicationMetrics) IncrementObjectsDeleted(count int64) {
	m.totalObjectsDeleted.Add(count)
}

// IncrementBytesSynced increments the bytes synced counter.
func (m *ReplicationMetrics) IncrementBytesSynced(bytes int64) {
	m.totalBytesSynced.Add(bytes)
}

// IncrementErrors increments the error counter.
func (m *ReplicationMetrics) IncrementErrors(count int64) {
	m.totalErrors.Add(count)
}

// RecordSync records the completion of a sync operation.
func (m *ReplicationMetrics) RecordSync(duration time.Duration) {
	m.lastSyncTime.Store(time.Now().UnixNano())
	m.totalSyncDuration.Add(duration.Nanoseconds())
	m.syncCount.Add(1)
}

// GetTotalObjectsSynced returns the total number of objects synced.
func (m *ReplicationMetrics) GetTotalObjectsSynced() int64 {
	return m.totalObjectsSynced.Load()
}

// GetTotalObjectsDeleted returns the total number of objects deleted.
func (m *ReplicationMetrics) GetTotalObjectsDeleted() int64 {
	return m.totalObjectsDeleted.Load()
}

// GetTotalBytesSynced returns the total number of bytes synced.
func (m *ReplicationMetrics) GetTotalBytesSynced() int64 {
	return m.totalBytesSynced.Load()
}

// GetTotalErrors returns the total number of errors.
func (m *ReplicationMetrics) GetTotalErrors() int64 {
	return m.totalErrors.Load()
}

// GetLastSyncTime returns the timestamp of the last sync.
func (m *ReplicationMetrics) GetLastSyncTime() time.Time {
	nanos := m.lastSyncTime.Load()
	if nanos == 0 {
		return time.Time{} // Zero value
	}
	return time.Unix(0, nanos)
}

// GetAverageSyncDuration returns the average duration of sync operations.
func (m *ReplicationMetrics) GetAverageSyncDuration() time.Duration {
	count := m.syncCount.Load()
	if count == 0 {
		return 0
	}
	totalNanos := m.totalSyncDuration.Load()
	return time.Duration(totalNanos / count)
}

// GetMetricsSnapshot returns a snapshot of all metrics.
func (m *ReplicationMetrics) GetMetricsSnapshot() MetricsSnapshot {
	return MetricsSnapshot{
		TotalObjectsSynced:   m.GetTotalObjectsSynced(),
		TotalObjectsDeleted:  m.GetTotalObjectsDeleted(),
		TotalBytesSynced:     m.GetTotalBytesSynced(),
		TotalErrors:          m.GetTotalErrors(),
		LastSyncTime:         m.GetLastSyncTime(),
		AverageSyncDuration:  m.GetAverageSyncDuration(),
		SyncCount:            m.syncCount.Load(),
	}
}

// Reset resets all metrics to zero.
// This should be used with caution as it loses historical data.
func (m *ReplicationMetrics) Reset() {
	m.totalObjectsSynced.Store(0)
	m.totalObjectsDeleted.Store(0)
	m.totalBytesSynced.Store(0)
	m.totalErrors.Store(0)
	m.lastSyncTime.Store(0)
	m.totalSyncDuration.Store(0)
	m.syncCount.Store(0)
}

// MetricsSnapshot represents a point-in-time snapshot of replication metrics.
type MetricsSnapshot struct {
	TotalObjectsSynced   int64         `json:"total_objects_synced"`
	TotalObjectsDeleted  int64         `json:"total_objects_deleted"`
	TotalBytesSynced     int64         `json:"total_bytes_synced"`
	TotalErrors          int64         `json:"total_errors"`
	LastSyncTime         time.Time     `json:"last_sync_time"`
	AverageSyncDuration  time.Duration `json:"average_sync_duration"`
	SyncCount            int64         `json:"sync_count"`
}

// ReplicationStatus contains both policy information and metrics.
type ReplicationStatus struct {
	PolicyID            string        `json:"policy_id"`
	SourceBackend       string        `json:"source_backend"`
	DestinationBackend  string        `json:"destination_backend"`
	Enabled             bool          `json:"enabled"`
	TotalObjectsSynced  int64         `json:"total_objects_synced"`
	TotalObjectsDeleted int64         `json:"total_objects_deleted"`
	TotalBytesSynced    int64         `json:"total_bytes_synced"`
	TotalErrors         int64         `json:"total_errors"`
	LastSyncTime        time.Time     `json:"last_sync_time"`
	AverageSyncDuration time.Duration `json:"average_sync_duration"`
	SyncCount           int64         `json:"sync_count"`
}
