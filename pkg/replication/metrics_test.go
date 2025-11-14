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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMetrics_ThreadSafety(t *testing.T) {
	metrics := NewReplicationMetrics()

	// Launch multiple goroutines that update metrics concurrently
	const goroutines = 100
	const iterationsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterationsPerGoroutine; j++ {
				metrics.IncrementObjectsSynced(1)
				metrics.IncrementObjectsDeleted(1)
				metrics.IncrementBytesSynced(1024)
				metrics.IncrementErrors(1)
				metrics.RecordSync(time.Second)
			}
		}()
	}

	wg.Wait()

	// Verify final counts
	expectedCount := int64(goroutines * iterationsPerGoroutine)
	assert.Equal(t, expectedCount, metrics.GetTotalObjectsSynced())
	assert.Equal(t, expectedCount, metrics.GetTotalObjectsDeleted())
	assert.Equal(t, expectedCount*1024, metrics.GetTotalBytesSynced())
	assert.Equal(t, expectedCount, metrics.GetTotalErrors())
}

func TestMetrics_Updates(t *testing.T) {
	metrics := NewReplicationMetrics()

	// Test initial state
	assert.Equal(t, int64(0), metrics.GetTotalObjectsSynced())
	assert.Equal(t, int64(0), metrics.GetTotalObjectsDeleted())
	assert.Equal(t, int64(0), metrics.GetTotalBytesSynced())
	assert.Equal(t, int64(0), metrics.GetTotalErrors())
	assert.True(t, metrics.GetLastSyncTime().IsZero())
	assert.Equal(t, time.Duration(0), metrics.GetAverageSyncDuration())

	// Increment counters
	metrics.IncrementObjectsSynced(5)
	assert.Equal(t, int64(5), metrics.GetTotalObjectsSynced())

	metrics.IncrementObjectsDeleted(3)
	assert.Equal(t, int64(3), metrics.GetTotalObjectsDeleted())

	metrics.IncrementBytesSynced(1024)
	assert.Equal(t, int64(1024), metrics.GetTotalBytesSynced())

	metrics.IncrementErrors(2)
	assert.Equal(t, int64(2), metrics.GetTotalErrors())

	// Record sync
	duration1 := 100 * time.Millisecond
	metrics.RecordSync(duration1)

	assert.False(t, metrics.GetLastSyncTime().IsZero())
	assert.Equal(t, duration1, metrics.GetAverageSyncDuration())

	// Record another sync
	duration2 := 200 * time.Millisecond
	metrics.RecordSync(duration2)

	expectedAvg := (duration1 + duration2) / 2
	assert.Equal(t, expectedAvg, metrics.GetAverageSyncDuration())
}

func TestMetrics_MultipleIncrements(t *testing.T) {
	metrics := NewReplicationMetrics()

	// Increment multiple times
	metrics.IncrementObjectsSynced(10)
	metrics.IncrementObjectsSynced(20)
	metrics.IncrementObjectsSynced(30)

	assert.Equal(t, int64(60), metrics.GetTotalObjectsSynced())

	// Increment bytes
	metrics.IncrementBytesSynced(1000)
	metrics.IncrementBytesSynced(2000)

	assert.Equal(t, int64(3000), metrics.GetTotalBytesSynced())
}

func TestMetrics_RecordSyncDuration(t *testing.T) {
	metrics := NewReplicationMetrics()

	durations := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		300 * time.Millisecond,
		400 * time.Millisecond,
	}

	for _, d := range durations {
		metrics.RecordSync(d)
	}

	// Calculate expected average
	var total time.Duration
	for _, d := range durations {
		total += d
	}
	expectedAvg := total / time.Duration(len(durations))

	assert.Equal(t, expectedAvg, metrics.GetAverageSyncDuration())
}

func TestMetrics_GetMetricsSnapshot(t *testing.T) {
	metrics := NewReplicationMetrics()

	// Set up some metrics
	metrics.IncrementObjectsSynced(100)
	metrics.IncrementObjectsDeleted(20)
	metrics.IncrementBytesSynced(10240)
	metrics.IncrementErrors(5)
	metrics.RecordSync(500 * time.Millisecond)
	metrics.RecordSync(600 * time.Millisecond)

	// Get snapshot
	snapshot := metrics.GetMetricsSnapshot()

	// Verify snapshot
	assert.Equal(t, int64(100), snapshot.TotalObjectsSynced)
	assert.Equal(t, int64(20), snapshot.TotalObjectsDeleted)
	assert.Equal(t, int64(10240), snapshot.TotalBytesSynced)
	assert.Equal(t, int64(5), snapshot.TotalErrors)
	assert.Equal(t, int64(2), snapshot.SyncCount)
	assert.False(t, snapshot.LastSyncTime.IsZero())
	assert.Equal(t, 550*time.Millisecond, snapshot.AverageSyncDuration)
}

func TestMetrics_Reset(t *testing.T) {
	metrics := NewReplicationMetrics()

	// Set up some metrics
	metrics.IncrementObjectsSynced(100)
	metrics.IncrementObjectsDeleted(20)
	metrics.IncrementBytesSynced(10240)
	metrics.IncrementErrors(5)
	metrics.RecordSync(500 * time.Millisecond)

	// Verify metrics are set
	assert.Greater(t, metrics.GetTotalObjectsSynced(), int64(0))

	// Reset
	metrics.Reset()

	// Verify all metrics are zero
	assert.Equal(t, int64(0), metrics.GetTotalObjectsSynced())
	assert.Equal(t, int64(0), metrics.GetTotalObjectsDeleted())
	assert.Equal(t, int64(0), metrics.GetTotalBytesSynced())
	assert.Equal(t, int64(0), metrics.GetTotalErrors())
	assert.True(t, metrics.GetLastSyncTime().IsZero())
	assert.Equal(t, time.Duration(0), metrics.GetAverageSyncDuration())
}

func TestMetrics_ConcurrentReads(t *testing.T) {
	metrics := NewReplicationMetrics()

	// Set up some initial data
	metrics.IncrementObjectsSynced(1000)
	metrics.IncrementBytesSynced(1024000)
	metrics.RecordSync(time.Second)

	// Launch multiple readers
	const readers = 50
	var wg sync.WaitGroup
	wg.Add(readers)

	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			// Read metrics multiple times
			for j := 0; j < 100; j++ {
				_ = metrics.GetTotalObjectsSynced()
				_ = metrics.GetTotalBytesSynced()
				_ = metrics.GetLastSyncTime()
				_ = metrics.GetAverageSyncDuration()
				_ = metrics.GetMetricsSnapshot()
			}
		}()
	}

	wg.Wait()

	// Verify metrics are still consistent
	assert.Equal(t, int64(1000), metrics.GetTotalObjectsSynced())
	assert.Equal(t, int64(1024000), metrics.GetTotalBytesSynced())
}

func TestMetrics_MixedReadWrite(t *testing.T) {
	metrics := NewReplicationMetrics()

	const workers = 20
	var wg sync.WaitGroup
	wg.Add(workers * 2) // writers and readers

	// Writers
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				metrics.IncrementObjectsSynced(1)
				metrics.IncrementBytesSynced(100)
			}
		}()
	}

	// Readers
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = metrics.GetTotalObjectsSynced()
				_ = metrics.GetTotalBytesSynced()
				_ = metrics.GetMetricsSnapshot()
			}
		}()
	}

	wg.Wait()

	// Final values should be correct
	expectedObjects := int64(workers * 100)
	expectedBytes := int64(workers * 100 * 100)

	assert.Equal(t, expectedObjects, metrics.GetTotalObjectsSynced())
	assert.Equal(t, expectedBytes, metrics.GetTotalBytesSynced())
}

func TestMetrics_LastSyncTime(t *testing.T) {
	metrics := NewReplicationMetrics()

	// Initially zero
	assert.True(t, metrics.GetLastSyncTime().IsZero())

	// Record first sync
	before := time.Now()
	time.Sleep(10 * time.Millisecond)
	metrics.RecordSync(100 * time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	after := time.Now()

	lastSync := metrics.GetLastSyncTime()
	assert.False(t, lastSync.IsZero())
	assert.True(t, lastSync.After(before))
	assert.True(t, lastSync.Before(after))

	// Record another sync
	time.Sleep(50 * time.Millisecond)
	before2 := time.Now()
	metrics.RecordSync(200 * time.Millisecond)

	lastSync2 := metrics.GetLastSyncTime()
	assert.True(t, lastSync2.After(lastSync))
	assert.True(t, lastSync2.After(before2) || lastSync2.Equal(before2))
}

func TestMetrics_ZeroDivision(t *testing.T) {
	metrics := NewReplicationMetrics()

	// Getting average with no syncs should return 0, not panic
	assert.Equal(t, time.Duration(0), metrics.GetAverageSyncDuration())

	// Record one sync
	metrics.RecordSync(100 * time.Millisecond)
	assert.Equal(t, 100*time.Millisecond, metrics.GetAverageSyncDuration())
}

func BenchmarkMetrics_IncrementObjectsSynced(b *testing.B) {
	metrics := NewReplicationMetrics()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		metrics.IncrementObjectsSynced(1)
	}
}

func BenchmarkMetrics_GetMetricsSnapshot(b *testing.B) {
	metrics := NewReplicationMetrics()
	metrics.IncrementObjectsSynced(1000)
	metrics.IncrementBytesSynced(1024000)
	metrics.RecordSync(time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = metrics.GetMetricsSnapshot()
	}
}

func BenchmarkMetrics_ConcurrentUpdates(b *testing.B) {
	metrics := NewReplicationMetrics()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			metrics.IncrementObjectsSynced(1)
			metrics.IncrementBytesSynced(1024)
		}
	})
}
