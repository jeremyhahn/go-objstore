# Filesystem Watcher for Real-Time Replication

## Overview

The filesystem watcher provides real-time monitoring of local filesystem changes using [fsnotify](https://github.com/fsnotify/fsnotify) for cross-platform compatibility. It enables immediate replication of changes instead of relying solely on periodic polling.

## Features

- **Cross-platform**: Works on Linux, macOS, Windows, BSD via fsnotify
- **Recursive watching**: Automatically watches subdirectories
- **Debouncing**: Prevents duplicate events for rapid file modifications
- **Event filtering**: Ignores hidden files, metadata files, and temporary files
- **Thread-safe**: Safe for concurrent access
- **Resource cleanup**: Proper goroutine and channel management
- **Configurable**: Adjustable debounce delay and event buffer size

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ FileSystemWatcher Interface                                 │
│ - Watch(path) error                                         │
│ - Stop() error                                              │
│ - Events() <-chan FileSystemEvent                           │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ FSNotifyWatcher Implementation                              │
│                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐ │
│  │   fsnotify   │───>│ Event Filter │───>│   Debouncer  │ │
│  │   Watcher    │    │ & Converter  │    │              │ │
│  └──────────────┘    └──────────────┘    └──────────────┘ │
│                            │                       │        │
│                            ↓                       ↓        │
│  ┌──────────────────────────────────────────────────────┐  │
│  │         Event Channel (buffered)                     │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Usage

### Basic Usage

```go
import (
    "github.com/jeremyhahn/go-objstore/pkg/adapters"
    "github.com/jeremyhahn/go-objstore/pkg/replication"
)

// Create watcher
watcher, err := replication.NewFSNotifyWatcher(replication.FSNotifyWatcherConfig{
    Logger:        adapters.NewNoOpLogger(),
    DebounceDelay: 100 * time.Millisecond,
    EventBuffer:   100,
})
if err != nil {
    log.Fatal(err)
}
defer watcher.Stop()

// Start watching
if err := watcher.Watch("/path/to/watch"); err != nil {
    log.Fatal(err)
}

// Process events
for event := range watcher.Events() {
    fmt.Printf("%s: %s\n", event.Operation, event.Path)
}
```

### Integration with Replication

```go
// Create watcher for source directory
watcher, _ := replication.NewFSNotifyWatcher(replication.FSNotifyWatcherConfig{
    Logger:        logger,
    DebounceDelay: 200 * time.Millisecond,
})
defer watcher.Stop()

watcher.Watch(sourceDirectory)

// Process events and trigger immediate replication
for event := range watcher.Events() {
    switch event.Operation {
    case "put":
        // File created or modified
        replicationManager.SyncObject(ctx, event.Path)

    case "delete":
        // File deleted
        destinationBackend.Delete(event.Path)
    }
}
```

### Recursive Directory Watching

The watcher automatically handles recursive directory watching:

```go
watcher.Watch("/data")  // Watches /data and all subdirectories

// Creating new subdirectory while watching
os.MkdirAll("/data/newdir/nested", 0755)
// /data/newdir and /data/newdir/nested are now also watched

// Files in nested directories trigger events
os.WriteFile("/data/newdir/nested/file.txt", []byte("data"), 0644)
// Event will be received
```

## Configuration

### FSNotifyWatcherConfig

```go
type FSNotifyWatcherConfig struct {
    // Logger for debugging and monitoring
    Logger adapters.Logger

    // DebounceDelay prevents duplicate events for the same file
    // Default: 100ms
    DebounceDelay time.Duration

    // EventBuffer size for the event channel
    // Default: 100
    EventBuffer int
}
```

### Recommended Settings

**High-frequency changes (development)**:
```go
FSNotifyWatcherConfig{
    DebounceDelay: 50 * time.Millisecond,
    EventBuffer:   200,
}
```

**Production replication**:
```go
FSNotifyWatcherConfig{
    DebounceDelay: 200 * time.Millisecond,
    EventBuffer:   100,
}
```

**Low-resource environments**:
```go
FSNotifyWatcherConfig{
    DebounceDelay: 500 * time.Millisecond,
    EventBuffer:   50,
}
```

## Event Types

### FileSystemEvent

```go
type FileSystemEvent struct {
    Path      string    // Full path to the file
    Operation string    // "put" or "delete"
    Timestamp time.Time // When the event occurred
}
```

### Operation Mapping

| fsnotify Event | Watcher Operation | Description |
|---------------|-------------------|-------------|
| CREATE        | put               | File created |
| WRITE         | put               | File modified |
| REMOVE        | delete            | File removed |
| RENAME        | delete            | File renamed (old name) |
| CHMOD         | (ignored)         | Permission change |

## Filtering

The watcher automatically ignores:

- **Hidden files**: Files starting with `.`
- **Metadata files**: Files ending with `.metadata.json`
- **Temporary files**: Files ending with `~` or `.tmp`

Example:
```bash
/data/file.txt              # Watched
/data/.hidden              # Ignored
/data/file.metadata.json   # Ignored
/data/backup~              # Ignored
/data/temp.tmp             # Ignored
```

## Debouncing

Debouncing prevents duplicate events when a file is modified multiple times in quick succession:

```go
// With 200ms debounce:
os.WriteFile("file.txt", []byte("v1"), 0644)  // t=0ms    -> Event emitted
os.WriteFile("file.txt", []byte("v2"), 0644)  // t=50ms   -> Ignored (debounced)
os.WriteFile("file.txt", []byte("v3"), 0644)  // t=100ms  -> Ignored (debounced)
os.WriteFile("file.txt", []byte("v4"), 0644)  // t=250ms  -> Event emitted
```

## Error Handling

### WatcherError

The watcher returns detailed errors with context:

```go
type WatcherError struct {
    Op   string  // Operation: "watch", "walk", etc.
    Path string  // Path that caused the error
    Err  error   // Underlying error
}

func (e *WatcherError) Error() string
func (e *WatcherError) Unwrap() error
```

Example:
```go
err := watcher.Watch("/nonexistent")
if errors.Is(err, ErrWatcherStopped) {
    // Watcher was stopped
} else if watcherErr, ok := err.(*WatcherError); ok {
    fmt.Printf("Failed to watch %s: %v\n", watcherErr.Path, watcherErr.Err)
}
```

## Resource Management

### Proper Cleanup

Always call `Stop()` to clean up resources:

```go
watcher, err := replication.NewFSNotifyWatcher(config)
if err != nil {
    return err
}
defer watcher.Stop()  // Essential for cleanup

// Use watcher...
```

### Stopping the Watcher

```go
// Stop is idempotent and thread-safe
watcher.Stop()  // Stops watching, closes channels
watcher.Stop()  // Safe to call multiple times

// Events channel will be closed
for event := range watcher.Events() {
    // Will exit when watcher stops
}
```

## Performance Considerations

### Memory Usage

- Each watched directory consumes a file descriptor
- Event buffer size affects memory: `EventBuffer * sizeof(FileSystemEvent)`
- Debounce map grows with unique paths accessed

### CPU Usage

- Event processing is asynchronous (dedicated goroutine)
- Debouncing reduces CPU load for frequent changes
- Filtering happens before channel send (minimal overhead)

### Scalability

| Directories Watched | File Descriptors | Memory (approx) |
|--------------------|------------------|-----------------|
| 100                | ~100             | 1-2 MB          |
| 1,000              | ~1,000           | 10-20 MB        |
| 10,000             | ~10,000          | 100-200 MB      |

## Integration Points

### 1. Replication Manager

Add watcher-based immediate sync as an alternative to ticker-based polling:

```go
type PersistentReplicationManager struct {
    // Existing fields...
    watcher FileSystemWatcher
}

func (prm *PersistentReplicationManager) EnableRealtimeSync(sourceDir string) error {
    watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
        Logger: prm.logger,
    })
    if err != nil {
        return err
    }

    prm.watcher = watcher

    go prm.processWatcherEvents()

    return watcher.Watch(sourceDir)
}

func (prm *PersistentReplicationManager) processWatcherEvents() {
    for event := range prm.watcher.Events() {
        // Trigger immediate sync for changed file
        prm.SyncObject(context.Background(), event.Path)
    }
}
```

### 2. Change Log

Record watcher events to the change log for incremental sync:

```go
for event := range watcher.Events() {
    changeLog.RecordChange(ChangeEvent{
        Key:       event.Path,
        Operation: event.Operation,
        Timestamp: event.Timestamp,
    })
}
```

### 3. Hybrid Approach

Combine watcher-based immediate sync with ticker-based full scans:

```go
// Immediate sync for detected changes
go processWatcherEvents(watcher, replicationManager)

// Periodic full scan as backup (detect missed events)
ticker := time.NewTicker(1 * time.Hour)
go func() {
    for range ticker.C {
        replicationManager.SyncAll(context.Background())
    }
}()
```

## Testing

### Test Coverage

The watcher implementation includes comprehensive tests with **88%+ coverage**:

```bash
go test -tags=local -run "^Test.*Watcher" -v ./pkg/replication
```

### Test Categories

- **Basic operations**: Watch, Stop, Events
- **Event types**: Create, Write, Delete, Rename
- **Recursive watching**: Subdirectories, new directories
- **Debouncing**: Rapid modifications
- **Filtering**: Hidden files, metadata, temp files
- **Edge cases**: Stopped watcher, multiple watchers, same path
- **Error handling**: WatcherError types

### Benchmarks

```bash
go test -tags=local -bench="^BenchmarkFSNotifyWatcher" -benchmem ./pkg/replication
```

Results on Intel Core Ultra 9 285K:
- Single file operations: ~50ms/op, 9 allocs/op
- Many files (1000+): Scales linearly

## Limitations

### Platform Differences

- **Linux (inotify)**: Excellent performance, no limits with proper config
- **macOS (FSEvents/kqueue)**: Good performance, may have file descriptor limits
- **Windows**: Works but may be slower for large directory trees

### Known Limitations

1. **Events may be coalesced**: Multiple rapid changes might produce one event
2. **Rename detection**: Rename appears as delete + create
3. **Move operations**: Moving files between watched directories produces delete + create
4. **Network filesystems**: May not reliably generate events (NFS, SMB)

### Workarounds

**For network filesystems**:
Use ticker-based polling as primary mechanism, watcher as optimization.

**For rename tracking**:
Implement higher-level rename detection by correlating delete/create events.

## Troubleshooting

### Events Not Received

1. Check if path exists: `os.Stat(path)`
2. Verify watcher is started: Check `Watch()` error
3. Check file descriptor limits: `ulimit -n` (Linux/macOS)
4. Ensure proper cleanup: Always call `Stop()`

### Too Many Events

1. Increase debounce delay
2. Improve filtering logic
3. Reduce watched directory scope
4. Use change log batching

### Performance Issues

1. Reduce recursive depth if possible
2. Increase event buffer size
3. Process events asynchronously
4. Consider batching replication operations

## References

- [fsnotify Documentation](https://pkg.go.dev/github.com/fsnotify/fsnotify)
- [Example Code](../../pkg/replication/watcher_example_test.go)
