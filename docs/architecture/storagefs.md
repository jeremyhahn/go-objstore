# StorageFS Architecture

StorageFS provides a filesystem abstraction layer over object storage. It wraps any storage backend with an interface similar to Go's `os` package, making object storage feel like a traditional filesystem.

## Design Goals

The primary goal is enabling existing code that works with filesystems to use object storage with minimal changes. This is particularly useful for:

- Legacy applications expecting filesystem APIs
- Standard library functions that take `fs.FS` interfaces
- Tools and libraries built around file operations
- Development workflows that mirror production object storage

## Interface Implementation

StorageFS implements several Go standard library interfaces:

### fs.FS
The core filesystem interface from Go 1.16+. Provides `Open()` method for reading files.

### fs.ReadDirFS
Extends `fs.FS` with directory listing via `ReadDir()`.

### Additional Operations
Beyond standard interfaces, StorageFS adds:
- File creation and writing
- Directory creation and removal
- File deletion
- Metadata access
- Seeking within files

## File Operations

### File Handles
Opening a file returns a `File` interface that mimics `os.File`:
- `Read()` - Read bytes from file
- `Write()` - Write bytes to file
- `Seek()` - Change read/write position
- `Close()` - Finish file operations
- `Stat()` - Get file information

### Reading Files
Files can be opened for reading like `os.Open()`. The implementation:
- Retrieves object from backend on open
- Buffers content for seeking
- Releases resources on close
- Supports partial reads

### Writing Files
Files can be created for writing like `os.Create()`. The implementation:
- Buffers writes in memory or on disk
- Flushes to backend on close
- Supports seeking during write
- Atomic uploads when possible

## Directory Operations

### Hierarchical Namespace
Object storage is flat, but StorageFS creates a hierarchical view using key delimiters. By convention, "/" separates path components.

### Directory Listing
Listing a directory queries the backend for objects with a matching prefix. Results can be filtered to show only immediate children, mimicking directory semantics.

### Creating Directories
Creating a directory may be a no-op or create a marker object, depending on the backend. Some backends require explicit directory markers, while others infer structure from object keys.

### Removing Directories
Removing a directory may require deleting all contained objects. Non-recursive removal fails if the directory contains objects.

## Path Handling

### Key Translation
Filesystem paths are translated to object keys:
- Absolute paths use the full path as key
- Relative paths resolve against a root prefix
- Path separators normalize to "/"
- Special paths like "." and ".." are handled

### Root Prefix
StorageFS can be rooted at a specific prefix in the backend. This allows multiple filesystems to share a backend without collisions.

## Metadata and File Info

### File Information
The `Stat()` operation returns file information:
- Name and full path
- Size in bytes
- Last modified time
- Mode bits and permissions
- Whether it's a directory

### Metadata Mapping
Backend metadata maps to filesystem concepts:
- Content-Type becomes file extension hint
- ETag provides version information
- Custom metadata accessible through extended APIs

## Buffering and Caching

### Read Buffering
Objects are typically buffered on read to support seeking. Small objects buffer entirely in memory, while large objects may use temporary disk storage.

### Write Buffering
Writes buffer until close to enable seeking and avoid partial uploads. Buffer strategy depends on object size and available resources.

### No Caching
StorageFS does not cache object content across file handle lifetimes. Each open operation fetches fresh data from the backend.

## Concurrency

### Thread Safety
Multiple goroutines can safely use the same StorageFS instance. Concurrent operations on different files work independently.

### File Handle Isolation
Each file handle maintains its own read/write position and buffer. Concurrent operations on the same logical file use separate handles and see a consistent view.

### Consistency Model
The consistency model matches the underlying backend:
- Local backend provides strong consistency
- Cloud backends may have eventual consistency for listings
- Individual file operations are atomic

## Limitations

### Not a Full Filesystem
StorageFS provides filesystem-like operations but is not a complete filesystem:
- No POSIX permissions or ownership
- No symbolic links or hard links
- No file locking or advisory locks
- Limited support for extended attributes
- No change notifications or watchers

### Performance Characteristics
Object storage has different performance than local filesystem:
- Higher latency per operation
- Excellent throughput for large objects
- Listing can be slow with many objects
- No random access without full download

### Backend Constraints
Some operations map poorly to object storage:
- Appending to files requires rewriting
- Seeking during write may not be efficient
- Directory markers may be implicit
- Renames are copy-and-delete operations

## Use Cases

### Development and Testing
Use local backend with StorageFS during development, then switch to cloud backend for production. Code remains unchanged.

### Migration from Filesystem
Migrate applications from filesystem to object storage incrementally. Replace `os` package calls with StorageFS equivalents.

### Standard Library Integration
Pass StorageFS to functions expecting `fs.FS` interface. Many standard library functions work with any filesystem implementation.

### Compatibility Layer
Wrap object storage with familiar filesystem semantics for tools that expect file-based input and output.
