# StorageFS Architecture

> **Status: NORMATIVE** · Scope: go-objstore · Last verified: 2026-11-12
> Conflicts with this document are defects. Fix them here first.

StorageFS provides a filesystem abstraction layer over object storage. It wraps any storage backend with an interface similar to Go's `os` package, making object storage feel like a traditional filesystem.

## Design Goals

The primary goal is enabling existing code that works with filesystems to use object storage with minimal changes. This is particularly useful for:

- Legacy applications expecting filesystem APIs
- Code written against an `os`-like filesystem API
- Tools and libraries built around file operations
- Development workflows that mirror production object storage

## Interface Implementation

StorageFS implements the package's own interfaces (`pkg/storagefs/interfaces.go`), modelled on the `os` package. It does not implement `io/fs.FS` or `io/fs.ReadDirFS`.

### storagefs.Fs
Implemented by `*StorageFS` (`storagefs.New(storage common.Storage) *StorageFS`): `Name`, `Create`, `Open`, `OpenFile`, `Mkdir`, `MkdirAll`, `Remove`, `RemoveAll`, `Rename`, `Stat`, `Chmod`, `Chown`, `Chtimes`.

### storagefs.File
Implemented by `*StorageFile`: `io.Reader`, `io.ReaderAt`, `io.Writer`, `io.WriterAt`, `io.Seeker`, `io.Closer`, plus `Name`, `Stat`, `Sync`, `Truncate`, `WriteString`, `Readdir` and `Readdirnames`.

### Additional Operations
Beyond the standard `io` interfaces, StorageFS adds:
- File creation and writing
- Directory creation and removal
- File deletion
- Metadata access
- Seeking within files

## File Operations

### File Handles
Opening a file returns a `storagefs.File` that mimics `os.File`:
- `Read()` - Read bytes from file
- `Write()` - Write bytes to file
- `Seek()` - Change read/write position
- `Close()` - Finish file operations
- `Stat()` - Get file information
- `Readdir()` / `Readdirnames()` - List a directory's direct children

### Reading Files
Files can be opened for reading like `os.Open()`. The implementation:
- Retrieves the whole object from the backend on open
- Buffers content in memory for seeking
- Releases resources on close
- Supports partial reads

### Writing Files
Files can be created for writing like `os.Create()`. The implementation:
- Buffers the whole content in memory
- Writes it to the backend with one `Put` on `Sync` or `Close`, then writes the metadata
- Supports seeking during write
- Does not write the data and the metadata atomically

## Directory Operations

### Hierarchical Namespace
Object storage is flat, but StorageFS creates a hierarchical view using key delimiters. By convention, "/" separates path components.

### Directory Listing
Listing a directory calls the backend's `ListWithOptions` with the prefix `<dir>/` and the delimiter `/`, so only immediate children are returned. Keys under `.meta/` and `.dir` markers are skipped.

### Creating Directories
Creating directory `d` always writes the empty marker object `d/.dir` and the metadata object `.meta/d`. `Mkdir` returns `os.ErrExist` when the marker already exists.

### Removing Directories
`Remove` on a directory deletes its marker and metadata only; it does not check for or delete the contained objects. `RemoveAll` deletes by key prefix only on a backend that offers an unexported `listKeys` method (the built-in backends do not), and always returns `nil`.

## Path Handling

### Key Translation
Filesystem paths are translated to object keys by `normalizePath`:
- `""` and `"/"` become `"."`
- Backslashes normalize to "/"
- The path is cleaned with `path.Clean`, which resolves "." and ".."
- One leading "/" is removed, so absolute and relative paths give the same key

### Root Prefix
StorageFS has no root prefix: keys are the normalized paths themselves, plus the `.meta/` metadata keys and the `.dir` markers.

## Metadata and File Info

### File Information
The `Stat()` operation returns file information:
- Base name
- Size in bytes
- Last modified time
- Mode bits and permissions
- Whether it's a directory

### Metadata Mapping
StorageFS keeps its own metadata as a JSON object (`name`, `size`, `mode`, `modTime`, `isDir`) at the key `.meta/<path>`. When that object is missing, directory listings fall back to the backend's object size and last-modified time, and `Stat` returns defaults.

## Buffering and Caching

### Read Buffering
Objects are buffered entirely in memory on open to support seeking.

### Write Buffering
Writes buffer in memory until `Sync` or `Close` to enable seeking and avoid partial uploads.

### No Caching
StorageFS does not cache object content across file handle lifetimes. Each open operation fetches fresh data from the backend.

## Concurrency

### Thread Safety
Each open file serialises its own operations with a `sync.Mutex`. `StorageFS` itself holds no lock, only the backend, so concurrent calls are as safe as the backend is; multi-step operations (`Rename`, `RemoveAll`, `Mkdir`) are not atomic across the backend.

### File Handle Isolation
Each file handle maintains its own read/write position and buffer. Two handles on the same logical file do not see each other's writes; the last `Sync` or `Close` wins.

### Consistency Model
The consistency model matches the underlying backend:
- Local backend provides strong consistency
- Cloud backends may have eventual consistency for listings
- A single backend `Put` or `Delete` is atomic; StorageFS operations that make several calls are not

## Limitations

### Not a Full Filesystem
StorageFS provides filesystem-like operations but is not a complete filesystem:
- No POSIX permissions or ownership (modes are stored, not enforced; `Chown` returns `os.ErrInvalid`)
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
- Directories exist only through their `.dir` markers
- Renames are copy-and-delete operations; renaming a directory moves only its marker and metadata

## Use Cases

### Development and Testing
Use local backend with StorageFS during development, then switch to cloud backend for production. Code remains unchanged.

### Migration from Filesystem
Migrate applications from filesystem to object storage incrementally. Replace `os` package calls with StorageFS equivalents.

### Standard Library Integration
A `storagefs.File` is an `io.Reader`, `io.Writer`, `io.Seeker` and `io.Closer`, so it works with `io.Copy`, `io.ReadAll`, `bufio` and similar. StorageFS does not implement `io/fs.FS`, so it cannot be passed to functions that take one.

### Compatibility Layer
Wrap object storage with familiar filesystem semantics for tools that expect file-based input and output.
