# StorageFS - Filesystem Abstraction

> **Status: NORMATIVE** · Scope: go-objstore · Last verified: 2026-11-12
> Conflicts with this document are defects. Fix them here first.

StorageFS provides a filesystem-like interface on top of any storage backend, allowing you to use familiar file operations with cloud storage.

## Overview

StorageFS wraps any `Storage` backend and provides a filesystem interface similar to the standard `os` package, but works with object storage backends like S3, GCS, and Azure.

## Features

- **Familiar API**: Works like `os` package
- **Standard file operations**: Create, Open, Read, Write, Seek
- **Directory support**: MkdirAll, Remove, RemoveAll
- **Metadata tracking**: File size, modification time, permissions
- **Cross-backend**: Works with any storage backend
- **No external dependencies**: Pure Go implementation

## Quick Start

```go
package main

import (
    "fmt"
    "io"
    "log"

    "github.com/jeremyhahn/go-objstore/pkg/common"
    "github.com/jeremyhahn/go-objstore/pkg/factory"
    "github.com/jeremyhahn/go-objstore/pkg/storagefs"
)

func main() {
    // Create storage backend
    storage, err := factory.NewStorage("local", map[string]string{
        "path": "/tmp/mydata",
    }, common.Credentials{})
    if err != nil {
        log.Fatal(err)
    }

    // Wrap with filesystem interface
    fs := storagefs.New(storage)

    // Now use like a regular filesystem
    file, err := fs.Create("docs/readme.txt")
    if err != nil {
        log.Fatal(err)
    }
    if _, err := file.WriteString("Hello, StorageFS!"); err != nil {
        log.Fatal(err)
    }
    // Close flushes the buffered content to the backend
    if err := file.Close(); err != nil {
        log.Fatal(err)
    }

    // Read it back
    readFile, err := fs.Open("docs/readme.txt")
    if err != nil {
        log.Fatal(err)
    }
    content, err := io.ReadAll(readFile)
    if err != nil {
        log.Fatal(err)
    }
    if err := readFile.Close(); err != nil {
        log.Fatal(err)
    }

    fmt.Println(string(content)) // "Hello, StorageFS!"
}
```

## API Reference

### Creating a Filesystem

```go
// func New(storage common.Storage) *StorageFS
fs := storagefs.New(storage)

name := fs.Name() // the filesystem's name
```

`*StorageFS` implements the `storagefs.Fs` interface (`pkg/storagefs/interfaces.go`); `Create`, `Open` and `OpenFile` return a `storagefs.File`.

### File Operations

#### Create a File

```go
file, err := fs.Create("path/to/file.txt")
defer file.Close()

file.WriteString("content")
```

#### Open a File

```go
// Read-only (OpenFile with os.O_RDONLY)
file, err := fs.Open("path/to/file.txt")

// With specific flags
file, err := fs.OpenFile("path/to/file.txt", os.O_RDWR|os.O_CREATE, 0644)
```

`Create(name)` is `OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)`. Opening a missing file without `os.O_CREATE` returns `os.ErrNotExist`. The content is held in memory while the file is open and is written to the backend only by `Sync` or `Close`, and only when the file was opened with `os.O_WRONLY` or `os.O_RDWR`.

#### Write to File

```go
file, _ := fs.Create("file.txt")
defer file.Close()

// Write bytes
n, err := file.Write([]byte("data"))

// Write string
n, err := file.WriteString("text")

// Write at position
n, err := file.WriteAt([]byte("data"), offset)
```

`Write`, `WriteString`, `WriteAt` and `Truncate` return `os.ErrPermission` on a file opened without `os.O_WRONLY` or `os.O_RDWR`; `Read` and `ReadAt` return `os.ErrPermission` on a file opened `os.O_WRONLY`. Every file operation on a closed file returns `fs.ErrClosed`, and every read, write, seek or truncate on a directory returns `storagefs.ErrIsDirectory`.

#### Read from File

```go
file, _ := fs.Open("file.txt")
defer file.Close()

// Read all
data, err := io.ReadAll(file)

// Read into buffer
buf := make([]byte, 1024)
n, err := file.Read(buf)

// Read at position
n, err := file.ReadAt(buf, offset)
```

#### Seek in File

```go
file, _ := fs.Open("file.txt")
defer file.Close()

// Seek to position
pos, err := file.Seek(100, io.SeekStart)

// Seek relative
pos, err := file.Seek(10, io.SeekCurrent)

// Seek from end
pos, err := file.Seek(-10, io.SeekEnd)
```

`Seek` returns `storagefs.ErrInvalidWhence` for an unknown `whence` and `storagefs.ErrNegativeOffset` when the resulting offset is negative.

### Directory Operations

#### Create Directory

```go
// Create single directory
err := fs.Mkdir("mydir", 0755)

// Create directory path (like mkdir -p)
err := fs.MkdirAll("path/to/nested/dir", 0755)
```

`Mkdir` returns `os.ErrExist` if the directory already exists; it does not check that the parent exists. `MkdirAll` creates each missing component and ignores `os.ErrExist`.

#### Remove Files/Directories

```go
// Remove a file, or a directory's marker and metadata
err := fs.Remove("file.txt")

// Remove every key under a prefix
err := fs.RemoveAll("path/to/dir")
```

`Remove` returns `os.ErrNotExist` when neither a directory marker nor a file exists at the path. On a directory it deletes only the marker and its metadata; it does not check that the directory is empty and does not delete the children. `RemoveAll` always returns `nil`; see [Limitations](#limitations).

### File Information

#### Get File Stats

```go
info, err := fs.Stat("path/to/file.txt")

fmt.Println(info.Name())    // file.txt
fmt.Println(info.Size())    // file size in bytes
fmt.Println(info.Mode())    // file permissions
fmt.Println(info.ModTime()) // modification time
fmt.Println(info.IsDir())   // false

// Stat of an open file
info, err = file.Stat()
```

`fs.Stat` returns `os.ErrNotExist` when the path has no metadata and no file or directory marker. A file or directory that exists without metadata gets default info (mode `0644`, or `os.ModeDir|0755`, size 0, the current time).

#### Check if File Exists

```go
if _, err := fs.Stat("file.txt"); err == nil {
    // file exists
} else if errors.Is(err, os.ErrNotExist) {
    // file doesn't exist
}
```

#### List Directory Contents

```go
// Open directory
dir, err := fs.Open("path/to/directory")
if err != nil {
    log.Fatal(err)
}
defer dir.Close()

// Read all entries at once
entries, err := dir.Readdir(-1)
if err != nil && err != io.EOF {
    log.Fatal(err)
}

for _, entry := range entries {
    fmt.Printf("%s (dir: %v, size: %d)\n",
        entry.Name(), entry.IsDir(), entry.Size())
}
```

#### List Directory Names

```go
dir, err := fs.Open("path/to/directory")
if err != nil {
    log.Fatal(err)
}
defer dir.Close()

// Read just the names
names, err := dir.Readdirnames(-1)
if err != nil && err != io.EOF {
    log.Fatal(err)
}

for _, name := range names {
    fmt.Println(name)
}
```

`Readdir(count int) ([]os.FileInfo, error)` and `Readdirnames(n int) ([]string, error)` list the direct children through the backend's `ListWithOptions` with the `/` delimiter, skipping `.meta/` keys and `.dir` markers. With `count <= 0` they return all remaining entries and a `nil` error. With `count > 0` they return up to `count` entries, and `io.EOF` together with the last batch (unlike `os.File`, which returns `io.EOF` only with an empty slice). On a file that is not a directory they return `storagefs.ErrNotDirectory`.

#### Paginated Directory Reading

```go
dir, err := fs.Open("path/to/directory")
if err != nil {
    log.Fatal(err)
}
defer dir.Close()

// Read 10 entries at a time
for {
    entries, err := dir.Readdir(10)
    if len(entries) == 0 {
        break
    }

    // Process entries
    for _, entry := range entries {
        fmt.Println(entry.Name())
    }

    if err == io.EOF {
        break
    }
    if err != nil {
        log.Fatal(err)
    }
}
```

### File Metadata Operations

#### Change Permissions

```go
err := fs.Chmod("file.txt", 0644)
```

#### Change Modification Time

```go
newTime := time.Now()
err := fs.Chtimes("file.txt", newTime, newTime)
```

#### Rename/Move

```go
err := fs.Rename("old/path.txt", "new/path.txt")
```

`Chmod` and `Chtimes` update the stored metadata only; they return the backend's error when the path has no metadata. `Chtimes` records `mtime` and ignores `atime`. `Chown` always returns `os.ErrInvalid`.

`Rename` returns `os.ErrNotExist` when the source does not exist, and the backend's error when the source has no metadata. It copies the object to the new key and deletes the old one; it does not check whether the destination exists, so it overwrites it. Renaming a directory moves only its `.dir` marker and metadata, not its children.

### Advanced Operations

#### Truncate File

```go
file, _ := fs.OpenFile("file.txt", os.O_RDWR, 0644)
defer file.Close()

err := file.Truncate(100) // Set file size to 100 bytes
```

`Truncate(size int64) error` shrinks the in-memory content or pads it with zeros; the change reaches the backend at `Sync` or `Close`.

#### Sync to Storage

```go
file, _ := fs.Create("file.txt")
file.WriteString("data")
err := file.Sync() // Flush to storage
file.Close()
```

`Sync() error` writes the whole content to the backend with `Put` and updates the size and modification time in the metadata. It does nothing on a directory or on a file opened without `os.O_WRONLY` or `os.O_RDWR`. `Close` does the same flush once; a second `Close` returns `fs.ErrClosed`.

## Usage with Different Backends

### Local Storage

```go
storage, _ := factory.NewStorage("local", map[string]string{
    "path": "/var/data",
}, common.Credentials{})
fs := storagefs.New(storage)
```

### S3

```go
storage, _ := factory.NewStorage("s3", map[string]string{
    "region": "us-east-1",
    "bucket": "my-bucket",
}, common.Credentials{})
fs := storagefs.New(storage)

// Use S3 like a filesystem
fs.MkdirAll("reports/2024", 0755)
file, _ := fs.Create("reports/2024/summary.txt")
file.WriteString("Q4 Summary")
file.Close()
```

### GCS

```go
storage, _ := factory.NewStorage("gcs", map[string]string{
    "bucket": "my-gcs-bucket",
}, common.Credentials{})
fs := storagefs.New(storage)
```

### Azure

```go
storage, _ := factory.NewStorage("azure", map[string]string{
    "accountName":   "myaccount",
    "containerName": "files",
}, common.Credentials{Secret: accountKey}) // accountKey is an enclave.ByteAccess
fs := storagefs.New(storage)
```

## Implementation Details

### Directory Representation

Since object storage is flat, directories are simulated using:
- **Directory markers**: a directory `d` exists when the empty object `d/.dir` exists (the key `<dir>/.dir`, built with `path.Join(dir, ".dir")`)
- **Metadata**: JSON metadata stored under the `.meta/` prefix
- **Path semantics**: Paths are normalized and cleaned

### Metadata Storage

File and directory metadata (size, permissions, modification time) is stored as JSON:
- **Location**: the key `.meta/<path>` (for example `.meta/docs/readme.txt`; directory `d` has `.meta/d`)
- **Format**: a JSON object with the fields `name`, `size`, `mode`, `modTime` and `isDir`

### Path Normalization

Every path passed to `StorageFS` is normalized:
- `""` and `"/"` become `"."`
- Backslashes are converted to forward slashes
- The path is cleaned with `path.Clean` (multiple slashes collapsed, `.` and `..` resolved)
- One leading slash is removed
- An empty result becomes `"."`

## Limitations

1. **No ownership**: `Chown()` always returns `os.ErrInvalid`
2. **No hard links**: Not supported by object storage
3. **No symbolic links**: Not supported by object storage
4. **Permissions**: Stored in metadata but not enforced
5. **RemoveAll**: it deletes keys only through an unexported `listKeys(prefix string) []string` method on the backend, which the built-in backends do not have; on them it deletes nothing. It always returns `nil`, and the match is a plain key prefix
6. **Directory Remove and Rename**: they act on the `.dir` marker and metadata only, not on the children
7. **Whole-object I/O**: an open file holds its whole content in memory; `Sync` and `Close` rewrite the whole object

## Performance Considerations

- Each operation may involve network I/O
- Metadata requires separate storage operations
- Use buffering for better performance
- Batch operations when possible

## Error Handling

StorageFS returns standard `os` errors and its own sentinels; check them with `errors.Is`:

| Error | Returned by |
|-------|-------------|
| `os.ErrNotExist` | `Open`/`OpenFile` of a missing file without `os.O_CREATE`; `Stat`, `Remove` and `Rename` of a missing path |
| `os.ErrExist` | `Mkdir` of an existing directory |
| `os.ErrInvalid` | `Chown`, always |
| `os.ErrPermission` | writes and `Truncate` on a file opened read-only; reads on a file opened `os.O_WRONLY` |
| `fs.ErrClosed` | any `File` method after `Close` |
| `storagefs.ErrIsDirectory` | `Read`, `ReadAt`, `Write`, `WriteAt`, `Seek` and `Truncate` on a directory |
| `storagefs.ErrNotDirectory` | `Readdir` and `Readdirnames` on a file |
| `storagefs.ErrInvalidWhence`, `storagefs.ErrNegativeOffset` | `Seek` |

`Chmod`, `Chtimes`, and `Rename` of a path without metadata return the backend's own error.

```go
file, err := fs.Open("nonexistent.txt")
if errors.Is(err, os.ErrNotExist) {
    // Handle file not found
}

err = fs.Mkdir("existing", 0755)
if errors.Is(err, os.ErrExist) {
    // Handle already exists
}

_, err = dir.Read(buf)
if errors.Is(err, storagefs.ErrIsDirectory) {
    // Handle read on a directory
}
```

## Thread Safety

Each open `File` serialises its own operations with a `sync.Mutex`. `StorageFS` itself holds no lock: it keeps only the backend, so its calls are as safe for concurrent use as the backend is, and the multi-step operations (`Rename`, `RemoveAll`, `Mkdir`, and the data-then-metadata writes of `Sync` and `Close`) are not atomic across the backend. Two `File` values open on the same path do not see each other's writes; the last `Sync` or `Close` wins.

## Complete Example

See [examples/storagefs/main.go](../../examples/storagefs/main.go) for a comprehensive example demonstrating all features.

## See Also

- [Storage Backends](../backends/README.md)
- [Implementation Details](./implementation.md)
