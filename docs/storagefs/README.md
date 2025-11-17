# StorageFS - Filesystem Abstraction

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
    "io"
    "log"
    "go-objstore/pkg/factory"
    "go-objstore/pkg/storagefs"
)

func main() {
    // Create storage backend
    storage, err := factory.NewStorage("local", map[string]string{
        "path": "/tmp/mydata",
    })
    if err != nil {
        log.Fatal(err)
    }

    // Wrap with filesystem interface
    fs := storagefs.New(storage)

    // Now use like a regular filesystem
    file, _ := fs.Create("docs/readme.txt")
    file.WriteString("Hello, StorageFS!")
    file.Close()

    // Read it back
    readFile, _ := fs.Open("docs/readme.txt")
    content, _ := io.ReadAll(readFile)
    readFile.Close()

    println(string(content)) // "Hello, StorageFS!"
}
```

## API Reference

### Creating a Filesystem

```go
fs := storagefs.New(storage)
```

### File Operations

#### Create a File

```go
file, err := fs.Create("path/to/file.txt")
defer file.Close()

file.WriteString("content")
```

#### Open a File

```go
// Read-only
file, err := fs.Open("path/to/file.txt")

// With specific flags
file, err := fs.OpenFile("path/to/file.txt", os.O_RDWR|os.O_CREATE, 0644)
```

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

### Directory Operations

#### Create Directory

```go
// Create single directory
err := fs.Mkdir("mydir", 0755)

// Create directory path (like mkdir -p)
err := fs.MkdirAll("path/to/nested/dir", 0755)
```

#### Remove Files/Directories

```go
// Remove file or empty directory
err := fs.Remove("file.txt")

// Remove recursively (like rm -rf)
err := fs.RemoveAll("path/to/dir")
```

### File Information

#### Get File Stats

```go
info, err := fs.Stat("path/to/file.txt")

fmt.Println(info.Name())    // file.txt
fmt.Println(info.Size())    // file size in bytes
fmt.Println(info.Mode())    // file permissions
fmt.Println(info.ModTime()) // modification time
fmt.Println(info.IsDir())   // false
```

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

### Advanced Operations

#### Truncate File

```go
file, _ := fs.OpenFile("file.txt", os.O_RDWR, 0644)
defer file.Close()

err := file.Truncate(100) // Set file size to 100 bytes
```

#### Sync to Storage

```go
file, _ := fs.Create("file.txt")
file.WriteString("data")
err := file.Sync() // Flush to storage
file.Close()
```

## Usage with Different Backends

### Local Storage

```go
storage, _ := factory.NewStorage("local", map[string]string{
    "path": "/var/data",
})
fs := storagefs.New(storage)
```

### S3

```go
storage, _ := factory.NewStorage("s3", map[string]string{
    "region": "us-east-1",
    "bucket": "my-bucket",
})
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
})
fs := storagefs.New(storage)
```

### Azure

```go
storage, _ := factory.NewStorage("azure", map[string]string{
    "accountName":   "myaccount",
    "accountKey":    "key==",
    "containerName": "files",
})
fs := storagefs.New(storage)
```

## Implementation Details

### Directory Representation

Since object storage is flat, directories are simulated using:
- **Directory markers**: Empty `.dir` files mark directories
- **Metadata**: JSON metadata stored in `.meta/` prefix
- **Path semantics**: Paths are normalized and cleaned

### Metadata Storage

File metadata (size, permissions, modification time) is stored as JSON:
- **Location**: `.meta/{path}` in storage
- **Format**: JSON with name, size, mode, modTime, isDir

### Path Normalization

All paths are normalized:
- Leading slashes removed
- Multiple slashes collapsed
- Empty path treated as "."
- Windows backslashes converted to forward slashes

## Limitations

1. **No ownership**: Chown() returns `os.ErrInvalid`
2. **No hard links**: Not supported by object storage
3. **No symbolic links**: Not supported by object storage
4. **Permissions**: Stored but not enforced by storage backends

## Performance Considerations

- Each operation may involve network I/O
- Metadata requires separate storage operations
- Use buffering for better performance
- Batch operations when possible

## Error Handling

StorageFS uses standard `os` package errors:

```go
file, err := fs.Open("nonexistent.txt")
if errors.Is(err, os.ErrNotExist) {
    // Handle file not found
}

err = fs.Mkdir("existing", 0755)
if errors.Is(err, os.ErrExist) {
    // Handle already exists
}
```

## Thread Safety

StorageFS operations are thread-safe when the underlying storage backend is thread-safe (all built-in backends are).

## Complete Example

See [examples/storagefs/main.go](../../examples/storagefs/main.go) for a comprehensive example demonstrating all features.

## See Also

- [Storage Backends](../backends/README.md)
- [Implementation Details](./implementation.md)
