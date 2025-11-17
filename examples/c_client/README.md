# go-objstore C API Client Example

This directory contains a complete example demonstrating how to use the go-objstore library from C code.

## Overview

The go-objstore library can be built as a shared object (.so) library and consumed by C programs. This example demonstrates:

- Creating storage backends
- Storing and retrieving data
- Deleting objects
- Error handling
- Binary data support
- Resource cleanup

## Building the Library

First, build the shared library from the project root:

```bash
cd /path/to/go-objstore
make lib
```

This creates `objstore.so` in `bin/` and `objstore.h` in `examples/c_client/`.

## Building the Example

From this directory:

```bash
make
```

This compiles `test_objstore.c` and links it against the shared library.

## Running the Example

```bash
make run
```

Or manually:

```bash
LD_LIBRARY_PATH=../../bin:$LD_LIBRARY_PATH ./test_objstore
```

## API Reference

### Version Information

```c
char* ObjstoreVersion(void);
```

Returns the library version string. The returned string must be freed using `ObjstoreFreeString()`.

### Error Handling

```c
char* ObjstoreGetLastError(void);
```

Returns the last error message as a string, or NULL if no error occurred. The returned string must be freed using `ObjstoreFreeString()`.

### Storage Operations

#### Create Storage Backend

```c
int ObjstoreNewStorage(
    char* backendType,
    char** settingsKeys,
    char** settingsValues,
    int settingsCount
);
```

Creates a new storage backend. Returns a handle (>= 0) on success, or -1 on error.

Parameters:
- `backendType`: Type of backend ("local", "s3", "gcs", "azure")
- `settingsKeys`: Array of setting key strings
- `settingsValues`: Array of setting value strings
- `settingsCount`: Number of settings in the arrays

Example:
```c
char *keys[] = {"path"};
char *values[] = {"/tmp/storage"};
int handle = ObjstoreNewStorage("local", keys, values, 1);
```

#### Put Data

```c
int ObjstorePut(int handle, char* key, char* data, int dataLen);
```

Stores data in the storage backend. Returns 0 on success, -1 on error.

Parameters:
- `handle`: Storage handle from ObjstoreNewStorage
- `key`: Object key/path
- `data`: Binary data to store
- `dataLen`: Length of data in bytes

#### Get Data

```c
int ObjstoreGet(int handle, char* key, char* buffer, int bufferSize);
```

Retrieves data from the storage backend. Returns the number of bytes read on success, or -1 on error.

Parameters:
- `handle`: Storage handle
- `key`: Object key/path
- `buffer`: Buffer to store retrieved data
- `bufferSize`: Size of the buffer

Note: If the buffer is too small, the function returns -1 and sets an error.

#### Delete Data

```c
int ObjstoreDelete(int handle, char* key);
```

Deletes an object from the storage backend. Returns 0 on success, -1 on error.

Parameters:
- `handle`: Storage handle
- `key`: Object key/path to delete

#### Close Storage

```c
void ObjstoreClose(int handle);
```

Closes the storage backend and releases resources. Always call this when done.

Parameters:
- `handle`: Storage handle to close

### Memory Management

```c
void ObjstoreFreeString(char* str);
```

Frees a string returned by objstore functions (e.g., from `ObjstoreVersion()` or `ObjstoreGetLastError()`).

## Backend Configuration

### Local Storage

Settings:
- `path`: Base directory path for storage (required)
- `runLifecycle`: "true" to enable lifecycle management (optional)

Example:
```c
char *keys[] = {"path"};
char *values[] = {"/tmp/storage"};
int handle = ObjstoreNewStorage("local", keys, values, 1);
```

### Amazon S3

Settings:
- `region`: AWS region (required)
- `bucket`: S3 bucket name (required)
- `endpoint`: Custom endpoint URL (optional, for S3-compatible services)
- `accessKeyId`: AWS access key (optional, uses AWS credentials chain if not provided)
- `secretAccessKey`: AWS secret key (optional)

### Google Cloud Storage

Settings:
- `bucket`: GCS bucket name (required)
- `credentialsJSON`: Service account credentials as JSON string (optional)

### Azure Blob Storage

Settings:
- `accountName`: Azure storage account name (required)
- `accountKey`: Azure storage account key (required)
- `containerName`: Blob container name (required)

## Error Handling Pattern

Always check return values and retrieve error messages:

```c
int result = ObjstorePut(handle, "key", data, len);
if (result != 0) {
    char *err = ObjstoreGetLastError();
    if (err) {
        fprintf(stderr, "Error: %s\n", err);
        ObjstoreFreeString(err);
    }
    return -1;
}
```

## Memory Management

1. All strings returned by objstore functions (version, errors) must be freed with `ObjstoreFreeString()`
2. Always call `ObjstoreClose()` when done with a storage handle
3. Data buffers for Put/Get operations are managed by the caller

## Binary Data

The API fully supports binary data. The `ObjstorePut()` function accepts arbitrary binary data via the `data` parameter and `dataLen` specifies the exact number of bytes.

Example:
```c
unsigned char binary[256];
for (int i = 0; i < 256; i++) {
    binary[i] = (unsigned char)i;
}
ObjstorePut(handle, "binary.dat", (char*)binary, 256);
```

## Thread Safety

The library uses internal locking to protect the storage handle registry. Multiple threads can safely:
- Create different storage backends
- Perform operations on different handles

However, concurrent operations on the same handle should be synchronized by the caller if the underlying backend requires it.

## Testing

The test program includes comprehensive tests:

1. Version check
2. Storage creation
3. Put operation
4. Get operation
5. Multiple puts
6. Delete operation
7. Error handling (invalid handle)
8. Error handling (buffer too small)
9. Binary data handling

Run tests:
```bash
make run
```

## Valgrind Memory Check

To verify there are no memory leaks:

```bash
make valgrind
```

This runs the test program under valgrind with full leak checking enabled.

## Troubleshooting

### Library Not Found

If you get "error while loading shared libraries", ensure `LD_LIBRARY_PATH` is set:

```bash
export LD_LIBRARY_PATH=/path/to/go-objstore/bin:$LD_LIBRARY_PATH
./test_objstore
```

### Compilation Errors

Ensure the library is built first:
```bash
cd ../../
make lib
cd examples/c_client
make
```

### Runtime Errors

Check error messages using `ObjstoreGetLastError()` - they provide detailed information about what went wrong.

## License

Same as go-objstore project.
