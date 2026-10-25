# C API Reference

> **Status: NORMATIVE** · Scope: go-objstore · Last verified: 2026-10-09
> Conflicts with this document are defects. Fix them here first.

Complete reference for using go-objstore from C applications.

## Overview

The go-objstore C API provides access to all storage backends through a simple C interface. The library is compiled as a shared object (.so) file that can be linked with C applications.

## Building the Library

From the project root:

```bash
make lib
```

This creates:
- `bin/libobjstore.so` - Shared library
- `examples/c_client/libobjstore.h` - C header file

## Quick Start

See [examples/c_client/quick-start.md](../../examples/c_client/quick-start.md) for a 5-minute introduction.

For detailed examples, see [examples/c_client/README.md](../../examples/c_client/README.md).

## API Functions

### Version Information

#### ObjstoreVersion

```c
char* ObjstoreVersion(void);
```

Returns the library version string. **Caller must free the returned string** using `ObjstoreFreeString()`.

**Example:**
```c
char *version = ObjstoreVersion();
printf("Version: %s\n", version);
ObjstoreFreeString(version);
```

---

### Storage Management

#### ObjstoreNewStorage

```c
int ObjstoreNewStorage(const char* backend, char** keys, char** values, int num_pairs);
```

Creates a new storage backend and returns a handle.

**Parameters:**
- `backend` - Backend type: "local", "s3", "gcs", "azure", "glacier", "azurearchive"
- `keys` - Array of configuration key strings
- `values` - Array of configuration value strings
- `num_pairs` - Number of key-value pairs

**Returns:**
- Handle ID (>= 0) on success
- -1 on error (check `ObjstoreGetLastError()`)

**Example:**
```c
char *keys[] = {"path"};
char *values[] = {"/tmp/storage"};
int handle = ObjstoreNewStorage("local", keys, values, 1);
if (handle < 0) {
    char *err = ObjstoreGetLastError();
    fprintf(stderr, "Error: %s\n", err);
    ObjstoreFreeString(err);
}
```

A key such as `accessKey`, `secretKey`, `accountKey`, `sasToken` or
`sessionToken` in `keys` is refused. Use `ObjstoreNewStorageWithCredentials`
to pass a credential.

#### ObjstoreNewStorageWithCredentials

```c
int ObjstoreNewStorageWithCredentials(const char* backend, char** keys, char** values,
                                      int num_pairs,
                                      const char* key, int key_len,
                                      const char* secret, int secret_len);
```

Like `ObjstoreNewStorage`, and additionally takes a credential as two byte
buffers with explicit lengths (not NUL-terminated strings). The library copies
each buffer into a protected-memory handle at the call boundary, builds the
backend from the handle, destroys the handle and zeroizes its own copy before
it returns, and keeps no copy of the credential afterwards.

**Parameters:**
- `backend`, `keys`, `values`, `num_pairs` - as for `ObjstoreNewStorage`
- `key`, `key_len` - access key (S3). `NULL` with `0` passes none
- `secret`, `secret_len` - secret key (S3) or account key (Azure). `NULL` with `0` passes none

Passing neither buffer selects the SDK default credential chain. A `NULL`
buffer with a non-zero length, or a negative length, is an error.

**Caller's duty:** the library cannot reach your buffers. Wipe them as soon as
the call returns, with `explicit_bzero` (POSIX) or `SecureZeroMemory`
(Windows), never `memset`, which the compiler may remove. The library receives
the bytes as given, so any copy you made earlier is yours to wipe.

**Example:**
```c
char key[] = "AKIA...";
char secret[] = "...";
char *keys[] = {"region", "bucket"};
char *values[] = {"us-east-1", "my-bucket"};

int h = ObjstoreNewStorageWithCredentials("s3", keys, values, 2,
                                          key, (int)strlen(key),
                                          secret, (int)strlen(secret));
explicit_bzero(key, sizeof key);
explicit_bzero(secret, sizeof secret);
if (h < 0) {
    char *err = ObjstoreGetLastError();
    fprintf(stderr, "Error: %s\n", err);
    ObjstoreFreeString(err);
}
```

#### ObjstoreClose

```c
void ObjstoreClose(int handle);
```

Closes and cleans up a storage handle.

**Parameters:**
- `handle` - Storage handle to close

**Example:**
```c
ObjstoreClose(handle);
```

---

### Data Operations

#### ObjstorePut

```c
int ObjstorePut(int handle, const char* key, char* data, int data_len);
```

Stores data in the storage backend.

**Parameters:**
- `handle` - Storage handle
- `key` - Object key/path
- `data` - Data to store
- `data_len` - Length of data in bytes

**Returns:**
- 0 on success
- -1 on error

**Example:**
```c
const char *data = "Hello, World!";
int result = ObjstorePut(handle, "greeting.txt", (char*)data, strlen(data));
if (result != 0) {
    char *err = ObjstoreGetLastError();
    fprintf(stderr, "Put failed: %s\n", err);
    ObjstoreFreeString(err);
}
```

#### ObjstoreGet

```c
int ObjstoreGet(int handle, const char* key, char* buffer, int buffer_size);
```

Retrieves data from storage into a buffer.

**Parameters:**
- `handle` - Storage handle
- `key` - Object key/path
- `buffer` - Buffer to receive data
- `buffer_size` - Size of buffer in bytes

**Returns:**
- Number of bytes read on success
- -1 on error

**Notes:**
- If data is larger than buffer, error is returned
- Check error message for "buffer too small"
- Buffer is null-terminated if there's space

**Example:**
```c
char buffer[1024];
int bytes = ObjstoreGet(handle, "greeting.txt", buffer, sizeof(buffer));
if (bytes < 0) {
    char *err = ObjstoreGetLastError();
    fprintf(stderr, "Get failed: %s\n", err);
    ObjstoreFreeString(err);
} else {
    printf("Read %d bytes: %s\n", bytes, buffer);
}
```

#### ObjstoreDelete

```c
int ObjstoreDelete(int handle, const char* key);
```

Deletes an object from storage.

**Parameters:**
- `handle` - Storage handle
- `key` - Object key/path

**Returns:**
- 0 on success
- -1 on error

**Example:**
```c
int result = ObjstoreDelete(handle, "old-file.txt");
if (result != 0) {
    char *err = ObjstoreGetLastError();
    fprintf(stderr, "Delete failed: %s\n", err);
    ObjstoreFreeString(err);
}
```

---

### Error Handling

#### ObjstoreGetLastError

```c
char* ObjstoreGetLastError(void);
```

Returns the last error message from the most recent operation.

**Returns:**
- Error message string
- NULL if no error
- **Caller must free the returned string** using `ObjstoreFreeString()`

**Example:**
```c
if (result < 0) {
    char *err = ObjstoreGetLastError();
    if (err != NULL) {
        fprintf(stderr, "Error: %s\n", err);
        ObjstoreFreeString(err);
    }
}
```

---

### Memory Management

#### ObjstoreFreeString

```c
void ObjstoreFreeString(char* str);
```

Frees a string allocated by the library.

**Parameters:**
- `str` - String to free (from `ObjstoreVersion()` or `ObjstoreGetLastError()`)

**Example:**
```c
char *version = ObjstoreVersion();
printf("%s\n", version);
ObjstoreFreeString(version);
```

---

## Backend Configuration

### Local Storage

```c
char *keys[] = {"path"};
char *values[] = {"/var/data"};
int h = ObjstoreNewStorage("local", keys, values, 1);
```

**Configuration:**
- `path` - Directory path for local storage

### Amazon S3

```c
char *keys[] = {"region", "bucket"};
char *values[] = {"us-east-1", "my-bucket"};
int h = ObjstoreNewStorage("s3", keys, values, 2);
```

**Configuration:**
- `region` - AWS region (e.g., "us-east-1")
- `bucket` - S3 bucket name

**Credentials:**
Pass the access key and secret key to `ObjstoreNewStorageWithCredentials` (see
above). When none is passed, the AWS SDK credential chain applies:
1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. AWS credentials file (`~/.aws/credentials`)
3. IAM instance role

### Google Cloud Storage

```c
char *keys[] = {"bucket"};
char *values[] = {"my-gcs-bucket"};
int h = ObjstoreNewStorage("gcs", keys, values, 1);
```

**Configuration:**
- `bucket` - GCS bucket name

**Credentials:**
Uses Google Application Default Credentials:
1. `GOOGLE_APPLICATION_CREDENTIALS` environment variable
2. gcloud CLI credentials
3. Compute Engine service account

### Azure Blob Storage

```c
char *keys[] = {"accountName", "containerName"};
char *values[] = {"myaccount", "mycontainer"};
int h = ObjstoreNewStorage("azure", keys, values, 2);
```

**Configuration:**
- `accountName` - Azure storage account name
- `containerName` - Blob container name

**Credentials:** a credential never travels in the settings arrays. A key such as
`accountKey`, `accessKey` or `secretKey` is rejected with an error. Pass the
account key as the `secret` of `ObjstoreNewStorageWithCredentials` (see above);
Azure reads only the secret and ignores `key`.

---

## Complete Example

```c
#include <stdio.h>
#include <string.h>
#include "objstore.h"

int main(void) {
    // Create storage
    char *keys[] = {"path"};
    char *values[] = {"/tmp/data"};
    int h = ObjstoreNewStorage("local", keys, values, 1);
    if (h < 0) {
        char *err = ObjstoreGetLastError();
        fprintf(stderr, "Error: %s\n", err);
        ObjstoreFreeString(err);
        return 1;
    }

    // Store data
    const char *data = "Important data";
    if (ObjstorePut(h, "file.txt", (char*)data, strlen(data)) != 0) {
        char *err = ObjstoreGetLastError();
        fprintf(stderr, "Put error: %s\n", err);
        ObjstoreFreeString(err);
        ObjstoreClose(h);
        return 1;
    }

    // Retrieve data
    char buffer[256];
    int bytes = ObjstoreGet(h, "file.txt", buffer, sizeof(buffer));
    if (bytes < 0) {
        char *err = ObjstoreGetLastError();
        fprintf(stderr, "Get error: %s\n", err);
        ObjstoreFreeString(err);
        ObjstoreClose(h);
        return 1;
    }
    printf("Retrieved: %s (%d bytes)\n", buffer, bytes);

    // Cleanup
    ObjstoreDelete(h, "file.txt");
    ObjstoreClose(h);
    return 0;
}
```

## Memory Rules

**You allocate:**
- Buffers for `ObjstoreGet()`
- Configuration key/value arrays

**You must free:**
- Strings from `ObjstoreVersion()`
- Strings from `ObjstoreGetLastError()`
- Use `ObjstoreFreeString()` for both

**Library manages:**
- Internal storage state
- Automatically freed on `ObjstoreClose()`

## Error Handling Best Practices

1. **Always check return values**
   ```c
   if (result < 0) {
       // Handle error
   }
   ```

2. **Get error details**
   ```c
   char *err = ObjstoreGetLastError();
   if (err != NULL) {
       fprintf(stderr, "%s\n", err);
       ObjstoreFreeString(err);
   }
   ```

3. **Clean up on errors**
   ```c
   if (handle >= 0) {
       ObjstoreClose(handle);
   }
   ```

## Thread Safety

- **Not thread-safe**: Don't use same handle from multiple threads
- **Safe approach**: Create separate handle per thread
- **Synchronization**: Use mutexes if sharing handles

## Compilation

### Linux

```bash
gcc -o myapp myapp.c /path/to/go-objstore/bin/objstore.so -lpthread -ldl
```

### Running

```bash
export LD_LIBRARY_PATH=/path/to/go-objstore/bin:$LD_LIBRARY_PATH
./myapp
```

## See Also

- [Quick Start Guide](../../examples/c_client/quick-start.md)
- [Detailed Examples](../../examples/c_client/README.md)
- [Storage Backends](../backends/README.md)
