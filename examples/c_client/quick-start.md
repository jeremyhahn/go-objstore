# Quick Start Guide - go-objstore C API

## 5-Minute Quick Start

### 1. Build the Library (one time)

```bash
cd /path/to/go-objstore
make lib
```

### 2. Write Your Code

```c
#include "libobjstore.h"

int main(void) {
    // Configure and create storage
    char *keys[] = {"path"};
    char *values[] = {"/tmp/mydata"};
    int h = ObjstoreNewStorage("local", keys, values, 1);

    // Store data
    ObjstorePut(h, "file.txt", "data", 4);

    // Get data
    char buf[256];
    int len = ObjstoreGet(h, "file.txt", buf, sizeof(buf) - 1);
    if (len >= 0) buf[len] = '\0';  // ObjstoreGet does not NUL-terminate

    // Cleanup
    ObjstoreDelete(h, "file.txt");
    ObjstoreClose(h);
}
```

### 3. Compile

```bash
gcc -I/path/to/go-objstore/examples/c_client -o myapp mycode.c \
    -L/path/to/go-objstore/bin -lobjstore -lpthread -ldl
```

### 4. Run

```bash
LD_LIBRARY_PATH=/path/to/go-objstore/bin:$LD_LIBRARY_PATH ./myapp
```

## API Cheat Sheet

### Create Storage
```c
char *keys[] = {"path"};
char *vals[] = {"/tmp/storage"};
int h = ObjstoreNewStorage("local", keys, vals, 1);
```

### Store Data
```c
ObjstorePut(h, "key", data, data_len);  // Returns 0 or -1
```

### Get Data
```c
char buf[1024];
int n = ObjstoreGet(h, "key", buf, sizeof(buf));  // Returns bytes or -1; not NUL-terminated
```

### Delete
```c
ObjstoreDelete(h, "key");  // Returns 0 or -1
```

### Error Handling
```c
if (result < 0) {
    char *err = ObjstoreGetLastError();
    printf("Error: %s\n", err);
    ObjstoreFreeString(err);
}
```

### Cleanup
```c
ObjstoreClose(h);
```

## Backend Configuration

### Local Storage
```c
char *k[] = {"path"};
char *v[] = {"/var/data"};
```

### Amazon S3
```c
char *k[] = {"region", "bucket"};
char *v[] = {"us-east-1", "my-bucket"};
```

### Google Cloud Storage
```c
char *k[] = {"bucket"};
char *v[] = {"my-gcs-bucket"};
```

### Azure Blob
```c
char *k[] = {"accountName", "containerName"};
char *v[] = {"myaccount", "mycontainer"};
```

A credential never goes in the arrays: a key such as `accessKey`, `secretKey`
or `accountKey` is rejected. Pass it to `ObjstoreNewStorageWithCredentials`
(S3: `key` and `secret`; Azure: account key as `secret`), then wipe your
buffers with `explicit_bzero`. With none, S3 uses the AWS credential chain and
GCS uses Application Default Credentials. `s3`, `gcs` and `azure` exist when the
library is built with the `awss3`, `gcpstorage` and `azureblob` tags (the
`make lib` default); `local` and `memory` always exist.

## Common Patterns

### Store and Verify
```c
if (ObjstorePut(h, key, data, len) != 0) {
    // handle error
}
```

### Safe Get with Size Check
```c
char buf[4096];
int n = ObjstoreGet(h, key, buf, sizeof(buf));
if (n < 0) {
    char *err = ObjstoreGetLastError();
    if (strstr(err, "buffer too small")) {
        // allocate larger buffer
    }
    ObjstoreFreeString(err);
}
```

### Binary Data
```c
unsigned char data[256];
// ... fill data ...
ObjstorePut(h, "bin.dat", (char*)data, 256);
```

## Examples

Run the examples to see working code:

```bash
cd examples/c_client
make example    # Simple demo
make run        # Full test suite
```

## Full Documentation

- **API Reference**: [docs/c_client/README.md](../../docs/c_client/README.md)
- **Detailed Guide**: [README.md](README.md)

## Troubleshooting

### Library not found
```bash
export LD_LIBRARY_PATH=/path/to/go-objstore/bin:$LD_LIBRARY_PATH
```

### Compilation errors
Ensure library is built:
```bash
cd /path/to/go-objstore && make lib
```

### Runtime errors
Check error messages:
```c
char *err = ObjstoreGetLastError();
if (err) {
    fprintf(stderr, "%s\n", err);
    ObjstoreFreeString(err);
}
```

## Best Practices

1. **Always check return values**
2. **Free error strings** with `ObjstoreFreeString()`
3. **Close handles** with `ObjstoreClose()`
4. **Use adequate buffer sizes** for Get operations
5. **Handle errors gracefully**

## Memory Rules

- **You allocate**: Buffers for Get operations
- **You free**: Nothing from Put/Get/Delete
- **You must free**: Strings from `ObjstoreVersion()` and `ObjstoreGetLastError()`
- **Always call**: `ObjstoreClose()` when done

## Need Help?

1. Check `simple_example.c` for minimal working code
2. Check `test_objstore.c` for comprehensive examples
3. Read `README.md` for detailed guide
4. Read [docs/c_client/README.md](../../docs/c_client/README.md) for the full API reference
