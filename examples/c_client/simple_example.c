/*
 * simple_example.c - Minimal example of using go-objstore from C
 *
 * This is a bare-bones example showing the basic workflow:
 * 1. Create storage backend
 * 2. Put data
 * 3. Get data
 * 4. Delete data
 * 5. Cleanup
 *
 * Compile:
 *   gcc -o simple_example simple_example.c ../../bin/objstore.so -lpthread -ldl
 *
 * Run:
 *   LD_LIBRARY_PATH=../../bin:$LD_LIBRARY_PATH ./simple_example
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "libobjstore.h"

int main(void) {
    int handle;
    char buffer[256];
    int result;

    printf("go-objstore Simple Example\n");
    printf("============================\n\n");

    /* Create local storage */
    printf("1. Creating local storage backend...\n");
    char *keys[] = {"path"};
    char *values[] = {"/tmp/simple_objstore"};

    handle = ObjstoreNewStorage("local", keys, values, 1);
    if (handle < 0) {
        char *err = ObjstoreGetLastError();
        fprintf(stderr, "ERROR: Failed to create storage: %s\n", err);
        ObjstoreFreeString(err);
        return 1;
    }
    printf("   Storage created successfully (handle: %d)\n\n", handle);

    /* Store some data */
    printf("2. Storing data...\n");
    const char *message = "Hello from C!";
    result = ObjstorePut(handle, "message.txt", (char*)message, strlen(message));
    if (result != 0) {
        char *err = ObjstoreGetLastError();
        fprintf(stderr, "ERROR: Put failed: %s\n", err);
        ObjstoreFreeString(err);
        ObjstoreClose(handle);
        return 1;
    }
    printf("   Stored: '%s'\n\n", message);

    /* Retrieve the data */
    printf("3. Retrieving data...\n");
    memset(buffer, 0, sizeof(buffer));
    result = ObjstoreGet(handle, "message.txt", buffer, sizeof(buffer));
    if (result < 0) {
        char *err = ObjstoreGetLastError();
        fprintf(stderr, "ERROR: Get failed: %s\n", err);
        ObjstoreFreeString(err);
        ObjstoreClose(handle);
        return 1;
    }
    printf("   Retrieved %d bytes: '%s'\n\n", result, buffer);

    /* Delete the data */
    printf("4. Deleting data...\n");
    result = ObjstoreDelete(handle, "message.txt");
    if (result != 0) {
        char *err = ObjstoreGetLastError();
        fprintf(stderr, "ERROR: Delete failed: %s\n", err);
        ObjstoreFreeString(err);
        ObjstoreClose(handle);
        return 1;
    }
    printf("   Deleted successfully\n\n");

    /* Cleanup */
    printf("5. Cleaning up...\n");
    ObjstoreClose(handle);
    printf("   Done!\n\n");

    return 0;
}
