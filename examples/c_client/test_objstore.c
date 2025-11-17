/*
 * test_objstore.c - Test program demonstrating go-objstore C API usage
 *
 * This program demonstrates:
 * 1. Creating a local storage backend
 * 2. Putting data into storage
 * 3. Getting data from storage
 * 4. Deleting data from storage
 * 5. Proper error handling
 * 6. Resource cleanup
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include "libobjstore.h"

/* ANSI color codes for pretty output */
#define COLOR_GREEN "\x1b[32m"
#define COLOR_RED "\x1b[31m"
#define COLOR_YELLOW "\x1b[33m"
#define COLOR_BLUE "\x1b[34m"
#define COLOR_RESET "\x1b[0m"

/* Helper macros for printing test results */
#define PRINT_TEST(name) printf(COLOR_BLUE "[TEST] %s" COLOR_RESET "\n", name)
#define PRINT_PASS(msg) printf(COLOR_GREEN "[PASS] %s" COLOR_RESET "\n", msg)
#define PRINT_FAIL(msg) printf(COLOR_RED "[FAIL] %s" COLOR_RESET "\n", msg)
#define PRINT_INFO(msg) printf(COLOR_YELLOW "[INFO] %s" COLOR_RESET "\n", msg)

/* Helper function to print last error from objstore */
static void print_objstore_error(const char *context) {
    char *err = ObjstoreGetLastError();
    if (err) {
        PRINT_FAIL(context);
        printf("       Error: %s\n", err);
        ObjstoreFreeString(err);
    } else {
        PRINT_FAIL(context);
        printf("       Error: Unknown error (no error message available)\n");
    }
}

/* Helper function to create temporary directory for testing */
static int create_temp_dir(const char *path) {
    struct stat st = {0};

    if (stat(path, &st) == -1) {
        if (mkdir(path, 0755) != 0) {
            return -1;
        }
    }
    return 0;
}

/* Helper function to cleanup test directory */
static void cleanup_temp_dir(const char *path) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", path);
    system(cmd);
}

/* Test 1: Version check */
static int test_version(void) {
    PRINT_TEST("Version Check");

    char *version = ObjstoreVersion();
    if (version == NULL) {
        PRINT_FAIL("Failed to get version");
        return -1;
    }

    printf("       Version: %s\n", version);
    ObjstoreFreeString(version);
    PRINT_PASS("Version check successful");
    return 0;
}

/* Test 2: Create local storage backend */
static int test_create_storage(int *handle_out) {
    PRINT_TEST("Create Local Storage Backend");

    const char *temp_dir = "/tmp/objstore_test";

    /* Create temporary directory */
    if (create_temp_dir(temp_dir) != 0) {
        PRINT_FAIL("Failed to create temporary directory");
        return -1;
    }

    /* Configure local storage */
    char *keys[] = {"path"};
    char *values[] = {(char *)temp_dir};

    int handle = ObjstoreNewStorage("local", keys, values, 1);
    if (handle < 0) {
        print_objstore_error("Failed to create storage backend");
        cleanup_temp_dir(temp_dir);
        return -1;
    }

    printf("       Storage handle: %d\n", handle);
    printf("       Base path: %s\n", temp_dir);
    *handle_out = handle;

    PRINT_PASS("Storage backend created successfully");
    return 0;
}

/* Test 3: Put operation */
static int test_put_operation(int handle) {
    PRINT_TEST("Put Operation");

    const char *key = "test/file1.txt";
    const char *data = "Hello, World! This is test data from C.";
    int data_len = strlen(data);

    int result = ObjstorePut(handle, (char *)key, (char *)data, data_len);
    if (result != 0) {
        print_objstore_error("Failed to put data");
        return -1;
    }

    printf("       Key: %s\n", key);
    printf("       Data length: %d bytes\n", data_len);
    PRINT_PASS("Put operation successful");
    return 0;
}

/* Test 4: Get operation */
static int test_get_operation(int handle) {
    PRINT_TEST("Get Operation");

    const char *key = "test/file1.txt";
    const char *expected_data = "Hello, World! This is test data from C.";
    char buffer[1024];

    memset(buffer, 0, sizeof(buffer));

    int bytes_read = ObjstoreGet(handle, (char *)key, buffer, sizeof(buffer));
    if (bytes_read < 0) {
        print_objstore_error("Failed to get data");
        return -1;
    }

    printf("       Key: %s\n", key);
    printf("       Bytes read: %d\n", bytes_read);
    printf("       Data: %s\n", buffer);

    /* Verify data matches */
    if (strcmp(buffer, expected_data) != 0) {
        PRINT_FAIL("Data mismatch");
        printf("       Expected: %s\n", expected_data);
        printf("       Got: %s\n", buffer);
        return -1;
    }

    PRINT_PASS("Get operation successful");
    return 0;
}

/* Test 5: Put multiple objects */
static int test_multiple_puts(int handle) {
    PRINT_TEST("Multiple Put Operations");

    const char *keys[] = {
        "data/file2.txt",
        "data/file3.txt",
        "documents/file4.txt"
    };
    const char *data[] = {
        "Content for file 2",
        "Content for file 3",
        "Content for file 4"
    };
    int num_files = 3;

    for (int i = 0; i < num_files; i++) {
        int result = ObjstorePut(handle, (char *)keys[i], (char *)data[i], strlen(data[i]));
        if (result != 0) {
            printf("       Failed on file %d: %s\n", i + 1, keys[i]);
            print_objstore_error("Put operation failed");
            return -1;
        }
        printf("       Stored: %s\n", keys[i]);
    }

    PRINT_PASS("Multiple put operations successful");
    return 0;
}

/* Test 6: Delete operation */
static int test_delete_operation(int handle) {
    PRINT_TEST("Delete Operation");

    const char *key = "test/file1.txt";

    /* First verify file exists */
    char buffer[256];
    int bytes_read = ObjstoreGet(handle, (char *)key, buffer, sizeof(buffer));
    if (bytes_read < 0) {
        print_objstore_error("File does not exist before delete");
        return -1;
    }
    printf("       File exists (size: %d bytes)\n", bytes_read);

    /* Delete the file */
    int result = ObjstoreDelete(handle, (char *)key);
    if (result != 0) {
        print_objstore_error("Failed to delete file");
        return -1;
    }
    printf("       Key deleted: %s\n", key);

    /* Verify file no longer exists */
    bytes_read = ObjstoreGet(handle, (char *)key, buffer, sizeof(buffer));
    if (bytes_read >= 0) {
        PRINT_FAIL("File still exists after delete");
        return -1;
    }
    printf("       Verified: file no longer accessible\n");

    PRINT_PASS("Delete operation successful");
    return 0;
}

/* Test 7: Error handling - invalid handle */
static int test_error_handling_invalid_handle(void) {
    PRINT_TEST("Error Handling - Invalid Handle");

    const char *key = "test/nonexistent.txt";
    const char *data = "test data";

    /* Use an invalid handle */
    int result = ObjstorePut(999999, (char *)key, (char *)data, strlen(data));
    if (result == 0) {
        PRINT_FAIL("Put with invalid handle should have failed");
        return -1;
    }

    /* Check error message */
    char *err = ObjstoreGetLastError();
    if (err == NULL) {
        PRINT_FAIL("Expected error message but got none");
        return -1;
    }

    printf("       Expected error received: %s\n", err);
    ObjstoreFreeString(err);

    PRINT_PASS("Error handling works correctly");
    return 0;
}

/* Test 8: Error handling - buffer too small */
static int test_error_handling_small_buffer(int handle) {
    PRINT_TEST("Error Handling - Buffer Too Small");

    const char *key = "data/file2.txt";
    char small_buffer[5];  /* Too small for the data */

    int result = ObjstoreGet(handle, (char *)key, small_buffer, sizeof(small_buffer));
    if (result >= 0) {
        PRINT_FAIL("Get with small buffer should have failed");
        return -1;
    }

    /* Check error message */
    char *err = ObjstoreGetLastError();
    if (err == NULL) {
        PRINT_FAIL("Expected error message but got none");
        return -1;
    }

    printf("       Expected error received: %s\n", err);
    ObjstoreFreeString(err);

    PRINT_PASS("Buffer size error handling works correctly");
    return 0;
}

/* Test 9: Binary data handling */
static int test_binary_data(int handle) {
    PRINT_TEST("Binary Data Handling");

    const char *key = "binary/data.bin";
    unsigned char binary_data[256];
    unsigned char read_buffer[256];

    /* Create binary data with all byte values */
    for (int i = 0; i < 256; i++) {
        binary_data[i] = (unsigned char)i;
    }

    /* Store binary data */
    int result = ObjstorePut(handle, (char *)key, (char *)binary_data, 256);
    if (result != 0) {
        print_objstore_error("Failed to store binary data");
        return -1;
    }

    /* Retrieve binary data */
    int bytes_read = ObjstoreGet(handle, (char *)key, (char *)read_buffer, sizeof(read_buffer));
    if (bytes_read != 256) {
        PRINT_FAIL("Binary data size mismatch");
        printf("       Expected: 256 bytes, Got: %d bytes\n", bytes_read);
        return -1;
    }

    /* Verify data integrity */
    if (memcmp(binary_data, read_buffer, 256) != 0) {
        PRINT_FAIL("Binary data corruption detected");
        return -1;
    }

    printf("       Successfully stored and retrieved 256 bytes of binary data\n");
    PRINT_PASS("Binary data handling successful");
    return 0;
}

int main(void) {
    int handle = -1;
    int failed_tests = 0;

    printf("\n");
    printf("========================================\n");
    printf("  go-objstore C API Test Suite\n");
    printf("========================================\n");
    printf("\n");

    /* Run all tests */
    if (test_version() != 0) failed_tests++;
    printf("\n");

    if (test_create_storage(&handle) != 0) {
        failed_tests++;
        goto cleanup;
    }
    printf("\n");

    if (test_put_operation(handle) != 0) failed_tests++;
    printf("\n");

    if (test_get_operation(handle) != 0) failed_tests++;
    printf("\n");

    if (test_multiple_puts(handle) != 0) failed_tests++;
    printf("\n");

    if (test_delete_operation(handle) != 0) failed_tests++;
    printf("\n");

    if (test_error_handling_invalid_handle() != 0) failed_tests++;
    printf("\n");

    if (test_error_handling_small_buffer(handle) != 0) failed_tests++;
    printf("\n");

    if (test_binary_data(handle) != 0) failed_tests++;
    printf("\n");

cleanup:
    /* Cleanup */
    if (handle >= 0) {
        PRINT_INFO("Closing storage handle and cleaning up...");
        ObjstoreClose(handle);
        cleanup_temp_dir("/tmp/objstore_test");
    }

    /* Print summary */
    printf("\n");
    printf("========================================\n");
    if (failed_tests == 0) {
        printf(COLOR_GREEN "  ALL TESTS PASSED!" COLOR_RESET "\n");
    } else {
        printf(COLOR_RED "  %d TEST(S) FAILED" COLOR_RESET "\n", failed_tests);
    }
    printf("========================================\n");
    printf("\n");

    return failed_tests == 0 ? 0 : 1;
}
