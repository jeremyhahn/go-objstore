#!/usr/bin/env python3
"""
Basic usage examples for go-objstore Python SDK.

This script demonstrates how to use the SDK with all three protocols:
- REST
- gRPC
- QUIC/HTTP3
"""

from objstore import ObjectStoreClient, Protocol, Metadata


def rest_example():
    """Example using REST protocol."""
    print("=== REST Example ===")

    # Create REST client
    client = ObjectStoreClient(
        protocol=Protocol.REST,
        base_url="http://localhost:8080",
        timeout=30,
    )

    with client:
        # Upload an object
        print("Uploading object...")
        response = client.put("example/test.txt", b"Hello, REST!")
        print(f"Upload successful: {response.success}")

        # Download the object
        print("Downloading object...")
        data, metadata = client.get("example/test.txt")
        print(f"Downloaded: {data.decode()}")
        print(f"Size: {metadata.size} bytes")

        # List objects
        print("Listing objects...")
        result = client.list(prefix="example/", max_results=10)
        for obj in result.objects:
            print(f"  - {obj.key}")

        # Check if object exists
        print("Checking existence...")
        exists = client.exists("example/test.txt")
        print(f"Object exists: {exists.exists}")

        # Delete the object
        print("Deleting object...")
        delete_response = client.delete("example/test.txt")
        print(f"Delete successful: {delete_response.success}")

    print()


def grpc_example():
    """Example using gRPC protocol."""
    print("=== gRPC Example ===")

    try:
        # Create gRPC client
        client = ObjectStoreClient(
            protocol=Protocol.GRPC, host="localhost", port=50051, timeout=30
        )

        with client:
            # Same API as REST
            print("Uploading object via gRPC...")
            response = client.put("example/grpc-test.txt", b"Hello, gRPC!")
            print(f"Upload successful: {response.success}")

            # Download
            print("Downloading object via gRPC...")
            data, metadata = client.get("example/grpc-test.txt")
            print(f"Downloaded: {data.decode()}")

            # Delete
            client.delete("example/grpc-test.txt")
            print("Deleted successfully")

    except ImportError:
        print("gRPC support requires proto files. Run: make generate-proto")

    print()


def quic_example():
    """Example using QUIC/HTTP3 protocol."""
    print("=== QUIC/HTTP3 Example ===")

    # Create QUIC client
    client = ObjectStoreClient(
        protocol=Protocol.QUIC,
        base_url="https://localhost:4433",
        timeout=30,
        verify_ssl=False,  # For development/testing
    )

    with client:
        # Same API as REST and gRPC
        print("Uploading object via QUIC...")
        response = client.put("example/quic-test.txt", b"Hello, QUIC!")
        print(f"Upload successful: {response.success}")

        # Download
        print("Downloading object via QUIC...")
        data, metadata = client.get("example/quic-test.txt")
        print(f"Downloaded: {data.decode()}")

        # Delete
        client.delete("example/quic-test.txt")
        print("Deleted successfully")

    print()


def metadata_example():
    """Example using metadata."""
    print("=== Metadata Example ===")

    client = ObjectStoreClient(protocol=Protocol.REST)

    with client:
        # Upload with metadata
        print("Uploading with metadata...")
        metadata = Metadata(
            content_type="text/plain",
            custom={"author": "John Doe", "version": "1.0", "tags": "example,test"},
        )
        client.put("example/metadata-test.txt", b"Data with metadata", metadata=metadata)

        # Get metadata only
        print("Fetching metadata...")
        retrieved_metadata = client.get_metadata("example/metadata-test.txt")
        print(f"Content-Type: {retrieved_metadata.content_type}")
        print(f"Size: {retrieved_metadata.size} bytes")
        print(f"Custom metadata: {retrieved_metadata.custom}")

        # Update metadata
        print("Updating metadata...")
        new_metadata = Metadata(
            content_type="application/json", custom={"version": "2.0", "updated": "true"}
        )
        client.update_metadata("example/metadata-test.txt", new_metadata)

        # Verify update
        updated = client.get_metadata("example/metadata-test.txt")
        print(f"Updated custom metadata: {updated.custom}")

        # Cleanup
        client.delete("example/metadata-test.txt")

    print()


def streaming_example():
    """Example using streaming for large files."""
    print("=== Streaming Example ===")

    client = ObjectStoreClient(protocol=Protocol.REST)

    with client:
        # Create a large file (1MB)
        large_data = b"x" * (1024 * 1024)

        print("Uploading large file...")
        client.put("example/large-file.bin", large_data)

        # Stream download
        print("Streaming download...")
        chunks = []
        for chunk in client.get_stream("example/large-file.bin"):
            chunks.append(chunk)

        downloaded_data = b"".join(chunks)
        print(f"Downloaded {len(downloaded_data)} bytes in {len(chunks)} chunks")

        # Cleanup
        client.delete("example/large-file.bin")

    print()


def error_handling_example():
    """Example demonstrating error handling."""
    print("=== Error Handling Example ===")

    from objstore.exceptions import ObjectNotFoundError, ValidationError

    client = ObjectStoreClient(protocol=Protocol.REST)

    with client:
        try:
            # Try to get a non-existent object
            print("Attempting to get non-existent object...")
            data, metadata = client.get("nonexistent/file.txt")
        except ObjectNotFoundError as e:
            print(f"Caught ObjectNotFoundError: {e}")

        try:
            # Health check
            print("Checking server health...")
            health = client.health()
            print(f"Server status: {health.status}")
        except Exception as e:
            print(f"Health check failed: {e}")

    print()


def main():
    """Run all examples."""
    print("=" * 60)
    print("Go-ObjStore Python SDK - Usage Examples")
    print("=" * 60)
    print()

    # Note: These examples require a running go-objstore server
    print("NOTE: These examples require a running go-objstore server")
    print("Start the server with: docker run -p 8080:8080 go-objstore")
    print()

    try:
        # Run REST example (most reliable)
        rest_example()

        # Run metadata example
        metadata_example()

        # Run streaming example
        streaming_example()

        # Run error handling example
        error_handling_example()

        # Optionally run gRPC and QUIC examples
        # grpc_example()
        # quic_example()

    except Exception as e:
        print(f"Error: {e}")
        print()
        print("Make sure the go-objstore server is running:")
        print("  docker run -p 8080:8080 -p 50051:50051 -p 4433:4433 go-objstore")

    print("=" * 60)
    print("Examples completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
