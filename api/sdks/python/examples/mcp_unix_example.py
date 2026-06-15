#!/usr/bin/env python3
"""MCP and Unix socket transport examples for go-objstore Python SDK.

Demonstrates:
- MCP (Model Context Protocol) HTTP transport
- Unix domain socket transport
- Application-layer auth: token, tenant_id, custom headers
- Streaming uploads with put_stream
"""

from io import BytesIO

from objstore import ObjectStoreClient, Metadata
from objstore.client import Protocol


# ---------------------------------------------------------------------------
# MCP transport example
# ---------------------------------------------------------------------------


def mcp_example() -> None:
    """Example using the MCP HTTP transport."""
    print("=== MCP Transport Example ===")

    # Basic MCP connection (no auth)
    client = ObjectStoreClient(
        protocol=Protocol.MCP,
        base_url="http://localhost:8081",
        timeout=30,
    )

    with client:
        print("Uploading object via MCP...")
        resp = client.put("mcp/hello.txt", b"Hello from MCP!")
        print(f"  Upload: {resp.success}")

        print("Downloading object via MCP...")
        data, _ = client.get("mcp/hello.txt")
        print(f"  Data: {data.decode()}")

        print("Listing objects via MCP...")
        listing = client.list(prefix="mcp/")
        for obj in listing.objects:
            print(f"  - {obj.key}")

        print("Checking health via MCP...")
        health = client.health()
        print(f"  Status: {health.status}")

        client.delete("mcp/hello.txt")

    print()


def mcp_auth_example() -> None:
    """Example using MCP with bearer token and tenant isolation."""
    print("=== MCP Auth Example ===")

    # With bearer token + tenant ID
    client = ObjectStoreClient(
        protocol=Protocol.MCP,
        base_url="http://localhost:8081",
        token="my-api-token",
        tenant_id="tenant-acme",
        headers={"X-Request-Source": "python-sdk"},
    )

    with client:
        # All requests carry Authorization: Bearer my-api-token
        # and X-Tenant-ID: tenant-acme
        resp = client.put("secure/data.bin", b"\x00\x01\x02\x03")
        print(f"  Secure upload: {resp.success}")

        client.delete("secure/data.bin")

    print()


# ---------------------------------------------------------------------------
# Unix domain socket example
# ---------------------------------------------------------------------------


def unix_example() -> None:
    """Example using the Unix domain socket transport."""
    print("=== Unix Socket Transport Example ===")

    # Connect to the local Unix socket
    client = ObjectStoreClient(
        protocol=Protocol.UNIX,
        socket_path="/tmp/objstore.sock",
        timeout=10,
    )

    with client:
        print("Uploading object via Unix socket...")
        resp = client.put("unix/hello.txt", b"Hello from Unix socket!")
        print(f"  Upload: {resp.success}")

        print("Downloading object via Unix socket...")
        data, meta = client.get("unix/hello.txt")
        print(f"  Data: {data.decode()}")
        print(f"  Content-type: {meta.content_type}")

        print("Checking exists...")
        exists_resp = client.exists("unix/hello.txt")
        print(f"  Exists: {exists_resp.exists}")

        print("Checking health via Unix socket...")
        health = client.health()
        print(f"  Status: {health.status}")

        client.delete("unix/hello.txt")

    print()


def unix_stream_example() -> None:
    """Example using streaming upload via Unix socket."""
    print("=== Unix Streaming Upload Example ===")

    client = ObjectStoreClient(
        protocol=Protocol.UNIX,
        socket_path="/tmp/objstore.sock",
    )

    with client:
        # Upload from a file-like object
        file_data = BytesIO(b"Binary content from file-like object\n" * 100)
        resp = client.put_stream("unix/large.bin", file_data)
        print(f"  Streamed upload: {resp.success}")

        # Download as a stream
        chunks = list(client.get_stream("unix/large.bin"))
        total = b"".join(chunks)
        print(f"  Downloaded {len(total)} bytes in {len(chunks)} chunk(s)")

        client.delete("unix/large.bin")

    print()


# ---------------------------------------------------------------------------
# Auth examples for REST and QUIC (showing the same API applies)
# ---------------------------------------------------------------------------


def rest_auth_example() -> None:
    """REST client with application-layer authentication."""
    print("=== REST Auth Example ===")

    # Bearer token auth
    client = ObjectStoreClient(
        protocol=Protocol.REST,
        base_url="http://localhost:8080",
        token="bearer-token-here",
        tenant_id="my-tenant",
        headers={
            "X-Correlation-ID": "req-abc-123",
        },
    )

    with client:
        resp = client.put("auth/test.txt", b"authenticated data")
        print(f"  Auth upload (REST): {resp.success}")
        client.delete("auth/test.txt")

    print()


def put_stream_example() -> None:
    """put_stream across all synchronous transports."""
    print("=== put_stream Comparison ===")

    transports = [
        ("REST", ObjectStoreClient(protocol=Protocol.REST)),
        ("MCP", ObjectStoreClient(protocol=Protocol.MCP)),
        ("UNIX", ObjectStoreClient(protocol=Protocol.UNIX, socket_path="/tmp/objstore.sock")),
    ]

    payload = b"chunked payload " * 64  # 1 KB

    for name, client in transports:
        with client:
            resp = client.put_stream(f"stream/{name.lower()}.bin", payload)
            print(f"  {name} put_stream: {resp.success}")

    print()


def main() -> None:
    """Run all examples (requires running go-objstore server)."""
    print("=" * 60)
    print("go-objstore Python SDK — MCP & Unix Examples")
    print("=" * 60)
    print()
    print("NOTE: These examples require running go-objstore servers.")
    print("  MCP server:  objstore-server --mcp --addr :8081")
    print("  Unix server: objstore-server --unix /tmp/objstore.sock")
    print()

    try:
        mcp_example()
    except Exception as exc:
        print(f"  MCP example skipped ({exc})")

    try:
        mcp_auth_example()
    except Exception as exc:
        print(f"  MCP auth example skipped ({exc})")

    try:
        unix_example()
    except Exception as exc:
        print(f"  Unix example skipped ({exc})")

    try:
        unix_stream_example()
    except Exception as exc:
        print(f"  Unix stream example skipped ({exc})")

    try:
        rest_auth_example()
    except Exception as exc:
        print(f"  REST auth example skipped ({exc})")

    print("=" * 60)
    print("Done.")
    print("=" * 60)


if __name__ == "__main__":
    main()
