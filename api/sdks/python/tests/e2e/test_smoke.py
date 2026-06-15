"""E2E smoke test: exercises the MCP and Unix transports against a live
server. Skipped unless SMOKE_MCP_ADDR / SMOKE_UNIX_SOCK are set; launch a
server with scripts/start-test-server.sh first (or use `make sdk-smoke`)."""

import os

import pytest

from objstore import McpClient, UnixClient

MCP_ADDR = os.environ.get("SMOKE_MCP_ADDR", "")
UNIX_SOCK = os.environ.get("SMOKE_UNIX_SOCK", "")

pytestmark = pytest.mark.skipif(
    not MCP_ADDR or not UNIX_SOCK,
    reason="SMOKE_MCP_ADDR / SMOKE_UNIX_SOCK not set",
)


def _clients():
    return [
        ("mcp", McpClient(base_url=f"http://{MCP_ADDR}")),
        ("unix", UnixClient(socket_path=UNIX_SOCK)),
    ]


def test_smoke_round_trip() -> None:
    for name, client in _clients():
        key = f"smoke/python/{name}/obj.bin"
        payload = b"\x00\x01hello from python " + name.encode() + b"\xff\xfe"

        resp = client.put(key, payload)
        assert resp.success, f"{name}: put failed"

        assert client.exists(key).exists, f"{name}: object should exist"

        data, _ = client.get(key)
        assert data == payload, f"{name}: round-trip mismatch"

        listing = client.list(prefix=f"smoke/python/{name}")
        assert any(obj.key == key for obj in listing.objects), f"{name}: list missing key"

        del_resp = client.delete(key)
        assert del_resp.success, f"{name}: delete failed"
        assert not client.exists(key).exists, f"{name}: object should be gone"
