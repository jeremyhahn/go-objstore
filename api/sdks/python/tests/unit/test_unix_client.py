"""Canonical unit tests for the Unix domain socket JSON-RPC 2.0 client.

Implements the SDK canonical test matrix for the Unix transport:

- success + error path for all 19 operations
- not_found path for designated operations
- put_stream and get_stream coverage

The Unix socket is mocked by patching ``socket.socket``; no live server is
needed. Test names follow ``test_unix_<op>_<variant>``.
"""

from __future__ import annotations

import base64
import json
import socket
from io import BytesIO
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from objstore.exceptions import (
    AlreadyExistsError,
    AuthenticationError,
    AuthorizationError,
    ConnectionError,
    ObjectNotFoundError,
    RateLimitError,
    ServerError,
    TimeoutError,
    ValidationError,
)
from objstore.models import (
    LifecyclePolicy,
    Metadata,
    ReplicationPolicy,
    TriggerReplicationOptions,
)
from objstore.unix_client import UnixClient


# ---- helpers ---------------------------------------------------------


def _client() -> UnixClient:
    return UnixClient(socket_path="/tmp/test.sock", timeout=5)


def _policy() -> LifecyclePolicy:
    return LifecyclePolicy(id="p1", prefix="x/", retention_seconds=86400, action="delete")


def _repl() -> ReplicationPolicy:
    return ReplicationPolicy(
        id="r1",
        source_backend="local",
        destination_backend="s3",
        check_interval_seconds=30,
    )


def _opts() -> TriggerReplicationOptions:
    return TriggerReplicationOptions(policy_id="r1")


def _encode(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _mock_socket(response_payload: Any) -> MagicMock:
    """Build a mock socket that returns the given payload as a newline-delimited JSON response."""
    response_line = (json.dumps({"jsonrpc": "2.0", "result": response_payload, "id": 1}) + "\n").encode()
    sock = MagicMock()
    sock.recv.return_value = response_line
    return sock


def _mock_socket_seq(payloads: list) -> MagicMock:
    """Build a mock socket returning one response per request with incrementing IDs."""
    lines = [
        (json.dumps({"jsonrpc": "2.0", "result": p, "id": i}) + "\n").encode()
        for i, p in enumerate(payloads, start=1)
    ]
    sock = MagicMock()
    sock.recv.side_effect = lines
    return sock


def _error_socket(code: int, message: str) -> MagicMock:
    """Build a mock socket that returns a JSON-RPC error response."""
    err = {"code": code, "message": message}
    response_line = (json.dumps({"jsonrpc": "2.0", "error": err, "id": 1}) + "\n").encode()
    sock = MagicMock()
    sock.recv.return_value = response_line
    return sock


def _patch_socket(sock: MagicMock):
    """Return a patch context manager for socket.socket.

    Returns a context manager that replaces ``socket.socket`` with a callable
    that accepts the (family, type) positional args and returns *sock*.
    """
    return patch("socket.socket", side_effect=lambda *a, **kw: sock)


# =====================================================================
# put
# =====================================================================


def test_unix_put_success() -> None:
    sock = _mock_socket({"success": True})
    with _patch_socket(sock):
        result = _client().put("k", b"hello")
    assert result.success is True


def test_unix_put_with_metadata() -> None:
    sock = _mock_socket({"success": True})
    with _patch_socket(sock):
        meta = Metadata(content_type="text/plain", custom={"x": "y"})
        result = _client().put("k", b"hello", metadata=meta)
    assert result.success is True


def test_unix_put_file_like() -> None:
    sock = _mock_socket({"success": True})
    with _patch_socket(sock):
        result = _client().put("k", BytesIO(b"hello"))
    assert result.success is True


def test_unix_put_error() -> None:
    sock = _error_socket(-32603, "internal error")
    with _patch_socket(sock):
        with pytest.raises(ServerError):
            _client().put("k", b"data")


def test_unix_put_connection_error() -> None:
    with patch("socket.socket") as mock_sock_cls:
        instance = MagicMock()
        instance.connect.side_effect = FileNotFoundError("no socket")
        mock_sock_cls.return_value = instance
        with pytest.raises(ConnectionError):
            _client().put("k", b"data")


def test_unix_put_timeout() -> None:
    with patch("socket.socket") as mock_sock_cls:
        instance = MagicMock()
        instance.connect.return_value = None
        instance.sendall.return_value = None
        instance.recv.side_effect = socket.timeout("timed out")
        mock_sock_cls.return_value = instance
        with pytest.raises(TimeoutError):
            _client().put("k", b"data")


# =====================================================================
# put_stream
# =====================================================================


def test_unix_put_stream_bytes() -> None:
    sock = _mock_socket({"success": True})
    with _patch_socket(sock):
        result = _client().put_stream("k", b"stream data")
    assert result.success is True


def test_unix_put_stream_iterator() -> None:
    sock = _mock_socket({"success": True})
    with _patch_socket(sock):
        chunks = [b"chunk1", b"chunk2"]
        result = _client().put_stream("k", iter(chunks))
    assert result.success is True


def test_unix_put_stream_file_like() -> None:
    sock = _mock_socket({"success": True})
    with _patch_socket(sock):
        result = _client().put_stream("k", BytesIO(b"file data"))
    assert result.success is True


# =====================================================================
# get
# =====================================================================


def test_unix_get_success() -> None:
    payload = {"data": _encode(b"hello"), "metadata": {"content_type": "text/plain", "custom": {}}}
    sock = _mock_socket(payload)
    with _patch_socket(sock):
        data, meta = _client().get("k")
    assert data == b"hello"
    assert meta.content_type == "text/plain"


def test_unix_get_error() -> None:
    sock = _error_socket(-32603, "internal error")
    with _patch_socket(sock):
        with pytest.raises(ServerError):
            _client().get("k")


def test_unix_get_not_found() -> None:
    sock = _error_socket(-32004, "object not found")
    with _patch_socket(sock):
        with pytest.raises(ObjectNotFoundError):
            _client().get("k")


# =====================================================================
# get_stream
# =====================================================================


def test_unix_get_stream_success() -> None:
    payload = {"data": _encode(b"stream content"), "metadata": {}}
    sock = _mock_socket(payload)
    with _patch_socket(sock):
        chunks = list(_client().get_stream("k"))
    assert b"".join(chunks) == b"stream content"


def test_unix_get_stream_empty() -> None:
    payload = {"data": _encode(b""), "metadata": {}}
    sock = _mock_socket(payload)
    with _patch_socket(sock):
        chunks = list(_client().get_stream("k"))
    assert chunks == []


# =====================================================================
# delete
# =====================================================================


def test_unix_delete_success() -> None:
    sock = _mock_socket({"success": True})
    with _patch_socket(sock):
        result = _client().delete("k")
    assert result.success is True


def test_unix_delete_error() -> None:
    sock = _error_socket(-32603, "internal error")
    with _patch_socket(sock):
        with pytest.raises(ServerError):
            _client().delete("k")


def test_unix_delete_not_found() -> None:
    sock = _error_socket(-32004, "not found")
    with _patch_socket(sock):
        with pytest.raises(ObjectNotFoundError):
            _client().delete("k")


# =====================================================================
# list
# =====================================================================


def test_unix_list_success() -> None:
    payload = {
        "objects": [{"key": "a/b", "size": 10, "last_modified": "", "etag": "e1"}],
        "next_cursor": "",
        "is_truncated": False,
    }
    sock = _mock_socket(payload)
    with _patch_socket(sock):
        result = _client().list()
    assert len(result.objects) == 1
    assert result.objects[0].key == "a/b"


def test_unix_list_with_prefix() -> None:
    payload = {"objects": [], "next_cursor": "", "is_truncated": False}
    sock = _mock_socket(payload)
    with _patch_socket(sock):
        result = _client().list(prefix="a/")
    assert result.objects == []


def test_unix_list_truncated() -> None:
    payload = {
        "objects": [{"key": "a", "size": 1, "last_modified": ""}],
        "next_cursor": "tok1",
        "is_truncated": True,
    }
    sock = _mock_socket(payload)
    with _patch_socket(sock):
        result = _client().list()
    assert result.truncated is True
    assert result.next_token == "tok1"


def test_unix_list_error() -> None:
    sock = _error_socket(-32603, "boom")
    with _patch_socket(sock):
        with pytest.raises(ServerError):
            _client().list()


# =====================================================================
# exists
# =====================================================================


def test_unix_exists_true() -> None:
    sock = _mock_socket({"exists": True})
    with _patch_socket(sock):
        result = _client().exists("k")
    assert result.exists is True


def test_unix_exists_false() -> None:
    sock = _mock_socket({"exists": False})
    with _patch_socket(sock):
        result = _client().exists("k")
    assert result.exists is False


def test_unix_exists_error() -> None:
    sock = _error_socket(-32603, "boom")
    with _patch_socket(sock):
        with pytest.raises(ServerError):
            _client().exists("k")


# =====================================================================
# get_metadata
# =====================================================================


def test_unix_get_metadata_success() -> None:
    payload = {"metadata": {"content_type": "application/json", "content_encoding": "gzip", "custom": {"x": "y"}}}
    sock = _mock_socket(payload)
    with _patch_socket(sock):
        meta = _client().get_metadata("k")
    assert meta.content_type == "application/json"
    assert meta.content_encoding == "gzip"
    assert meta.custom == {"x": "y"}


def test_unix_get_metadata_flat() -> None:
    """Server returning fields at top level (not nested under metadata) is handled."""
    payload = {"content_type": "text/plain", "custom": {}}
    sock = _mock_socket(payload)
    with _patch_socket(sock):
        meta = _client().get_metadata("k")
    assert meta.content_type == "text/plain"


def test_unix_get_metadata_error() -> None:
    sock = _error_socket(-32603, "boom")
    with _patch_socket(sock):
        with pytest.raises(ServerError):
            _client().get_metadata("k")


def test_unix_get_metadata_not_found() -> None:
    sock = _error_socket(-32004, "not found")
    with _patch_socket(sock):
        with pytest.raises(ObjectNotFoundError):
            _client().get_metadata("k")


# =====================================================================
# update_metadata
# =====================================================================


def test_unix_update_metadata_success() -> None:
    sock = _mock_socket({"success": True})
    with _patch_socket(sock):
        result = _client().update_metadata("k", Metadata(content_type="text/plain"))
    assert result.success is True


def test_unix_update_metadata_error() -> None:
    sock = _error_socket(-32603, "boom")
    with _patch_socket(sock):
        with pytest.raises(ServerError):
            _client().update_metadata("k", Metadata())


# =====================================================================
# health
# =====================================================================


def test_unix_health_success_serving() -> None:
    from objstore.models import HealthStatus
    sock = _mock_socket({"status": "SERVING", "version": "1.0"})
    with _patch_socket(sock):
        result = _client().health()
    assert result.status == HealthStatus.SERVING


def test_unix_health_healthy() -> None:
    from objstore.models import HealthStatus
    sock = _mock_socket({"status": "healthy", "version": "1.0"})
    with _patch_socket(sock):
        result = _client().health()
    assert result.status == HealthStatus.SERVING


def test_unix_health_unknown() -> None:
    from objstore.models import HealthStatus
    sock = _mock_socket({"status": "WEIRD", "version": ""})
    with _patch_socket(sock):
        result = _client().health()
    assert result.status == HealthStatus.UNKNOWN


def test_unix_health_error() -> None:
    sock = _error_socket(-32603, "boom")
    with _patch_socket(sock):
        with pytest.raises(ServerError):
            _client().health()


# =====================================================================
# archive
# =====================================================================


def test_unix_archive_success() -> None:
    sock = _mock_socket({"success": True})
    with _patch_socket(sock):
        result = _client().archive("k", "glacier", {"vault": "v1"})
    assert result.success is True


def test_unix_archive_error() -> None:
    sock = _error_socket(-32603, "boom")
    with _patch_socket(sock):
        with pytest.raises(ServerError):
            _client().archive("k", "glacier", {})


def test_unix_archive_not_found() -> None:
    sock = _error_socket(-32004, "not found")
    with _patch_socket(sock):
        with pytest.raises(ObjectNotFoundError):
            _client().archive("k", "glacier", {})


# =====================================================================
# add_policy
# =====================================================================


def test_unix_add_policy_success() -> None:
    sock = _mock_socket({"success": True})
    with _patch_socket(sock):
        result = _client().add_policy(_policy())
    assert result.success is True
    sent = json.loads(sock.sendall.call_args[0][0].decode())
    assert sent["params"]["retention_seconds"] == 86400


def test_unix_add_policy_sub_day_retention() -> None:
    """Retentions that are not whole days are sent as exact seconds."""
    sock = _mock_socket({"success": True})
    policy = LifecyclePolicy(id="p2", prefix="x/", retention_seconds=90000, action="delete")
    with _patch_socket(sock):
        result = _client().add_policy(policy)
    assert result.success is True
    sent = json.loads(sock.sendall.call_args[0][0].decode())
    assert sent["params"]["retention_seconds"] == 90000


def test_unix_add_policy_error() -> None:
    sock = _error_socket(-32603, "boom")
    with _patch_socket(sock):
        with pytest.raises(ServerError):
            _client().add_policy(_policy())


# =====================================================================
# remove_policy
# =====================================================================


def test_unix_remove_policy_success() -> None:
    sock = _mock_socket({"success": True})
    with _patch_socket(sock):
        result = _client().remove_policy("p1")
    assert result.success is True


def test_unix_remove_policy_error() -> None:
    sock = _error_socket(-32603, "boom")
    with _patch_socket(sock):
        with pytest.raises(ServerError):
            _client().remove_policy("p1")


def test_unix_remove_policy_not_found() -> None:
    sock = _error_socket(-32004, "not found")
    with _patch_socket(sock):
        with pytest.raises(ObjectNotFoundError):
            _client().remove_policy("p1")


# =====================================================================
# get_policies
# =====================================================================


def test_unix_get_policies_success() -> None:
    # The unix server returns a BARE JSON array of policies.
    payload = [{"id": "p1", "prefix": "x/", "action": "delete", "after_days": 1}]
    sock = _mock_socket(payload)
    with _patch_socket(sock):
        result = _client().get_policies()
    assert len(result.policies) == 1
    assert result.policies[0].id == "p1"
    assert result.policies[0].retention_seconds == 86400


def test_unix_get_policies_dict_fallback() -> None:
    """A dict wrapper with a "policies" key is still accepted."""
    payload = {
        "policies": [{"id": "p1", "prefix": "x/", "action": "delete", "after_days": 1}]
    }
    sock = _mock_socket(payload)
    with _patch_socket(sock):
        result = _client().get_policies()
    assert len(result.policies) == 1
    assert result.policies[0].id == "p1"


def test_unix_get_policies_prefers_retention_seconds() -> None:
    """retention_seconds wins over after_days when both are present."""
    payload = [
        {"id": "p1", "prefix": "x/", "action": "delete", "after_days": 1, "retention_seconds": 90000}
    ]
    sock = _mock_socket(payload)
    with _patch_socket(sock):
        result = _client().get_policies()
    assert result.policies[0].retention_seconds == 90000


def test_unix_get_policies_empty() -> None:
    sock = _mock_socket([])
    with _patch_socket(sock):
        result = _client().get_policies()
    assert result.policies == []


def test_unix_get_policies_error() -> None:
    sock = _error_socket(-32603, "boom")
    with _patch_socket(sock):
        with pytest.raises(ServerError):
            _client().get_policies()


# =====================================================================
# apply_policies
# =====================================================================


def test_unix_apply_policies_success() -> None:
    sock = _mock_socket({"policies_count": 2, "objects_processed": 5})
    with _patch_socket(sock):
        result = _client().apply_policies()
    assert result.success is True
    assert result.policies_count == 2
    assert result.objects_processed == 5


def test_unix_apply_policies_error() -> None:
    sock = _error_socket(-32603, "boom")
    with _patch_socket(sock):
        with pytest.raises(ServerError):
            _client().apply_policies()


# =====================================================================
# add_replication_policy
# =====================================================================


def test_unix_add_replication_policy_success() -> None:
    sock = _mock_socket({"success": True})
    with _patch_socket(sock):
        result = _client().add_replication_policy(_repl())
    assert result.success is True


def test_unix_add_replication_policy_error() -> None:
    sock = _error_socket(-32603, "boom")
    with _patch_socket(sock):
        with pytest.raises(ServerError):
            _client().add_replication_policy(_repl())


# =====================================================================
# remove_replication_policy
# =====================================================================


def test_unix_remove_replication_policy_success() -> None:
    sock = _mock_socket({"success": True})
    with _patch_socket(sock):
        result = _client().remove_replication_policy("r1")
    assert result.success is True


def test_unix_remove_replication_policy_error() -> None:
    sock = _error_socket(-32603, "boom")
    with _patch_socket(sock):
        with pytest.raises(ServerError):
            _client().remove_replication_policy("r1")


def test_unix_remove_replication_policy_not_found() -> None:
    sock = _error_socket(-32004, "not found")
    with _patch_socket(sock):
        with pytest.raises(ObjectNotFoundError):
            _client().remove_replication_policy("r1")


# =====================================================================
# get_replication_policies
# =====================================================================


def test_unix_get_replication_policies_success() -> None:
    payload = {
        "policies": [
            {
                "id": "r1",
                "source_backend": "local",
                "destination_type": "s3",
                "destination": {"bucket": "b"},
                "source_prefix": "",
                "enabled": True,
            }
        ]
    }
    sock = _mock_socket(payload)
    with _patch_socket(sock):
        result = _client().get_replication_policies()
    assert len(result.policies) == 1
    assert result.policies[0].id == "r1"


def test_unix_get_replication_policies_error() -> None:
    sock = _error_socket(-32603, "boom")
    with _patch_socket(sock):
        with pytest.raises(ServerError):
            _client().get_replication_policies()


# =====================================================================
# get_replication_policy
# =====================================================================


def test_unix_get_replication_policy_success() -> None:
    payload = {
        "id": "r1",
        "source_backend": "local",
        "destination_type": "s3",
        "destination": {},
        "source_prefix": "",
        "enabled": True,
    }
    sock = _mock_socket(payload)
    with _patch_socket(sock):
        result = _client().get_replication_policy("r1")
    assert result.id == "r1"


def test_unix_get_replication_policy_error() -> None:
    sock = _error_socket(-32603, "boom")
    with _patch_socket(sock):
        with pytest.raises(ServerError):
            _client().get_replication_policy("r1")


def test_unix_get_replication_policy_not_found() -> None:
    sock = _error_socket(-32004, "not found")
    with _patch_socket(sock):
        with pytest.raises(ObjectNotFoundError):
            _client().get_replication_policy("r1")


# =====================================================================
# trigger_replication
# =====================================================================


def test_unix_trigger_replication_success() -> None:
    payload = {
        "objects_synced": 3,
        "objects_failed": 0,
        "bytes_transferred": 1024,
        "errors": [],
    }
    sock = _mock_socket(payload)
    with _patch_socket(sock):
        result = _client().trigger_replication(_opts())
    assert result.success is True


def test_unix_trigger_replication_error() -> None:
    sock = _error_socket(-32603, "boom")
    with _patch_socket(sock):
        with pytest.raises(ServerError):
            _client().trigger_replication(_opts())


# =====================================================================
# get_replication_status
# =====================================================================


def test_unix_get_replication_status_success() -> None:
    payload = {
        "policy_id": "r1",
        "status": "active",
        "objects_synced": 10,
        "objects_failed": 0,
        "objects_pending": 0,
        "source_backend": "local",
        "destination_backend": "s3",
        "enabled": True,
    }
    sock = _mock_socket(payload)
    with _patch_socket(sock):
        result = _client().get_replication_status("r1")
    assert result.success is True


def test_unix_get_replication_status_error() -> None:
    sock = _error_socket(-32603, "boom")
    with _patch_socket(sock):
        with pytest.raises(ServerError):
            _client().get_replication_status("r1")


def test_unix_get_replication_status_not_found() -> None:
    sock = _error_socket(-32004, "not found")
    with _patch_socket(sock):
        with pytest.raises(ObjectNotFoundError):
            _client().get_replication_status("r1")


# =====================================================================
# context manager / close
# =====================================================================


def test_unix_context_manager() -> None:
    with _client() as c:
        assert isinstance(c, UnixClient)


def test_unix_close_closes_socket() -> None:
    sock = _mock_socket({"success": True})
    with _patch_socket(sock):
        client = _client()
        client.exists("k")
        client.close()
    sock.close.assert_called_once()
    assert client._sock is None


# =====================================================================
# persistent connection
# =====================================================================


def test_unix_connection_is_reused_across_requests() -> None:
    sock = _mock_socket_seq([{"exists": True}, {"exists": False}])
    with patch("socket.socket", side_effect=lambda *a, **kw: sock) as sock_cls:
        client = _client()
        assert client.exists("a").exists is True
        assert client.exists("b").exists is False
    sock_cls.assert_called_once()
    sock.connect.assert_called_once_with("/tmp/test.sock")
    assert sock.sendall.call_count == 2


def test_unix_reconnects_after_server_close() -> None:
    """An EOF mid-request raises ConnectionError; the next call reconnects."""
    dead = MagicMock()
    dead.recv.return_value = b""  # server closed the connection
    live = MagicMock()
    # The failed call consumed request id 1, so the retry carries id 2.
    live.recv.return_value = (
        json.dumps({"jsonrpc": "2.0", "result": {"exists": True}, "id": 2}) + "\n"
    ).encode()
    sockets = [dead, live]
    with patch("socket.socket", side_effect=lambda *a, **kw: sockets.pop(0)) as sock_cls:
        client = _client()
        with pytest.raises(ConnectionError):
            client.exists("a")
        # The dead socket was closed and a new connection is established.
        dead.close.assert_called_once()
        assert client.exists("a").exists is True
    assert sock_cls.call_count == 2


def test_unix_response_id_mismatch_raises_and_closes() -> None:
    response_line = (json.dumps({"jsonrpc": "2.0", "result": {"exists": True}, "id": 999}) + "\n").encode()
    sock = MagicMock()
    sock.recv.return_value = response_line
    with _patch_socket(sock):
        with pytest.raises(ServerError):
            _client().exists("k")
    sock.close.assert_called_once()


# =====================================================================
# error mapping (by JSON-RPC error code, never by message text)
# =====================================================================


def test_unix_validation_error_from_invalid_params() -> None:
    sock = _error_socket(-32602, "invalid params")
    with _patch_socket(sock):
        with pytest.raises(ValidationError):
            _client().get("k")


def test_unix_unauthenticated_maps_to_authentication_error() -> None:
    sock = _error_socket(-32002, "missing credentials")
    with _patch_socket(sock):
        with pytest.raises(AuthenticationError):
            _client().get("k")


def test_unix_forbidden_maps_to_authorization_error() -> None:
    sock = _error_socket(-32001, "access denied")
    with _patch_socket(sock):
        with pytest.raises(AuthorizationError):
            _client().get("k")


def test_unix_already_exists_maps_to_already_exists_error() -> None:
    sock = _error_socket(-32005, "object exists")
    with _patch_socket(sock):
        with pytest.raises(AlreadyExistsError):
            _client().get("k")


def test_unix_rate_limited_maps_to_rate_limit_error() -> None:
    sock = _error_socket(-32029, "too many requests")
    with _patch_socket(sock):
        with pytest.raises(RateLimitError):
            _client().get("k")


def test_unix_not_found_code_maps_to_object_not_found() -> None:
    sock = _error_socket(-32004, "no such object")
    with _patch_socket(sock):
        with pytest.raises(ObjectNotFoundError):
            _client().get("k")


def test_unix_internal_error_with_not_found_text_is_server_error() -> None:
    """Mapping is by code; a -32603 whose message mentions 'not found' stays ServerError."""
    sock = _error_socket(-32603, "backend not found in registry")
    with _patch_socket(sock):
        with pytest.raises(ServerError):
            _client().get("k")


def test_unix_generic_rpc_error() -> None:
    """Codes without a dedicated mapping default to ServerError."""
    sock = _error_socket(-32000, "custom error")
    with _patch_socket(sock):
        with pytest.raises(ServerError):
            _client().get("k")


def test_unix_bad_json_response() -> None:
    """A non-JSON response from the socket raises ServerError."""
    with patch("socket.socket") as mock_sock_cls:
        instance = MagicMock()
        instance.connect.return_value = None
        instance.sendall.return_value = None
        instance.recv.return_value = b"not json\n"
        mock_sock_cls.return_value = instance
        with pytest.raises(ServerError):
            _client().get("k")
