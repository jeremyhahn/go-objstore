"""Canonical unit tests for the MCP (Model Context Protocol) HTTP client.

Implements the SDK canonical test matrix for the MCP transport:

- success + error path for all 19 operations
- not_found path for designated operations
- put_stream coverage
- auth header injection (token, tenant_id, custom headers)

HTTP calls are mocked with the ``responses`` library; no live server is
needed. Test names follow ``test_mcp_<op>_<variant>``.
"""

from __future__ import annotations

import base64
import json

import pytest
import responses
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout

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
from objstore.mcp_client import McpClient
from objstore.models import (
    LifecyclePolicy,
    Metadata,
    ReplicationPolicy,
    TriggerReplicationOptions,
)


BASE = "http://localhost:8081"
RPC_URL = f"{BASE}/"


# ---- helpers ---------------------------------------------------------


def _client(**kwargs) -> McpClient:
    return McpClient(base_url=BASE, max_retries=1, **kwargs)


def _policy() -> LifecyclePolicy:
    return LifecyclePolicy(id="p1", prefix="x/", retention_seconds=10, action="delete")


def _repl() -> ReplicationPolicy:
    return ReplicationPolicy(
        id="r1",
        source_backend="local",
        destination_backend="s3",
        check_interval_seconds=30,
    )


def _opts() -> TriggerReplicationOptions:
    return TriggerReplicationOptions(policy_id="r1")


def _tool_result(payload: object) -> dict:
    """Wrap a tool result dict in the MCP envelope."""
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "content": [{"type": "text", "text": json.dumps(payload)}]
        },
    }


def _rpc_error(code: int, message: str) -> dict:
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "error": {"code": code, "message": message},
    }


# =====================================================================
# put
# =====================================================================


@responses.activate
def test_mcp_put_success() -> None:
    responses.add(responses.POST, RPC_URL, json=_tool_result({"success": True, "key": "k"}), status=200)
    result = _client().put("k", b"hello")
    assert result.success is True
    # The wire payload must carry base64-encoded data.
    sent = json.loads(responses.calls[0].request.body)
    assert sent["params"]["arguments"]["data"] == base64.b64encode(b"hello").decode("ascii")


@responses.activate
def test_mcp_put_binary_data_is_base64() -> None:
    """Binary payloads with non-UTF-8 bytes must survive encoding."""
    responses.add(responses.POST, RPC_URL, json=_tool_result({"success": True}), status=200)
    payload = bytes([0x00, 0x01, 0xFF, 0xFE, 0x80])
    result = _client().put("k", payload)
    assert result.success is True
    sent = json.loads(responses.calls[0].request.body)
    assert base64.b64decode(sent["params"]["arguments"]["data"]) == payload


@responses.activate
def test_mcp_put_with_metadata() -> None:
    responses.add(responses.POST, RPC_URL, json=_tool_result({"success": True}), status=200)
    meta = Metadata(content_type="text/plain", custom={"x": "y"})
    result = _client().put("k", b"hello", metadata=meta)
    assert result.success is True


@responses.activate
def test_mcp_put_error() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32603, "internal error"), status=200)
    with pytest.raises(ServerError):
        _client().put("k", b"data")


@responses.activate
def test_mcp_put_not_found() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32004, "not found"), status=200)
    with pytest.raises(ObjectNotFoundError):
        _client().put("k", b"data")


# =====================================================================
# put_stream
# =====================================================================


@responses.activate
def test_mcp_put_stream_bytes() -> None:
    responses.add(responses.POST, RPC_URL, json=_tool_result({"success": True}), status=200)
    result = _client().put_stream("k", b"stream data")
    assert result.success is True


@responses.activate
def test_mcp_put_stream_iterator() -> None:
    responses.add(responses.POST, RPC_URL, json=_tool_result({"success": True}), status=200)
    result = _client().put_stream("k", iter([b"a", b"b"]))
    assert result.success is True


# =====================================================================
# get
# =====================================================================


@responses.activate
def test_mcp_get_success() -> None:
    payload = {"success": True, "key": "k", "data": "aGVsbG8=", "size": 5}
    responses.add(responses.POST, RPC_URL, json=_tool_result(payload), status=200)
    data, meta = _client().get("k")
    assert data == b"hello"


@responses.activate
def test_mcp_get_binary_round_trip() -> None:
    raw = bytes([0x00, 0x01, 0xFF, 0xFE, 0x80])
    payload = {"success": True, "key": "k", "data": base64.b64encode(raw).decode("ascii"), "size": len(raw)}
    responses.add(responses.POST, RPC_URL, json=_tool_result(payload), status=200)
    data, _ = _client().get("k")
    assert data == raw


@responses.activate
def test_mcp_get_invalid_base64_raises() -> None:
    """Non-base64 data must raise, not be silently passed through."""
    payload = {"success": True, "key": "k", "data": "not valid base64!!!", "size": 3}
    responses.add(responses.POST, RPC_URL, json=_tool_result(payload), status=200)
    with pytest.raises(ServerError):
        _client().get("k")


@responses.activate
def test_mcp_get_error() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32603, "boom"), status=200)
    with pytest.raises(ServerError):
        _client().get("k")


@responses.activate
def test_mcp_get_not_found() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32004, "object not found"), status=200)
    with pytest.raises(ObjectNotFoundError):
        _client().get("k")


# =====================================================================
# get_stream
# =====================================================================


@responses.activate
def test_mcp_get_stream_success() -> None:
    payload = {"success": True, "data": "c3RyZWFt", "size": 6}
    responses.add(responses.POST, RPC_URL, json=_tool_result(payload), status=200)
    chunks = list(_client().get_stream("k"))
    assert b"".join(chunks) == b"stream"


# =====================================================================
# delete
# =====================================================================


@responses.activate
def test_mcp_delete_success() -> None:
    responses.add(responses.POST, RPC_URL, json=_tool_result({"success": True, "deleted": True}), status=200)
    result = _client().delete("k")
    assert result.success is True


@responses.activate
def test_mcp_delete_error() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32603, "boom"), status=200)
    with pytest.raises(ServerError):
        _client().delete("k")


@responses.activate
def test_mcp_delete_not_found() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32004, "not found"), status=200)
    with pytest.raises(ObjectNotFoundError):
        _client().delete("k")


# =====================================================================
# list
# =====================================================================


@responses.activate
def test_mcp_list_success() -> None:
    payload = {"success": True, "keys": ["a/b", "a/c"], "count": 2, "truncated": False}
    responses.add(responses.POST, RPC_URL, json=_tool_result(payload), status=200)
    result = _client().list()
    assert len(result.objects) == 2
    assert result.objects[0].key == "a/b"


@responses.activate
def test_mcp_list_truncated() -> None:
    payload = {"success": True, "keys": ["a"], "truncated": True, "next_token": "tok1"}
    responses.add(responses.POST, RPC_URL, json=_tool_result(payload), status=200)
    result = _client().list()
    assert result.truncated is True
    assert result.next_token == "tok1"


@responses.activate
def test_mcp_list_error() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32603, "boom"), status=200)
    with pytest.raises(ServerError):
        _client().list()


# =====================================================================
# exists
# =====================================================================


@responses.activate
def test_mcp_exists_true() -> None:
    responses.add(responses.POST, RPC_URL, json=_tool_result({"success": True, "exists": True}), status=200)
    result = _client().exists("k")
    assert result.exists is True


@responses.activate
def test_mcp_exists_false() -> None:
    responses.add(responses.POST, RPC_URL, json=_tool_result({"success": True, "exists": False}), status=200)
    result = _client().exists("k")
    assert result.exists is False


@responses.activate
def test_mcp_exists_error() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32603, "boom"), status=200)
    with pytest.raises(ServerError):
        _client().exists("k")


# =====================================================================
# get_metadata
# =====================================================================


@responses.activate
def test_mcp_get_metadata_success() -> None:
    payload = {
        "success": True,
        "key": "k",
        "content_type": "application/json",
        "content_encoding": "gzip",
        "size": 100,
        "etag": "e1",
        "custom": {"author": "alice"},
    }
    responses.add(responses.POST, RPC_URL, json=_tool_result(payload), status=200)
    meta = _client().get_metadata("k")
    assert meta.content_type == "application/json"
    assert meta.size == 100
    assert meta.custom == {"author": "alice"}


@responses.activate
def test_mcp_get_metadata_error() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32603, "boom"), status=200)
    with pytest.raises(ServerError):
        _client().get_metadata("k")


@responses.activate
def test_mcp_get_metadata_not_found() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32004, "not found"), status=200)
    with pytest.raises(ObjectNotFoundError):
        _client().get_metadata("k")


# =====================================================================
# update_metadata
# =====================================================================


@responses.activate
def test_mcp_update_metadata_success() -> None:
    responses.add(responses.POST, RPC_URL, json=_tool_result({"success": True, "updated": True}), status=200)
    result = _client().update_metadata("k", Metadata(content_type="text/plain"))
    assert result.success is True


@responses.activate
def test_mcp_update_metadata_error() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32603, "boom"), status=200)
    with pytest.raises(ServerError):
        _client().update_metadata("k", Metadata())


# =====================================================================
# health
# =====================================================================


@responses.activate
def test_mcp_health_success_serving() -> None:
    from objstore.models import HealthStatus
    responses.add(responses.POST, RPC_URL, json=_tool_result({"status": "healthy", "version": "1.0"}), status=200)
    result = _client().health()
    assert result.status == HealthStatus.SERVING


@responses.activate
def test_mcp_health_unknown() -> None:
    from objstore.models import HealthStatus
    responses.add(responses.POST, RPC_URL, json=_tool_result({"status": "WEIRD"}), status=200)
    result = _client().health()
    assert result.status == HealthStatus.UNKNOWN


@responses.activate
def test_mcp_health_error() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32603, "boom"), status=200)
    with pytest.raises(ServerError):
        _client().health()


# =====================================================================
# archive
# =====================================================================


@responses.activate
def test_mcp_archive_success() -> None:
    responses.add(responses.POST, RPC_URL, json=_tool_result({"success": True, "archived": True}), status=200)
    result = _client().archive("k", "glacier", {"vault": "v1"})
    assert result.success is True


@responses.activate
def test_mcp_archive_error() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32603, "boom"), status=200)
    with pytest.raises(ServerError):
        _client().archive("k", "glacier", {})


@responses.activate
def test_mcp_archive_not_found() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32004, "not found"), status=200)
    with pytest.raises(ObjectNotFoundError):
        _client().archive("k", "glacier", {})


# =====================================================================
# add_policy
# =====================================================================


@responses.activate
def test_mcp_add_policy_success() -> None:
    responses.add(responses.POST, RPC_URL, json=_tool_result({"success": True, "added": True}), status=200)
    result = _client().add_policy(_policy())
    assert result.success is True
    sent = json.loads(responses.calls[0].request.body)
    assert sent["params"]["arguments"]["retention_seconds"] == 10


@responses.activate
def test_mcp_add_policy_error() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32603, "boom"), status=200)
    with pytest.raises(ServerError):
        _client().add_policy(_policy())


# =====================================================================
# remove_policy
# =====================================================================


@responses.activate
def test_mcp_remove_policy_success() -> None:
    responses.add(responses.POST, RPC_URL, json=_tool_result({"success": True, "removed": True}), status=200)
    result = _client().remove_policy("p1")
    assert result.success is True


@responses.activate
def test_mcp_remove_policy_error() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32603, "boom"), status=200)
    with pytest.raises(ServerError):
        _client().remove_policy("p1")


@responses.activate
def test_mcp_remove_policy_not_found() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32004, "not found"), status=200)
    with pytest.raises(ObjectNotFoundError):
        _client().remove_policy("p1")


# =====================================================================
# get_policies
# =====================================================================


@responses.activate
def test_mcp_get_policies_success() -> None:
    payload = {
        "success": True,
        "policies": [{"id": "p1", "prefix": "x/", "action": "delete", "retention_seconds": 86400}],
        "count": 1,
    }
    responses.add(responses.POST, RPC_URL, json=_tool_result(payload), status=200)
    result = _client().get_policies()
    assert len(result.policies) == 1
    assert result.policies[0].id == "p1"


@responses.activate
def test_mcp_get_policies_success_retention_round_trip() -> None:
    payload = {
        "success": True,
        "policies": [{"id": "p1", "prefix": "x/", "action": "delete", "retention_seconds": 90000}],
        "count": 1,
    }
    responses.add(responses.POST, RPC_URL, json=_tool_result(payload), status=200)
    result = _client().get_policies()
    assert result.policies[0].retention_seconds == 90000


@responses.activate
def test_mcp_get_policies_after_days_fallback() -> None:
    """Without retention_seconds the client falls back to after_days * 86400."""
    payload = {
        "success": True,
        "policies": [{"id": "p1", "prefix": "x/", "action": "delete", "after_days": 2}],
        "count": 1,
    }
    responses.add(responses.POST, RPC_URL, json=_tool_result(payload), status=200)
    result = _client().get_policies()
    assert result.policies[0].retention_seconds == 172800


@responses.activate
def test_mcp_get_policies_empty() -> None:
    responses.add(responses.POST, RPC_URL, json=_tool_result({"success": True, "policies": [], "count": 0}), status=200)
    result = _client().get_policies()
    assert result.policies == []


@responses.activate
def test_mcp_get_policies_error() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32603, "boom"), status=200)
    with pytest.raises(ServerError):
        _client().get_policies()


# =====================================================================
# apply_policies
# =====================================================================


@responses.activate
def test_mcp_apply_policies_success() -> None:
    payload = {"success": True, "policies_count": 3, "objects_processed": 7, "message": "done"}
    responses.add(responses.POST, RPC_URL, json=_tool_result(payload), status=200)
    result = _client().apply_policies()
    assert result.success is True
    assert result.policies_count == 3
    assert result.objects_processed == 7


@responses.activate
def test_mcp_apply_policies_error() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32603, "boom"), status=200)
    with pytest.raises(ServerError):
        _client().apply_policies()


# =====================================================================
# add_replication_policy
# =====================================================================


@responses.activate
def test_mcp_add_replication_policy_success() -> None:
    responses.add(responses.POST, RPC_URL, json=_tool_result({"success": True}), status=200)
    result = _client().add_replication_policy(_repl())
    assert result.success is True


@responses.activate
def test_mcp_add_replication_policy_error() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32603, "boom"), status=200)
    with pytest.raises(ServerError):
        _client().add_replication_policy(_repl())


# =====================================================================
# remove_replication_policy
# =====================================================================


@responses.activate
def test_mcp_remove_replication_policy_success() -> None:
    responses.add(responses.POST, RPC_URL, json=_tool_result({"success": True}), status=200)
    result = _client().remove_replication_policy("r1")
    assert result.success is True


@responses.activate
def test_mcp_remove_replication_policy_error() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32603, "boom"), status=200)
    with pytest.raises(ServerError):
        _client().remove_replication_policy("r1")


@responses.activate
def test_mcp_remove_replication_policy_not_found() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32004, "not found"), status=200)
    with pytest.raises(ObjectNotFoundError):
        _client().remove_replication_policy("r1")


# =====================================================================
# get_replication_policies
# =====================================================================


@responses.activate
def test_mcp_get_replication_policies_success() -> None:
    payload = {
        "success": True,
        "policies": [
            {
                "id": "r1",
                "source_backend": "local",
                "destination_backend": "s3",
                "source_settings": {},
                "destination_settings": {},
                "source_prefix": "",
                "check_interval": 30,
                "enabled": True,
            }
        ],
        "count": 1,
    }
    responses.add(responses.POST, RPC_URL, json=_tool_result(payload), status=200)
    result = _client().get_replication_policies()
    assert len(result.policies) == 1
    assert result.policies[0].id == "r1"


@responses.activate
def test_mcp_get_replication_policies_error() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32603, "boom"), status=200)
    with pytest.raises(ServerError):
        _client().get_replication_policies()


# =====================================================================
# get_replication_policy
# =====================================================================


@responses.activate
def test_mcp_get_replication_policy_success() -> None:
    payload = {
        "success": True,
        "id": "r1",
        "source_backend": "local",
        "destination_backend": "s3",
        "source_settings": {},
        "destination_settings": {},
        "source_prefix": "",
        "check_interval": 30,
        "enabled": True,
    }
    responses.add(responses.POST, RPC_URL, json=_tool_result(payload), status=200)
    result = _client().get_replication_policy("r1")
    assert result.id == "r1"


@responses.activate
def test_mcp_get_replication_policy_error() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32603, "boom"), status=200)
    with pytest.raises(ServerError):
        _client().get_replication_policy("r1")


@responses.activate
def test_mcp_get_replication_policy_not_found() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32004, "not found"), status=200)
    with pytest.raises(ObjectNotFoundError):
        _client().get_replication_policy("r1")


# =====================================================================
# trigger_replication
# =====================================================================


@responses.activate
def test_mcp_trigger_replication_success() -> None:
    payload = {
        "success": True,
        "result": {
            "policy_id": "r1",
            "synced": 3,
            "deleted": 0,
            "failed": 0,
            "bytes_total": 1024,
            "duration": "1.5s",
            "errors": [],
        },
        "message": "done",
    }
    responses.add(responses.POST, RPC_URL, json=_tool_result(payload), status=200)
    result = _client().trigger_replication(_opts())
    assert result.success is True
    assert result.result is not None
    assert result.result.synced == 3


@responses.activate
def test_mcp_trigger_replication_error() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32603, "boom"), status=200)
    with pytest.raises(ServerError):
        _client().trigger_replication(_opts())


# =====================================================================
# get_replication_status
# =====================================================================


@responses.activate
def test_mcp_get_replication_status_success() -> None:
    payload = {
        "success": True,
        "policy_id": "r1",
        "source_backend": "local",
        "destination_backend": "s3",
        "enabled": True,
        "total_objects_synced": 10,
        "total_objects_deleted": 0,
        "total_bytes_synced": 4096,
        "total_errors": 0,
        "average_sync_duration": "1s",
        "sync_count": 5,
        "message": "ok",
    }
    responses.add(responses.POST, RPC_URL, json=_tool_result(payload), status=200)
    result = _client().get_replication_status("r1")
    assert result.success is True
    assert result.status is not None
    assert result.status.total_objects_synced == 10


@responses.activate
def test_mcp_get_replication_status_error() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32603, "boom"), status=200)
    with pytest.raises(ServerError):
        _client().get_replication_status("r1")


@responses.activate
def test_mcp_get_replication_status_not_found() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32004, "not found"), status=200)
    with pytest.raises(ObjectNotFoundError):
        _client().get_replication_status("r1")


# =====================================================================
# JSON-RPC error mapping (by code, never by message text)
# =====================================================================


@responses.activate
def test_mcp_unauthenticated_maps_to_authentication_error() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32002, "missing credentials"), status=200)
    with pytest.raises(AuthenticationError):
        _client().health()


@responses.activate
def test_mcp_forbidden_maps_to_authorization_error() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32001, "access denied"), status=200)
    with pytest.raises(AuthorizationError):
        _client().health()


@responses.activate
def test_mcp_already_exists_maps_to_already_exists_error() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32005, "object exists"), status=200)
    with pytest.raises(AlreadyExistsError):
        _client().health()


@responses.activate
def test_mcp_rate_limited_maps_to_rate_limit_error() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32029, "too many requests"), status=200)
    with pytest.raises(RateLimitError):
        _client().health()


@responses.activate
def test_mcp_invalid_params_maps_to_validation_error() -> None:
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32602, "invalid params"), status=200)
    with pytest.raises(ValidationError):
        _client().health()


@responses.activate
def test_mcp_internal_error_with_not_found_text_is_server_error() -> None:
    """Mapping is by code; a -32603 whose message mentions 'not found' stays ServerError."""
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32603, "backend not found in registry"), status=200)
    with pytest.raises(ServerError):
        _client().health()


@responses.activate
def test_mcp_generic_rpc_error() -> None:
    """Codes without a dedicated mapping default to ServerError."""
    responses.add(responses.POST, RPC_URL, json=_rpc_error(-32000, "custom error"), status=200)
    with pytest.raises(ServerError):
        _client().health()


# =====================================================================
# HTTP-level error handling
# =====================================================================


@responses.activate
def test_mcp_http_401() -> None:
    responses.add(responses.POST, RPC_URL, status=401)
    with pytest.raises(AuthenticationError):
        _client().health()


@responses.activate
def test_mcp_http_404() -> None:
    responses.add(responses.POST, RPC_URL, status=404)
    with pytest.raises(ObjectNotFoundError):
        _client().health()


@responses.activate
def test_mcp_http_500() -> None:
    responses.add(responses.POST, RPC_URL, status=500, body="crash")
    with pytest.raises(ServerError):
        _client().health()


@responses.activate
def test_mcp_http_400() -> None:
    responses.add(responses.POST, RPC_URL, json={"message": "bad"}, status=400)
    with pytest.raises(ValidationError):
        _client().health()


@responses.activate
def test_mcp_http_403() -> None:
    responses.add(responses.POST, RPC_URL, status=403)
    with pytest.raises(AuthorizationError):
        _client().health()


@responses.activate
def test_mcp_http_409() -> None:
    responses.add(responses.POST, RPC_URL, json={"message": "exists"}, status=409)
    with pytest.raises(AlreadyExistsError):
        _client().health()


@responses.activate
def test_mcp_http_429() -> None:
    responses.add(responses.POST, RPC_URL, status=429)
    with pytest.raises(RateLimitError):
        _client().health()


def test_mcp_connection_error() -> None:
    from requests.exceptions import ConnectionError as ReqConnError
    client = _client()
    client.session.post = lambda *a, **kw: (_ for _ in ()).throw(ReqConnError("refused"))
    with pytest.raises(ConnectionError):
        client.health()


def test_mcp_timeout_error() -> None:
    from requests.exceptions import Timeout as ReqTimeout
    client = _client()
    client.session.post = lambda *a, **kw: (_ for _ in ()).throw(ReqTimeout("timeout"))
    with pytest.raises(TimeoutError):
        client.health()


@responses.activate
def test_mcp_invalid_json_response() -> None:
    responses.add(responses.POST, RPC_URL, body=b"not json", status=200,
                  content_type="application/json")
    with pytest.raises(ServerError):
        _client().health()


@responses.activate
def test_mcp_empty_content() -> None:
    responses.add(responses.POST, RPC_URL,
                  json={"jsonrpc": "2.0", "id": 1, "result": {"content": []}},
                  status=200)
    with pytest.raises(ServerError):
        _client().health()


# =====================================================================
# auth header injection
# =====================================================================


@responses.activate
def test_mcp_token_sets_authorization_header() -> None:
    def _check(request):
        assert request.headers.get("Authorization") == "Bearer secret"
        return (200, {}, json.dumps(_tool_result({"status": "healthy"})))

    responses.add_callback(responses.POST, RPC_URL, callback=_check,
                           content_type="application/json")
    _client(token="secret").health()


@responses.activate
def test_mcp_tenant_id_header() -> None:
    def _check(request):
        assert request.headers.get("X-Tenant-ID") == "t1"
        return (200, {}, json.dumps(_tool_result({"status": "healthy"})))

    responses.add_callback(responses.POST, RPC_URL, callback=_check,
                           content_type="application/json")
    _client(tenant_id="t1").health()


@responses.activate
def test_mcp_custom_headers() -> None:
    def _check(request):
        assert request.headers.get("X-Custom") == "value"
        return (200, {}, json.dumps(_tool_result({"status": "healthy"})))

    responses.add_callback(responses.POST, RPC_URL, callback=_check,
                           content_type="application/json")
    _client(headers={"X-Custom": "value"}).health()


# =====================================================================
# context manager / close
# =====================================================================


def test_mcp_context_manager() -> None:
    with _client() as c:
        assert isinstance(c, McpClient)
