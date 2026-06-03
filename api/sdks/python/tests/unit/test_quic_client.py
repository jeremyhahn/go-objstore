"""Canonical unit tests for the QUIC (HTTP/3) client.

Implements the SDK canonical test matrix for the QUIC transport:

- success + error path for all 19 operations
- not_found path for the 9 designated operations
- metadata_round_trip
- validation_empty_key

The QUIC client is asynchronous and built on httpx; its underlying
``client.client`` request methods are replaced with ``AsyncMock`` so no
live server is needed. Test names follow ``test_quic_<op>_<variant>``.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from objstore.exceptions import (
    AuthenticationError,
    ConnectionError,
    ObjectNotFoundError,
    ObjectStoreError,
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
from objstore.quic_client import QuicClient


pytestmark = pytest.mark.asyncio


# ---- helpers ---------------------------------------------------------


def _client() -> QuicClient:
    return QuicClient(base_url="https://localhost:4433", api_version="v1")


def _resp(status: int, *, json: Any = None, headers: dict | None = None,
          content: bytes | None = None) -> MagicMock:
    r = MagicMock()
    r.status_code = status
    if json is not None:
        r.json.return_value = json
    if headers is not None:
        r.headers = headers
    if content is not None:
        r.content = content
    return r


def _mock(client: QuicClient, method: str) -> AsyncMock:
    """Patch a request method on the underlying httpx client and return it."""
    m = AsyncMock()
    setattr(client.client, method, m)
    return m


def _policy() -> LifecyclePolicy:
    return LifecyclePolicy(id="p1", prefix="x/", retention_seconds=10, action="delete")


def _repl() -> ReplicationPolicy:
    return ReplicationPolicy(
        id="r1", source_backend="local", destination_backend="s3",
        check_interval_seconds=30,
    )


def _opts() -> TriggerReplicationOptions:
    return TriggerReplicationOptions(policy_id="r1")


# =====================================================================
# put
# =====================================================================


async def test_quic_put_success() -> None:
    client = _client()
    _mock(client, "put").return_value = _resp(201, json={"message": "ok"},
                                              headers={"ETag": "e1"})
    result = await client.put("k", b"data")
    assert result.success is True
    assert result.etag == "e1"


async def test_quic_put_error() -> None:
    client = _client()
    _mock(client, "put").return_value = _resp(500, json={"message": "boom"})
    with pytest.raises(ServerError):
        await client.put("k", b"data")


# =====================================================================
# get
# =====================================================================


async def test_quic_get_success() -> None:
    client = _client()
    _mock(client, "get").return_value = _resp(
        200, content=b"hello",
        headers={"Content-Type": "text/plain", "Content-Length": "5", "ETag": "e1"})
    data, meta = await client.get("k")
    assert data == b"hello"
    assert meta.content_type == "text/plain"
    assert meta.size == 5


async def test_quic_get_error() -> None:
    client = _client()
    _mock(client, "get").return_value = _resp(500, json={"message": "boom"})
    with pytest.raises(ServerError):
        await client.get("k")


async def test_quic_get_not_found() -> None:
    client = _client()
    _mock(client, "get").return_value = _resp(404)
    with pytest.raises(ObjectNotFoundError):
        await client.get("k")


# =====================================================================
# delete
# =====================================================================


async def test_quic_delete_success() -> None:
    client = _client()
    _mock(client, "delete").return_value = _resp(200, json={"message": "deleted"})
    assert (await client.delete("k")).success is True


async def test_quic_delete_error() -> None:
    client = _client()
    _mock(client, "delete").return_value = _resp(500, json={"message": "boom"})
    with pytest.raises(ServerError):
        await client.delete("k")


async def test_quic_delete_not_found() -> None:
    client = _client()
    _mock(client, "delete").return_value = _resp(404)
    with pytest.raises(ObjectNotFoundError):
        await client.delete("k")


# =====================================================================
# list
# =====================================================================


async def test_quic_list_success() -> None:
    client = _client()
    _mock(client, "get").return_value = _resp(200, json={
        "objects": [{"key": "o1", "size": 1, "etag": "e1"},
                    {"key": "o2", "size": 2, "etag": "e2"}],
        "common_prefixes": ["d/"], "next_token": "tok", "truncated": True})
    result = await client.list(prefix="p", max_results=10)
    assert [o.key for o in result.objects] == ["o1", "o2"]
    assert result.truncated is True


async def test_quic_list_error() -> None:
    client = _client()
    _mock(client, "get").return_value = _resp(500, json={"message": "boom"})
    with pytest.raises(ServerError):
        await client.list()


# =====================================================================
# exists
# =====================================================================


async def test_quic_exists_success() -> None:
    client = _client()
    get = _mock(client, "get")
    get.return_value = _resp(200, json={"exists": True})
    result = await client.exists("k")
    assert result.exists is True
    assert get.call_args.kwargs["params"] == {"exists": "1"}


async def test_quic_exists_error() -> None:
    client = _client()
    _mock(client, "get").return_value = _resp(500, json={"message": "boom"})
    with pytest.raises(ServerError):
        await client.exists("k")


async def test_quic_exists_not_found() -> None:
    client = _client()
    _mock(client, "get").return_value = _resp(404)
    assert (await client.exists("k")).exists is False


# =====================================================================
# get_metadata
# =====================================================================


async def test_quic_get_metadata_success() -> None:
    client = _client()
    _mock(client, "head").return_value = _resp(200, headers={
        "Content-Type": "text/plain", "Content-Length": "100",
        "ETag": "e1", "X-Meta-author": "alice"})
    meta = await client.get_metadata("k")
    assert meta.size == 100
    assert meta.custom["author"] == "alice"


async def test_quic_get_metadata_error() -> None:
    client = _client()
    _mock(client, "head").return_value = _resp(500, json={"message": "boom"})
    with pytest.raises(ServerError):
        await client.get_metadata("k")


async def test_quic_get_metadata_not_found() -> None:
    client = _client()
    _mock(client, "head").return_value = _resp(404)
    with pytest.raises(ObjectNotFoundError):
        await client.get_metadata("k")


# =====================================================================
# update_metadata
# =====================================================================


async def test_quic_update_metadata_success() -> None:
    client = _client()
    patch_mock = _mock(client, "patch")
    patch_mock.return_value = _resp(200, json={"message": "updated"})
    result = await client.update_metadata("k", Metadata(content_type="application/json",
                                                        custom={"x": "y"}))
    assert result.success is True
    sent = patch_mock.call_args.kwargs["json"]
    assert sent["content_type"] == "application/json"
    assert sent["custom"] == {"x": "y"}


async def test_quic_update_metadata_error() -> None:
    client = _client()
    _mock(client, "patch").return_value = _resp(500, json={"message": "boom"})
    with pytest.raises(ServerError):
        await client.update_metadata("k", Metadata())


async def test_quic_update_metadata_not_found() -> None:
    client = _client()
    _mock(client, "patch").return_value = _resp(404)
    with pytest.raises(ObjectNotFoundError):
        await client.update_metadata("k", Metadata())


# =====================================================================
# health
# =====================================================================


async def test_quic_health_success() -> None:
    from objstore.models import HealthStatus

    client = _client()
    _mock(client, "get").return_value = _resp(200, json={"status": "SERVING",
                                                         "message": "ok"})
    result = await client.health()
    assert result.status == HealthStatus.SERVING


async def test_quic_health_error() -> None:
    client = _client()
    _mock(client, "get").return_value = _resp(503, json={"message": "down"})
    with pytest.raises(ServerError):
        await client.health()


# =====================================================================
# archive
# =====================================================================


async def test_quic_archive_success() -> None:
    client = _client()
    _mock(client, "post").return_value = _resp(200, json={"message": "archived"})
    assert (await client.archive("k", "s3", {"bucket": "b"})).success is True


async def test_quic_archive_error() -> None:
    client = _client()
    _mock(client, "post").return_value = _resp(500, json={"message": "boom"})
    with pytest.raises(ServerError):
        await client.archive("k", "s3", {})


# =====================================================================
# add_policy
# =====================================================================


async def test_quic_add_policy_success() -> None:
    client = _client()
    _mock(client, "post").return_value = _resp(201, json={"message": "added"})
    assert (await client.add_policy(_policy())).success is True


async def test_quic_add_policy_error() -> None:
    client = _client()
    _mock(client, "post").return_value = _resp(500, json={"message": "boom"})
    with pytest.raises(ServerError):
        await client.add_policy(_policy())


# =====================================================================
# remove_policy
# =====================================================================


async def test_quic_remove_policy_success() -> None:
    client = _client()
    _mock(client, "delete").return_value = _resp(200, json={"message": "removed"})
    assert (await client.remove_policy("p1")).success is True


async def test_quic_remove_policy_error() -> None:
    client = _client()
    _mock(client, "delete").return_value = _resp(500, json={"message": "boom"})
    with pytest.raises(ServerError):
        await client.remove_policy("p1")


async def test_quic_remove_policy_not_found() -> None:
    client = _client()
    _mock(client, "delete").return_value = _resp(404)
    with pytest.raises(ObjectNotFoundError):
        await client.remove_policy("p1")


# =====================================================================
# get_policies
# =====================================================================


async def test_quic_get_policies_success() -> None:
    client = _client()
    _mock(client, "get").return_value = _resp(200, json={
        "policies": [{"id": "p1", "prefix": "x/", "retention_seconds": 10,
                      "action": "delete"}], "message": "ok"})
    result = await client.get_policies()
    assert result.success is True
    assert len(result.policies) == 1


async def test_quic_get_policies_error() -> None:
    client = _client()
    _mock(client, "get").return_value = _resp(500, json={"message": "boom"})
    with pytest.raises(ServerError):
        await client.get_policies()


# =====================================================================
# apply_policies
# =====================================================================


async def test_quic_apply_policies_success() -> None:
    client = _client()
    _mock(client, "post").return_value = _resp(200, json={
        "policies_count": 3, "objects_processed": 100, "message": "applied"})
    result = await client.apply_policies()
    assert result.success is True
    assert result.policies_count == 3


async def test_quic_apply_policies_error() -> None:
    client = _client()
    _mock(client, "post").return_value = _resp(500, json={"message": "boom"})
    with pytest.raises(ServerError):
        await client.apply_policies()


# =====================================================================
# add_replication_policy
# =====================================================================


async def test_quic_add_replication_policy_success() -> None:
    client = _client()
    post = _mock(client, "post")
    post.return_value = _resp(201, json={"message": "added"})
    result = await client.add_replication_policy(_repl())
    assert result.success is True
    # check_interval_seconds is renamed to check_interval on the wire.
    payload = post.call_args.kwargs["json"]
    assert payload["check_interval"] == 30
    assert "check_interval_seconds" not in payload


async def test_quic_add_replication_policy_error() -> None:
    client = _client()
    _mock(client, "post").return_value = _resp(500, json={"message": "boom"})
    with pytest.raises(ServerError):
        await client.add_replication_policy(_repl())


# =====================================================================
# remove_replication_policy
# =====================================================================


async def test_quic_remove_replication_policy_success() -> None:
    client = _client()
    _mock(client, "delete").return_value = _resp(200, json={"message": "removed"})
    assert (await client.remove_replication_policy("r1")).success is True


async def test_quic_remove_replication_policy_error() -> None:
    client = _client()
    _mock(client, "delete").return_value = _resp(500, json={"message": "boom"})
    with pytest.raises(ServerError):
        await client.remove_replication_policy("r1")


async def test_quic_remove_replication_policy_not_found() -> None:
    client = _client()
    _mock(client, "delete").return_value = _resp(404)
    with pytest.raises(ObjectNotFoundError):
        await client.remove_replication_policy("r1")


# =====================================================================
# get_replication_policies
# =====================================================================


async def test_quic_get_replication_policies_success() -> None:
    client = _client()
    _mock(client, "get").return_value = _resp(200, json={
        "policies": [{"id": "r1", "source_backend": "local",
                      "destination_backend": "s3", "check_interval": 77}]})
    result = await client.get_replication_policies()
    assert len(result.policies) == 1
    assert result.policies[0].check_interval_seconds == 77


async def test_quic_get_replication_policies_error() -> None:
    client = _client()
    _mock(client, "get").return_value = _resp(500, json={"message": "boom"})
    with pytest.raises(ServerError):
        await client.get_replication_policies()


# =====================================================================
# get_replication_policy
# =====================================================================


async def test_quic_get_replication_policy_success() -> None:
    client = _client()
    _mock(client, "get").return_value = _resp(200, json={
        "success": True, "id": "r1", "source_backend": "local",
        "destination_backend": "s3", "check_interval": 300})
    policy = await client.get_replication_policy("r1")
    assert policy.id == "r1"
    assert policy.check_interval_seconds == 300


async def test_quic_get_replication_policy_error() -> None:
    client = _client()
    _mock(client, "get").return_value = _resp(500, json={"message": "boom"})
    with pytest.raises(ServerError):
        await client.get_replication_policy("r1")


async def test_quic_get_replication_policy_not_found() -> None:
    client = _client()
    _mock(client, "get").return_value = _resp(404)
    with pytest.raises(ObjectNotFoundError):
        await client.get_replication_policy("r1")


# =====================================================================
# trigger_replication
# =====================================================================


async def test_quic_trigger_replication_success() -> None:
    client = _client()
    post = _mock(client, "post")
    post.return_value = _resp(200, json={
        "result": {"policy_id": "r1", "synced": 100, "deleted": 5, "failed": 0,
                   "bytes_total": 1048576, "duration": "5s"}, "message": "ok"})
    result = await client.trigger_replication(_opts())
    assert result.success is True
    assert result.result.synced == 100
    assert result.result.duration_ms == 5000
    assert post.call_args.kwargs["params"] == {"policy_id": "r1"}


async def test_quic_trigger_replication_error() -> None:
    client = _client()
    _mock(client, "post").return_value = _resp(500, json={"message": "boom"})
    with pytest.raises(ServerError):
        await client.trigger_replication(_opts())


# =====================================================================
# get_replication_status
# =====================================================================


async def test_quic_get_replication_status_success() -> None:
    client = _client()
    _mock(client, "get").return_value = _resp(200, json={
        "success": True, "policy_id": "r1", "source_backend": "local",
        "destination_backend": "s3", "enabled": True,
        "total_objects_synced": 1000, "total_objects_deleted": 10,
        "total_bytes_synced": 10485760, "total_errors": 0,
        "average_sync_duration": "2s", "sync_count": 5, "message": "ok"})
    result = await client.get_replication_status("r1")
    assert result.success is True
    assert result.status.total_objects_synced == 1000
    assert result.status.average_sync_duration_ms == 2000


async def test_quic_get_replication_status_error() -> None:
    client = _client()
    _mock(client, "get").return_value = _resp(500, json={"message": "boom"})
    with pytest.raises(ServerError):
        await client.get_replication_status("r1")


async def test_quic_get_replication_status_not_found() -> None:
    client = _client()
    _mock(client, "get").return_value = _resp(404)
    with pytest.raises(ObjectNotFoundError):
        await client.get_replication_status("r1")


# =====================================================================
# metadata_round_trip
# =====================================================================


async def test_quic_metadata_round_trip() -> None:
    """content_type, content_encoding and custom map survive put -> HEAD.

    Wire scheme: PUT sets Content-Type, Content-Encoding and one
    ``X-Meta-<key>`` header per custom entry; metadata is read back via the
    HEAD response headers.
    """
    client = _client()
    custom = {"author": "carol", "project": "objstore"}

    put = _mock(client, "put")
    put.return_value = _resp(201, json={"message": "ok"}, headers={"ETag": "rt"})
    await client.put("rt", b"payload",
                     metadata=Metadata(content_type="application/json",
                                       content_encoding="gzip", custom=custom))

    sent = put.call_args.kwargs["headers"]
    assert sent["Content-Type"] == "application/json"
    assert sent["Content-Encoding"] == "gzip"
    assert sent["X-Meta-author"] == "carol"
    assert sent["X-Meta-project"] == "objstore"

    _mock(client, "head").return_value = _resp(200, headers={
        "Content-Type": "application/json", "Content-Encoding": "gzip",
        "X-Meta-author": "carol", "X-Meta-project": "objstore"})
    meta = await client.get_metadata("rt")
    assert meta.content_type == "application/json"
    assert meta.content_encoding == "gzip"
    assert meta.custom == custom


# =====================================================================
# validation_empty_key
# =====================================================================


async def test_quic_validation_empty_key() -> None:
    """An empty key is rejected by the server (HTTP 400 -> ValidationError)."""
    client = _client()
    _mock(client, "get").return_value = _resp(400, json={"message": "key must not be empty"})
    with pytest.raises(ValidationError):
        await client.get("")


# =====================================================================
# language-specific extras: construction, streaming, transport failures
# =====================================================================


async def test_quic_url_construction() -> None:
    client = _client()
    assert client._url("objects/k") == "https://localhost:4433/objects/k"
    assert client._url("/objects/k") == "https://localhost:4433/objects/k"
    assert client._url("health") == "https://localhost:4433/health"


async def test_quic_url_trailing_slash_base() -> None:
    client = QuicClient(base_url="https://localhost:4433/", api_version="v1")
    assert client.base_url == "https://localhost:4433"


async def test_quic_init_falls_back_to_http2() -> None:
    real = httpx.AsyncClient
    calls = {"n": 0}

    def fake(*a: object, **kw: object) -> httpx.AsyncClient:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("http/3 unavailable")
        return real(*a, **kw)

    with patch("objstore.quic_client.httpx.AsyncClient", side_effect=fake):
        client = QuicClient()
    assert client.client is not None
    assert calls["n"] == 2


async def test_quic_context_manager() -> None:
    with patch.object(QuicClient, "close", new_callable=AsyncMock) as mock_close:
        async with QuicClient() as client:
            assert client is not None
        mock_close.assert_called_once()


async def test_quic_close() -> None:
    client = _client()
    with patch.object(client.client, "aclose", new_callable=AsyncMock) as aclose:
        await client.close()
        aclose.assert_called_once()


async def test_quic_authentication_error() -> None:
    client = _client()
    _mock(client, "get").return_value = _resp(401)
    with pytest.raises(AuthenticationError):
        await client.get("k")


async def test_quic_generic_error_code() -> None:
    client = _client()
    r = _resp(418)
    r.text = "teapot"
    _mock(client, "get").return_value = r
    with pytest.raises(ObjectStoreError):
        await client.get("k")


async def test_quic_get_timeout() -> None:
    client = _client()
    _mock(client, "get").side_effect = httpx.TimeoutException("t")
    with pytest.raises(TimeoutError):
        await client.get("k")


async def test_quic_get_connection_error() -> None:
    client = _client()
    _mock(client, "get").side_effect = httpx.ConnectError("c")
    with pytest.raises(ConnectionError):
        await client.get("k")


async def test_quic_get_stream_success() -> None:
    client = _client()

    async def aiter(chunk_size: int = 8192):
        for c in (b"a", b"", b"bc"):
            yield c

    resp = MagicMock()
    resp.status_code = 200
    resp.aiter_bytes = aiter
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=resp)
    cm.__aexit__ = AsyncMock(return_value=None)
    with patch.object(client.client, "stream", return_value=cm):
        chunks = [c async for c in client.get_stream("k")]
    assert chunks == [b"a", b"bc"]


async def test_quic_get_stream_not_found() -> None:
    client = _client()

    async def aiter(chunk_size: int = 8192):
        if False:
            yield b""

    resp = MagicMock()
    resp.status_code = 404
    resp.aiter_bytes = aiter
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=resp)
    cm.__aexit__ = AsyncMock(return_value=None)
    with patch.object(client.client, "stream", return_value=cm):
        with pytest.raises(ObjectNotFoundError):
            async for _ in client.get_stream("k"):
                pass


async def test_quic_put_file_like_object() -> None:
    import io

    client = _client()
    put = _mock(client, "put")
    put.return_value = _resp(201, json={"message": "ok"}, headers={"ETag": "e"})
    result = await client.put("k", io.BytesIO(b"file data"))
    assert result.success is True
    assert put.call_args.kwargs["content"] == b"file data"


# =====================================================================
# transport-failure sweep (timeout + connect) for every operation
#
# The canonical matrix asserts one success + one error per op; this sweep
# additionally drives the per-method httpx.TimeoutException /
# httpx.ConnectError handlers so the client's transport-error mapping is
# fully covered. Kept compact and table-driven.
# =====================================================================


_HTTP_METHOD = {
    "put": "put",
    "get": "get",
    "delete": "delete",
    "list": "get",
    "exists": "get",
    "get_metadata": "head",
    "update_metadata": "patch",
    "health": "get",
    "archive": "post",
    "add_policy": "post",
    "remove_policy": "delete",
    "get_policies": "get",
    "apply_policies": "post",
    "add_replication_policy": "post",
    "remove_replication_policy": "delete",
    "get_replication_policies": "get",
    "get_replication_policy": "get",
    "trigger_replication": "post",
    "get_replication_status": "get",
}


def _invoke(client: QuicClient, op: str):
    args = {
        "put": ("k", b"d"),
        "get": ("k",),
        "delete": ("k",),
        "list": (),
        "exists": ("k",),
        "get_metadata": ("k",),
        "update_metadata": ("k", Metadata()),
        "health": (),
        "archive": ("k", "s3", {}),
        "add_policy": (_policy(),),
        "remove_policy": ("p1",),
        "get_policies": (),
        "apply_policies": (),
        "add_replication_policy": (_repl(),),
        "remove_replication_policy": ("r1",),
        "get_replication_policies": (),
        "get_replication_policy": ("r1",),
        "trigger_replication": (_opts(),),
        "get_replication_status": ("r1",),
    }[op]
    return getattr(client, op)(*args)


@pytest.mark.parametrize("op", list(_HTTP_METHOD))
async def test_quic_timeout_branch(op: str) -> None:
    client = _client()
    _mock(client, _HTTP_METHOD[op]).side_effect = httpx.TimeoutException("t")
    with pytest.raises(TimeoutError):
        await _invoke(client, op)


@pytest.mark.parametrize("op", list(_HTTP_METHOD))
async def test_quic_connection_error_branch(op: str) -> None:
    client = _client()
    _mock(client, _HTTP_METHOD[op]).side_effect = httpx.ConnectError("c")
    with pytest.raises(ConnectionError):
        await _invoke(client, op)


async def test_quic_get_stream_timeout() -> None:
    client = _client()
    with patch.object(client.client, "stream", side_effect=httpx.TimeoutException("t")):
        with pytest.raises(TimeoutError):
            async for _ in client.get_stream("k"):
                pass


async def test_quic_get_stream_connection_error() -> None:
    client = _client()
    with patch.object(client.client, "stream", side_effect=httpx.ConnectError("c")):
        with pytest.raises(ConnectionError):
            async for _ in client.get_stream("k"):
                pass


async def test_quic_health_generic_exception_maps_connection() -> None:
    client = _client()
    _mock(client, "get").side_effect = RuntimeError("weird")
    with pytest.raises(ConnectionError):
        await client.health()


async def test_quic_exists_non_json_200_defaults_true() -> None:
    client = _client()
    r = _resp(200)
    r.json.side_effect = ValueError("no json")
    _mock(client, "get").return_value = r
    assert (await client.exists("k")).exists is True


async def test_quic_delete_204_is_success() -> None:
    client = _client()
    _mock(client, "delete").return_value = _resp(204)
    assert (await client.delete("k")).success is True


async def test_quic_handle_error_text_fallback() -> None:
    client = _client()
    r = _resp(400)
    r.json.side_effect = ValueError("no json")
    r.text = "bad request text"
    _mock(client, "get").return_value = r
    with pytest.raises(ValidationError):
        await client.get("k")


async def test_quic_server_error_text_fallback() -> None:
    client = _client()
    r = _resp(500)
    r.json.side_effect = ValueError("no json")
    r.text = "server text"
    _mock(client, "get").return_value = r
    with pytest.raises(ServerError):
        await client.get("k")


async def test_quic_metadata_from_headers_invalid_values() -> None:
    client = _client()
    headers = httpx.Headers({
        "Content-Length": "not-a-number",
        "Last-Modified": "not-a-date",
        "X-Meta-foo": "bar",
    })
    meta = client._metadata_from_headers(headers)
    assert meta.size is None
    assert meta.last_modified is None
    assert meta.custom["foo"] == "bar"


async def test_quic_metadata_from_headers_valid_last_modified() -> None:
    client = _client()
    headers = httpx.Headers({"Last-Modified": "Wed, 21 Oct 2015 07:28:00 GMT"})
    meta = client._metadata_from_headers(headers)
    assert meta.last_modified is not None
    assert meta.last_modified.year == 2015


async def test_quic_go_duration_to_ms_variants() -> None:
    client = _client()
    assert client._go_duration_to_ms(None) == 0
    assert client._go_duration_to_ms(250) == 250
    assert client._go_duration_to_ms(1.0) == 1
    assert client._go_duration_to_ms({"bad": "type"}) == 0
    assert client._go_duration_to_ms("") == 0
    assert client._go_duration_to_ms("   ") == 0
    assert client._go_duration_to_ms("250ms") == 250
    assert client._go_duration_to_ms("1.5s") == 1500
    assert client._go_duration_to_ms("2m") == 120000
    assert client._go_duration_to_ms("1h30m") == 5400000
    assert client._go_duration_to_ms("500us") == 0
    assert client._go_duration_to_ms("2000us") == 2
    assert client._go_duration_to_ms("abc") == 0
    assert client._go_duration_to_ms("5xyz") == 0


async def test_quic_sync_result_from_dict_prefers_existing_ms() -> None:
    client = _client()
    result = client._sync_result_from_dict({
        "policy_id": "p", "synced": 1, "deleted": 0, "failed": 0,
        "bytes_total": 10, "duration_ms": 999, "duration": "5s",
    })
    assert result.duration_ms == 999


async def test_quic_replication_status_from_dict_prefers_existing_ms() -> None:
    client = _client()
    status = client._replication_status_from_dict({
        "policy_id": "p", "source_backend": "local", "destination_backend": "s3",
        "enabled": True, "total_objects_synced": 0, "total_objects_deleted": 0,
        "total_bytes_synced": 0, "total_errors": 0, "sync_count": 0,
        "average_sync_duration_ms": 42, "average_sync_duration": "5s",
    })
    assert status.average_sync_duration_ms == 42
