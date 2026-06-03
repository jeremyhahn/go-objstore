"""Canonical unit tests for the unified ObjectStoreClient.

Implements the unified-client portion of the SDK canonical matrix:

- delegation: for each of REST / gRPC / QUIC the unified client forwards a
  representative call to the right protocol client.
- close: close()/dispose releases resources without error and is safe.

Plus a small set of construction/representative-op delegation checks so the
unified facade's branch logic (sync vs async dispatch) is fully exercised.

Test names follow ``test_unified_<aspect>`` / ``test_unified_<op>_<proto>``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import responses

from objstore.client import ObjectStoreClient, Protocol
from objstore.exceptions import ValidationError
from objstore.grpc_client import GrpcClient
from objstore.models import (
    ArchiveResponse,
    DeleteResponse,
    ExistsResponse,
    HealthResponse,
    HealthStatus,
    Metadata,
    PutResponse,
)
from objstore.quic_client import QuicClient
from objstore.rest_client import RestClient


BASE = "http://localhost:8080"
API = f"{BASE}/api/v1"


# =====================================================================
# construction / backend selection
# =====================================================================


def test_unified_init_rest_selects_rest_client() -> None:
    client = ObjectStoreClient(protocol=Protocol.REST, base_url=BASE)
    assert client.protocol == Protocol.REST
    assert isinstance(client._client, RestClient)


def test_unified_init_quic_selects_quic_client() -> None:
    client = ObjectStoreClient(protocol=Protocol.QUIC, base_url="https://localhost:4433")
    assert client.protocol == Protocol.QUIC
    assert isinstance(client._client, QuicClient)


def test_unified_init_grpc_selects_grpc_client_or_raises() -> None:
    # gRPC needs generated protos; without them construction raises ImportError.
    from objstore.grpc_client import GRPC_AVAILABLE

    if GRPC_AVAILABLE:
        client = ObjectStoreClient(protocol=Protocol.GRPC, host="localhost", port=50051)
        assert isinstance(client._client, GrpcClient)
    else:
        with pytest.raises(ImportError):
            ObjectStoreClient(protocol=Protocol.GRPC, host="localhost", port=50051)


def test_unified_init_default_values() -> None:
    client = ObjectStoreClient(protocol=Protocol.REST)
    assert client.timeout == 30
    assert client.max_retries == 3


def test_unified_unsupported_protocol_raises() -> None:
    with pytest.raises(ValidationError):
        ObjectStoreClient(protocol="ftp")  # type: ignore[arg-type]


# =====================================================================
# delegation — REST (representative ops via the real REST client + responses)
# =====================================================================


@responses.activate
def test_unified_put_rest() -> None:
    responses.add(responses.PUT, f"{API}/objects/k",
                  json={"message": "ok", "data": {"etag": "e1"}}, status=201)
    client = ObjectStoreClient(protocol=Protocol.REST)
    result = client.put("k", b"data")
    assert isinstance(result, PutResponse)
    assert result.success is True


@responses.activate
def test_unified_get_rest() -> None:
    responses.add(responses.GET, f"{API}/objects/k", body=b"data",
                  headers={"Content-Type": "text/plain", "Content-Length": "4"}, status=200)
    client = ObjectStoreClient(protocol=Protocol.REST)
    data, _ = client.get("k")
    assert data == b"data"


@responses.activate
def test_unified_health_rest() -> None:
    responses.add(responses.GET, f"{BASE}/health", json={"status": "SERVING"}, status=200)
    client = ObjectStoreClient(protocol=Protocol.REST)
    assert client.health().status == HealthStatus.SERVING


@responses.activate
def test_unified_delete_rest() -> None:
    responses.add(responses.DELETE, f"{API}/objects/k", json={"message": "deleted"}, status=200)
    client = ObjectStoreClient(protocol=Protocol.REST)
    assert isinstance(client.delete("k"), DeleteResponse)


@responses.activate
def test_unified_get_stream_rest() -> None:
    responses.add(responses.GET, f"{API}/objects/k", body=b"abc", status=200)
    client = ObjectStoreClient(protocol=Protocol.REST)
    assert b"".join(client.get_stream("k")) == b"abc"


# =====================================================================
# delegation — gRPC (representative call delegated to the gRPC backend)
# =====================================================================


def test_unified_delegates_grpc() -> None:
    """A gRPC unified client forwards a representative call to GrpcClient.

    Construction is patched so no real channel/protos are needed; the point
    is that ObjectStoreClient dispatches synchronously to the gRPC backend.
    """
    fake_backend = MagicMock()
    fake_backend.health.return_value = HealthResponse(status=HealthStatus.SERVING)

    with patch("objstore.client.GrpcClient", return_value=fake_backend):
        client = ObjectStoreClient(protocol=Protocol.GRPC, host="localhost", port=50051)
        result = client.health()

    assert result.status == HealthStatus.SERVING
    fake_backend.health.assert_called_once_with()


def test_unified_put_grpc() -> None:
    fake_backend = MagicMock()
    fake_backend.put.return_value = PutResponse(success=True, etag="e1")
    with patch("objstore.client.GrpcClient", return_value=fake_backend):
        client = ObjectStoreClient(protocol=Protocol.GRPC, host="localhost", port=50051)
        result = client.put("k", b"data")
    assert result.success is True
    fake_backend.put.assert_called_once()


# =====================================================================
# delegation — QUIC (representative call routed through the async bridge)
# =====================================================================


def test_unified_delegates_quic() -> None:
    """A QUIC unified client awaits a representative call on QuicClient."""
    client = ObjectStoreClient(protocol=Protocol.QUIC)
    with patch.object(client._client, "health", new_callable=AsyncMock) as mock_health:
        mock_health.return_value = HealthResponse(status=HealthStatus.SERVING)
        result = client.health()
    assert result.status == HealthStatus.SERVING


def test_unified_put_quic() -> None:
    client = ObjectStoreClient(protocol=Protocol.QUIC)
    with patch.object(client._client, "put", new_callable=AsyncMock) as mock_put:
        mock_put.return_value = PutResponse(success=True, etag="e1")
        result = client.put("k", b"data")
    assert result.success is True


def test_unified_get_quic() -> None:
    client = ObjectStoreClient(protocol=Protocol.QUIC)
    with patch.object(client._client, "get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = (b"data", Metadata(content_type="text/plain"))
        data, _ = client.get("k")
    assert data == b"data"


def test_unified_exists_quic() -> None:
    client = ObjectStoreClient(protocol=Protocol.QUIC)
    with patch.object(client._client, "exists", new_callable=AsyncMock) as mock_exists:
        mock_exists.return_value = ExistsResponse(exists=True)
        assert client.exists("k").exists is True


def test_unified_archive_quic() -> None:
    client = ObjectStoreClient(protocol=Protocol.QUIC)
    with patch.object(client._client, "archive", new_callable=AsyncMock) as mock_archive:
        mock_archive.return_value = ArchiveResponse(success=True)
        assert client.archive("k", "s3", {"bucket": "b"}).success is True


def test_unified_get_stream_quic() -> None:
    client = ObjectStoreClient(protocol=Protocol.QUIC)

    async def stream(key: str):
        for chunk in (b"a", b"bc"):
            yield chunk

    with patch.object(client._client, "get_stream", side_effect=stream):
        chunks = list(client.get_stream("k"))
    assert b"".join(chunks) == b"abc"


def test_unified_run_async_from_running_loop() -> None:
    """_run_async dispatches to a worker thread when an event loop is running.

    Calling a sync QUIC op from inside an active asyncio loop exercises the
    ``get_running_loop`` / threaded-bridge branch of the unified facade.
    """
    import asyncio

    client = ObjectStoreClient(protocol=Protocol.QUIC)

    with patch.object(client._client, "health", new_callable=AsyncMock) as mock_health:
        mock_health.return_value = HealthResponse(status=HealthStatus.SERVING)

        async def driver() -> HealthResponse:
            # A loop is running here, so _run_async must use the thread path.
            return client.health()

        result = asyncio.run(driver())

    assert result.status == HealthStatus.SERVING


# =====================================================================
# close
# =====================================================================


def test_unified_close_rest() -> None:
    client = ObjectStoreClient(protocol=Protocol.REST)
    with patch.object(client._client, "close") as mock_close:
        client.close()
        mock_close.assert_called_once_with()


def test_unified_close_quic() -> None:
    client = ObjectStoreClient(protocol=Protocol.QUIC)
    with patch.object(client._client, "close", new_callable=AsyncMock) as mock_close:
        client.close()
        mock_close.assert_called_once_with()


def test_unified_context_manager_rest() -> None:
    with ObjectStoreClient(protocol=Protocol.REST) as client:
        assert client is not None


# =====================================================================
# Protocol enum
# =====================================================================


def test_unified_protocol_enum_values() -> None:
    assert Protocol.REST.value == "rest"
    assert Protocol.GRPC.value == "grpc"
    assert Protocol.QUIC.value == "quic"


# =====================================================================
# delegation sweep — every logical op forwards to the active backend
#
# The canonical matrix only requires a representative delegated call per
# protocol; this sweep covers the unified facade's per-op dispatch lines for
# both the synchronous (REST/gRPC) and async-bridged (QUIC) paths so the
# thin forwarding methods are fully exercised.
# =====================================================================


def _call(client: ObjectStoreClient, op: str):
    """Invoke a unified-client op with representative arguments."""
    from objstore.models import (
        LifecyclePolicy,
        Metadata,
        ReplicationPolicy,
        TriggerReplicationOptions,
    )

    lifecycle = LifecyclePolicy(id="p1", prefix="x/", retention_seconds=10,
                               action="delete")
    repl = ReplicationPolicy(id="r1", source_backend="local",
                            destination_backend="s3", check_interval_seconds=30)
    opts = TriggerReplicationOptions(policy_id="r1")
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
        "add_policy": (lifecycle,),
        "remove_policy": ("p1",),
        "get_policies": (),
        "apply_policies": (),
        "add_replication_policy": (repl,),
        "remove_replication_policy": ("r1",),
        "get_replication_policies": (),
        "get_replication_policy": ("r1",),
        "trigger_replication": (opts,),
        "get_replication_status": ("r1",),
    }[op]
    return getattr(client, op)(*args)


_OPS = [
    "put", "get", "delete", "list", "exists", "get_metadata", "update_metadata",
    "health", "archive", "add_policy", "remove_policy", "get_policies",
    "apply_policies", "add_replication_policy", "remove_replication_policy",
    "get_replication_policies", "get_replication_policy", "trigger_replication",
    "get_replication_status",
]


@pytest.mark.parametrize("op", _OPS)
def test_unified_rest_delegates(op: str) -> None:
    """Each REST op forwards synchronously to the REST backend."""
    client = ObjectStoreClient(protocol=Protocol.REST)
    with patch.object(client._client, op, return_value="sentinel") as mock_op:
        assert _call(client, op) == "sentinel"
        mock_op.assert_called_once()


@pytest.mark.parametrize("op", _OPS)
def test_unified_quic_delegates(op: str) -> None:
    """Each QUIC op forwards through the async bridge to the QUIC backend."""
    client = ObjectStoreClient(protocol=Protocol.QUIC)
    with patch.object(client._client, op, new_callable=AsyncMock) as mock_op:
        mock_op.return_value = "sentinel"
        assert _call(client, op) == "sentinel"
        mock_op.assert_awaited_once()


def test_unified_grpc_uses_default_host_port() -> None:
    """gRPC construction with neither host nor port falls back to defaults."""
    captured = {}

    def _factory(host: str, port: int, timeout: int, max_retries: int):
        captured["host"] = host
        captured["port"] = port
        return MagicMock()

    with patch("objstore.client.GrpcClient", side_effect=_factory):
        ObjectStoreClient(protocol=Protocol.GRPC)

    assert captured["host"] == "localhost"
    assert captured["port"] == 50051


def test_unified_rest_uses_default_base_url() -> None:
    client = ObjectStoreClient(protocol=Protocol.REST)
    assert client._client.base_url == "http://localhost:8080"


def test_unified_quic_uses_default_base_url() -> None:
    client = ObjectStoreClient(protocol=Protocol.QUIC)
    assert client._client.base_url == "https://localhost:4433"


def test_unified_close_quic_closes_event_loop() -> None:
    """close() on QUIC also tears down a reused event loop if one exists."""
    import asyncio

    client = ObjectStoreClient(protocol=Protocol.QUIC)
    loop = asyncio.new_event_loop()
    client._event_loop = loop
    with patch.object(client._client, "close", new_callable=AsyncMock):
        client.close()
    assert loop.is_closed()
