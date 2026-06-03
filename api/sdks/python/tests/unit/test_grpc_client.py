"""Canonical unit tests for the gRPC client.

Implements the SDK canonical test matrix for the gRPC transport:

- success + error path for all 19 operations
- not_found path for the 9 designated operations
- metadata_round_trip
- validation_empty_key

The generated proto stubs are not shipped in this checkout (so
``GRPC_AVAILABLE`` is False and the real ``__init__`` raises). Each test
builds the *real, frozen* ``GrpcClient`` by bypassing the constructor,
injects a fake stub, and patches a lightweight ``objstore_pb2`` onto the
module so request construction works. This exercises the genuine client
parsing/error-mapping code without a live server or generated protos.

Test names follow ``test_grpc_<op>_<variant>``.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import grpc
import pytest

import objstore.grpc_client as grpc_module
from objstore.grpc_client import GrpcClient
from objstore.exceptions import (
    ObjectNotFoundError,
    ObjectStoreError,
    ServerError,
)


# ---- helpers ---------------------------------------------------------


class _ProtoMessage(SimpleNamespace):
    """A namespace that mimics a protobuf message closely enough for tests.

    In particular ``custom`` defaults to a real dict so the client's
    ``proto_metadata.custom.update(...)`` call works, matching the generated
    map-field behaviour.
    """

    def __init__(self, **kw: Any) -> None:
        kw.setdefault("custom", {})
        super().__init__(**kw)


class _FakePB2:
    """Stand-in for the generated ``objstore_pb2`` module.

    Every accessed attribute (PutRequest, GetRequest, Metadata, ...) becomes
    a callable that accepts any keyword arguments and returns a message-like
    namespace, so the client's ``objstore_pb2.XxxRequest(...)`` calls succeed.
    """

    def __getattr__(self, name: str):
        return lambda **kw: _ProtoMessage(**kw)


@pytest.fixture()
def grpc_client(monkeypatch: pytest.MonkeyPatch) -> GrpcClient:
    """A real GrpcClient with a fake stub and patched proto module."""
    monkeypatch.setattr(grpc_module, "objstore_pb2", _FakePB2(), raising=False)
    client = object.__new__(GrpcClient)
    client.host = "localhost"
    client.port = 50051
    client.timeout = 30
    client.max_retries = 3
    client.channel = Mock()
    client.stub = Mock()
    return client


def _rpc_error(code: grpc.StatusCode, details: str = "boom") -> grpc.RpcError:
    err = grpc.RpcError()
    err.code = Mock(return_value=code)
    err.details = Mock(return_value=details)
    return err


def _not_found(details: str = "object missing") -> grpc.RpcError:
    return _rpc_error(grpc.StatusCode.NOT_FOUND, details)


def _internal(details: str = "boom") -> grpc.RpcError:
    return _rpc_error(grpc.StatusCode.INTERNAL, details)


def _meta(**kw: Any) -> SimpleNamespace:
    base = {
        "content_type": "text/plain",
        "content_encoding": "",
        "size": 3,
        "etag": "etag-1",
        "custom": {},
    }
    base.update(kw)
    ns = SimpleNamespace(**base)
    ns.HasField = Mock(return_value=False)
    return ns


# =====================================================================
# put
# =====================================================================


def test_grpc_put_success(grpc_client: GrpcClient) -> None:
    grpc_client.stub.Put.return_value = Mock(success=True, message="ok", etag="etag-1")
    result = grpc_client.put("k", b"abc")
    assert result.success is True
    assert result.etag == "etag-1"


def test_grpc_put_error(grpc_client: GrpcClient) -> None:
    grpc_client.stub.Put.side_effect = _internal()
    with pytest.raises(ServerError):
        grpc_client.put("k", b"abc")


# =====================================================================
# get
# =====================================================================


def test_grpc_get_success(grpc_client: GrpcClient) -> None:
    chunk = SimpleNamespace(data=b"hello")
    chunk.HasField = Mock(return_value=True)
    chunk.metadata = _meta()
    grpc_client.stub.Get.return_value = [chunk]
    data, metadata = grpc_client.get("k")
    assert data == b"hello"
    assert metadata.content_type == "text/plain"


def test_grpc_get_error(grpc_client: GrpcClient) -> None:
    grpc_client.stub.Get.side_effect = _internal()
    with pytest.raises(ServerError):
        grpc_client.get("k")


def test_grpc_get_not_found(grpc_client: GrpcClient) -> None:
    grpc_client.stub.Get.side_effect = _not_found()
    with pytest.raises(ObjectNotFoundError):
        grpc_client.get("k")


# =====================================================================
# delete
# =====================================================================


def test_grpc_delete_success(grpc_client: GrpcClient) -> None:
    grpc_client.stub.Delete.return_value = Mock(success=True, message="deleted")
    result = grpc_client.delete("k")
    assert result.success is True


def test_grpc_delete_error(grpc_client: GrpcClient) -> None:
    grpc_client.stub.Delete.side_effect = _internal()
    with pytest.raises(ServerError):
        grpc_client.delete("k")


def test_grpc_delete_not_found(grpc_client: GrpcClient) -> None:
    grpc_client.stub.Delete.side_effect = _not_found()
    with pytest.raises(ObjectNotFoundError):
        grpc_client.delete("k")


# =====================================================================
# list
# =====================================================================


def test_grpc_list_success(grpc_client: GrpcClient) -> None:
    obj1 = SimpleNamespace(key="k1", metadata=_meta())
    obj2 = SimpleNamespace(key="k2", metadata=_meta())
    grpc_client.stub.List.return_value = SimpleNamespace(
        objects=[obj1, obj2],
        common_prefixes=["p/"],
        next_token="",
        truncated=True,
    )
    result = grpc_client.list(prefix="p", max_results=10)
    assert [o.key for o in result.objects] == ["k1", "k2"]
    assert result.truncated is True


def test_grpc_list_error(grpc_client: GrpcClient) -> None:
    grpc_client.stub.List.side_effect = _internal()
    with pytest.raises(ServerError):
        grpc_client.list()


# =====================================================================
# exists
# =====================================================================


def test_grpc_exists_success(grpc_client: GrpcClient) -> None:
    grpc_client.stub.Exists.return_value = SimpleNamespace(exists=True)
    assert grpc_client.exists("k").exists is True


def test_grpc_exists_error(grpc_client: GrpcClient) -> None:
    grpc_client.stub.Exists.side_effect = _internal()
    with pytest.raises(ServerError):
        grpc_client.exists("k")


def test_grpc_exists_not_found(grpc_client: GrpcClient) -> None:
    # NOT_FOUND propagates through _handle_grpc_error as ObjectNotFoundError.
    grpc_client.stub.Exists.side_effect = _not_found()
    with pytest.raises(ObjectNotFoundError):
        grpc_client.exists("k")


# =====================================================================
# get_metadata
# =====================================================================


def test_grpc_get_metadata_success(grpc_client: GrpcClient) -> None:
    resp = SimpleNamespace(success=True, metadata=_meta(custom={"foo": "bar"}))
    resp.HasField = Mock(return_value=True)
    grpc_client.stub.GetMetadata.return_value = resp
    metadata = grpc_client.get_metadata("k")
    assert metadata.custom == {"foo": "bar"}


def test_grpc_get_metadata_error(grpc_client: GrpcClient) -> None:
    grpc_client.stub.GetMetadata.side_effect = _internal()
    with pytest.raises(ServerError):
        grpc_client.get_metadata("k")


def test_grpc_get_metadata_not_found(grpc_client: GrpcClient) -> None:
    grpc_client.stub.GetMetadata.side_effect = _not_found()
    with pytest.raises(ObjectNotFoundError):
        grpc_client.get_metadata("k")


# =====================================================================
# update_metadata
# =====================================================================


def test_grpc_update_metadata_success(grpc_client: GrpcClient) -> None:
    from objstore.models import Metadata

    grpc_client.stub.UpdateMetadata.return_value = Mock(success=True, message="ok")
    result = grpc_client.update_metadata("k", Metadata(custom={"foo": "bar"}))
    assert result.success is True


def test_grpc_update_metadata_error(grpc_client: GrpcClient) -> None:
    from objstore.models import Metadata

    grpc_client.stub.UpdateMetadata.side_effect = _internal()
    with pytest.raises(ServerError):
        grpc_client.update_metadata("k", Metadata())


def test_grpc_update_metadata_not_found(grpc_client: GrpcClient) -> None:
    from objstore.models import Metadata

    grpc_client.stub.UpdateMetadata.side_effect = _not_found()
    with pytest.raises(ObjectNotFoundError):
        grpc_client.update_metadata("k", Metadata())


# =====================================================================
# health
# =====================================================================


def test_grpc_health_success(grpc_client: GrpcClient) -> None:
    from objstore.models import HealthStatus

    grpc_client.stub.Health.return_value = SimpleNamespace(status=1, message="ok")
    result = grpc_client.health()
    assert result.status == HealthStatus.SERVING


def test_grpc_health_error(grpc_client: GrpcClient) -> None:
    grpc_client.stub.Health.side_effect = _internal()
    with pytest.raises(ServerError):
        grpc_client.health()


# =====================================================================
# archive
# =====================================================================


def test_grpc_archive_success(grpc_client: GrpcClient) -> None:
    grpc_client.stub.Archive.return_value = Mock(success=True, message="archived")
    result = grpc_client.archive("k", "s3", {"bucket": "b"})
    assert result.success is True


def test_grpc_archive_error(grpc_client: GrpcClient) -> None:
    grpc_client.stub.Archive.side_effect = _internal()
    with pytest.raises(ServerError):
        grpc_client.archive("k", "s3", {})


# =====================================================================
# add_policy
# =====================================================================


def _lifecycle_policy():
    from objstore.models import LifecyclePolicy

    return LifecyclePolicy(
        id="p1", prefix="x/", retention_seconds=10, action="delete"
    )


def test_grpc_add_policy_success(grpc_client: GrpcClient) -> None:
    grpc_client.stub.AddPolicy.return_value = Mock(success=True, message="added")
    result = grpc_client.add_policy(_lifecycle_policy())
    assert result.success is True


def test_grpc_add_policy_error(grpc_client: GrpcClient) -> None:
    grpc_client.stub.AddPolicy.side_effect = _internal()
    with pytest.raises(ServerError):
        grpc_client.add_policy(_lifecycle_policy())


# =====================================================================
# remove_policy
# =====================================================================


def test_grpc_remove_policy_success(grpc_client: GrpcClient) -> None:
    grpc_client.stub.RemovePolicy.return_value = Mock(success=True, message="removed")
    result = grpc_client.remove_policy("p1")
    assert result.success is True


def test_grpc_remove_policy_error(grpc_client: GrpcClient) -> None:
    grpc_client.stub.RemovePolicy.side_effect = _internal()
    with pytest.raises(ServerError):
        grpc_client.remove_policy("p1")


def test_grpc_remove_policy_not_found(grpc_client: GrpcClient) -> None:
    grpc_client.stub.RemovePolicy.side_effect = _not_found()
    with pytest.raises(ObjectNotFoundError):
        grpc_client.remove_policy("p1")


# =====================================================================
# get_policies
# =====================================================================


def test_grpc_get_policies_success(grpc_client: GrpcClient) -> None:
    proto_policy = SimpleNamespace(
        id="p1",
        prefix="x/",
        retention_seconds=10,
        action="delete",
        destination_type="",
        destination_settings={},
    )
    grpc_client.stub.GetPolicies.return_value = SimpleNamespace(
        policies=[proto_policy], success=True, message="ok"
    )
    result = grpc_client.get_policies()
    assert result.success is True
    assert len(result.policies) == 1


def test_grpc_get_policies_error(grpc_client: GrpcClient) -> None:
    grpc_client.stub.GetPolicies.side_effect = _internal()
    with pytest.raises(ServerError):
        grpc_client.get_policies()


# =====================================================================
# apply_policies
# =====================================================================


def test_grpc_apply_policies_success(grpc_client: GrpcClient) -> None:
    grpc_client.stub.ApplyPolicies.return_value = SimpleNamespace(
        success=True, policies_count=2, objects_processed=5, message="ok"
    )
    result = grpc_client.apply_policies()
    assert result.success is True
    assert result.policies_count == 2


def test_grpc_apply_policies_error(grpc_client: GrpcClient) -> None:
    grpc_client.stub.ApplyPolicies.side_effect = _internal()
    with pytest.raises(ServerError):
        grpc_client.apply_policies()


# =====================================================================
# add_replication_policy
# =====================================================================


def _replication_policy():
    from objstore.models import ReplicationPolicy

    return ReplicationPolicy(
        id="r1",
        source_backend="local",
        destination_backend="s3",
        check_interval_seconds=30,
    )


def test_grpc_add_replication_policy_success(grpc_client: GrpcClient) -> None:
    grpc_client.stub.AddReplicationPolicy.return_value = Mock(
        success=True, message="added"
    )
    result = grpc_client.add_replication_policy(_replication_policy())
    assert result.success is True


def test_grpc_add_replication_policy_error(grpc_client: GrpcClient) -> None:
    grpc_client.stub.AddReplicationPolicy.side_effect = _internal()
    with pytest.raises(ServerError):
        grpc_client.add_replication_policy(_replication_policy())


# =====================================================================
# remove_replication_policy
# =====================================================================


def test_grpc_remove_replication_policy_success(grpc_client: GrpcClient) -> None:
    grpc_client.stub.RemoveReplicationPolicy.return_value = Mock(
        success=True, message="removed"
    )
    result = grpc_client.remove_replication_policy("r1")
    assert result.success is True


def test_grpc_remove_replication_policy_error(grpc_client: GrpcClient) -> None:
    grpc_client.stub.RemoveReplicationPolicy.side_effect = _internal()
    with pytest.raises(ServerError):
        grpc_client.remove_replication_policy("r1")


def test_grpc_remove_replication_policy_not_found(grpc_client: GrpcClient) -> None:
    grpc_client.stub.RemoveReplicationPolicy.side_effect = _not_found()
    with pytest.raises(ObjectNotFoundError):
        grpc_client.remove_replication_policy("r1")


# =====================================================================
# get_replication_policies
# =====================================================================


def _proto_repl_policy():
    return SimpleNamespace(
        id="r1",
        source_backend="local",
        source_settings={},
        source_prefix="",
        destination_backend="s3",
        destination_settings={},
        check_interval_seconds=30,
        enabled=True,
    )


def test_grpc_get_replication_policies_success(grpc_client: GrpcClient) -> None:
    grpc_client.stub.GetReplicationPolicies.return_value = SimpleNamespace(
        policies=[_proto_repl_policy()]
    )
    result = grpc_client.get_replication_policies()
    assert len(result.policies) == 1


def test_grpc_get_replication_policies_error(grpc_client: GrpcClient) -> None:
    grpc_client.stub.GetReplicationPolicies.side_effect = _internal()
    with pytest.raises(ServerError):
        grpc_client.get_replication_policies()


# =====================================================================
# get_replication_policy
# =====================================================================


def test_grpc_get_replication_policy_success(grpc_client: GrpcClient) -> None:
    resp = SimpleNamespace(success=True, policy=_proto_repl_policy())
    resp.HasField = Mock(return_value=True)
    grpc_client.stub.GetReplicationPolicy.return_value = resp
    policy = grpc_client.get_replication_policy("r1")
    assert policy.id == "r1"


def test_grpc_get_replication_policy_error(grpc_client: GrpcClient) -> None:
    grpc_client.stub.GetReplicationPolicy.side_effect = _internal()
    with pytest.raises(ServerError):
        grpc_client.get_replication_policy("r1")


def test_grpc_get_replication_policy_not_found(grpc_client: GrpcClient) -> None:
    grpc_client.stub.GetReplicationPolicy.side_effect = _not_found()
    with pytest.raises(ObjectNotFoundError):
        grpc_client.get_replication_policy("r1")


# =====================================================================
# trigger_replication
# =====================================================================


def _trigger_opts():
    from objstore.models import TriggerReplicationOptions

    return TriggerReplicationOptions(policy_id="r1")


def test_grpc_trigger_replication_success(grpc_client: GrpcClient) -> None:
    result_proto = SimpleNamespace(
        policy_id="r1",
        synced=10,
        deleted=1,
        failed=0,
        bytes_total=100,
        duration_ms=50,
        errors=[],
    )
    resp = SimpleNamespace(success=True, result=result_proto, message="ok")
    resp.HasField = Mock(return_value=True)
    grpc_client.stub.TriggerReplication.return_value = resp
    result = grpc_client.trigger_replication(_trigger_opts())
    assert result.success is True
    assert result.result.synced == 10


def test_grpc_trigger_replication_error(grpc_client: GrpcClient) -> None:
    grpc_client.stub.TriggerReplication.side_effect = _internal()
    with pytest.raises(ServerError):
        grpc_client.trigger_replication(_trigger_opts())


# =====================================================================
# get_replication_status
# =====================================================================


def _proto_status():
    s = SimpleNamespace(
        policy_id="r1",
        source_backend="local",
        destination_backend="s3",
        enabled=True,
        total_objects_synced=5,
        total_objects_deleted=0,
        total_bytes_synced=100,
        total_errors=0,
        average_sync_duration_ms=10,
        sync_count=2,
    )
    s.HasField = Mock(return_value=False)
    return s


def test_grpc_get_replication_status_success(grpc_client: GrpcClient) -> None:
    resp = SimpleNamespace(success=True, status=_proto_status(), message="ok")
    resp.HasField = Mock(return_value=True)
    grpc_client.stub.GetReplicationStatus.return_value = resp
    result = grpc_client.get_replication_status("r1")
    assert result.success is True
    assert result.status.total_objects_synced == 5


def test_grpc_get_replication_status_error(grpc_client: GrpcClient) -> None:
    grpc_client.stub.GetReplicationStatus.side_effect = _internal()
    with pytest.raises(ServerError):
        grpc_client.get_replication_status("r1")


def test_grpc_get_replication_status_not_found(grpc_client: GrpcClient) -> None:
    grpc_client.stub.GetReplicationStatus.side_effect = _not_found()
    with pytest.raises(ObjectNotFoundError):
        grpc_client.get_replication_status("r1")


# =====================================================================
# metadata_round_trip
# =====================================================================


def test_grpc_metadata_round_trip(grpc_client: GrpcClient) -> None:
    """Custom metadata + content_type/encoding travel in proto message fields.

    update_metadata sends a Metadata proto built from the model; get_metadata
    parses a Metadata proto back. Assert all three pieces survive.
    """
    from objstore.models import Metadata

    sent = Metadata(
        content_type="application/json",
        content_encoding="gzip",
        custom={"author": "alice", "team": "infra"},
    )

    captured = {}

    def _update(request, timeout=None):
        captured["proto"] = request.metadata
        return Mock(success=True, message="ok")

    grpc_client.stub.UpdateMetadata.side_effect = _update
    grpc_client.update_metadata("k", sent)

    # The proto built by the client carries content_type/encoding and custom.
    proto = captured["proto"]
    assert proto.content_type == "application/json"
    assert proto.content_encoding == "gzip"
    assert dict(proto.custom) == {"author": "alice", "team": "infra"}

    # And get_metadata parses an equivalent proto back into the model.
    resp = SimpleNamespace(success=True, metadata=_meta(
        content_type="application/json",
        content_encoding="gzip",
        custom={"author": "alice", "team": "infra"},
    ))
    resp.HasField = Mock(return_value=True)
    grpc_client.stub.GetMetadata.return_value = resp
    got = grpc_client.get_metadata("k")
    assert got.content_type == "application/json"
    assert got.content_encoding == "gzip"
    assert got.custom == {"author": "alice", "team": "infra"}


# =====================================================================
# validation_empty_key
# =====================================================================


def test_grpc_validation_empty_key(grpc_client: GrpcClient) -> None:
    """An empty key is rejected by the server as INVALID_ARGUMENT -> error."""
    grpc_client.stub.Get.side_effect = _rpc_error(
        grpc.StatusCode.INVALID_ARGUMENT, "key must not be empty"
    )
    with pytest.raises(ObjectStoreError):
        grpc_client.get("")


# =====================================================================
# construction / lifecycle / error mapping
# =====================================================================


def test_grpc_import_without_protos() -> None:
    """Without generated protos the constructor raises a helpful ImportError."""
    from objstore.grpc_client import GRPC_AVAILABLE

    if not GRPC_AVAILABLE:
        with pytest.raises(ImportError, match="gRPC support requires proto files"):
            GrpcClient()
    else:  # pragma: no cover - depends on environment
        client = GrpcClient(host="localhost", port=50051)
        assert client.host == "localhost"


def test_grpc_close(grpc_client: GrpcClient) -> None:
    grpc_client.close()
    grpc_client.channel.close.assert_called_once()


def test_grpc_error_mapping_unavailable(grpc_client: GrpcClient) -> None:
    from objstore.exceptions import ConnectionError as ObjConnectionError

    grpc_client.stub.Get.side_effect = _rpc_error(
        grpc.StatusCode.UNAVAILABLE, "down"
    )
    with pytest.raises(ObjConnectionError):
        grpc_client.get("k")


def test_grpc_error_mapping_deadline(grpc_client: GrpcClient) -> None:
    from objstore.exceptions import TimeoutError as ObjTimeoutError

    grpc_client.stub.Get.side_effect = _rpc_error(
        grpc.StatusCode.DEADLINE_EXCEEDED, "slow"
    )
    with pytest.raises(ObjTimeoutError):
        grpc_client.get("k")


def test_grpc_error_mapping_generic(grpc_client: GrpcClient) -> None:
    grpc_client.stub.Get.side_effect = _rpc_error(
        grpc.StatusCode.PERMISSION_DENIED, "nope"
    )
    with pytest.raises(ObjectStoreError):
        grpc_client.get("k")


# =====================================================================
# streaming + metadata-conversion coverage extras
# =====================================================================


def test_grpc_get_stream_success(grpc_client: GrpcClient) -> None:
    """get_stream yields the data field of each streamed GetResponse chunk."""
    c1 = SimpleNamespace(data=b"ab")
    c2 = SimpleNamespace(data=b"cd")
    grpc_client.stub.Get.return_value = [c1, c2]
    chunks = list(grpc_client.get_stream("k"))
    assert b"".join(chunks) == b"abcd"


def test_grpc_get_stream_error(grpc_client: GrpcClient) -> None:
    grpc_client.stub.Get.side_effect = _internal()
    with pytest.raises(ServerError):
        list(grpc_client.get_stream("k"))


def test_grpc_get_stream_not_found(grpc_client: GrpcClient) -> None:
    grpc_client.stub.Get.side_effect = _not_found()
    with pytest.raises(ObjectNotFoundError):
        list(grpc_client.get_stream("k"))


def test_grpc_proto_to_metadata_with_timestamp(grpc_client: GrpcClient) -> None:
    """A Metadata proto with last_modified set is parsed into a datetime."""
    from datetime import datetime

    ts = SimpleNamespace(seconds=1609459200)  # 2021-01-01T00:00:00Z
    proto = SimpleNamespace(
        content_type="text/plain", content_encoding="gzip", size=5,
        etag="e1", custom={"k": "v"}, last_modified=ts,
    )
    proto.HasField = Mock(return_value=True)
    metadata = grpc_client._proto_to_metadata(proto)
    assert metadata.content_encoding == "gzip"
    assert metadata.custom == {"k": "v"}
    assert metadata.last_modified == datetime.fromtimestamp(ts.seconds)


def test_grpc_get_returns_metadata_from_stream(grpc_client: GrpcClient) -> None:
    """The metadata carried on a streamed chunk is parsed and returned."""
    chunk = SimpleNamespace(data=b"hello")
    chunk.HasField = Mock(return_value=True)
    chunk.metadata = _meta(content_type="application/json", etag="e9")
    grpc_client.stub.Get.return_value = [chunk]
    data, metadata = grpc_client.get("k")
    assert data == b"hello"
    assert metadata.content_type == "application/json"
    assert metadata.etag == "e9"


def test_grpc_replication_status_with_last_sync_time(grpc_client: GrpcClient) -> None:
    """get_replication_status parses a status whose last_sync_time is set."""
    from datetime import datetime

    ts = SimpleNamespace(seconds=1609459200)
    status = SimpleNamespace(
        policy_id="r1", source_backend="local", destination_backend="s3",
        enabled=True, total_objects_synced=5, total_objects_deleted=0,
        total_bytes_synced=100, total_errors=0, average_sync_duration_ms=10,
        sync_count=2, last_sync_time=ts,
    )
    status.HasField = Mock(return_value=True)
    resp = SimpleNamespace(success=True, status=status, message="ok")
    resp.HasField = Mock(return_value=True)
    grpc_client.stub.GetReplicationStatus.return_value = resp
    result = grpc_client.get_replication_status("r1")
    assert result.status.last_sync_time == datetime.fromtimestamp(ts.seconds)
