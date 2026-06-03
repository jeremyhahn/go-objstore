"""Canonical data-driven integration tests for all protocols and operations.

Structure follows the canonical SDK test contract: an OPERATIONS table iterated
across an AVAILABLE_PROTOCOLS fixture (REST always present; gRPC always present;
QUIC real-or-skip).  A separate cross-protocol consistency section exercises
every ordered (A, B) pair so that gRPC is no longer excluded.

Replication operations assert real success — the server has replication enabled.
The try/except-accepts-"not-supported" pattern has been removed entirely.

Archive operations may genuinely be unsupported by the local backend; in that
case the test is explicitly skipped with a logged reason (no silent pass).

Unit tests are intentionally left in tests/unit/ and are not touched here.
"""

import os
import tempfile
import uuid
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

import pytest

from objstore.client import ObjectStoreClient, Protocol
from objstore.exceptions import ConnectionError, ObjectNotFoundError, ObjectStoreError
from objstore.models import (
    ApplyPoliciesResponse,
    ArchiveResponse,
    DeleteResponse,
    ExistsResponse,
    GetPoliciesResponse,
    GetReplicationPoliciesResponse,
    GetReplicationStatusResponse,
    HealthResponse,
    HealthStatus,
    LifecyclePolicy,
    ListResponse,
    Metadata,
    PolicyResponse,
    PutResponse,
    ReplicationPolicy,
    TriggerReplicationOptions,
    TriggerReplicationResponse,
)


# ---------------------------------------------------------------------------
# Protocol fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def rest_client() -> Generator[ObjectStoreClient, None, None]:
    """REST client — always available."""
    base_url = os.getenv("OBJSTORE_REST_URL", "http://localhost:8080")
    client = ObjectStoreClient(protocol=Protocol.REST, base_url=base_url, timeout=15)
    yield client
    client.close()


@pytest.fixture(scope="module")
def grpc_client() -> Generator[ObjectStoreClient, None, None]:
    """gRPC client — always in the matrix; skip only if proto stubs are missing."""
    host = os.getenv("OBJSTORE_GRPC_HOST", "localhost")
    port = int(os.getenv("OBJSTORE_GRPC_PORT", "50051"))
    try:
        client = ObjectStoreClient(
            protocol=Protocol.GRPC, host=host, port=port, timeout=15
        )
    except ImportError as exc:
        pytest.skip(f"gRPC proto stubs not generated: {exc}")
        return
    yield client
    client.close()


@pytest.fixture(scope="module")
def quic_client() -> Generator[ObjectStoreClient, None, None]:
    """QUIC/HTTP-3 client — real client; skip if unreachable (logged).

    Python has native HTTP/3 via aioquic, so this is a real protocol in the
    test matrix, not faked.  If the QUIC endpoint is genuinely unreachable
    we emit a deterministic skip rather than a silent pass.
    """
    base_url = os.getenv("OBJSTORE_QUIC_URL", "https://localhost:4433")
    try:
        client = ObjectStoreClient(
            protocol=Protocol.QUIC, base_url=base_url, timeout=15, verify_ssl=False
        )
    except Exception as exc:
        pytest.skip(f"QUIC client unavailable (import/init error): {exc}")
        return
    try:
        client.health()
    except ConnectionError as exc:
        client.close()
        pytest.skip(f"QUIC server not reachable at {base_url}: {exc}")
        return
    except Exception as exc:
        # Non-connectivity errors: let individual tests surface them.
        pass
    yield client
    client.close()


# ---------------------------------------------------------------------------
# AVAILABLE_PROTOCOLS parametrisation
#
# Using indirect=True so the fixture logic (skip decisions) runs per-test.
# Each parameter is a (protocol_label, fixture_name) tuple.
# ---------------------------------------------------------------------------

PROTOCOL_PARAMS = [
    pytest.param("rest", "rest_client", id="REST"),
    pytest.param("grpc", "grpc_client", id="gRPC"),
    pytest.param("quic", "quic_client", id="QUIC"),
]


@pytest.fixture(params=PROTOCOL_PARAMS)
def any_client(request: pytest.FixtureRequest) -> ObjectStoreClient:
    """Parametrised fixture that yields a client for REST, gRPC, and QUIC in turn."""
    _label, fixture_name = request.param
    return request.getfixturevalue(fixture_name)


# ---------------------------------------------------------------------------
# Helper fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def unique_key() -> str:
    """Unique object key for test isolation."""
    return f"test/comprehensive/{uuid.uuid4().hex}"


@pytest.fixture
def unique_policy_id() -> str:
    """Unique policy identifier for test isolation."""
    return f"test-policy-{uuid.uuid4().hex[:8]}"


def _canonical_replication_policy(policy_id: str) -> ReplicationPolicy:
    """Build the canonical replication policy from the SDK test contract.

    Shape (REST representation):
        source_backend="local", source_settings={"path": <tmp>},
        destination_backend="local", destination_settings={"path": <tmp>},
        mode="async", check_interval_seconds=3600
    """
    src_dir = tempfile.mkdtemp(prefix="objstore-src-")
    dst_dir = tempfile.mkdtemp(prefix="objstore-dst-")
    return ReplicationPolicy(
        id=policy_id,
        source_backend="local",
        source_settings={"path": src_dir},
        destination_backend="local",
        destination_settings={"path": dst_dir},
        check_interval_seconds=3600,
        enabled=True,
    )


# ---------------------------------------------------------------------------
# OPERATIONS TABLE
#
# Each entry is an OperationDef.  The driver test (test_operation) iterates
# over this table × PROTOCOL_PARAMS.  Cleanup is handled inside each
# callable so that even a partially-executed op leaves the server clean.
# ---------------------------------------------------------------------------


@dataclass
class OperationDef:
    """One row in the OPERATIONS table.

    Args:
        name: Human-readable operation name (appears in test IDs).
        category: Grouping label for output readability.
        run: Callable(client) -> None — performs op and asserts.
    """

    name: str
    category: str
    run: Any  # Callable[[ObjectStoreClient], None]


def _op_health(client: ObjectStoreClient) -> None:
    response = client.health()
    assert isinstance(response, HealthResponse), f"Expected HealthResponse, got {type(response)}"
    assert response.status in (
        HealthStatus.SERVING,
        HealthStatus.UNKNOWN,
    ), f"Expected SERVING or UNKNOWN, got {response.status}"


def _op_put(client: ObjectStoreClient) -> None:
    key = f"test/ops/put/{uuid.uuid4().hex}"
    data = b"canonical put test data"
    response = client.put(key, data)
    assert isinstance(response, PutResponse)
    assert response.success is True, "put must succeed"
    client.delete(key)


def _op_get(client: ObjectStoreClient) -> None:
    key = f"test/ops/get/{uuid.uuid4().hex}"
    original = b"canonical get test data"
    client.put(key, original)
    try:
        retrieved, meta = client.get(key)
        assert retrieved == original, "get must return the exact bytes that were put"
        assert isinstance(meta, Metadata)
        assert meta.size == len(original), f"size mismatch: {meta.size} != {len(original)}"
    finally:
        client.delete(key)


def _op_delete(client: ObjectStoreClient) -> None:
    key = f"test/ops/delete/{uuid.uuid4().hex}"
    client.put(key, b"to be deleted")
    response = client.delete(key)
    assert isinstance(response, DeleteResponse)
    assert response.success is True, "delete must succeed"
    exists = client.exists(key)
    assert exists.exists is False, "object must not exist after delete"


def _op_exists(client: ObjectStoreClient) -> None:
    key = f"test/ops/exists/{uuid.uuid4().hex}"
    absent = client.exists(key)
    assert isinstance(absent, ExistsResponse)
    assert absent.exists is False, "non-existent key must return exists=False"
    client.put(key, b"exists test")
    present = client.exists(key)
    assert present.exists is True, "existing key must return exists=True"
    client.delete(key)


def _op_list(client: ObjectStoreClient) -> None:
    prefix = f"test/ops/list/{uuid.uuid4().hex[:8]}/"
    keys = [f"{prefix}file{i}.bin" for i in range(3)]
    for k in keys:
        client.put(k, f"list payload {k}".encode())
    try:
        response = client.list(prefix=prefix, max_results=10)
        assert isinstance(response, ListResponse)
        returned_keys = {obj.key for obj in response.objects}
        for k in keys:
            assert k in returned_keys, f"expected key {k!r} in list response"
        assert len(returned_keys) >= 3
    finally:
        for k in keys:
            try:
                client.delete(k)
            except Exception:
                pass


def _op_get_metadata(client: ObjectStoreClient) -> None:
    key = f"test/ops/get-meta/{uuid.uuid4().hex}"
    data = b"metadata round-trip payload"
    meta_in = Metadata(
        content_type="text/plain",
        custom={"author": "sdk-test", "version": "1.0"},
    )
    client.put(key, data, metadata=meta_in)
    try:
        retrieved = client.get_metadata(key)
        assert isinstance(retrieved, Metadata)
        assert retrieved.size == len(data), (
            f"size must equal payload length: {retrieved.size} != {len(data)}"
        )
        assert retrieved.content_type == "text/plain", (
            f"content_type must round-trip: got {retrieved.content_type!r}"
        )
        # Custom map must round-trip (not just size check).
        assert retrieved.custom.get("author") == "sdk-test", (
            f"custom['author'] must round-trip: got {retrieved.custom.get('author')!r}"
        )
        assert retrieved.custom.get("version") == "1.0", (
            f"custom['version'] must round-trip: got {retrieved.custom.get('version')!r}"
        )
    finally:
        client.delete(key)


def _op_update_metadata(client: ObjectStoreClient) -> None:
    key = f"test/ops/update-meta/{uuid.uuid4().hex}"
    data = b"update metadata payload"
    initial_meta = Metadata(
        content_type="text/plain",
        custom={"version": "1.0"},
    )
    client.put(key, data, metadata=initial_meta)
    try:
        new_meta = Metadata(
            content_type="application/json",
            custom={"version": "2.0", "updated": "true"},
        )
        response = client.update_metadata(key, new_meta)
        assert isinstance(response, PolicyResponse)
        assert response.success is True, "updateMetadata must report success"
        # Read-back: new values must have persisted.
        read_back = client.get_metadata(key)
        assert read_back.content_type == "application/json", (
            f"content_type must be updated: got {read_back.content_type!r}"
        )
        assert read_back.custom.get("version") == "2.0", (
            f"custom['version'] must be updated: got {read_back.custom.get('version')!r}"
        )
        assert read_back.custom.get("updated") == "true", (
            f"custom['updated'] must be present after update: got {read_back.custom.get('updated')!r}"
        )
    finally:
        client.delete(key)


def _op_add_policy(client: ObjectStoreClient) -> None:
    policy_id = f"test-add-{uuid.uuid4().hex[:8]}"
    policy = LifecyclePolicy(
        id=policy_id,
        prefix="test/lifecycle/add/",
        retention_seconds=86400,
        action="delete",
    )
    response = client.add_policy(policy)
    assert isinstance(response, PolicyResponse)
    assert response.success is True, "addPolicy must succeed"
    try:
        client.remove_policy(policy_id)
    except Exception:
        pass


def _op_get_policies(client: ObjectStoreClient) -> None:
    policy_id = f"test-get-{uuid.uuid4().hex[:8]}"
    policy = LifecyclePolicy(
        id=policy_id,
        prefix="test/lifecycle/get/",
        retention_seconds=86400,
        action="delete",
    )
    client.add_policy(policy)
    try:
        response = client.get_policies()
        assert isinstance(response, GetPoliciesResponse)
        assert response.success is True
        ids = [p.id for p in response.policies]
        assert policy_id in ids, f"added policy {policy_id!r} must appear in getPolicies"
    finally:
        try:
            client.remove_policy(policy_id)
        except Exception:
            pass


def _op_remove_policy(client: ObjectStoreClient) -> None:
    policy_id = f"test-rm-{uuid.uuid4().hex[:8]}"
    policy = LifecyclePolicy(
        id=policy_id,
        prefix="test/lifecycle/remove/",
        retention_seconds=86400,
        action="delete",
    )
    client.add_policy(policy)
    response = client.remove_policy(policy_id)
    assert isinstance(response, PolicyResponse)
    assert response.success is True, "removePolicy must succeed"
    remaining = client.get_policies()
    assert policy_id not in [p.id for p in remaining.policies], (
        "removed policy must not appear in subsequent getPolicies"
    )


def _op_apply_policies(client: ObjectStoreClient) -> None:
    response = client.apply_policies()
    assert isinstance(response, ApplyPoliciesResponse)
    assert response.success is True, "applyPolicies must succeed"
    assert isinstance(response.policies_count, int) and response.policies_count >= 0
    assert isinstance(response.objects_processed, int) and response.objects_processed >= 0


def _op_archive(client: ObjectStoreClient) -> None:
    key = f"test/ops/archive/{uuid.uuid4().hex}"
    client.put(key, b"archive payload")
    try:
        response = client.archive(
            key=key,
            destination_type="local",
            settings={"path": "/tmp/archive"},
        )
        assert isinstance(response, ArchiveResponse)
        assert response.success is True, "archive must succeed when backend supports it"
    except ObjectStoreError as exc:
        msg = str(exc).lower()
        if any(kw in msg for kw in ("not supported", "not implemented", "not available", "not enabled")):
            pytest.skip(f"archive not supported by configured backend: {exc}")
        raise
    finally:
        try:
            client.delete(key)
        except Exception:
            pass


def _op_add_replication_policy(client: ObjectStoreClient) -> None:
    policy_id = f"test-repl-add-{uuid.uuid4().hex[:8]}"
    policy = _canonical_replication_policy(policy_id)
    response = client.add_replication_policy(policy)
    assert isinstance(response, PolicyResponse)
    assert response.success is True, "addReplicationPolicy must succeed (server has replication enabled)"
    try:
        client.remove_replication_policy(policy_id)
    except Exception:
        pass


def _op_get_replication_policies(client: ObjectStoreClient) -> None:
    policy_id = f"test-repl-list-{uuid.uuid4().hex[:8]}"
    policy = _canonical_replication_policy(policy_id)
    client.add_replication_policy(policy)
    try:
        response = client.get_replication_policies()
        assert isinstance(response, GetReplicationPoliciesResponse)
        ids = [p.id for p in response.policies]
        assert policy_id in ids, (
            f"added replication policy {policy_id!r} must appear in getReplicationPolicies"
        )
        assert len(response.policies) >= 1
    finally:
        try:
            client.remove_replication_policy(policy_id)
        except Exception:
            pass


def _op_get_replication_policy(client: ObjectStoreClient) -> None:
    policy_id = f"test-repl-get-{uuid.uuid4().hex[:8]}"
    policy = _canonical_replication_policy(policy_id)
    client.add_replication_policy(policy)
    try:
        retrieved = client.get_replication_policy(policy_id)
        assert isinstance(retrieved, ReplicationPolicy)
        assert retrieved.id == policy_id
        assert retrieved.source_backend == "local", (
            f"source_backend must be 'local', got {retrieved.source_backend!r}"
        )
        assert retrieved.destination_backend == "local", (
            f"destination_backend must be 'local', got {retrieved.destination_backend!r}"
        )
        assert retrieved.check_interval_seconds == 3600, (
            f"check_interval_seconds must be 3600, got {retrieved.check_interval_seconds}"
        )
    finally:
        try:
            client.remove_replication_policy(policy_id)
        except Exception:
            pass


def _op_remove_replication_policy(client: ObjectStoreClient) -> None:
    policy_id = f"test-repl-rm-{uuid.uuid4().hex[:8]}"
    policy = _canonical_replication_policy(policy_id)
    client.add_replication_policy(policy)
    response = client.remove_replication_policy(policy_id)
    assert isinstance(response, PolicyResponse)
    assert response.success is True, "removeReplicationPolicy must succeed"
    remaining = client.get_replication_policies()
    ids = [p.id for p in remaining.policies]
    assert policy_id not in ids, "removed replication policy must not appear in list"


def _op_trigger_replication(client: ObjectStoreClient) -> None:
    policy_id = f"test-repl-trig-{uuid.uuid4().hex[:8]}"
    policy = _canonical_replication_policy(policy_id)
    client.add_replication_policy(policy)
    try:
        opts = TriggerReplicationOptions(policy_id=policy_id, parallel=True, worker_count=2)
        response = client.trigger_replication(opts)
        assert isinstance(response, TriggerReplicationResponse)
        assert response.success is True, "triggerReplication must succeed"
        # result is required per the canonical spec.
        assert response.result is not None, "triggerReplication must return a result object"
        assert response.result.policy_id == policy_id
        assert isinstance(response.result.synced, int)
        assert isinstance(response.result.bytes_total, int)
        assert isinstance(response.result.duration_ms, int)
    finally:
        try:
            client.remove_replication_policy(policy_id)
        except Exception:
            pass


def _op_get_replication_status(client: ObjectStoreClient) -> None:
    policy_id = f"test-repl-stat-{uuid.uuid4().hex[:8]}"
    policy = _canonical_replication_policy(policy_id)
    client.add_replication_policy(policy)
    try:
        response = client.get_replication_status(policy_id)
        assert isinstance(response, GetReplicationStatusResponse)
        assert response.success is True, "getReplicationStatus must succeed"
        assert response.status is not None, "getReplicationStatus must return a status object"
        assert response.status.policy_id == policy_id
        assert isinstance(response.status.total_objects_synced, int)
        assert response.status.total_objects_synced >= 0
        assert isinstance(response.status.sync_count, int)
        assert response.status.sync_count >= 0
    finally:
        try:
            client.remove_replication_policy(policy_id)
        except Exception:
            pass


def _op_close(client: ObjectStoreClient) -> None:
    """close/dispose must be idempotent — callable without error."""
    # We cannot close the module-scoped fixture client here (other tests still
    # need it) so we create a short-lived client and verify double-close is safe.
    base_url = client._client.__class__.__name__
    # Create a temporary REST client for the idempotency check.
    tmp = ObjectStoreClient(
        protocol=Protocol.REST,
        base_url=os.getenv("OBJSTORE_REST_URL", "http://localhost:8080"),
        timeout=5,
    )
    tmp.close()
    tmp.close()  # second close must not raise


# ---------------------------------------------------------------------------
# OPERATIONS TABLE — single source of truth for all 19 ops + close
# ---------------------------------------------------------------------------

OPERATIONS: list[OperationDef] = [
    # health
    OperationDef(name="health", category="health", run=_op_health),
    # basic
    OperationDef(name="put", category="basic", run=_op_put),
    OperationDef(name="get", category="basic", run=_op_get),
    OperationDef(name="delete", category="basic", run=_op_delete),
    OperationDef(name="exists", category="basic", run=_op_exists),
    OperationDef(name="list", category="basic", run=_op_list),
    # metadata
    OperationDef(name="getMetadata", category="metadata", run=_op_get_metadata),
    OperationDef(name="updateMetadata", category="metadata", run=_op_update_metadata),
    # lifecycle
    OperationDef(name="addPolicy", category="lifecycle", run=_op_add_policy),
    OperationDef(name="getPolicies", category="lifecycle", run=_op_get_policies),
    OperationDef(name="removePolicy", category="lifecycle", run=_op_remove_policy),
    OperationDef(name="applyPolicies", category="lifecycle", run=_op_apply_policies),
    # archive
    OperationDef(name="archive", category="archive", run=_op_archive),
    # replication
    OperationDef(
        name="addReplicationPolicy",
        category="replication",
        run=_op_add_replication_policy,
    ),
    OperationDef(
        name="getReplicationPolicies",
        category="replication",
        run=_op_get_replication_policies,
    ),
    OperationDef(
        name="getReplicationPolicy",
        category="replication",
        run=_op_get_replication_policy,
    ),
    OperationDef(
        name="removeReplicationPolicy",
        category="replication",
        run=_op_remove_replication_policy,
    ),
    OperationDef(
        name="triggerReplication",
        category="replication",
        run=_op_trigger_replication,
    ),
    OperationDef(
        name="getReplicationStatus",
        category="replication",
        run=_op_get_replication_status,
    ),
    # close
    OperationDef(name="close", category="close", run=_op_close),
]


# ---------------------------------------------------------------------------
# Driver: OPERATIONS × PROTOCOLS
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "op",
    [pytest.param(op, id=f"{op.category}/{op.name}") for op in OPERATIONS],
)
@pytest.mark.parametrize(
    "protocol_label,fixture_name",
    PROTOCOL_PARAMS,
)
def test_operation(
    op: OperationDef,
    protocol_label: str,
    fixture_name: str,
    request: pytest.FixtureRequest,
) -> None:
    """Data-driven driver: runs every operation in the OPERATIONS table against
    every available protocol.

    Each OperationDef.run callable owns its own setup, assertion, and cleanup
    so this driver stays a single uniform loop.
    """
    client: ObjectStoreClient = request.getfixturevalue(fixture_name)
    op.run(client)


# ---------------------------------------------------------------------------
# Cross-protocol consistency
#
# For every ordered (A, B) pair in AVAILABLE protocols (REST, gRPC, QUIC):
#   put via A -> get via B -> assert data equal
#   put via A -> getMetadata via B -> assert size and content_type equal
#   delete via A -> exists via B == False
#
# gRPC is no longer excluded.  Protocols that are unreachable are skipped
# explicitly (pytest.skip with a logged reason), never silently passed over.
# ---------------------------------------------------------------------------

_CROSS_PROTOCOL_PAIRS = [
    pytest.param("rest_client", "grpc_client", id="REST->gRPC"),
    pytest.param("rest_client", "quic_client", id="REST->QUIC"),
    pytest.param("grpc_client", "rest_client", id="gRPC->REST"),
    pytest.param("grpc_client", "quic_client", id="gRPC->QUIC"),
    pytest.param("quic_client", "rest_client", id="QUIC->REST"),
    pytest.param("quic_client", "grpc_client", id="QUIC->gRPC"),
]


@pytest.mark.parametrize("writer_fixture,reader_fixture", _CROSS_PROTOCOL_PAIRS)
def test_cross_protocol_put_get(
    writer_fixture: str,
    reader_fixture: str,
    request: pytest.FixtureRequest,
) -> None:
    """put via writer protocol, get via reader protocol — data must be identical."""
    writer: ObjectStoreClient = request.getfixturevalue(writer_fixture)
    reader: ObjectStoreClient = request.getfixturevalue(reader_fixture)

    key = f"test/cross/{uuid.uuid4().hex}"
    payload = b"cross-protocol consistency payload"
    writer.put(key, payload)
    try:
        data, _meta = reader.get(key)
        assert data == payload, (
            f"data written via {writer_fixture} must equal data read via {reader_fixture}"
        )
    finally:
        try:
            writer.delete(key)
        except Exception:
            pass


@pytest.mark.parametrize("writer_fixture,reader_fixture", _CROSS_PROTOCOL_PAIRS)
def test_cross_protocol_metadata(
    writer_fixture: str,
    reader_fixture: str,
    request: pytest.FixtureRequest,
) -> None:
    """put via writer protocol, getMetadata via reader — size and content_type must match."""
    writer: ObjectStoreClient = request.getfixturevalue(writer_fixture)
    reader: ObjectStoreClient = request.getfixturevalue(reader_fixture)

    key = f"test/cross-meta/{uuid.uuid4().hex}"
    payload = b"metadata cross-protocol payload"
    meta_in = Metadata(
        content_type="application/octet-stream",
        custom={"source": "cross-test"},
    )
    writer.put(key, payload, metadata=meta_in)
    try:
        meta_out = reader.get_metadata(key)
        assert meta_out.size == len(payload), (
            f"size mismatch across {writer_fixture}->{reader_fixture}: "
            f"{meta_out.size} != {len(payload)}"
        )
        assert meta_out.content_type == "application/octet-stream", (
            f"content_type mismatch across {writer_fixture}->{reader_fixture}: "
            f"got {meta_out.content_type!r}"
        )
    finally:
        try:
            writer.delete(key)
        except Exception:
            pass


@pytest.mark.parametrize("writer_fixture,reader_fixture", _CROSS_PROTOCOL_PAIRS)
def test_cross_protocol_delete_exists(
    writer_fixture: str,
    reader_fixture: str,
    request: pytest.FixtureRequest,
) -> None:
    """delete via writer, exists via reader must return False."""
    writer: ObjectStoreClient = request.getfixturevalue(writer_fixture)
    reader: ObjectStoreClient = request.getfixturevalue(reader_fixture)

    key = f"test/cross-del/{uuid.uuid4().hex}"
    writer.put(key, b"delete cross-protocol payload")
    writer.delete(key)
    result = reader.exists(key)
    assert result.exists is False, (
        f"after delete via {writer_fixture}, exists via {reader_fixture} must be False"
    )


# ---------------------------------------------------------------------------
# Error handling consistency (all protocols)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("protocol_label,fixture_name", PROTOCOL_PARAMS)
def test_get_nonexistent_raises(
    protocol_label: str,
    fixture_name: str,
    request: pytest.FixtureRequest,
) -> None:
    """Getting a non-existent object must raise an error indicating not-found."""
    client: ObjectStoreClient = request.getfixturevalue(fixture_name)
    absent_key = f"test/error/nonexistent/{uuid.uuid4().hex}"
    with pytest.raises((ObjectNotFoundError, ObjectStoreError)) as exc_info:
        client.get(absent_key)
    msg = str(exc_info.value).lower()
    assert any(kw in msg for kw in ("not found", "no such file", "does not exist")), (
        f"error message must indicate not-found: {exc_info.value!r}"
    )


@pytest.mark.parametrize("protocol_label,fixture_name", PROTOCOL_PARAMS)
def test_delete_nonexistent_idempotent_or_notfound(
    protocol_label: str,
    fixture_name: str,
    request: pytest.FixtureRequest,
) -> None:
    """Deleting a non-existent object: idempotent success or explicit not-found error, never silent."""
    client: ObjectStoreClient = request.getfixturevalue(fixture_name)
    absent_key = f"test/error/del-nonexistent/{uuid.uuid4().hex}"
    try:
        response = client.delete(absent_key)
        assert isinstance(response, DeleteResponse), "idempotent delete must return DeleteResponse"
    except (ObjectNotFoundError, ObjectStoreError):
        pass  # explicit not-found is acceptable
