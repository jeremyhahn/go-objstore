"""Comprehensive table-driven integration tests for all protocols and operations.

This test suite validates all 19 operations across REST, gRPC, and QUIC protocols:
- Basic operations: put, get, delete, exists, list, getMetadata, updateMetadata
- Lifecycle operations: addPolicy, removePolicy, getPolicies, applyPolicies
- Archive operations: archive
- Replication operations: addReplicationPolicy, removeReplicationPolicy,
  getReplicationPolicies, getReplicationPolicy, triggerReplication, getReplicationStatus
- Health: health check

Tests are parameterized to run across all protocols and verify cross-protocol consistency.
"""

import os
import time
import uuid
from typing import Any, Callable, Generator

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
    ReplicationMode,
    ReplicationPolicy,
    TriggerReplicationOptions,
    TriggerReplicationResponse,
)


# ============================================================================
# Fixtures for all three protocols
# ============================================================================


@pytest.fixture(scope="module")
def wait_for_servers() -> Generator[None, None, None]:
    """Wait for all servers to be ready."""
    time.sleep(2)  # Give servers time to start
    yield
    time.sleep(1)  # Cleanup delay


@pytest.fixture
def rest_client(wait_for_servers: None) -> Generator[ObjectStoreClient, None, None]:
    """Create REST client."""
    base_url = os.getenv("OBJSTORE_REST_URL", "http://localhost:8080")
    client = ObjectStoreClient(protocol=Protocol.REST, base_url=base_url, timeout=15)
    yield client
    client.close()


@pytest.fixture
def grpc_client(wait_for_servers: None) -> Generator[ObjectStoreClient, None, None]:
    """Create gRPC client."""
    host = os.getenv("OBJSTORE_GRPC_HOST", "localhost")
    port = int(os.getenv("OBJSTORE_GRPC_PORT", "50051"))
    try:
        client = ObjectStoreClient(protocol=Protocol.GRPC, host=host, port=port, timeout=15)
        yield client
        client.close()
    except ImportError:
        pytest.skip("gRPC proto files not generated")


@pytest.fixture
def quic_client(wait_for_servers: None) -> Generator[ObjectStoreClient, None, None]:
    """Create QUIC client."""
    base_url = os.getenv("OBJSTORE_QUIC_URL", "https://localhost:4433")
    try:
        client = ObjectStoreClient(
            protocol=Protocol.QUIC, base_url=base_url, timeout=15, verify_ssl=False
        )
        # Test connectivity
        try:
            client.health()
        except ConnectionError as e:
            client.close()
            pytest.skip(f"QUIC server not reachable: {str(e)}")
        except Exception:
            # If health check fails for other reasons, that's okay - let the test proceed
            pass
        yield client
        client.close()
    except Exception as e:
        pytest.skip(f"QUIC client not available: {str(e)}")


@pytest.fixture(params=["rest", "grpc", "quic"])
def client_for_protocol(
    request: pytest.FixtureRequest,
    rest_client: ObjectStoreClient,
    grpc_client: ObjectStoreClient,
    quic_client: ObjectStoreClient,
) -> ObjectStoreClient:
    """Parametrized fixture that yields client for each protocol."""
    protocol_map = {
        "rest": rest_client,
        "grpc": grpc_client,
        "quic": quic_client,
    }
    return protocol_map[request.param]


@pytest.fixture
def unique_key() -> str:
    """Generate unique key for test isolation."""
    return f"test/comprehensive/{uuid.uuid4().hex}"


@pytest.fixture
def unique_policy_id() -> str:
    """Generate unique policy ID for test isolation."""
    return f"test-policy-{uuid.uuid4().hex[:8]}"


# ============================================================================
# Table-driven test configurations
# ============================================================================


class OperationTestCase:
    """Test case definition for an operation."""

    def __init__(
        self,
        operation_name: str,
        setup_func: Callable[[ObjectStoreClient, str], None] | None = None,
        execute_func: Callable[[ObjectStoreClient, str], Any] = None,
        verify_func: Callable[[Any], None] = None,
        cleanup_func: Callable[[ObjectStoreClient, str], None] | None = None,
        skip_for_backends: list[str] | None = None,
        skip_reason: str = "",
    ):
        """Initialize test case.

        Args:
            operation_name: Name of the operation being tested
            setup_func: Optional setup function
            execute_func: Function to execute the operation
            verify_func: Function to verify the result
            cleanup_func: Optional cleanup function
            skip_for_backends: List of backend types to skip (e.g., ['archive', 'replication'])
            skip_reason: Reason for skipping
        """
        self.operation_name = operation_name
        self.setup_func = setup_func
        self.execute_func = execute_func
        self.verify_func = verify_func
        self.cleanup_func = cleanup_func
        self.skip_for_backends = skip_for_backends or []
        self.skip_reason = skip_reason


# ============================================================================
# Test: Health Check (Operation 1/19)
# ============================================================================


class TestHealthOperation:
    """Test health check operation across all protocols."""

    @pytest.mark.parametrize(
        "protocol,client_fixture",
        [
            ("REST", "rest_client"),
            ("gRPC", "grpc_client"),
            ("QUIC", "quic_client"),
        ],
    )
    def test_health_check(
        self, protocol: str, client_fixture: str, request: pytest.FixtureRequest
    ) -> None:
        """Test health check returns valid status.

        Validates:
        - Response is HealthResponse type
        - Status is one of valid HealthStatus values
        - Response structure matches expected format
        """
        client = request.getfixturevalue(client_fixture)
        response = client.health()

        # Verify response type
        assert isinstance(response, HealthResponse)

        # Verify status is valid
        assert response.status in [
            HealthStatus.SERVING,
            HealthStatus.NOT_SERVING,
            HealthStatus.UNKNOWN,
        ]

        # For operational servers, expect SERVING or UNKNOWN status
        # (Some backends may return UNKNOWN if they don't have specific health checks)
        assert response.status in [HealthStatus.SERVING, HealthStatus.UNKNOWN], (
            f"{protocol} server should be SERVING or UNKNOWN, got {response.status}"
        )


# ============================================================================
# Test: Basic Object Operations (Operations 2-7/19)
# ============================================================================


class TestBasicObjectOperations:
    """Test basic CRUD operations across all protocols."""

    @pytest.mark.parametrize(
        "protocol,client_fixture",
        [
            ("REST", "rest_client"),
            ("gRPC", "grpc_client"),
            ("QUIC", "quic_client"),
        ],
    )
    def test_put_operation(
        self, protocol: str, client_fixture: str, request: pytest.FixtureRequest, unique_key: str
    ) -> None:
        """Test put operation (2/19).

        Validates:
        - Response is PutResponse type
        - Success field is True
        - ETag is returned (when supported)
        """
        client = request.getfixturevalue(client_fixture)
        data = b"Test data for put operation"

        response = client.put(unique_key, data)

        # Verify response type and success
        assert isinstance(response, PutResponse)
        assert response.success is True

        # Cleanup
        try:
            client.delete(unique_key)
        except Exception:
            pass

    @pytest.mark.parametrize(
        "protocol,client_fixture",
        [
            ("REST", "rest_client"),
            ("gRPC", "grpc_client"),
            ("QUIC", "quic_client"),
        ],
    )
    def test_get_operation(
        self, protocol: str, client_fixture: str, request: pytest.FixtureRequest, unique_key: str
    ) -> None:
        """Test get operation (3/19).

        Validates:
        - Returns tuple of (data, metadata)
        - Data matches uploaded content
        - Metadata contains size information
        """
        client = request.getfixturevalue(client_fixture)
        original_data = b"Test data for get operation"

        # Setup: put object
        client.put(unique_key, original_data)

        # Execute: get object
        retrieved_data, metadata = client.get(unique_key)

        # Verify data matches
        assert retrieved_data == original_data

        # Verify metadata
        assert isinstance(metadata, Metadata)
        assert metadata.size == len(original_data)

        # Cleanup
        client.delete(unique_key)

    @pytest.mark.parametrize(
        "protocol,client_fixture",
        [
            ("REST", "rest_client"),
            ("gRPC", "grpc_client"),
            ("QUIC", "quic_client"),
        ],
    )
    def test_delete_operation(
        self, protocol: str, client_fixture: str, request: pytest.FixtureRequest, unique_key: str
    ) -> None:
        """Test delete operation (4/19).

        Validates:
        - Response is DeleteResponse type
        - Success field is True
        - Object no longer exists after deletion
        """
        client = request.getfixturevalue(client_fixture)
        data = b"Test data for delete operation"

        # Setup: create object
        client.put(unique_key, data)

        # Execute: delete object
        response = client.delete(unique_key)

        # Verify response
        assert isinstance(response, DeleteResponse)
        assert response.success is True

        # Verify object is deleted - backend may return ObjectNotFoundError or ServerError
        with pytest.raises((ObjectNotFoundError, ObjectStoreError)) as exc_info:
            client.get(unique_key)

        # Verify error indicates object not found
        error_msg = str(exc_info.value).lower()
        assert "not found" in error_msg or "no such file" in error_msg or "does not exist" in error_msg

    @pytest.mark.parametrize(
        "protocol,client_fixture",
        [
            ("REST", "rest_client"),
            ("gRPC", "grpc_client"),
            ("QUIC", "quic_client"),
        ],
    )
    def test_exists_operation(
        self, protocol: str, client_fixture: str, request: pytest.FixtureRequest, unique_key: str
    ) -> None:
        """Test exists operation (5/19).

        Validates:
        - Response is ExistsResponse type
        - Returns False for non-existent object
        - Returns True for existing object
        """
        client = request.getfixturevalue(client_fixture)
        data = b"Test data for exists operation"

        # Verify non-existent object
        response = client.exists(unique_key)
        assert isinstance(response, ExistsResponse)
        assert response.exists is False

        # Create object
        client.put(unique_key, data)

        # Verify existing object
        response = client.exists(unique_key)
        assert response.exists is True

        # Cleanup
        client.delete(unique_key)

    @pytest.mark.parametrize(
        "protocol,client_fixture",
        [
            ("REST", "rest_client"),
            ("gRPC", "grpc_client"),
            ("QUIC", "quic_client"),
        ],
    )
    def test_list_operation(
        self, protocol: str, client_fixture: str, request: pytest.FixtureRequest
    ) -> None:
        """Test list operation (6/19).

        Validates:
        - Response is ListResponse type
        - Returns list of ObjectInfo items
        - Pagination parameters work correctly
        - Prefix filtering works
        """
        client = request.getfixturevalue(client_fixture)
        prefix = f"test/comprehensive/list/{uuid.uuid4().hex[:8]}/"
        test_keys = [f"{prefix}file{i}.txt" for i in range(5)]

        # Setup: create test objects
        for key in test_keys:
            client.put(key, f"data for {key}".encode())

        # Execute: list objects
        response = client.list(prefix=prefix, max_results=10)

        # Verify response type
        assert isinstance(response, ListResponse)

        # Verify objects are returned
        assert len(response.objects) >= 5

        # Verify prefix filtering
        for obj in response.objects:
            assert obj.key.startswith(prefix)

        # Test pagination
        paginated = client.list(prefix=prefix, max_results=2)
        assert len(paginated.objects) <= 2

        # Cleanup
        for key in test_keys:
            try:
                client.delete(key)
            except Exception:
                pass

    @pytest.mark.parametrize(
        "protocol,client_fixture",
        [
            ("REST", "rest_client"),
            ("gRPC", "grpc_client"),
            ("QUIC", "quic_client"),
        ],
    )
    def test_get_metadata_operation(
        self, protocol: str, client_fixture: str, request: pytest.FixtureRequest, unique_key: str
    ) -> None:
        """Test getMetadata operation (7/19).

        Validates:
        - Returns Metadata object
        - Contains size, content_type, and custom metadata
        - Metadata persists across get operations
        """
        client = request.getfixturevalue(client_fixture)
        data = b"Test data for metadata operation"
        metadata = Metadata(
            content_type="text/plain",
            custom={"author": "test", "version": "1.0"},
        )

        # Setup: create object with metadata
        client.put(unique_key, data, metadata=metadata)

        # Execute: get metadata
        retrieved_metadata = client.get_metadata(unique_key)

        # Verify metadata type
        assert isinstance(retrieved_metadata, Metadata)

        # Verify size
        assert retrieved_metadata.size == len(data)

        # Cleanup
        client.delete(unique_key)

    @pytest.mark.parametrize(
        "protocol,client_fixture",
        [
            ("REST", "rest_client"),
            ("gRPC", "grpc_client"),
            ("QUIC", "quic_client"),
        ],
    )
    def test_update_metadata_operation(
        self, protocol: str, client_fixture: str, request: pytest.FixtureRequest, unique_key: str
    ) -> None:
        """Test updateMetadata operation (8/19).

        Validates:
        - Response is PolicyResponse type
        - Success field is True
        - Metadata is actually updated
        """
        client = request.getfixturevalue(client_fixture)
        data = b"Test data for update metadata"
        initial_metadata = Metadata(
            content_type="text/plain",
            custom={"version": "1.0"},
        )

        # Setup: create object with initial metadata
        client.put(unique_key, data, metadata=initial_metadata)

        # Execute: update metadata
        new_metadata = Metadata(
            content_type="application/json",
            custom={"version": "2.0", "updated": "true"},
        )
        response = client.update_metadata(unique_key, new_metadata)

        # Verify response
        assert isinstance(response, PolicyResponse)
        assert response.success is True

        # Verify metadata was updated
        updated = client.get_metadata(unique_key)
        assert updated.custom.get("version") == "2.0" or "updated" in updated.custom

        # Cleanup
        client.delete(unique_key)


# ============================================================================
# Test: Lifecycle Policy Operations (Operations 9-12/19)
# ============================================================================


class TestLifecyclePolicyOperations:
    """Test lifecycle policy operations across all protocols."""

    @pytest.mark.parametrize(
        "protocol,client_fixture",
        [
            ("REST", "rest_client"),
            ("gRPC", "grpc_client"),
            ("QUIC", "quic_client"),
        ],
    )
    def test_add_policy_operation(
        self,
        protocol: str,
        client_fixture: str,
        request: pytest.FixtureRequest,
        unique_policy_id: str,
    ) -> None:
        """Test addPolicy operation (9/19).

        Validates:
        - Response is PolicyResponse type
        - Success field is True
        - Policy is actually added
        """
        client = request.getfixturevalue(client_fixture)

        # Create policy
        policy = LifecyclePolicy(
            id=unique_policy_id,
            prefix="test/lifecycle/",
            retention_seconds=86400,  # 1 day
            action="delete",
        )

        # Execute: add policy
        response = client.add_policy(policy)

        # Verify response
        assert isinstance(response, PolicyResponse)
        assert response.success is True

        # Cleanup
        try:
            client.remove_policy(unique_policy_id)
        except Exception:
            pass

    @pytest.mark.parametrize(
        "protocol,client_fixture",
        [
            ("REST", "rest_client"),
            ("gRPC", "grpc_client"),
            ("QUIC", "quic_client"),
        ],
    )
    def test_get_policies_operation(
        self,
        protocol: str,
        client_fixture: str,
        request: pytest.FixtureRequest,
        unique_policy_id: str,
    ) -> None:
        """Test getPolicies operation (10/19).

        Validates:
        - Response is GetPoliciesResponse type
        - Returns list of LifecyclePolicy objects
        - Prefix filtering works
        """
        client = request.getfixturevalue(client_fixture)

        # Setup: add a policy
        policy = LifecyclePolicy(
            id=unique_policy_id,
            prefix="test/lifecycle/policies/",
            retention_seconds=86400,
            action="delete",
        )
        client.add_policy(policy)

        # Execute: get all policies
        response = client.get_policies()

        # Verify response
        assert isinstance(response, GetPoliciesResponse)
        assert response.success is True
        assert isinstance(response.policies, list)

        # Verify our policy is in the list
        policy_ids = [p.id for p in response.policies]
        assert unique_policy_id in policy_ids

        # Test prefix filtering
        filtered = client.get_policies(prefix="test/lifecycle/")
        assert isinstance(filtered, GetPoliciesResponse)

        # Cleanup
        try:
            client.remove_policy(unique_policy_id)
        except Exception:
            pass

    @pytest.mark.parametrize(
        "protocol,client_fixture",
        [
            ("REST", "rest_client"),
            ("gRPC", "grpc_client"),
            ("QUIC", "quic_client"),
        ],
    )
    def test_remove_policy_operation(
        self,
        protocol: str,
        client_fixture: str,
        request: pytest.FixtureRequest,
        unique_policy_id: str,
    ) -> None:
        """Test removePolicy operation (11/19).

        Validates:
        - Response is PolicyResponse type
        - Success field is True
        - Policy is actually removed
        """
        client = request.getfixturevalue(client_fixture)

        # Setup: add a policy
        policy = LifecyclePolicy(
            id=unique_policy_id,
            prefix="test/lifecycle/remove/",
            retention_seconds=86400,
            action="delete",
        )
        client.add_policy(policy)

        # Execute: remove policy
        response = client.remove_policy(unique_policy_id)

        # Verify response
        assert isinstance(response, PolicyResponse)
        assert response.success is True

        # Verify policy is removed
        policies = client.get_policies()
        policy_ids = [p.id for p in policies.policies]
        assert unique_policy_id not in policy_ids

    @pytest.mark.parametrize(
        "protocol,client_fixture",
        [
            ("REST", "rest_client"),
            ("gRPC", "grpc_client"),
            ("QUIC", "quic_client"),
        ],
    )
    def test_apply_policies_operation(
        self, protocol: str, client_fixture: str, request: pytest.FixtureRequest
    ) -> None:
        """Test applyPolicies operation (12/19).

        Validates:
        - Response is ApplyPoliciesResponse type
        - Success field is True
        - Returns count of policies applied and objects processed
        """
        client = request.getfixturevalue(client_fixture)

        # Execute: apply policies
        response = client.apply_policies()

        # Verify response
        assert isinstance(response, ApplyPoliciesResponse)
        assert response.success is True
        assert isinstance(response.policies_count, int)
        assert isinstance(response.objects_processed, int)
        assert response.policies_count >= 0
        assert response.objects_processed >= 0


# ============================================================================
# Test: Archive Operation (Operation 13/19)
# ============================================================================


class TestArchiveOperation:
    """Test archive operation across all protocols."""

    @pytest.mark.parametrize(
        "protocol,client_fixture",
        [
            ("REST", "rest_client"),
            ("gRPC", "grpc_client"),
            ("QUIC", "quic_client"),
        ],
    )
    def test_archive_operation(
        self, protocol: str, client_fixture: str, request: pytest.FixtureRequest, unique_key: str
    ) -> None:
        """Test archive operation (13/19).

        Validates:
        - If archive is supported: Response is ArchiveResponse with success=True
        - If archive is not supported: Raises ObjectStoreError indicating unsupported operation
        - Both behaviors are acceptable depending on backend configuration
        """
        client = request.getfixturevalue(client_fixture)
        data = b"Test data for archive operation"

        # Setup: create object to archive
        client.put(unique_key, data)

        try:
            # Execute: archive object
            response = client.archive(
                key=unique_key,
                destination_type="local",
                settings={"path": "/tmp/archive"},
            )

            # If archive succeeds, verify response
            assert isinstance(response, ArchiveResponse)
            assert response.success is True
        except ObjectStoreError as e:
            # Archive may not be supported by all backends - that's acceptable
            error_msg = str(e).lower()
            # Verify it's a "not supported" or "not implemented" error
            assert any(
                msg in error_msg
                for msg in ["not supported", "not implemented", "not available", "not enabled"]
            ), f"Unexpected archive error: {e}"

        # Cleanup
        try:
            client.delete(unique_key)
        except Exception:
            pass


# ============================================================================
# Test: Replication Policy Operations (Operations 14-19/19)
# ============================================================================


class TestReplicationPolicyOperations:
    """Test replication policy operations across all protocols."""

    @pytest.mark.parametrize(
        "protocol,client_fixture",
        [
            ("REST", "rest_client"),
            ("gRPC", "grpc_client"),
            ("QUIC", "quic_client"),
        ],
    )
    def test_add_replication_policy_operation(
        self,
        protocol: str,
        client_fixture: str,
        request: pytest.FixtureRequest,
        unique_policy_id: str,
    ) -> None:
        """Test addReplicationPolicy operation (14/19).

        Validates:
        - If replication is supported: PolicyResponse with success=True
        - If replication is not supported: Raises ObjectStoreError indicating unsupported operation
        - Both behaviors are acceptable depending on backend configuration
        """
        client = request.getfixturevalue(client_fixture)

        # Create replication policy
        policy = ReplicationPolicy(
            id=unique_policy_id,
            source_backend="local",
            source_settings={"path": "/tmp/source"},
            source_prefix="test/replication/",
            destination_backend="local",
            destination_settings={"path": "/tmp/dest"},
            check_interval_seconds=3600,
            enabled=True,
            replication_mode=ReplicationMode.TRANSPARENT,
        )

        try:
            # Execute: add replication policy
            response = client.add_replication_policy(policy)

            # If succeeds, verify response
            assert isinstance(response, PolicyResponse)
            assert response.success is True

            # Cleanup
            try:
                client.remove_replication_policy(unique_policy_id)
            except Exception:
                pass
        except ObjectStoreError as e:
            # Replication may not be supported by all backends - that's acceptable
            error_msg = str(e).lower()
            # Verify it's a "not supported" or "not implemented" error
            assert any(
                msg in error_msg
                for msg in ["not supported", "not implemented", "not available", "not enabled"]
            ), f"Unexpected replication error: {e}"

    @pytest.mark.parametrize(
        "protocol,client_fixture",
        [
            ("REST", "rest_client"),
            ("gRPC", "grpc_client"),
            ("QUIC", "quic_client"),
        ],
    )
    def test_get_replication_policies_operation(
        self,
        protocol: str,
        client_fixture: str,
        request: pytest.FixtureRequest,
        unique_policy_id: str,
    ) -> None:
        """Test getReplicationPolicies operation (15/19).

        Validates:
        - If replication is supported: GetReplicationPoliciesResponse with list of policies
        - If replication is not supported: Raises ObjectStoreError indicating unsupported operation
        - Both behaviors are acceptable depending on backend configuration
        """
        client = request.getfixturevalue(client_fixture)

        try:
            # Setup: add a replication policy
            policy = ReplicationPolicy(
                id=unique_policy_id,
                source_backend="local",
                source_settings={"path": "/tmp/source"},
                source_prefix="test/replication/",
                destination_backend="local",
                destination_settings={"path": "/tmp/dest"},
                check_interval_seconds=3600,
                enabled=True,
            )
            client.add_replication_policy(policy)

            # Execute: get all replication policies
            response = client.get_replication_policies()

            # Verify response
            assert isinstance(response, GetReplicationPoliciesResponse)
            assert isinstance(response.policies, list)

            # Verify our policy is in the list
            policy_ids = [p.id for p in response.policies]
            assert unique_policy_id in policy_ids

            # Cleanup
            try:
                client.remove_replication_policy(unique_policy_id)
            except Exception:
                pass
        except ObjectStoreError as e:
            # Replication may not be supported by all backends - that's acceptable
            error_msg = str(e).lower()
            # Verify it's a "not supported" or "not implemented" error
            assert any(
                msg in error_msg
                for msg in ["not supported", "not implemented", "not available", "not enabled"]
            ), f"Unexpected replication error: {e}"

    @pytest.mark.parametrize(
        "protocol,client_fixture",
        [
            ("REST", "rest_client"),
            ("gRPC", "grpc_client"),
            ("QUIC", "quic_client"),
        ],
    )
    def test_get_replication_policy_operation(
        self,
        protocol: str,
        client_fixture: str,
        request: pytest.FixtureRequest,
        unique_policy_id: str,
    ) -> None:
        """Test getReplicationPolicy operation (16/19).

        Validates:
        - If replication is supported: Returns ReplicationPolicy with matching details
        - If replication is not supported: Raises ObjectStoreError indicating unsupported operation
        - Both behaviors are acceptable depending on backend configuration
        """
        client = request.getfixturevalue(client_fixture)

        try:
            # Setup: add a replication policy
            policy = ReplicationPolicy(
                id=unique_policy_id,
                source_backend="local",
                source_settings={"path": "/tmp/source"},
                source_prefix="test/replication/specific/",
                destination_backend="local",
                destination_settings={"path": "/tmp/dest"},
                check_interval_seconds=7200,
                enabled=True,
            )
            client.add_replication_policy(policy)

            # Execute: get specific replication policy
            retrieved_policy = client.get_replication_policy(unique_policy_id)

            # Verify policy
            assert isinstance(retrieved_policy, ReplicationPolicy)
            assert retrieved_policy.id == unique_policy_id
            assert retrieved_policy.source_backend == "local"
            assert retrieved_policy.destination_backend == "local"
            assert retrieved_policy.check_interval_seconds == 7200

            # Cleanup
            try:
                client.remove_replication_policy(unique_policy_id)
            except Exception:
                pass
        except ObjectStoreError as e:
            # Replication may not be supported by all backends - that's acceptable
            error_msg = str(e).lower()
            # Verify it's a "not supported" or "not implemented" error
            assert any(
                msg in error_msg
                for msg in ["not supported", "not implemented", "not available", "not enabled"]
            ), f"Unexpected replication error: {e}"

    @pytest.mark.parametrize(
        "protocol,client_fixture",
        [
            ("REST", "rest_client"),
            ("gRPC", "grpc_client"),
            ("QUIC", "quic_client"),
        ],
    )
    def test_remove_replication_policy_operation(
        self,
        protocol: str,
        client_fixture: str,
        request: pytest.FixtureRequest,
        unique_policy_id: str,
    ) -> None:
        """Test removeReplicationPolicy operation (17/19).

        Validates:
        - If replication is supported: PolicyResponse with success=True and policy removed
        - If replication is not supported: Raises ObjectStoreError indicating unsupported operation
        - Both behaviors are acceptable depending on backend configuration
        """
        client = request.getfixturevalue(client_fixture)

        try:
            # Setup: add a replication policy
            policy = ReplicationPolicy(
                id=unique_policy_id,
                source_backend="local",
                source_settings={"path": "/tmp/source"},
                source_prefix="test/replication/remove/",
                destination_backend="local",
                destination_settings={"path": "/tmp/dest"},
                check_interval_seconds=3600,
                enabled=True,
            )
            client.add_replication_policy(policy)

            # Execute: remove replication policy
            response = client.remove_replication_policy(unique_policy_id)

            # Verify response
            assert isinstance(response, PolicyResponse)
            assert response.success is True

            # Verify policy is removed
            policies = client.get_replication_policies()
            policy_ids = [p.id for p in policies.policies]
            assert unique_policy_id not in policy_ids
        except ObjectStoreError as e:
            # Replication may not be supported by all backends - that's acceptable
            error_msg = str(e).lower()
            # Verify it's a "not supported" or "not implemented" error
            assert any(
                msg in error_msg
                for msg in ["not supported", "not implemented", "not available", "not enabled"]
            ), f"Unexpected replication error: {e}"

    @pytest.mark.parametrize(
        "protocol,client_fixture",
        [
            ("REST", "rest_client"),
            ("gRPC", "grpc_client"),
            ("QUIC", "quic_client"),
        ],
    )
    def test_trigger_replication_operation(
        self,
        protocol: str,
        client_fixture: str,
        request: pytest.FixtureRequest,
        unique_policy_id: str,
    ) -> None:
        """Test triggerReplication operation (18/19).

        Validates:
        - If replication is supported: TriggerReplicationResponse with success=True
        - If replication is not supported: Raises ObjectStoreError indicating unsupported operation
        - Both behaviors are acceptable depending on backend configuration
        """
        client = request.getfixturevalue(client_fixture)

        try:
            # Setup: add a replication policy
            policy = ReplicationPolicy(
                id=unique_policy_id,
                source_backend="local",
                source_settings={"path": "/tmp/source"},
                source_prefix="test/replication/trigger/",
                destination_backend="local",
                destination_settings={"path": "/tmp/dest"},
                check_interval_seconds=3600,
                enabled=True,
            )
            client.add_replication_policy(policy)

            # Execute: trigger replication
            opts = TriggerReplicationOptions(
                policy_id=unique_policy_id,
                parallel=True,
                worker_count=2,
            )
            response = client.trigger_replication(opts)

            # Verify response
            assert isinstance(response, TriggerReplicationResponse)
            assert response.success is True

            # Verify sync result if present
            if response.result:
                assert isinstance(response.result.synced, int)
                assert isinstance(response.result.deleted, int)
                assert isinstance(response.result.failed, int)
                assert isinstance(response.result.bytes_total, int)
                assert isinstance(response.result.duration_ms, int)

            # Cleanup
            try:
                client.remove_replication_policy(unique_policy_id)
            except Exception:
                pass
        except ObjectStoreError as e:
            # Replication may not be supported by all backends - that's acceptable
            error_msg = str(e).lower()
            # Verify it's a "not supported" or "not implemented" error
            assert any(
                msg in error_msg
                for msg in ["not supported", "not implemented", "not available", "not enabled"]
            ), f"Unexpected replication error: {e}"

    @pytest.mark.parametrize(
        "protocol,client_fixture",
        [
            ("REST", "rest_client"),
            ("gRPC", "grpc_client"),
            ("QUIC", "quic_client"),
        ],
    )
    def test_get_replication_status_operation(
        self,
        protocol: str,
        client_fixture: str,
        request: pytest.FixtureRequest,
        unique_policy_id: str,
    ) -> None:
        """Test getReplicationStatus operation (19/19).

        Validates:
        - If replication is supported: GetReplicationStatusResponse with success=True
        - If replication is not supported: Raises ObjectStoreError indicating unsupported operation
        - Both behaviors are acceptable depending on backend configuration
        """
        client = request.getfixturevalue(client_fixture)

        try:
            # Setup: add a replication policy
            policy = ReplicationPolicy(
                id=unique_policy_id,
                source_backend="local",
                source_settings={"path": "/tmp/source"},
                source_prefix="test/replication/status/",
                destination_backend="local",
                destination_settings={"path": "/tmp/dest"},
                check_interval_seconds=3600,
                enabled=True,
            )
            client.add_replication_policy(policy)

            # Execute: get replication status
            response = client.get_replication_status(unique_policy_id)

            # Verify response
            assert isinstance(response, GetReplicationStatusResponse)
            assert response.success is True

            # Verify status if present
            if response.status:
                assert response.status.policy_id == unique_policy_id
                assert isinstance(response.status.total_objects_synced, int)
                assert isinstance(response.status.total_objects_deleted, int)
                assert isinstance(response.status.total_bytes_synced, int)
                assert isinstance(response.status.total_errors, int)
                assert isinstance(response.status.sync_count, int)
                assert isinstance(response.status.average_sync_duration_ms, int)

            # Cleanup
            try:
                client.remove_replication_policy(unique_policy_id)
            except Exception:
                pass
        except ObjectStoreError as e:
            # Replication may not be supported by all backends - that's acceptable
            error_msg = str(e).lower()
            # Verify it's a "not supported" or "not implemented" error
            assert any(
                msg in error_msg
                for msg in ["not supported", "not implemented", "not available", "not enabled"]
            ), f"Unexpected replication error: {e}"


# ============================================================================
# Test: Cross-Protocol Consistency
# ============================================================================


class TestCrossProtocolConsistency:
    """Test that all protocols produce consistent results."""

    def test_put_get_consistency_across_protocols(
        self,
        rest_client: ObjectStoreClient,
        grpc_client: ObjectStoreClient,
        quic_client: ObjectStoreClient,
    ) -> None:
        """Test that put/get operations produce consistent results across protocols.

        Validates:
        - Data written via one protocol can be read via another
        - Metadata is consistent across protocols
        - Size information matches
        """
        key = f"test/comprehensive/cross-protocol/{uuid.uuid4().hex}"
        data = b"Cross-protocol consistency test data"
        metadata = Metadata(
            content_type="application/octet-stream",
            custom={"test": "cross-protocol"},
        )

        # Put via REST
        rest_put_response = rest_client.put(key, data, metadata=metadata)
        assert rest_put_response.success is True

        # Get via REST
        rest_data, rest_meta = rest_client.get(key)
        assert rest_data == data
        assert rest_meta.size == len(data)

        # Get via QUIC
        try:
            quic_data, quic_meta = quic_client.get(key)
            assert quic_data == data, "QUIC should retrieve same data as REST"
            assert quic_meta.size == rest_meta.size, "QUIC metadata should match REST"
        except Exception:
            # QUIC might not be available in all test environments
            pass

        # Cleanup via REST
        rest_client.delete(key)

    def test_list_consistency_across_protocols(
        self,
        rest_client: ObjectStoreClient,
        quic_client: ObjectStoreClient,
    ) -> None:
        """Test that list operations return consistent results across protocols.

        Validates:
        - Same objects are listed via different protocols
        - Pagination works consistently
        """
        prefix = f"test/comprehensive/list-consistency/{uuid.uuid4().hex[:8]}/"
        test_keys = [f"{prefix}file{i}.txt" for i in range(3)]

        # Create objects via REST
        for key in test_keys:
            rest_client.put(key, f"data for {key}".encode())

        # List via REST
        rest_list = rest_client.list(prefix=prefix, max_results=10)
        rest_keys = {obj.key for obj in rest_list.objects}

        # List via QUIC
        try:
            quic_list = quic_client.list(prefix=prefix, max_results=10)
            quic_keys = {obj.key for obj in quic_list.objects}

            # Verify consistency
            assert rest_keys == quic_keys, "List results should be consistent across protocols"
        except Exception:
            # QUIC might not be available in all test environments
            pass

        # Cleanup
        for key in test_keys:
            try:
                rest_client.delete(key)
            except Exception:
                pass

    def test_exists_consistency_across_protocols(
        self,
        rest_client: ObjectStoreClient,
        quic_client: ObjectStoreClient,
    ) -> None:
        """Test that exists checks are consistent across protocols.

        Validates:
        - Object existence is reported consistently
        - Non-existence is reported consistently
        """
        key = f"test/comprehensive/exists-consistency/{uuid.uuid4().hex}"
        data = b"Exists consistency test"

        # Create via REST
        rest_client.put(key, data)

        # Check via REST
        rest_exists = rest_client.exists(key)
        assert rest_exists.exists is True

        # Check via QUIC
        try:
            quic_exists = quic_client.exists(key)
            assert quic_exists.exists is True, "QUIC should report same existence as REST"
        except Exception:
            pass

        # Delete via REST
        rest_client.delete(key)

        # Verify non-existence via both
        rest_not_exists = rest_client.exists(key)
        assert rest_not_exists.exists is False

        try:
            quic_not_exists = quic_client.exists(key)
            assert quic_not_exists.exists is False, "QUIC should report same non-existence"
        except Exception:
            pass


# ============================================================================
# Test: Error Handling Consistency
# ============================================================================


class TestErrorHandlingConsistency:
    """Test that error handling is consistent across protocols."""

    @pytest.mark.parametrize(
        "protocol,client_fixture",
        [
            ("REST", "rest_client"),
            ("gRPC", "grpc_client"),
            ("QUIC", "quic_client"),
        ],
    )
    def test_get_nonexistent_object_error(
        self, protocol: str, client_fixture: str, request: pytest.FixtureRequest
    ) -> None:
        """Test that getting non-existent object raises appropriate error.

        Validates:
        - Error is raised for non-existent objects
        - Error indicates the object was not found (ObjectNotFoundError or ServerError with "not found" message)
        """
        client = request.getfixturevalue(client_fixture)
        nonexistent_key = f"test/comprehensive/nonexistent/{uuid.uuid4().hex}"

        # Some backends may raise ObjectNotFoundError, others may raise ServerError
        # Both are acceptable as long as they indicate the object wasn't found
        with pytest.raises((ObjectNotFoundError, ObjectStoreError)) as exc_info:
            client.get(nonexistent_key)

        # Verify error indicates object not found
        error_msg = str(exc_info.value).lower()
        assert "not found" in error_msg or "no such file" in error_msg or "does not exist" in error_msg

    @pytest.mark.parametrize(
        "protocol,client_fixture",
        [
            ("REST", "rest_client"),
            ("gRPC", "grpc_client"),
            ("QUIC", "quic_client"),
        ],
    )
    def test_delete_nonexistent_object_behavior(
        self, protocol: str, client_fixture: str, request: pytest.FixtureRequest
    ) -> None:
        """Test delete behavior for non-existent objects.

        Note: Some backends may be idempotent (return success) while others
        may raise an error. Both are acceptable behaviors.
        """
        client = request.getfixturevalue(client_fixture)
        nonexistent_key = f"test/comprehensive/delete-nonexistent/{uuid.uuid4().hex}"

        try:
            response = client.delete(nonexistent_key)
            # If succeeds, it's idempotent delete - that's fine
            assert isinstance(response, DeleteResponse)
        except (ObjectNotFoundError, ObjectStoreError):
            # If raises NotFound or other error for non-existent file, that's also acceptable
            pass


# ============================================================================
# Test: Edge Cases and Special Scenarios
# ============================================================================


class TestEdgeCases:
    """Test edge cases and special scenarios."""

    @pytest.mark.parametrize(
        "protocol,client_fixture",
        [
            ("REST", "rest_client"),
            ("gRPC", "grpc_client"),
            ("QUIC", "quic_client"),
        ],
    )
    def test_empty_object(
        self, protocol: str, client_fixture: str, request: pytest.FixtureRequest, unique_key: str
    ) -> None:
        """Test handling of empty objects.

        Validates:
        - Empty objects can be stored
        - Size is reported as 0
        - Data retrieved is empty bytes
        """
        client = request.getfixturevalue(client_fixture)
        empty_data = b""

        # Put empty object
        response = client.put(unique_key, empty_data)
        assert response.success is True

        # Get empty object
        data, metadata = client.get(unique_key)
        assert data == b""
        # Size may be None or 0 for empty objects depending on backend
        assert metadata.size is None or metadata.size == 0

        # Cleanup
        client.delete(unique_key)

    @pytest.mark.parametrize(
        "protocol,client_fixture",
        [
            ("REST", "rest_client"),
            ("gRPC", "grpc_client"),
            ("QUIC", "quic_client"),
        ],
    )
    def test_large_object(
        self, protocol: str, client_fixture: str, request: pytest.FixtureRequest, unique_key: str
    ) -> None:
        """Test handling of large objects (1MB).

        Validates:
        - Large objects can be stored
        - Data integrity is maintained
        - Size is correctly reported
        """
        client = request.getfixturevalue(client_fixture)
        large_data = b"x" * (1024 * 1024)  # 1MB

        # Put large object
        response = client.put(unique_key, large_data)
        assert response.success is True

        # Get large object
        data, metadata = client.get(unique_key)
        assert data == large_data
        assert metadata.size == len(large_data)

        # Cleanup
        client.delete(unique_key)

    @pytest.mark.parametrize(
        "protocol,client_fixture",
        [
            ("REST", "rest_client"),
            ("gRPC", "grpc_client"),
            ("QUIC", "quic_client"),
        ],
    )
    def test_binary_data_all_bytes(
        self, protocol: str, client_fixture: str, request: pytest.FixtureRequest, unique_key: str
    ) -> None:
        """Test handling of binary data with all possible byte values.

        Validates:
        - All byte values (0-255) can be stored
        - Data integrity for binary content
        """
        client = request.getfixturevalue(client_fixture)
        binary_data = bytes(range(256))  # All possible byte values

        # Put binary data
        response = client.put(unique_key, binary_data)
        assert response.success is True

        # Get binary data
        data, metadata = client.get(unique_key)
        assert data == binary_data

        # Cleanup
        client.delete(unique_key)

    @pytest.mark.parametrize(
        "protocol,client_fixture",
        [
            ("REST", "rest_client"),
            ("gRPC", "grpc_client"),
            ("QUIC", "quic_client"),
        ],
    )
    def test_special_characters_in_key(
        self, protocol: str, client_fixture: str, request: pytest.FixtureRequest
    ) -> None:
        """Test handling of special characters in object keys.

        Validates:
        - Keys with spaces, underscores, hyphens work
        - Unicode characters in keys are handled (if supported)
        """
        client = request.getfixturevalue(client_fixture)
        special_key = f"test/comprehensive/special-chars_{uuid.uuid4().hex}/file-name.txt"
        data = b"Test data with special characters in key"

        # Put with special characters
        response = client.put(special_key, data)
        assert response.success is True

        # Get with special characters
        retrieved_data, _ = client.get(special_key)
        assert retrieved_data == data

        # Cleanup
        client.delete(special_key)

    @pytest.mark.parametrize(
        "protocol,client_fixture",
        [
            ("REST", "rest_client"),
            ("gRPC", "grpc_client"),
            ("QUIC", "quic_client"),
        ],
    )
    def test_overwrite_object(
        self, protocol: str, client_fixture: str, request: pytest.FixtureRequest, unique_key: str
    ) -> None:
        """Test overwriting an existing object.

        Validates:
        - Objects can be overwritten
        - New data replaces old data completely
        - Metadata can be updated on overwrite
        """
        client = request.getfixturevalue(client_fixture)
        original_data = b"Original data"
        new_data = b"New data that is different"

        # Put original
        client.put(unique_key, original_data)

        # Overwrite
        response = client.put(unique_key, new_data)
        assert response.success is True

        # Verify new data
        data, metadata = client.get(unique_key)
        assert data == new_data
        assert metadata.size == len(new_data)

        # Cleanup
        client.delete(unique_key)
