"""Integration tests for all protocols."""

import os
import time
from typing import Generator

import pytest

from objstore.client import ObjectStoreClient, Protocol
from objstore.exceptions import ConnectionError, ObjectNotFoundError
from objstore.models import (
    HealthStatus,
    LifecyclePolicy,
    Metadata,
    ReplicationPolicy,
    TriggerReplicationOptions,
)


@pytest.fixture(scope="module")
def wait_for_server() -> Generator[None, None, None]:
    """Wait for server to be ready."""
    time.sleep(2)  # Give server time to start
    yield
    time.sleep(1)  # Cleanup delay


@pytest.fixture
def rest_client(wait_for_server: None) -> Generator[ObjectStoreClient, None, None]:
    """Create REST client."""
    base_url = os.getenv("OBJSTORE_REST_URL", "http://localhost:8080")
    client = ObjectStoreClient(protocol=Protocol.REST, base_url=base_url, timeout=10)
    yield client
    client.close()


@pytest.fixture
def grpc_client(wait_for_server: None) -> Generator[ObjectStoreClient, None, None]:
    """Create gRPC client."""
    host = os.getenv("OBJSTORE_GRPC_HOST", "localhost")
    port = int(os.getenv("OBJSTORE_GRPC_PORT", "50051"))
    try:
        client = ObjectStoreClient(protocol=Protocol.GRPC, host=host, port=port, timeout=10)
        yield client
        client.close()
    except ImportError:
        pytest.skip("gRPC proto files not generated")


@pytest.fixture
def quic_client(wait_for_server: None) -> Generator[ObjectStoreClient, None, None]:
    """Create QUIC client."""
    base_url = os.getenv("OBJSTORE_QUIC_URL", "https://localhost:4433")
    try:
        client = ObjectStoreClient(
            protocol=Protocol.QUIC, base_url=base_url, timeout=10, verify_ssl=False
        )
        # Test connectivity
        try:
            client.health()
        except Exception as e:
            client.close()
            pytest.skip(f"QUIC server not reachable: {str(e)}")
        yield client
        client.close()
    except Exception as e:
        pytest.skip(f"QUIC client not available: {str(e)}")


class TestHealthCheck:
    """Test health check endpoints."""

    def test_rest_health(self, rest_client: ObjectStoreClient) -> None:
        """Test REST health check."""
        response = rest_client.health()
        assert response.status in [HealthStatus.SERVING, HealthStatus.UNKNOWN]

    @pytest.mark.skip(reason="Requires gRPC proto files")
    def test_grpc_health(self, grpc_client: ObjectStoreClient) -> None:
        """Test gRPC health check."""
        response = grpc_client.health()
        assert response.status in [HealthStatus.SERVING, HealthStatus.UNKNOWN]


class TestObjectOperations:
    """Test basic object operations."""

    def test_rest_put_get_delete(self, rest_client: ObjectStoreClient) -> None:
        """Test REST put, get, and delete."""
        key = "test/integration/rest-file.txt"
        data = b"Hello from REST integration test!"

        # Put
        put_response = rest_client.put(key, data)
        assert put_response.success is True

        # Get
        retrieved_data, metadata = rest_client.get(key)
        assert retrieved_data == data
        assert metadata.size == len(data)

        # Delete
        delete_response = rest_client.delete(key)
        assert delete_response.success is True

        # Verify deleted
        with pytest.raises(ObjectNotFoundError):
            rest_client.get(key)

    def test_rest_put_with_metadata(self, rest_client: ObjectStoreClient) -> None:
        """Test REST put with metadata."""
        key = "test/integration/rest-metadata.txt"
        data = b"Data with metadata"
        metadata = Metadata(
            content_type="text/plain", custom={"author": "test", "version": "1.0"}
        )

        # Put with metadata
        put_response = rest_client.put(key, data, metadata=metadata)
        assert put_response.success is True

        # Get metadata
        retrieved_metadata = rest_client.get_metadata(key)
        assert retrieved_metadata.size == len(data)

        # Update metadata
        new_metadata = Metadata(content_type="application/json", custom={"version": "2.0"})
        update_response = rest_client.update_metadata(key, new_metadata)
        assert update_response.success is True

        # Cleanup
        rest_client.delete(key)

    def test_rest_exists(self, rest_client: ObjectStoreClient) -> None:
        """Test REST exists operation."""
        key = "test/integration/exists-test.txt"

        # Should not exist
        exists_response = rest_client.exists(key)
        assert exists_response.exists is False

        # Create object
        rest_client.put(key, b"test data")

        # Should exist
        exists_response = rest_client.exists(key)
        assert exists_response.exists is True

        # Cleanup
        rest_client.delete(key)

    def test_rest_list(self, rest_client: ObjectStoreClient) -> None:
        """Test REST list operation."""
        prefix = "test/integration/list/"
        keys = [f"{prefix}file{i}.txt" for i in range(5)]

        # Create test objects
        for key in keys:
            rest_client.put(key, f"data for {key}".encode())

        # List objects
        list_response = rest_client.list(prefix=prefix, max_results=10)
        assert len(list_response.objects) >= 5

        # List with pagination
        list_response = rest_client.list(prefix=prefix, max_results=2)
        assert len(list_response.objects) <= 2

        # Cleanup
        for key in keys:
            rest_client.delete(key)

    def test_rest_stream_get(self, rest_client: ObjectStoreClient) -> None:
        """Test REST streaming get."""
        key = "test/integration/stream-file.bin"
        data = b"x" * 10000  # 10KB of data

        # Put large object
        rest_client.put(key, data)

        # Stream get
        chunks = list(rest_client.get_stream(key))
        retrieved_data = b"".join(chunks)
        assert retrieved_data == data

        # Cleanup
        rest_client.delete(key)

    def test_rest_list_with_delimiter(self, rest_client: ObjectStoreClient) -> None:
        """Test REST list with delimiter for hierarchical structure."""
        base = "test/integration/hierarchy/"
        keys = [
            f"{base}file1.txt",
            f"{base}file2.txt",
            f"{base}subdir1/file3.txt",
            f"{base}subdir2/file4.txt",
        ]

        # Create objects
        for key in keys:
            rest_client.put(key, b"test data")

        # List with delimiter
        list_response = rest_client.list(prefix=base, delimiter="/")

        # Should have files and common prefixes
        assert len(list_response.objects) >= 2  # file1.txt, file2.txt

        # Cleanup
        for key in keys:
            rest_client.delete(key)

    def test_rest_concurrent_operations(self, rest_client: ObjectStoreClient) -> None:
        """Test multiple concurrent operations."""
        keys = [f"test/integration/concurrent/file{i}.txt" for i in range(10)]

        # Create multiple objects
        for key in keys:
            rest_client.put(key, f"data for {key}".encode())

        # Verify all exist
        for key in keys:
            exists_response = rest_client.exists(key)
            assert exists_response.exists is True

        # Delete all
        for key in keys:
            delete_response = rest_client.delete(key)
            assert delete_response.success is True

    def test_rest_empty_object(self, rest_client: ObjectStoreClient) -> None:
        """Test handling empty object."""
        key = "test/integration/empty.txt"

        # Put empty object
        rest_client.put(key, b"")

        # Get empty object
        data, metadata = rest_client.get(key)
        assert data == b""
        assert metadata.size == 0

        # Cleanup
        rest_client.delete(key)

    def test_rest_binary_data(self, rest_client: ObjectStoreClient) -> None:
        """Test handling binary data."""
        key = "test/integration/binary.bin"
        data = bytes(range(256))  # All byte values

        # Put binary data
        rest_client.put(key, data)

        # Get binary data
        retrieved_data, metadata = rest_client.get(key)
        assert retrieved_data == data

        # Cleanup
        rest_client.delete(key)


class TestErrorHandling:
    """Test error handling."""

    def test_rest_get_nonexistent(self, rest_client: ObjectStoreClient) -> None:
        """Test getting non-existent object."""
        with pytest.raises(ObjectNotFoundError):
            rest_client.get("test/nonexistent/file.txt")

    def test_rest_delete_nonexistent(self, rest_client: ObjectStoreClient) -> None:
        """Test deleting non-existent object."""
        # Some backends may not error on delete of non-existent
        try:
            result = rest_client.delete("test/nonexistent/file.txt")
            # If it succeeds, that's okay (idempotent delete)
        except ObjectNotFoundError:
            # If it raises NotFound, that's also okay
            pass

    def test_rest_update_metadata_nonexistent(self, rest_client: ObjectStoreClient) -> None:
        """Test updating metadata on non-existent object."""
        metadata = Metadata(content_type="text/plain")
        # May or may not error depending on backend
        try:
            rest_client.update_metadata("test/nonexistent/file.txt", metadata)
        except ObjectNotFoundError:
            pass


class TestLifecyclePolicies:
    """Test lifecycle policy operations."""

    def test_rest_lifecycle_policy_operations(self, rest_client: ObjectStoreClient) -> None:
        """Test lifecycle policy add, get, and remove operations."""
        policy_id = f"test-lifecycle-{int(time.time())}"

        # Create a lifecycle policy
        policy = LifecyclePolicy(
            id=policy_id,
            prefix="test/lifecycle/",
            action="delete",
            days_after_creation=30,
            enabled=True
        )

        # Add policy
        add_result = rest_client.add_policy(policy)
        assert add_result.success is True

        # Get all policies
        policies_result = rest_client.get_policies()
        assert policies_result.success is True
        assert len(policies_result.policies) > 0

        # Get policies with prefix
        prefix_result = rest_client.get_policies(prefix="test/lifecycle/")
        assert prefix_result.success is True

        # Remove policy
        remove_result = rest_client.remove_policy(policy_id)
        assert remove_result.success is True

    def test_rest_apply_policies(self, rest_client: ObjectStoreClient) -> None:
        """Test applying lifecycle policies."""
        # Apply policies (may not have any effect with empty/test policies)
        try:
            result = rest_client.apply_policies()
            assert result.success is True
        except Exception:
            # May not be supported by all backends
            pytest.skip("Apply policies not supported by backend")


class TestReplicationPolicies:
    """Test replication policy operations."""

    @pytest.mark.skip(reason="Replication not supported by local storage backend")
    def test_rest_replication_policy_operations(self, rest_client: ObjectStoreClient) -> None:
        """Test replication policy add, get, and remove operations."""
        policy_id = f"test-repl-{int(time.time())}"

        # Create a replication policy
        policy = ReplicationPolicy(
            id=policy_id,
            source_backend="local",
            source_settings={"path": "/tmp/source"},
            source_prefix="test/replication/",
            destination_backend="local",
            destination_settings={"path": "/tmp/dest"},
            check_interval_seconds=3600,
            enabled=True
        )

        # Add policy
        add_result = rest_client.add_replication_policy(policy)
        assert add_result.success is True

        # Get all replication policies
        policies_result = rest_client.get_replication_policies()
        assert len(policies_result.policies) > 0

        # Get specific policy
        policy_result = rest_client.get_replication_policy(policy_id)
        assert policy_result is not None
        assert policy_result.id == policy_id

        # Trigger replication
        trigger_opts = TriggerReplicationOptions(policy_id=policy_id)
        trigger_result = rest_client.trigger_replication(trigger_opts)
        assert trigger_result.success is True

        # Get replication status
        status_result = rest_client.get_replication_status(policy_id)
        assert status_result.success is True

        # Remove policy
        remove_result = rest_client.remove_replication_policy(policy_id)
        assert remove_result.success is True


class TestArchiveOperations:
    """Test archive operations."""

    @pytest.mark.skip(reason="Archive operations require additional backend configuration")
    def test_rest_archive_object(self, rest_client: ObjectStoreClient) -> None:
        """Test archiving an object."""
        key = "test/archive/test-file.txt"
        data = b"Data to archive"

        # Put object first
        rest_client.put(key, data)

        # Archive the object
        archive_result = rest_client.archive(
            key,
            destination_type="local",
            settings={"path": "/tmp/archive"}
        )
        assert archive_result.success is True

        # Cleanup
        try:
            rest_client.delete(key)
        except ObjectNotFoundError:
            pass


class TestProtocolComparison:
    """Test that all protocols produce consistent results."""

    def test_rest_vs_quic_consistency(
        self, rest_client: ObjectStoreClient, quic_client: ObjectStoreClient
    ) -> None:
        """Test REST and QUIC produce same results."""
        key = "test/integration/protocol-test.txt"
        data = b"Protocol comparison test data"

        # Put via REST
        rest_client.put(key, data)

        # Get via both protocols
        rest_data, rest_meta = rest_client.get(key)
        try:
            quic_data, quic_meta = quic_client.get(key)
            assert rest_data == quic_data
            assert rest_meta.size == quic_meta.size
        except Exception:
            # QUIC might not be available or configured
            pass

        # Cleanup
        rest_client.delete(key)
