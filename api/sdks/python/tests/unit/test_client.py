"""Unit tests for unified ObjectStoreClient."""

import pytest
import responses

from objstore.client import ObjectStoreClient, Protocol
from objstore.exceptions import ValidationError
from objstore.models import HealthStatus, Metadata


class TestObjectStoreClient:
    """Test cases for ObjectStoreClient."""

    def test_init_rest(self) -> None:
        """Test initialization with REST protocol."""
        client = ObjectStoreClient(protocol=Protocol.REST, base_url="http://localhost:8080")
        assert client.protocol == Protocol.REST

    def test_init_grpc(self) -> None:
        """Test initialization with gRPC protocol."""
        # Will fail if proto files not generated, but tests the code path
        try:
            client = ObjectStoreClient(protocol=Protocol.GRPC, host="localhost", port=50051)
            assert client.protocol == Protocol.GRPC
        except ImportError:
            # Expected if proto files not generated
            pass

    def test_init_quic(self) -> None:
        """Test initialization with QUIC protocol."""
        client = ObjectStoreClient(protocol=Protocol.QUIC, base_url="https://localhost:4433")
        assert client.protocol == Protocol.QUIC

    def test_init_default_values(self) -> None:
        """Test initialization with default values."""
        client = ObjectStoreClient(protocol=Protocol.REST)
        assert client.timeout == 30
        assert client.max_retries == 3

    @responses.activate
    def test_put_rest(self) -> None:
        """Test put operation with REST."""
        responses.add(
            responses.PUT,
            "http://localhost:8080/api/v1/objects/test-key",
            json={"message": "success", "data": {"etag": "abc123"}},
            status=201,
        )

        client = ObjectStoreClient(protocol=Protocol.REST)
        result = client.put("test-key", b"test data")
        assert result.success is True

    @responses.activate
    def test_get_rest(self) -> None:
        """Test get operation with REST."""
        responses.add(
            responses.GET,
            "http://localhost:8080/api/v1/objects/test-key",
            body=b"test data",
            headers={"Content-Type": "text/plain", "Content-Length": "9"},
            status=200,
        )

        client = ObjectStoreClient(protocol=Protocol.REST)
        data, metadata = client.get("test-key")
        assert data == b"test data"

    @responses.activate
    def test_delete_rest(self) -> None:
        """Test delete operation with REST."""
        responses.add(
            responses.DELETE,
            "http://localhost:8080/api/v1/objects/test-key",
            json={"message": "deleted"},
            status=200,
        )

        client = ObjectStoreClient(protocol=Protocol.REST)
        result = client.delete("test-key")
        assert result.success is True

    @responses.activate
    def test_list_rest(self) -> None:
        """Test list operation with REST."""
        responses.add(
            responses.GET,
            "http://localhost:8080/api/v1/objects",
            json={"objects": [], "truncated": False},
            status=200,
        )

        client = ObjectStoreClient(protocol=Protocol.REST)
        result = client.list()
        assert isinstance(result.objects, list)

    @responses.activate
    def test_exists_rest(self) -> None:
        """Test exists operation with REST."""
        responses.add(
            responses.HEAD,
            "http://localhost:8080/api/v1/objects/test-key",
            status=200,
        )

        client = ObjectStoreClient(protocol=Protocol.REST)
        result = client.exists("test-key")
        assert result.exists is True

    @responses.activate
    def test_get_metadata_rest(self) -> None:
        """Test get metadata with REST."""
        responses.add(
            responses.GET,
            "http://localhost:8080/api/v1/metadata/test-key",
            json={"size": 100, "etag": "abc123", "metadata": {}},
            status=200,
        )

        client = ObjectStoreClient(protocol=Protocol.REST)
        metadata = client.get_metadata("test-key")
        assert metadata.size == 100

    @responses.activate
    def test_update_metadata_rest(self) -> None:
        """Test update metadata with REST."""
        responses.add(
            responses.PUT,
            "http://localhost:8080/api/v1/metadata/test-key",
            json={"message": "updated"},
            status=200,
        )

        client = ObjectStoreClient(protocol=Protocol.REST)
        metadata = Metadata(content_type="text/plain")
        result = client.update_metadata("test-key", metadata)
        assert result.success is True

    @responses.activate
    def test_health_rest(self) -> None:
        """Test health check with REST."""
        responses.add(
            responses.GET,
            "http://localhost:8080/health",
            json={"status": "SERVING"},
            status=200,
        )

        client = ObjectStoreClient(protocol=Protocol.REST)
        result = client.health()
        assert result.status == HealthStatus.SERVING

    def test_context_manager(self) -> None:
        """Test context manager."""
        with ObjectStoreClient(protocol=Protocol.REST) as client:
            assert client is not None

    @responses.activate
    def test_get_stream_rest(self) -> None:
        """Test streaming get with REST."""
        responses.add(
            responses.GET,
            "http://localhost:8080/api/v1/objects/test-key",
            body=b"test data",
            status=200,
        )

        client = ObjectStoreClient(protocol=Protocol.REST)
        chunks = list(client.get_stream("test-key"))
        assert len(chunks) > 0

    @responses.activate
    def test_archive_rest(self) -> None:
        """Test archive operation with REST."""
        responses.add(
            responses.POST,
            "http://localhost:8080/api/v1/archive",
            json={"message": "archived"},
            status=200,
        )

        client = ObjectStoreClient(protocol=Protocol.REST)
        result = client.archive("test-key", "s3", {"bucket": "backup"})
        assert result.success is True

    @responses.activate
    def test_add_policy_rest(self) -> None:
        """Test add policy with REST."""
        from objstore.models import LifecyclePolicy

        responses.add(
            responses.POST,
            "http://localhost:8080/api/v1/policies",
            json={"message": "Policy added"},
            status=201,
        )

        client = ObjectStoreClient(protocol=Protocol.REST)
        policy = LifecyclePolicy(
            id="policy-1",
            prefix="logs/",
            retention_seconds=86400,
            action="delete",
        )
        result = client.add_policy(policy)
        assert result.success is True

    @responses.activate
    def test_remove_policy_rest(self) -> None:
        """Test remove policy with REST."""
        responses.add(
            responses.DELETE,
            "http://localhost:8080/api/v1/policies/policy-1",
            json={"message": "Policy removed"},
            status=200,
        )

        client = ObjectStoreClient(protocol=Protocol.REST)
        result = client.remove_policy("policy-1")
        assert result.success is True

    @responses.activate
    def test_get_policies_rest(self) -> None:
        """Test get policies with REST."""
        responses.add(
            responses.GET,
            "http://localhost:8080/api/v1/policies",
            json={
                "policies": [
                    {"id": "policy-1", "prefix": "logs/", "retention_seconds": 86400, "action": "delete"}
                ],
                "message": "Policies retrieved",
            },
            status=200,
        )

        client = ObjectStoreClient(protocol=Protocol.REST)
        result = client.get_policies()
        assert result.success is True

    @responses.activate
    def test_apply_policies_rest(self) -> None:
        """Test apply policies with REST."""
        responses.add(
            responses.POST,
            "http://localhost:8080/api/v1/policies/apply",
            json={
                "policies_count": 3,
                "objects_processed": 100,
                "message": "Policies applied",
            },
            status=200,
        )

        client = ObjectStoreClient(protocol=Protocol.REST)
        result = client.apply_policies()
        assert result.success is True

    @responses.activate
    def test_add_replication_policy_rest(self) -> None:
        """Test add replication policy with REST."""
        from objstore.models import ReplicationPolicy

        responses.add(
            responses.POST,
            "http://localhost:8080/api/v1/replication/policies",
            json={"message": "Replication policy added"},
            status=201,
        )

        client = ObjectStoreClient(protocol=Protocol.REST)
        policy = ReplicationPolicy(
            id="repl-1",
            source_backend="local",
            source_settings={"path": "/data"},
            destination_backend="s3",
            destination_settings={"bucket": "backup"},
            check_interval_seconds=300,
        )
        result = client.add_replication_policy(policy)
        assert result.success is True

    @responses.activate
    def test_remove_replication_policy_rest(self) -> None:
        """Test remove replication policy with REST."""
        responses.add(
            responses.DELETE,
            "http://localhost:8080/api/v1/replication/policies/repl-1",
            json={"message": "Replication policy removed"},
            status=200,
        )

        client = ObjectStoreClient(protocol=Protocol.REST)
        result = client.remove_replication_policy("repl-1")
        assert result.success is True

    @responses.activate
    def test_get_replication_policies_rest(self) -> None:
        """Test get replication policies with REST."""
        responses.add(
            responses.GET,
            "http://localhost:8080/api/v1/replication/policies",
            json={
                "policies": [
                    {
                        "id": "repl-1",
                        "source_backend": "local",
                        "destination_backend": "s3",
                        "check_interval_seconds": 300,
                    }
                ]
            },
            status=200,
        )

        client = ObjectStoreClient(protocol=Protocol.REST)
        result = client.get_replication_policies()
        assert len(result.policies) == 1

    @responses.activate
    def test_get_replication_policy_rest(self) -> None:
        """Test get replication policy with REST."""
        responses.add(
            responses.GET,
            "http://localhost:8080/api/v1/replication/policies/repl-1",
            json={
                "policy": {
                    "id": "repl-1",
                    "source_backend": "local",
                    "destination_backend": "s3",
                    "check_interval_seconds": 300,
                }
            },
            status=200,
        )

        client = ObjectStoreClient(protocol=Protocol.REST)
        policy = client.get_replication_policy("repl-1")
        assert policy.id == "repl-1"

    @responses.activate
    def test_trigger_replication_rest(self) -> None:
        """Test trigger replication with REST."""
        from objstore.models import TriggerReplicationOptions

        responses.add(
            responses.POST,
            "http://localhost:8080/api/v1/replication/trigger",
            json={
                "result": {
                    "policy_id": "repl-1",
                    "synced": 100,
                    "deleted": 5,
                    "failed": 0,
                    "bytes_total": 1048576,
                    "duration_ms": 5000,
                },
                "message": "Replication triggered",
            },
            status=200,
        )

        client = ObjectStoreClient(protocol=Protocol.REST)
        opts = TriggerReplicationOptions(policy_id="repl-1", parallel=True)
        result = client.trigger_replication(opts)
        assert result.success is True

    @responses.activate
    def test_get_replication_status_rest(self) -> None:
        """Test get replication status with REST."""
        responses.add(
            responses.GET,
            "http://localhost:8080/api/v1/replication/status/repl-1",
            json={
                "status": {
                    "policy_id": "repl-1",
                    "source_backend": "local",
                    "destination_backend": "s3",
                    "enabled": True,
                    "total_objects_synced": 1000,
                    "total_objects_deleted": 10,
                    "total_bytes_synced": 10485760,
                    "total_errors": 0,
                    "average_sync_duration_ms": 2000,
                    "sync_count": 5,
                },
                "message": "Status retrieved",
            },
            status=200,
        )

        client = ObjectStoreClient(protocol=Protocol.REST)
        result = client.get_replication_status("repl-1")
        assert result.success is True


class TestProtocolEnum:
    """Test Protocol enum."""

    def test_protocol_rest(self) -> None:
        """Test REST protocol value."""
        assert Protocol.REST.value == "rest"

    def test_protocol_grpc(self) -> None:
        """Test gRPC protocol value."""
        assert Protocol.GRPC.value == "grpc"

    def test_protocol_quic(self) -> None:
        """Test QUIC protocol value."""
        assert Protocol.QUIC.value == "quic"


class TestQuicProtocolOperations:
    """Test operations with QUIC protocol using mocks."""

    def test_put_quic(self) -> None:
        """Test put with QUIC protocol."""
        from unittest.mock import MagicMock, AsyncMock, patch
        import asyncio

        client = ObjectStoreClient(protocol=Protocol.QUIC)
        mock_response = MagicMock()
        mock_response.success = True
        mock_response.etag = "abc123"

        with patch.object(client._client, "put", new_callable=AsyncMock) as mock_put:
            mock_put.return_value = mock_response
            result = client.put("test-key", b"test data")

        assert result.success is True

    def test_get_quic(self) -> None:
        """Test get with QUIC protocol."""
        from unittest.mock import MagicMock, AsyncMock, patch

        client = ObjectStoreClient(protocol=Protocol.QUIC)
        mock_metadata = Metadata(content_type="text/plain", size=9)

        with patch.object(client._client, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = (b"test data", mock_metadata)
            data, metadata = client.get("test-key")

        assert data == b"test data"

    def test_delete_quic(self) -> None:
        """Test delete with QUIC protocol."""
        from unittest.mock import MagicMock, AsyncMock, patch

        client = ObjectStoreClient(protocol=Protocol.QUIC)
        mock_response = MagicMock()
        mock_response.success = True

        with patch.object(client._client, "delete", new_callable=AsyncMock) as mock_delete:
            mock_delete.return_value = mock_response
            result = client.delete("test-key")

        assert result.success is True

    def test_list_quic(self) -> None:
        """Test list with QUIC protocol."""
        from unittest.mock import MagicMock, AsyncMock, patch
        from objstore.models import ListResponse

        client = ObjectStoreClient(protocol=Protocol.QUIC)
        mock_response = ListResponse(objects=[], truncated=False)

        with patch.object(client._client, "list", new_callable=AsyncMock) as mock_list:
            mock_list.return_value = mock_response
            result = client.list()

        assert isinstance(result.objects, list)

    def test_exists_quic(self) -> None:
        """Test exists with QUIC protocol."""
        from unittest.mock import MagicMock, AsyncMock, patch
        from objstore.models import ExistsResponse

        client = ObjectStoreClient(protocol=Protocol.QUIC)
        mock_response = ExistsResponse(exists=True)

        with patch.object(client._client, "exists", new_callable=AsyncMock) as mock_exists:
            mock_exists.return_value = mock_response
            result = client.exists("test-key")

        assert result.exists is True

    def test_get_metadata_quic(self) -> None:
        """Test get metadata with QUIC protocol."""
        from unittest.mock import MagicMock, AsyncMock, patch

        client = ObjectStoreClient(protocol=Protocol.QUIC)
        mock_metadata = Metadata(content_type="text/plain", size=100)

        with patch.object(client._client, "get_metadata", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_metadata
            metadata = client.get_metadata("test-key")

        assert metadata.size == 100

    def test_update_metadata_quic(self) -> None:
        """Test update metadata with QUIC protocol."""
        from unittest.mock import MagicMock, AsyncMock, patch
        from objstore.models import PolicyResponse

        client = ObjectStoreClient(protocol=Protocol.QUIC)
        mock_response = PolicyResponse(success=True)

        with patch.object(client._client, "update_metadata", new_callable=AsyncMock) as mock_update:
            mock_update.return_value = mock_response
            metadata = Metadata(content_type="text/plain")
            result = client.update_metadata("test-key", metadata)

        assert result.success is True

    def test_health_quic(self) -> None:
        """Test health with QUIC protocol."""
        from unittest.mock import MagicMock, AsyncMock, patch
        from objstore.models import HealthResponse

        client = ObjectStoreClient(protocol=Protocol.QUIC)
        mock_response = HealthResponse(status=HealthStatus.SERVING)

        with patch.object(client._client, "health", new_callable=AsyncMock) as mock_health:
            mock_health.return_value = mock_response
            result = client.health()

        assert result.status == HealthStatus.SERVING

    def test_archive_quic(self) -> None:
        """Test archive with QUIC protocol."""
        from unittest.mock import MagicMock, AsyncMock, patch
        from objstore.models import ArchiveResponse

        client = ObjectStoreClient(protocol=Protocol.QUIC)
        mock_response = ArchiveResponse(success=True)

        with patch.object(client._client, "archive", new_callable=AsyncMock) as mock_archive:
            mock_archive.return_value = mock_response
            result = client.archive("test-key", "s3", {"bucket": "backup"})

        assert result.success is True

    def test_add_policy_quic(self) -> None:
        """Test add policy with QUIC protocol."""
        from unittest.mock import MagicMock, AsyncMock, patch
        from objstore.models import PolicyResponse, LifecyclePolicy

        client = ObjectStoreClient(protocol=Protocol.QUIC)
        mock_response = PolicyResponse(success=True)

        with patch.object(client._client, "add_policy", new_callable=AsyncMock) as mock_add:
            mock_add.return_value = mock_response
            policy = LifecyclePolicy(
                id="policy-1",
                prefix="logs/",
                retention_seconds=86400,
                action="delete",
            )
            result = client.add_policy(policy)

        assert result.success is True

    def test_remove_policy_quic(self) -> None:
        """Test remove policy with QUIC protocol."""
        from unittest.mock import MagicMock, AsyncMock, patch
        from objstore.models import PolicyResponse

        client = ObjectStoreClient(protocol=Protocol.QUIC)
        mock_response = PolicyResponse(success=True)

        with patch.object(client._client, "remove_policy", new_callable=AsyncMock) as mock_remove:
            mock_remove.return_value = mock_response
            result = client.remove_policy("policy-1")

        assert result.success is True

    def test_get_policies_quic(self) -> None:
        """Test get policies with QUIC protocol."""
        from unittest.mock import MagicMock, AsyncMock, patch
        from objstore.models import GetPoliciesResponse

        client = ObjectStoreClient(protocol=Protocol.QUIC)
        mock_response = GetPoliciesResponse(policies=[], success=True)

        with patch.object(client._client, "get_policies", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            result = client.get_policies()

        assert result.success is True

    def test_apply_policies_quic(self) -> None:
        """Test apply policies with QUIC protocol."""
        from unittest.mock import MagicMock, AsyncMock, patch
        from objstore.models import ApplyPoliciesResponse

        client = ObjectStoreClient(protocol=Protocol.QUIC)
        mock_response = ApplyPoliciesResponse(
            success=True, policies_count=3, objects_processed=100
        )

        with patch.object(client._client, "apply_policies", new_callable=AsyncMock) as mock_apply:
            mock_apply.return_value = mock_response
            result = client.apply_policies()

        assert result.success is True

    def test_add_replication_policy_quic(self) -> None:
        """Test add replication policy with QUIC protocol."""
        from unittest.mock import MagicMock, AsyncMock, patch
        from objstore.models import PolicyResponse, ReplicationPolicy

        client = ObjectStoreClient(protocol=Protocol.QUIC)
        mock_response = PolicyResponse(success=True)

        with patch.object(client._client, "add_replication_policy", new_callable=AsyncMock) as mock_add:
            mock_add.return_value = mock_response
            policy = ReplicationPolicy(
                id="repl-1",
                source_backend="local",
                destination_backend="s3",
                check_interval_seconds=300,
            )
            result = client.add_replication_policy(policy)

        assert result.success is True

    def test_remove_replication_policy_quic(self) -> None:
        """Test remove replication policy with QUIC protocol."""
        from unittest.mock import MagicMock, AsyncMock, patch
        from objstore.models import PolicyResponse

        client = ObjectStoreClient(protocol=Protocol.QUIC)
        mock_response = PolicyResponse(success=True)

        with patch.object(client._client, "remove_replication_policy", new_callable=AsyncMock) as mock_remove:
            mock_remove.return_value = mock_response
            result = client.remove_replication_policy("repl-1")

        assert result.success is True

    def test_get_replication_policies_quic(self) -> None:
        """Test get replication policies with QUIC protocol."""
        from unittest.mock import MagicMock, AsyncMock, patch
        from objstore.models import GetReplicationPoliciesResponse

        client = ObjectStoreClient(protocol=Protocol.QUIC)
        mock_response = GetReplicationPoliciesResponse(policies=[])

        with patch.object(client._client, "get_replication_policies", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            result = client.get_replication_policies()

        assert len(result.policies) == 0

    def test_get_replication_policy_quic(self) -> None:
        """Test get replication policy with QUIC protocol."""
        from unittest.mock import MagicMock, AsyncMock, patch
        from objstore.models import ReplicationPolicy

        client = ObjectStoreClient(protocol=Protocol.QUIC)
        mock_policy = ReplicationPolicy(
            id="repl-1",
            source_backend="local",
            destination_backend="s3",
            check_interval_seconds=300,
        )

        with patch.object(client._client, "get_replication_policy", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_policy
            policy = client.get_replication_policy("repl-1")

        assert policy.id == "repl-1"

    def test_trigger_replication_quic(self) -> None:
        """Test trigger replication with QUIC protocol."""
        from unittest.mock import MagicMock, AsyncMock, patch
        from objstore.models import TriggerReplicationResponse, TriggerReplicationOptions

        client = ObjectStoreClient(protocol=Protocol.QUIC)
        mock_response = TriggerReplicationResponse(success=True)

        with patch.object(client._client, "trigger_replication", new_callable=AsyncMock) as mock_trigger:
            mock_trigger.return_value = mock_response
            opts = TriggerReplicationOptions(policy_id="repl-1", parallel=True)
            result = client.trigger_replication(opts)

        assert result.success is True

    def test_get_replication_status_quic(self) -> None:
        """Test get replication status with QUIC protocol."""
        from unittest.mock import MagicMock, AsyncMock, patch
        from objstore.models import GetReplicationStatusResponse

        client = ObjectStoreClient(protocol=Protocol.QUIC)
        mock_response = GetReplicationStatusResponse(success=True)

        with patch.object(client._client, "get_replication_status", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            result = client.get_replication_status("repl-1")

        assert result.success is True
