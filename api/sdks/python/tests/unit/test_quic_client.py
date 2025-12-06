"""Unit tests for QUIC/HTTP3 client."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import httpx

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
    ArchiveResponse,
    ApplyPoliciesResponse,
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
    ObjectInfo,
    PolicyResponse,
    PutResponse,
    ReplicationPolicy,
    ReplicationStatus,
    SyncResult,
    TriggerReplicationOptions,
    TriggerReplicationResponse,
)
from objstore.quic_client import QuicClient


class TestQuicClient:
    """Test cases for QuicClient."""

    def test_init(self) -> None:
        """Test client initialization."""
        client = QuicClient(
            base_url="https://localhost:4433",
            api_version="v1",
            timeout=30,
            verify_ssl=False,
        )
        assert client.base_url == "https://localhost:4433"
        assert client.api_version == "v1"
        assert client.timeout == 30
        assert client.verify_ssl is False

    def test_url_construction(self) -> None:
        """Test URL construction."""
        client = QuicClient(base_url="https://localhost:4433", api_version="v1")
        assert client._url("objects/test") == "https://localhost:4433/api/v1/objects/test"
        assert client._url("/objects/test") == "https://localhost:4433/api/v1/objects/test"

    def test_url_construction_with_trailing_slash(self) -> None:
        """Test URL construction with trailing slash in base_url."""
        client = QuicClient(base_url="https://localhost:4433/", api_version="v1")
        assert client.base_url == "https://localhost:4433"

    @pytest.mark.asyncio
    async def test_put_success(self) -> None:
        """Test successful put operation."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"message": "success", "data": {"etag": "abc123"}}

        with patch.object(client.client, "put", new_callable=AsyncMock) as mock_put:
            mock_put.return_value = mock_response
            result = await client.put("test-key", b"test data")

        assert result.success is True
        assert result.etag == "abc123"

    @pytest.mark.asyncio
    async def test_put_with_metadata(self) -> None:
        """Test put with metadata."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"message": "success", "data": {"etag": "abc123"}}

        with patch.object(client.client, "put", new_callable=AsyncMock) as mock_put:
            mock_put.return_value = mock_response
            metadata = Metadata(content_type="text/plain", custom={"author": "test"})
            result = await client.put("test-key", b"test data", metadata=metadata)

        assert result.success is True

    @pytest.mark.asyncio
    async def test_put_not_found(self) -> None:
        """Test put with 404 error."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 404

        with patch.object(client.client, "put", new_callable=AsyncMock) as mock_put:
            mock_put.return_value = mock_response
            with pytest.raises(ObjectNotFoundError):
                await client.put("test-key", b"test data")

    @pytest.mark.asyncio
    async def test_put_validation_error(self) -> None:
        """Test put with validation error."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.json.return_value = {"message": "invalid key"}

        with patch.object(client.client, "put", new_callable=AsyncMock) as mock_put:
            mock_put.return_value = mock_response
            with pytest.raises(ValidationError):
                await client.put("test-key", b"test data")

    @pytest.mark.asyncio
    async def test_put_timeout(self) -> None:
        """Test put with timeout."""
        client = QuicClient()

        with patch.object(client.client, "put", new_callable=AsyncMock) as mock_put:
            mock_put.side_effect = httpx.TimeoutException("timeout")
            with pytest.raises(TimeoutError):
                await client.put("test-key", b"test data")

    @pytest.mark.asyncio
    async def test_put_connection_error(self) -> None:
        """Test put with connection error."""
        client = QuicClient()

        with patch.object(client.client, "put", new_callable=AsyncMock) as mock_put:
            mock_put.side_effect = httpx.ConnectError("connection refused")
            with pytest.raises(ConnectionError):
                await client.put("test-key", b"test data")

    @pytest.mark.asyncio
    async def test_get_success(self) -> None:
        """Test successful get operation."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"test data"
        mock_response.headers = {
            "Content-Type": "text/plain",
            "Content-Length": "9",
            "ETag": "abc123",
        }

        with patch.object(client.client, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            data, metadata = await client.get("test-key")

        assert data == b"test data"
        assert metadata.content_type == "text/plain"
        assert metadata.size == 9
        assert metadata.etag == "abc123"

    @pytest.mark.asyncio
    async def test_get_not_found(self) -> None:
        """Test get with object not found."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 404

        with patch.object(client.client, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            with pytest.raises(ObjectNotFoundError):
                await client.get("test-key")

    @pytest.mark.asyncio
    async def test_get_timeout(self) -> None:
        """Test get with timeout."""
        client = QuicClient()

        with patch.object(client.client, "get", new_callable=AsyncMock) as mock_get:
            mock_get.side_effect = httpx.TimeoutException("timeout")
            with pytest.raises(TimeoutError):
                await client.get("test-key")

    @pytest.mark.asyncio
    async def test_delete_success(self) -> None:
        """Test successful delete operation."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message": "deleted"}

        with patch.object(client.client, "delete", new_callable=AsyncMock) as mock_delete:
            mock_delete.return_value = mock_response
            result = await client.delete("test-key")

        assert result.success is True

    @pytest.mark.asyncio
    async def test_delete_not_found(self) -> None:
        """Test delete with object not found."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 404

        with patch.object(client.client, "delete", new_callable=AsyncMock) as mock_delete:
            mock_delete.return_value = mock_response
            with pytest.raises(ObjectNotFoundError):
                await client.delete("test-key")

    @pytest.mark.asyncio
    async def test_list_success(self) -> None:
        """Test successful list operation."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "objects": [
                {"key": "obj1", "size": 100, "etag": "etag1"},
                {"key": "obj2", "size": 200, "etag": "etag2"},
            ],
            "common_prefixes": ["dir1/", "dir2/"],
            "next_token": "token123",
            "truncated": True,
        }

        with patch.object(client.client, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            result = await client.list(prefix="test/", max_results=10)

        assert len(result.objects) == 2
        assert result.objects[0].key == "obj1"
        assert len(result.common_prefixes) == 2
        assert result.next_token == "token123"
        assert result.truncated is True

    @pytest.mark.asyncio
    async def test_list_with_all_params(self) -> None:
        """Test list with all parameters."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"objects": [], "truncated": False}

        with patch.object(client.client, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            result = await client.list(
                prefix="test/",
                delimiter="/",
                max_results=50,
                continue_from="token123",
            )

        assert result.truncated is False

    @pytest.mark.asyncio
    async def test_exists_true(self) -> None:
        """Test exists returns true."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch.object(client.client, "head", new_callable=AsyncMock) as mock_head:
            mock_head.return_value = mock_response
            result = await client.exists("test-key")

        assert result.exists is True

    @pytest.mark.asyncio
    async def test_exists_false(self) -> None:
        """Test exists returns false."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 404

        with patch.object(client.client, "head", new_callable=AsyncMock) as mock_head:
            mock_head.return_value = mock_response
            result = await client.exists("test-key")

        assert result.exists is False

    @pytest.mark.asyncio
    async def test_get_metadata_success(self) -> None:
        """Test get metadata."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "size": 100,
            "etag": "abc123",
            "metadata": {"content_type": "text/plain", "author": "test"},
        }

        with patch.object(client.client, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            metadata = await client.get_metadata("test-key")

        assert metadata.size == 100
        assert metadata.etag == "abc123"

    @pytest.mark.asyncio
    async def test_update_metadata_success(self) -> None:
        """Test update metadata."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message": "updated"}

        with patch.object(client.client, "put", new_callable=AsyncMock) as mock_put:
            mock_put.return_value = mock_response
            metadata = Metadata(content_type="application/json")
            result = await client.update_metadata("test-key", metadata)

        assert result.success is True

    @pytest.mark.asyncio
    async def test_health_check(self) -> None:
        """Test health check."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "SERVING", "message": "healthy"}

        with patch.object(client.client, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            result = await client.health()

        assert result.status == HealthStatus.SERVING
        assert result.message == "healthy"

    @pytest.mark.asyncio
    async def test_health_unknown_status(self) -> None:
        """Test health check with unknown status."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "INVALID_STATUS", "message": "unknown"}

        with patch.object(client.client, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            result = await client.health()

        assert result.status == HealthStatus.UNKNOWN

    @pytest.mark.asyncio
    async def test_archive_success(self) -> None:
        """Test successful archive operation."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message": "archived"}

        with patch.object(client.client, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = mock_response
            result = await client.archive("test-key", "s3", {"bucket": "backup"})

        assert result.success is True

    @pytest.mark.asyncio
    async def test_archive_not_found(self) -> None:
        """Test archive with object not found."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 404

        with patch.object(client.client, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = mock_response
            with pytest.raises(ObjectNotFoundError):
                await client.archive("test-key", "s3", {"bucket": "backup"})

    @pytest.mark.asyncio
    async def test_add_policy_success(self) -> None:
        """Test add lifecycle policy."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"message": "Policy added"}

        with patch.object(client.client, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = mock_response
            policy = LifecyclePolicy(
                id="policy-1",
                prefix="logs/",
                retention_seconds=86400,
                action="delete",
            )
            result = await client.add_policy(policy)

        assert result.success is True

    @pytest.mark.asyncio
    async def test_remove_policy_success(self) -> None:
        """Test remove lifecycle policy."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message": "Policy removed"}

        with patch.object(client.client, "delete", new_callable=AsyncMock) as mock_delete:
            mock_delete.return_value = mock_response
            result = await client.remove_policy("policy-1")

        assert result.success is True

    @pytest.mark.asyncio
    async def test_get_policies_success(self) -> None:
        """Test get lifecycle policies."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "policies": [
                {"id": "policy-1", "prefix": "logs/", "retention_seconds": 86400, "action": "delete"}
            ],
            "message": "Policies retrieved",
        }

        with patch.object(client.client, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            result = await client.get_policies()

        assert result.success is True
        assert len(result.policies) == 1

    @pytest.mark.asyncio
    async def test_get_policies_with_prefix(self) -> None:
        """Test get policies with prefix filter."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"policies": []}

        with patch.object(client.client, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            result = await client.get_policies(prefix="logs/")

        assert result.success is True

    @pytest.mark.asyncio
    async def test_apply_policies_success(self) -> None:
        """Test apply lifecycle policies."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "policies_count": 3,
            "objects_processed": 100,
            "message": "Policies applied",
        }

        with patch.object(client.client, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = mock_response
            result = await client.apply_policies()

        assert result.success is True
        assert result.policies_count == 3
        assert result.objects_processed == 100

    @pytest.mark.asyncio
    async def test_add_replication_policy_success(self) -> None:
        """Test add replication policy."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"message": "Replication policy added"}

        with patch.object(client.client, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = mock_response
            policy = ReplicationPolicy(
                id="repl-1",
                source_backend="local",
                source_settings={"path": "/data"},
                destination_backend="s3",
                destination_settings={"bucket": "backup"},
                check_interval_seconds=300,
            )
            result = await client.add_replication_policy(policy)

        assert result.success is True

    @pytest.mark.asyncio
    async def test_remove_replication_policy_success(self) -> None:
        """Test remove replication policy."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message": "Replication policy removed"}

        with patch.object(client.client, "delete", new_callable=AsyncMock) as mock_delete:
            mock_delete.return_value = mock_response
            result = await client.remove_replication_policy("repl-1")

        assert result.success is True

    @pytest.mark.asyncio
    async def test_get_replication_policies_success(self) -> None:
        """Test get replication policies."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "policies": [
                {
                    "id": "repl-1",
                    "source_backend": "local",
                    "destination_backend": "s3",
                    "check_interval_seconds": 300,
                }
            ]
        }

        with patch.object(client.client, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            result = await client.get_replication_policies()

        assert len(result.policies) == 1

    @pytest.mark.asyncio
    async def test_get_replication_policy_success(self) -> None:
        """Test get specific replication policy."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "policy": {
                "id": "repl-1",
                "source_backend": "local",
                "destination_backend": "s3",
                "check_interval_seconds": 300,
            }
        }

        with patch.object(client.client, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            policy = await client.get_replication_policy("repl-1")

        assert policy.id == "repl-1"

    @pytest.mark.asyncio
    async def test_trigger_replication_success(self) -> None:
        """Test trigger replication."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "result": {
                "policy_id": "repl-1",
                "synced": 100,
                "deleted": 5,
                "failed": 0,
                "bytes_total": 1048576,
                "duration_ms": 5000,
            },
            "message": "Replication triggered",
        }

        with patch.object(client.client, "post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = mock_response
            opts = TriggerReplicationOptions(policy_id="repl-1", parallel=True)
            result = await client.trigger_replication(opts)

        assert result.success is True
        assert result.result is not None
        assert result.result.synced == 100

    @pytest.mark.asyncio
    async def test_get_replication_status_success(self) -> None:
        """Test get replication status."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
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
        }

        with patch.object(client.client, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            result = await client.get_replication_status("repl-1")

        assert result.success is True
        assert result.status is not None
        assert result.status.total_objects_synced == 1000

    @pytest.mark.asyncio
    async def test_server_error(self) -> None:
        """Test server error handling."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.json.return_value = {"message": "internal error"}

        with patch.object(client.client, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            with pytest.raises(ServerError):
                await client.get("test-key")

    @pytest.mark.asyncio
    async def test_authentication_error(self) -> None:
        """Test authentication error."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 401

        with patch.object(client.client, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            with pytest.raises(AuthenticationError):
                await client.get("test-key")

    @pytest.mark.asyncio
    async def test_generic_error(self) -> None:
        """Test generic HTTP error."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 418
        mock_response.text = "I'm a teapot"

        with patch.object(client.client, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            with pytest.raises(ObjectStoreError):
                await client.get("test-key")

    @pytest.mark.asyncio
    async def test_context_manager(self) -> None:
        """Test async context manager."""
        with patch.object(QuicClient, "close", new_callable=AsyncMock) as mock_close:
            async with QuicClient() as client:
                assert client is not None
            mock_close.assert_called_once()

    @pytest.mark.asyncio
    async def test_close(self) -> None:
        """Test close method."""
        client = QuicClient()
        with patch.object(client.client, "aclose", new_callable=AsyncMock) as mock_aclose:
            await client.close()
            mock_aclose.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_error_with_text_response(self) -> None:
        """Test error handling when json parsing fails."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.json.side_effect = ValueError("invalid json")
        mock_response.text = "Bad request text"

        with patch.object(client.client, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            with pytest.raises(ValidationError):
                await client.get("test-key")

    @pytest.mark.asyncio
    async def test_server_error_text_fallback(self) -> None:
        """Test server error with text fallback when json fails."""
        client = QuicClient()
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.json.side_effect = ValueError("invalid json")
        mock_response.text = "Server error text"

        with patch.object(client.client, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            with pytest.raises(ServerError):
                await client.get("test-key")
