"""Unit tests for REST client."""

import pytest
import responses
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout

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
    DeleteResponse,
    ExistsResponse,
    HealthResponse,
    HealthStatus,
    ListResponse,
    Metadata,
    ObjectInfo,
    PolicyResponse,
    PutResponse,
)
from objstore.rest_client import RestClient


class TestRestClient:
    """Test cases for RestClient."""

    def test_init(self) -> None:
        """Test client initialization."""
        client = RestClient(
            base_url="http://localhost:8080", api_version="v1", timeout=30, max_retries=3
        )
        assert client.base_url == "http://localhost:8080"
        assert client.api_version == "v1"
        assert client.timeout == 30
        assert client.max_retries == 3

    def test_url_construction(self) -> None:
        """Test URL construction."""
        client = RestClient(base_url="http://localhost:8080", api_version="v1")
        assert client._url("objects/test") == "http://localhost:8080/api/v1/objects/test"
        assert client._url("/objects/test") == "http://localhost:8080/api/v1/objects/test"

    @responses.activate
    def test_put_success(self) -> None:
        """Test successful put operation."""
        responses.add(
            responses.PUT,
            "http://localhost:8080/api/v1/objects/test-key",
            json={"message": "success", "data": {"etag": "abc123"}},
            status=201,
        )

        client = RestClient()
        result = client.put("test-key", b"test data")

        assert result.success is True
        assert result.etag == "abc123"

    @responses.activate
    def test_put_with_metadata(self) -> None:
        """Test put with metadata."""
        responses.add(
            responses.PUT,
            "http://localhost:8080/api/v1/objects/test-key",
            json={"message": "success", "data": {"etag": "abc123"}},
            status=201,
        )

        client = RestClient()
        metadata = Metadata(content_type="text/plain", custom={"author": "test"})
        result = client.put("test-key", b"test data", metadata=metadata)

        assert result.success is True

    @responses.activate
    def test_put_not_found(self) -> None:
        """Test put with 404 error."""
        responses.add(
            responses.PUT,
            "http://localhost:8080/api/v1/objects/test-key",
            status=404,
        )

        client = RestClient()
        with pytest.raises(ObjectNotFoundError):
            client.put("test-key", b"test data")

    @responses.activate
    def test_put_validation_error(self) -> None:
        """Test put with validation error."""
        responses.add(
            responses.PUT,
            "http://localhost:8080/api/v1/objects/test-key",
            json={"message": "invalid key"},
            status=400,
        )

        client = RestClient()
        with pytest.raises(ValidationError):
            client.put("test-key", b"test data")

    @responses.activate
    def test_get_success(self) -> None:
        """Test successful get operation."""
        responses.add(
            responses.GET,
            "http://localhost:8080/api/v1/objects/test-key",
            body=b"test data",
            headers={
                "Content-Type": "text/plain",
                "Content-Length": "9",
                "ETag": "abc123",
            },
            status=200,
        )

        client = RestClient()
        data, metadata = client.get("test-key")

        assert data == b"test data"
        assert metadata.content_type == "text/plain"
        assert metadata.size == 9
        assert metadata.etag == "abc123"

    @responses.activate
    def test_get_not_found(self) -> None:
        """Test get with object not found."""
        responses.add(
            responses.GET,
            "http://localhost:8080/api/v1/objects/test-key",
            status=404,
        )

        client = RestClient()
        with pytest.raises(ObjectNotFoundError):
            client.get("test-key")

    @responses.activate
    def test_delete_success(self) -> None:
        """Test successful delete operation."""
        responses.add(
            responses.DELETE,
            "http://localhost:8080/api/v1/objects/test-key",
            json={"message": "deleted"},
            status=200,
        )

        client = RestClient()
        result = client.delete("test-key")

        assert result.success is True

    @responses.activate
    def test_list_success(self) -> None:
        """Test successful list operation."""
        responses.add(
            responses.GET,
            "http://localhost:8080/api/v1/objects",
            json={
                "objects": [
                    {"key": "obj1", "size": 100, "etag": "etag1"},
                    {"key": "obj2", "size": 200, "etag": "etag2"},
                ],
                "common_prefixes": ["dir1/", "dir2/"],
                "next_token": "token123",
                "truncated": True,
            },
            status=200,
        )

        client = RestClient()
        result = client.list(prefix="test/", max_results=10)

        assert len(result.objects) == 2
        assert result.objects[0].key == "obj1"
        assert len(result.common_prefixes) == 2
        assert result.next_token == "token123"
        assert result.truncated is True

    @responses.activate
    def test_exists_true(self) -> None:
        """Test exists returns true."""
        responses.add(
            responses.HEAD,
            "http://localhost:8080/api/v1/objects/test-key",
            status=200,
        )

        client = RestClient()
        result = client.exists("test-key")

        assert result.exists is True

    @responses.activate
    def test_exists_false(self) -> None:
        """Test exists returns false."""
        responses.add(
            responses.HEAD,
            "http://localhost:8080/api/v1/objects/test-key",
            status=404,
        )

        client = RestClient()
        result = client.exists("test-key")

        assert result.exists is False

    @responses.activate
    def test_get_metadata_success(self) -> None:
        """Test get metadata."""
        responses.add(
            responses.GET,
            "http://localhost:8080/api/v1/metadata/test-key",
            json={
                "size": 100,
                "etag": "abc123",
                "metadata": {"content_type": "text/plain", "author": "test"},
            },
            status=200,
        )

        client = RestClient()
        metadata = client.get_metadata("test-key")

        assert metadata.size == 100
        assert metadata.etag == "abc123"
        assert metadata.custom.get("author") == "test"

    @responses.activate
    def test_update_metadata_success(self) -> None:
        """Test update metadata."""
        responses.add(
            responses.PUT,
            "http://localhost:8080/api/v1/metadata/test-key",
            json={"message": "updated"},
            status=200,
        )

        client = RestClient()
        metadata = Metadata(content_type="application/json")
        result = client.update_metadata("test-key", metadata)

        assert result.success is True

    @responses.activate
    def test_health_check(self) -> None:
        """Test health check."""
        responses.add(
            responses.GET,
            "http://localhost:8080/health",
            json={"status": "SERVING", "message": "healthy"},
            status=200,
        )

        client = RestClient()
        result = client.health()

        assert result.status == HealthStatus.SERVING
        assert result.message == "healthy"

    @responses.activate
    def test_server_error(self) -> None:
        """Test server error handling."""
        responses.add(
            responses.GET,
            "http://localhost:8080/api/v1/objects/test-key",
            json={"message": "internal error"},
            status=500,
        )

        client = RestClient()
        with pytest.raises(ServerError):
            client.get("test-key")

    @responses.activate
    def test_authentication_error(self) -> None:
        """Test authentication error."""
        responses.add(
            responses.GET,
            "http://localhost:8080/api/v1/objects/test-key",
            status=401,
        )

        client = RestClient()
        with pytest.raises(AuthenticationError):
            client.get("test-key")

    def test_timeout_error(self) -> None:
        """Test timeout error."""
        client = RestClient(timeout=0.001)

        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.GET,
                "http://localhost:8080/api/v1/objects/test-key",
                body=Timeout(),
            )

            with pytest.raises(TimeoutError):
                client.get("test-key")

    def test_connection_error(self) -> None:
        """Test connection error."""
        client = RestClient()

        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.GET,
                "http://localhost:8080/api/v1/objects/test-key",
                body=RequestsConnectionError(),
            )

            with pytest.raises(ConnectionError):
                client.get("test-key")

    def test_context_manager(self) -> None:
        """Test context manager."""
        with RestClient() as client:
            assert client is not None

    @responses.activate
    def test_get_stream(self) -> None:
        """Test streaming get."""
        responses.add(
            responses.GET,
            "http://localhost:8080/api/v1/objects/test-key",
            body=b"chunk1chunk2chunk3",
            status=200,
        )

        client = RestClient()
        chunks = list(client.get_stream("test-key"))

        assert len(chunks) > 0
        assert b"".join(chunks) == b"chunk1chunk2chunk3"

    @responses.activate
    def test_archive_success(self) -> None:
        """Test archive operation."""
        responses.add(
            responses.POST,
            "http://localhost:8080/api/v1/archive",
            json={"message": "archived"},
            status=200,
        )

        client = RestClient()
        result = client.archive("test-key", "s3", {"bucket": "backup"})

        assert result.success is True

    @responses.activate
    def test_archive_not_found(self) -> None:
        """Test archive with object not found."""
        responses.add(
            responses.POST,
            "http://localhost:8080/api/v1/archive",
            status=404,
        )

        client = RestClient()
        with pytest.raises(ObjectNotFoundError):
            client.archive("test-key", "s3", {"bucket": "backup"})

    @responses.activate
    def test_add_policy_success(self) -> None:
        """Test add lifecycle policy."""
        from objstore.models import LifecyclePolicy

        responses.add(
            responses.POST,
            "http://localhost:8080/api/v1/policies",
            json={"message": "Policy added"},
            status=201,
        )

        client = RestClient()
        policy = LifecyclePolicy(
            id="policy-1",
            prefix="logs/",
            retention_seconds=86400,
            action="delete",
        )
        result = client.add_policy(policy)

        assert result.success is True

    @responses.activate
    def test_remove_policy_success(self) -> None:
        """Test remove lifecycle policy."""
        responses.add(
            responses.DELETE,
            "http://localhost:8080/api/v1/policies/policy-1",
            json={"message": "Policy removed"},
            status=200,
        )

        client = RestClient()
        result = client.remove_policy("policy-1")

        assert result.success is True

    @responses.activate
    def test_get_policies_success(self) -> None:
        """Test get lifecycle policies."""
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

        client = RestClient()
        result = client.get_policies()

        assert result.success is True
        assert len(result.policies) == 1

    @responses.activate
    def test_get_policies_with_prefix(self) -> None:
        """Test get policies with prefix filter."""
        responses.add(
            responses.GET,
            "http://localhost:8080/api/v1/policies",
            json={"policies": []},
            status=200,
        )

        client = RestClient()
        result = client.get_policies(prefix="logs/")

        assert result.success is True

    @responses.activate
    def test_apply_policies_success(self) -> None:
        """Test apply lifecycle policies."""
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

        client = RestClient()
        result = client.apply_policies()

        assert result.success is True
        assert result.policies_count == 3
        assert result.objects_processed == 100

    @responses.activate
    def test_add_replication_policy_success(self) -> None:
        """Test add replication policy."""
        from objstore.models import ReplicationPolicy

        responses.add(
            responses.POST,
            "http://localhost:8080/api/v1/replication/policies",
            json={"message": "Replication policy added"},
            status=201,
        )

        client = RestClient()
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
    def test_remove_replication_policy_success(self) -> None:
        """Test remove replication policy."""
        responses.add(
            responses.DELETE,
            "http://localhost:8080/api/v1/replication/policies/repl-1",
            json={"message": "Replication policy removed"},
            status=200,
        )

        client = RestClient()
        result = client.remove_replication_policy("repl-1")

        assert result.success is True

    @responses.activate
    def test_get_replication_policies_success(self) -> None:
        """Test get replication policies."""
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

        client = RestClient()
        result = client.get_replication_policies()

        assert len(result.policies) == 1

    @responses.activate
    def test_get_replication_policy_success(self) -> None:
        """Test get specific replication policy."""
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

        client = RestClient()
        policy = client.get_replication_policy("repl-1")

        assert policy.id == "repl-1"

    @responses.activate
    def test_trigger_replication_success(self) -> None:
        """Test trigger replication."""
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

        client = RestClient()
        opts = TriggerReplicationOptions(policy_id="repl-1", parallel=True)
        result = client.trigger_replication(opts)

        assert result.success is True
        assert result.result is not None
        assert result.result.synced == 100

    @responses.activate
    def test_get_replication_status_success(self) -> None:
        """Test get replication status."""
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

        client = RestClient()
        result = client.get_replication_status("repl-1")

        assert result.success is True
        assert result.status is not None
        assert result.status.total_objects_synced == 1000

    @responses.activate
    def test_delete_500_not_found(self) -> None:
        """Test delete with 500 error that is actually not found."""
        responses.add(
            responses.DELETE,
            "http://localhost:8080/api/v1/objects/test-key",
            json={"message": "object not found"},
            status=500,
        )

        client = RestClient()
        with pytest.raises(ObjectNotFoundError):
            client.delete("test-key")

    @responses.activate
    def test_update_metadata_201(self) -> None:
        """Test update metadata returns 201."""
        responses.add(
            responses.PUT,
            "http://localhost:8080/api/v1/metadata/test-key",
            json={"message": "created"},
            status=201,
        )

        client = RestClient()
        metadata = Metadata(content_type="application/json")
        result = client.update_metadata("test-key", metadata)

        assert result.success is True

    @responses.activate
    def test_health_unknown_status(self) -> None:
        """Test health check with unknown status."""
        responses.add(
            responses.GET,
            "http://localhost:8080/health",
            json={"status": "INVALID", "message": "unknown"},
            status=200,
        )

        client = RestClient()
        result = client.health()

        assert result.status == HealthStatus.UNKNOWN

    @responses.activate
    def test_put_with_file_object(self) -> None:
        """Test put with file-like object."""
        from io import BytesIO

        responses.add(
            responses.PUT,
            "http://localhost:8080/api/v1/objects/test-key",
            json={"message": "success", "data": {"etag": "abc123"}},
            status=201,
        )

        client = RestClient()
        file_obj = BytesIO(b"test data from file")
        result = client.put("test-key", file_obj)

        assert result.success is True

    @responses.activate
    def test_put_with_content_encoding(self) -> None:
        """Test put with content encoding in metadata."""
        responses.add(
            responses.PUT,
            "http://localhost:8080/api/v1/objects/test-key",
            json={"message": "success", "data": {"etag": "abc123"}},
            status=201,
        )

        client = RestClient()
        metadata = Metadata(
            content_type="text/plain",
            content_encoding="gzip",
            custom={"author": "test"},
        )
        result = client.put("test-key", b"test data", metadata=metadata)

        assert result.success is True

    @responses.activate
    def test_get_stream_error(self) -> None:
        """Test streaming get with error."""
        responses.add(
            responses.GET,
            "http://localhost:8080/api/v1/objects/test-key",
            status=404,
        )

        client = RestClient()
        with pytest.raises(ObjectNotFoundError):
            list(client.get_stream("test-key"))

    @responses.activate
    def test_generic_error_code(self) -> None:
        """Test handling of non-standard error codes."""
        responses.add(
            responses.GET,
            "http://localhost:8080/api/v1/objects/test-key",
            body="I'm a teapot",
            status=418,
        )

        client = RestClient()
        with pytest.raises(ObjectStoreError):
            client.get("test-key")
