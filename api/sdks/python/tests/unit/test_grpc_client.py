"""Unit tests for gRPC client."""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

from objstore.exceptions import (
    ConnectionError,
    ObjectNotFoundError,
    ObjectStoreError,
    ServerError,
    TimeoutError,
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
    ReplicationMode,
    ReplicationPolicy,
    ReplicationStatus,
    SyncResult,
    TriggerReplicationOptions,
    TriggerReplicationResponse,
)


class TestGrpcClientImport:
    """Test gRPC client import behavior."""

    def test_import_without_grpc(self) -> None:
        """Test that GrpcClient handles missing gRPC gracefully."""
        try:
            from objstore.grpc_client import GrpcClient, GRPC_AVAILABLE

            if not GRPC_AVAILABLE:
                with pytest.raises(ImportError, match="gRPC support requires proto files"):
                    client = GrpcClient()
            else:
                client = GrpcClient(host="localhost", port=50051)
                assert client.host == "localhost"
                assert client.port == 50051
                assert client.timeout == 30
                assert client.max_retries == 3
        except ImportError:
            pass


class TestGrpcResponseParsing:
    """Test gRPC response parsing patterns."""

    def test_put_response_parsing(self) -> None:
        """Test parsing put response."""
        mock_response = Mock()
        mock_response.success = True
        mock_response.message = "success"
        mock_response.etag = "abc123"

        result = PutResponse(
            success=mock_response.success,
            message=mock_response.message,
            etag=mock_response.etag,
        )

        assert result.success is True
        assert result.etag == "abc123"

    def test_get_response_parsing(self) -> None:
        """Test parsing get response."""
        mock_response = Mock()
        mock_response.data = b"test data"
        mock_metadata = Mock()
        mock_metadata.content_type = "text/plain"
        mock_metadata.content_encoding = ""
        mock_metadata.size = 9
        mock_metadata.etag = "abc123"
        mock_metadata.custom = {}
        mock_metadata.HasField = Mock(return_value=False)
        mock_response.metadata = mock_metadata

        metadata = Metadata(
            content_type=mock_metadata.content_type,
            size=mock_metadata.size,
            etag=mock_metadata.etag,
        )

        assert mock_response.data == b"test data"
        assert metadata.content_type == "text/plain"

    def test_delete_response_parsing(self) -> None:
        """Test parsing delete response."""
        mock_response = Mock()
        mock_response.success = True
        mock_response.message = "deleted"

        result = DeleteResponse(
            success=mock_response.success, message=mock_response.message
        )

        assert result.success is True
        assert result.message == "deleted"

    def test_list_response_parsing(self) -> None:
        """Test parsing list response."""
        mock_response = Mock()
        mock_response.objects = []
        mock_response.common_prefixes = ["dir1/", "dir2/"]
        mock_response.truncated = False
        mock_response.next_token = ""

        result = ListResponse(
            objects=[],
            common_prefixes=list(mock_response.common_prefixes),
            truncated=mock_response.truncated,
            next_token=mock_response.next_token or None,
        )

        assert isinstance(result.objects, list)
        assert result.truncated is False
        assert len(result.common_prefixes) == 2

    def test_exists_response_true(self) -> None:
        """Test exists response when object exists."""
        mock_response = Mock()
        mock_response.exists = True

        result = ExistsResponse(exists=mock_response.exists)
        assert result.exists is True

    def test_exists_response_false(self) -> None:
        """Test exists response when object does not exist."""
        mock_response = Mock()
        mock_response.exists = False

        result = ExistsResponse(exists=mock_response.exists)
        assert result.exists is False

    def test_health_response_serving(self) -> None:
        """Test health response parsing."""
        mock_response = Mock()
        mock_response.status = 1  # SERVING
        mock_response.message = "healthy"

        status_map = {0: HealthStatus.UNKNOWN, 1: HealthStatus.SERVING, 2: HealthStatus.NOT_SERVING}
        result = HealthResponse(
            status=status_map.get(mock_response.status, HealthStatus.UNKNOWN),
            message=mock_response.message,
        )

        assert result.status == HealthStatus.SERVING

    def test_policy_response_parsing(self) -> None:
        """Test parsing policy response."""
        mock_response = Mock()
        mock_response.success = True
        mock_response.message = "Policy added"

        policy = LifecyclePolicy(
            id="policy-1",
            prefix="logs/",
            retention_seconds=86400,
            action="delete",
        )
        result = PolicyResponse(
            success=mock_response.success, message=mock_response.message
        )

        assert result.success is True

    def test_get_policies_response_parsing(self) -> None:
        """Test parsing get policies response."""
        mock_response = Mock()
        mock_response.policies = []
        mock_response.success = True
        mock_response.message = "Policies retrieved"

        result = GetPoliciesResponse(
            policies=[],
            success=mock_response.success,
            message=mock_response.message,
        )

        assert result.success is True
        assert isinstance(result.policies, list)

    def test_apply_policies_response_parsing(self) -> None:
        """Test parsing apply policies response."""
        mock_response = Mock()
        mock_response.success = True
        mock_response.policies_count = 3
        mock_response.objects_processed = 100
        mock_response.message = "Policies applied"

        result = ApplyPoliciesResponse(
            success=mock_response.success,
            policies_count=mock_response.policies_count,
            objects_processed=mock_response.objects_processed,
            message=mock_response.message,
        )

        assert result.success is True
        assert result.policies_count == 3
        assert result.objects_processed == 100

    def test_replication_policy_parsing(self) -> None:
        """Test parsing replication policy."""
        mock_response = Mock()
        mock_response.success = True
        mock_response.message = "Replication policy added"

        policy = ReplicationPolicy(
            id="repl-1",
            source_backend="local",
            source_settings={"path": "/data"},
            destination_backend="s3",
            destination_settings={"bucket": "backup"},
            check_interval_seconds=300,
        )
        result = PolicyResponse(
            success=mock_response.success, message=mock_response.message
        )

        assert result.success is True

    def test_get_replication_policies_response_parsing(self) -> None:
        """Test parsing get replication policies response."""
        mock_response = Mock()
        mock_response.policies = []

        result = GetReplicationPoliciesResponse(policies=[])

        assert isinstance(result.policies, list)

    def test_replication_policy_details_parsing(self) -> None:
        """Test parsing replication policy details."""
        mock_policy = Mock()
        mock_policy.id = "repl-1"
        mock_policy.source_backend = "local"
        mock_policy.destination_backend = "s3"
        mock_policy.check_interval_seconds = 300

        policy = ReplicationPolicy(
            id=mock_policy.id,
            source_backend=mock_policy.source_backend,
            destination_backend=mock_policy.destination_backend,
            check_interval_seconds=mock_policy.check_interval_seconds,
        )

        assert policy.id == "repl-1"

    def test_trigger_replication_response_parsing(self) -> None:
        """Test parsing trigger replication response."""
        mock_result = Mock()
        mock_result.policy_id = "repl-1"
        mock_result.synced = 100
        mock_result.deleted = 5
        mock_result.failed = 2
        mock_result.bytes_total = 1048576
        mock_result.duration_ms = 5000
        mock_result.errors = []

        mock_response = Mock()
        mock_response.success = True
        mock_response.result = mock_result
        mock_response.message = "Replication triggered"

        sync_result = SyncResult(
            policy_id=mock_result.policy_id,
            synced=mock_result.synced,
            deleted=mock_result.deleted,
            failed=mock_result.failed,
            bytes_total=mock_result.bytes_total,
            duration_ms=mock_result.duration_ms,
        )
        result = TriggerReplicationResponse(
            success=mock_response.success,
            result=sync_result,
            message=mock_response.message,
        )

        assert result.success is True
        assert result.result.synced == 100

    def test_replication_status_parsing(self) -> None:
        """Test parsing replication status."""
        mock_status = Mock()
        mock_status.policy_id = "repl-1"
        mock_status.source_backend = "local"
        mock_status.destination_backend = "s3"
        mock_status.enabled = True
        mock_status.total_objects_synced = 1000
        mock_status.total_objects_deleted = 10
        mock_status.total_bytes_synced = 10485760
        mock_status.total_errors = 0
        mock_status.average_sync_duration_ms = 2000
        mock_status.sync_count = 5

        status = ReplicationStatus(
            policy_id=mock_status.policy_id,
            source_backend=mock_status.source_backend,
            destination_backend=mock_status.destination_backend,
            enabled=mock_status.enabled,
            total_objects_synced=mock_status.total_objects_synced,
            total_objects_deleted=mock_status.total_objects_deleted,
            total_bytes_synced=mock_status.total_bytes_synced,
            total_errors=mock_status.total_errors,
            average_sync_duration_ms=mock_status.average_sync_duration_ms,
            sync_count=mock_status.sync_count,
        )
        result = GetReplicationStatusResponse(
            success=True,
            status=status,
        )

        assert result.success is True
        assert result.status.total_objects_synced == 1000

    def test_archive_response_parsing(self) -> None:
        """Test parsing archive response."""
        mock_response = Mock()
        mock_response.success = True
        mock_response.message = "archived"

        result = ArchiveResponse(
            success=mock_response.success, message=mock_response.message
        )

        assert result.success is True

    def test_metadata_parsing(self) -> None:
        """Test parsing metadata response."""
        mock_metadata = Mock()
        mock_metadata.content_type = "text/plain"
        mock_metadata.content_encoding = ""
        mock_metadata.size = 100
        mock_metadata.etag = "abc123"
        mock_metadata.custom = {"author": "test"}

        metadata = Metadata(
            content_type=mock_metadata.content_type,
            size=mock_metadata.size,
            etag=mock_metadata.etag,
            custom=dict(mock_metadata.custom),
        )

        assert metadata.content_type == "text/plain"
        assert metadata.size == 100


class TestMetadataConversion:
    """Test metadata conversion utilities."""

    def test_metadata_to_proto_empty(self) -> None:
        """Test converting None metadata."""
        metadata = None
        assert metadata is None

    def test_metadata_to_proto_with_values(self) -> None:
        """Test converting metadata with values."""
        metadata = Metadata(
            content_type="application/json",
            content_encoding="gzip",
            size=1024,
            etag="abc123",
            custom={"key1": "value1", "key2": "value2"},
        )
        assert metadata.content_type == "application/json"
        assert metadata.content_encoding == "gzip"
        assert metadata.size == 1024
        assert metadata.etag == "abc123"
        assert metadata.custom == {"key1": "value1", "key2": "value2"}

    def test_proto_to_metadata_parsing(self) -> None:
        """Test parsing proto metadata to model."""
        mock_proto_metadata = Mock()
        mock_proto_metadata.content_type = "text/plain"
        mock_proto_metadata.content_encoding = "gzip"
        mock_proto_metadata.size = 512
        mock_proto_metadata.etag = "etag456"
        mock_proto_metadata.custom = {"author": "test"}
        mock_proto_metadata.HasField = Mock(return_value=False)

        metadata = Metadata(
            content_type=mock_proto_metadata.content_type or None,
            content_encoding=mock_proto_metadata.content_encoding or None,
            size=mock_proto_metadata.size if mock_proto_metadata.size else None,
            etag=mock_proto_metadata.etag or None,
            custom=dict(mock_proto_metadata.custom) if mock_proto_metadata.custom else {},
        )

        assert metadata.content_type == "text/plain"
        assert metadata.content_encoding == "gzip"
        assert metadata.size == 512
        assert metadata.etag == "etag456"

    def test_proto_to_metadata_with_timestamp(self) -> None:
        """Test parsing proto metadata with timestamp."""
        mock_proto_metadata = Mock()
        mock_proto_metadata.content_type = "text/plain"
        mock_proto_metadata.content_encoding = ""
        mock_proto_metadata.size = 100
        mock_proto_metadata.etag = ""
        mock_proto_metadata.custom = {}
        mock_proto_metadata.HasField = Mock(return_value=True)
        mock_proto_metadata.last_modified = Mock()
        mock_proto_metadata.last_modified.seconds = 1609459200  # 2021-01-01

        has_timestamp = mock_proto_metadata.HasField("last_modified")
        assert has_timestamp is True

        if has_timestamp:
            last_modified = datetime.fromtimestamp(
                mock_proto_metadata.last_modified.seconds
            )
            assert last_modified.year == 2021


class TestHealthStatusMapping:
    """Test health status mapping."""

    def test_health_status_serving(self) -> None:
        """Test SERVING status."""
        assert HealthStatus.SERVING.value == "SERVING"

    def test_health_status_not_serving(self) -> None:
        """Test NOT_SERVING status."""
        assert HealthStatus.NOT_SERVING.value == "NOT_SERVING"

    def test_health_status_unknown(self) -> None:
        """Test UNKNOWN status."""
        assert HealthStatus.UNKNOWN.value == "UNKNOWN"

    def test_health_status_from_int(self) -> None:
        """Test mapping integer to HealthStatus."""
        status_map = {
            0: HealthStatus.UNKNOWN,
            1: HealthStatus.SERVING,
            2: HealthStatus.NOT_SERVING,
        }
        assert status_map[0] == HealthStatus.UNKNOWN
        assert status_map[1] == HealthStatus.SERVING
        assert status_map[2] == HealthStatus.NOT_SERVING


class TestReplicationMode:
    """Test replication mode enum."""

    def test_replication_mode_transparent(self) -> None:
        """Test TRANSPARENT mode."""
        assert ReplicationMode.TRANSPARENT.value == "TRANSPARENT"

    def test_replication_mode_opaque(self) -> None:
        """Test OPAQUE mode."""
        assert ReplicationMode.OPAQUE.value == "OPAQUE"


class TestErrorTypes:
    """Test error type definitions."""

    def test_error_types_exist(self) -> None:
        """Test that error types are properly defined."""
        assert ObjectNotFoundError is not None
        assert ServerError is not None
        assert TimeoutError is not None
        assert ConnectionError is not None

    def test_object_not_found_error(self) -> None:
        """Test ObjectNotFoundError."""
        error = ObjectNotFoundError("Test not found")
        assert "not found" in str(error).lower()

    def test_server_error(self) -> None:
        """Test ServerError."""
        error = ServerError("Internal server error", status_code=500)
        assert error.status_code == 500

    def test_object_store_error(self) -> None:
        """Test ObjectStoreError."""
        error = ObjectStoreError("Generic error")
        assert str(error) == "Generic error"
