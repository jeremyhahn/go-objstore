"""Unit tests for data models."""

from datetime import datetime

import pytest

from objstore.models import (
    EncryptionConfig,
    EncryptionPolicy,
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
)


class TestMetadata:
    """Test Metadata model."""

    def test_create_empty(self) -> None:
        """Test creating empty metadata."""
        metadata = Metadata()
        assert metadata.content_type is None
        assert metadata.size is None
        assert len(metadata.custom) == 0

    def test_create_with_fields(self) -> None:
        """Test creating metadata with fields."""
        metadata = Metadata(
            content_type="text/plain",
            content_encoding="gzip",
            size=1024,
            etag="abc123",
            custom={"author": "test"},
        )
        assert metadata.content_type == "text/plain"
        assert metadata.content_encoding == "gzip"
        assert metadata.size == 1024
        assert metadata.etag == "abc123"
        assert metadata.custom["author"] == "test"

    def test_model_dump(self) -> None:
        """Test model serialization."""
        metadata = Metadata(content_type="text/plain", size=100)
        data = metadata.model_dump(exclude_none=True)
        assert "content_type" in data
        assert "size" in data
        assert "content_encoding" not in data


class TestObjectInfo:
    """Test ObjectInfo model."""

    def test_create(self) -> None:
        """Test creating object info."""
        obj = ObjectInfo(
            key="test/file.txt", metadata=Metadata(size=100, content_type="text/plain")
        )
        assert obj.key == "test/file.txt"
        assert obj.metadata.size == 100


class TestLifecyclePolicy:
    """Test LifecyclePolicy model."""

    def test_create_delete_policy(self) -> None:
        """Test creating delete policy."""
        policy = LifecyclePolicy(
            id="policy1", prefix="logs/", retention_seconds=86400, action="delete"
        )
        assert policy.id == "policy1"
        assert policy.prefix == "logs/"
        assert policy.retention_seconds == 86400
        assert policy.action == "delete"

    def test_create_archive_policy(self) -> None:
        """Test creating archive policy."""
        policy = LifecyclePolicy(
            id="policy2",
            prefix="archive/",
            retention_seconds=2592000,
            action="archive",
            destination_type="glacier",
            destination_settings={"region": "us-east-1"},
        )
        assert policy.action == "archive"
        assert policy.destination_type == "glacier"
        assert policy.destination_settings["region"] == "us-east-1"

    def test_create_with_days_after_creation(self) -> None:
        """Test creating policy with days_after_creation."""
        policy = LifecyclePolicy(
            id="policy3",
            prefix="logs/",
            action="delete",
            days_after_creation=30,
        )
        # Should auto-convert to retention_seconds
        assert policy.retention_seconds == 30 * 86400
        assert policy.days_after_creation == 30

        # Should exclude days_after_creation from serialization
        data = policy.model_dump(exclude_none=True)
        assert "retention_seconds" in data
        assert "days_after_creation" not in data

    def test_create_with_default_retention(self) -> None:
        """Test creating policy without retention fields uses default."""
        policy = LifecyclePolicy(
            id="policy4",
            prefix="logs/",
            action="delete",
        )
        # Should default to 30 days
        assert policy.retention_seconds == 30 * 86400


class TestReplicationPolicy:
    """Test ReplicationPolicy model."""

    def test_create(self) -> None:
        """Test creating replication policy."""
        policy = ReplicationPolicy(
            id="repl1",
            source_backend="s3",
            source_settings={"bucket": "source"},
            destination_backend="gcs",
            destination_settings={"bucket": "dest"},
            check_interval_seconds=300,
        )
        assert policy.id == "repl1"
        assert policy.source_backend == "s3"
        assert policy.destination_backend == "gcs"
        assert policy.check_interval_seconds == 300

    def test_replication_mode_default(self) -> None:
        """Test default replication mode."""
        policy = ReplicationPolicy(
            id="repl1",
            source_backend="s3",
            destination_backend="gcs",
            check_interval_seconds=300,
        )
        assert policy.replication_mode == ReplicationMode.TRANSPARENT


class TestEncryptionConfig:
    """Test EncryptionConfig model."""

    def test_create_disabled(self) -> None:
        """Test creating disabled encryption config."""
        config = EncryptionConfig()
        assert config.enabled is False
        assert config.provider == "noop"

    def test_create_enabled(self) -> None:
        """Test creating enabled encryption config."""
        config = EncryptionConfig(enabled=True, provider="custom", default_key="key123")
        assert config.enabled is True
        assert config.provider == "custom"
        assert config.default_key == "key123"


class TestEncryptionPolicy:
    """Test EncryptionPolicy model."""

    def test_create(self) -> None:
        """Test creating encryption policy."""
        policy = EncryptionPolicy(
            backend=EncryptionConfig(enabled=True, provider="aws-kms"),
            source=EncryptionConfig(enabled=True, provider="custom"),
            destination=EncryptionConfig(enabled=False),
        )
        assert policy.backend.enabled is True
        assert policy.source.enabled is True
        assert policy.destination.enabled is False


class TestResponses:
    """Test response models."""

    def test_put_response(self) -> None:
        """Test PutResponse."""
        response = PutResponse(success=True, message="uploaded", etag="abc123")
        assert response.success is True
        assert response.etag == "abc123"

    def test_health_response(self) -> None:
        """Test HealthResponse."""
        response = HealthResponse(status=HealthStatus.SERVING, message="healthy")
        assert response.status == HealthStatus.SERVING

    def test_policy_response(self) -> None:
        """Test PolicyResponse."""
        response = PolicyResponse(success=True, message="policy added")
        assert response.success is True

    def test_list_response(self) -> None:
        """Test ListResponse."""
        response = ListResponse(
            objects=[ObjectInfo(key="file1"), ObjectInfo(key="file2")],
            common_prefixes=["dir1/"],
            next_token="token123",
            truncated=True,
        )
        assert len(response.objects) == 2
        assert response.truncated is True


class TestEnums:
    """Test enum types."""

    def test_health_status(self) -> None:
        """Test HealthStatus enum."""
        assert HealthStatus.UNKNOWN == "UNKNOWN"
        assert HealthStatus.SERVING == "SERVING"
        assert HealthStatus.NOT_SERVING == "NOT_SERVING"

    def test_replication_mode(self) -> None:
        """Test ReplicationMode enum."""
        assert ReplicationMode.TRANSPARENT == "TRANSPARENT"
        assert ReplicationMode.OPAQUE == "OPAQUE"
