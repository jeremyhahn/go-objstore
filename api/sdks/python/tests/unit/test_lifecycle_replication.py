"""Unit tests for lifecycle and replication operations."""

import pytest
import responses
from datetime import datetime

from objstore.models import (
    ApplyPoliciesResponse,
    ArchiveResponse,
    GetPoliciesResponse,
    GetReplicationPoliciesResponse,
    GetReplicationStatusResponse,
    LifecyclePolicy,
    PolicyResponse,
    ReplicationMode,
    ReplicationPolicy,
    ReplicationStatus,
    SyncResult,
    TriggerReplicationOptions,
    TriggerReplicationResponse,
)
from objstore.rest_client import RestClient


class TestLifecyclePolicies:
    """Test cases for lifecycle policy operations."""

    @responses.activate
    def test_archive(self) -> None:
        """Test archive operation."""
        responses.add(
            responses.POST,
            "http://localhost:8080/api/v1/archive",
            json={"success": True, "message": "Object archived successfully"},
            status=200,
        )

        client = RestClient()
        result = client.archive("test-key", "s3", {"bucket": "archive-bucket"})

        assert result.success is True
        assert "archived" in result.message.lower()

    @responses.activate
    def test_add_policy(self) -> None:
        """Test add lifecycle policy."""
        responses.add(
            responses.POST,
            "http://localhost:8080/api/v1/policies",
            json={"success": True, "message": "Policy added successfully"},
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
        assert "added" in result.message.lower()

    @responses.activate
    def test_remove_policy(self) -> None:
        """Test remove lifecycle policy."""
        responses.add(
            responses.DELETE,
            "http://localhost:8080/api/v1/policies/policy-1",
            json={"success": True, "message": "Policy removed successfully"},
            status=200,
        )

        client = RestClient()
        result = client.remove_policy("policy-1")

        assert result.success is True
        assert "removed" in result.message.lower()

    @responses.activate
    def test_get_policies(self) -> None:
        """Test get lifecycle policies."""
        responses.add(
            responses.GET,
            "http://localhost:8080/api/v1/policies",
            json={
                "policies": [
                    {
                        "id": "policy-1",
                        "prefix": "logs/",
                        "retention_seconds": 86400,
                        "action": "delete",
                        "destination_type": None,
                        "destination_settings": {},
                    }
                ],
                "success": True,
                "message": "Policies retrieved successfully",
            },
            status=200,
        )

        client = RestClient()
        result = client.get_policies()

        assert result.success is True
        assert len(result.policies) == 1
        assert result.policies[0].id == "policy-1"
        assert result.policies[0].action == "delete"

    @responses.activate
    def test_get_policies_with_prefix(self) -> None:
        """Test get lifecycle policies with prefix filter."""
        responses.add(
            responses.GET,
            "http://localhost:8080/api/v1/policies?prefix=logs%2F",
            json={
                "policies": [
                    {
                        "id": "policy-1",
                        "prefix": "logs/",
                        "retention_seconds": 86400,
                        "action": "delete",
                        "destination_type": None,
                        "destination_settings": {},
                    }
                ],
                "success": True,
                "message": "Policies retrieved successfully",
            },
            status=200,
        )

        client = RestClient()
        result = client.get_policies(prefix="logs/")

        assert result.success is True
        assert len(result.policies) == 1

    @responses.activate
    def test_apply_policies(self) -> None:
        """Test apply lifecycle policies."""
        responses.add(
            responses.POST,
            "http://localhost:8080/api/v1/policies/apply",
            json={
                "success": True,
                "policies_count": 2,
                "objects_processed": 150,
                "message": "Policies applied successfully",
            },
            status=200,
        )

        client = RestClient()
        result = client.apply_policies()

        assert result.success is True
        assert result.policies_count == 2
        assert result.objects_processed == 150


class TestReplicationPolicies:
    """Test cases for replication policy operations."""

    @responses.activate
    def test_add_replication_policy(self) -> None:
        """Test add replication policy."""
        responses.add(
            responses.POST,
            "http://localhost:8080/api/v1/replication/policies",
            json={"success": True, "message": "Replication policy added successfully"},
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
            enabled=True,
        )
        result = client.add_replication_policy(policy)

        assert result.success is True
        assert "added" in result.message.lower()

    @responses.activate
    def test_remove_replication_policy(self) -> None:
        """Test remove replication policy."""
        responses.add(
            responses.DELETE,
            "http://localhost:8080/api/v1/replication/policies/repl-1",
            json={"success": True, "message": "Replication policy removed successfully"},
            status=200,
        )

        client = RestClient()
        result = client.remove_replication_policy("repl-1")

        assert result.success is True
        assert "removed" in result.message.lower()

    @responses.activate
    def test_get_replication_policies(self) -> None:
        """Test get replication policies."""
        responses.add(
            responses.GET,
            "http://localhost:8080/api/v1/replication/policies",
            json={
                "policies": [
                    {
                        "id": "repl-1",
                        "source_backend": "local",
                        "source_settings": {"path": "/data"},
                        "source_prefix": "",
                        "destination_backend": "s3",
                        "destination_settings": {"bucket": "backup"},
                        "check_interval_seconds": 300,
                        "last_sync_time": None,
                        "enabled": True,
                        "encryption": None,
                        "replication_mode": "TRANSPARENT",
                    }
                ]
            },
            status=200,
        )

        client = RestClient()
        result = client.get_replication_policies()

        assert len(result.policies) == 1
        assert result.policies[0].id == "repl-1"
        assert result.policies[0].source_backend == "local"
        assert result.policies[0].destination_backend == "s3"

    @responses.activate
    def test_get_replication_policy(self) -> None:
        """Test get specific replication policy."""
        responses.add(
            responses.GET,
            "http://localhost:8080/api/v1/replication/policies/repl-1",
            json={
                "policy": {
                    "id": "repl-1",
                    "source_backend": "local",
                    "source_settings": {"path": "/data"},
                    "source_prefix": "",
                    "destination_backend": "s3",
                    "destination_settings": {"bucket": "backup"},
                    "check_interval_seconds": 300,
                    "last_sync_time": None,
                    "enabled": True,
                    "encryption": None,
                    "replication_mode": "TRANSPARENT",
                }
            },
            status=200,
        )

        client = RestClient()
        policy = client.get_replication_policy("repl-1")

        assert policy.id == "repl-1"
        assert policy.source_backend == "local"
        assert policy.destination_backend == "s3"

    @responses.activate
    def test_trigger_replication(self) -> None:
        """Test trigger replication."""
        responses.add(
            responses.POST,
            "http://localhost:8080/api/v1/replication/trigger",
            json={
                "success": True,
                "result": {
                    "policy_id": "repl-1",
                    "synced": 150,
                    "deleted": 5,
                    "failed": 2,
                    "bytes_total": 1048576,
                    "duration_ms": 5200,
                    "errors": ["Failed to sync object1", "Failed to sync object2"],
                },
                "message": "Replication triggered successfully",
            },
            status=200,
        )

        client = RestClient()
        opts = TriggerReplicationOptions(
            policy_id="repl-1", parallel=True, worker_count=4
        )
        result = client.trigger_replication(opts)

        assert result.success is True
        assert result.result is not None
        assert result.result.policy_id == "repl-1"
        assert result.result.synced == 150
        assert result.result.deleted == 5
        assert result.result.failed == 2
        assert len(result.result.errors) == 2

    @responses.activate
    def test_get_replication_status(self) -> None:
        """Test get replication status."""
        responses.add(
            responses.GET,
            "http://localhost:8080/api/v1/replication/status/repl-1",
            json={
                "success": True,
                "status": {
                    "policy_id": "repl-1",
                    "source_backend": "local",
                    "destination_backend": "s3",
                    "enabled": True,
                    "total_objects_synced": 1500,
                    "total_objects_deleted": 50,
                    "total_bytes_synced": 10485760,
                    "total_errors": 3,
                    "last_sync_time": "2025-11-25T10:00:00Z",
                    "average_sync_duration_ms": 2500,
                    "sync_count": 10,
                },
                "message": "Status retrieved successfully",
            },
            status=200,
        )

        client = RestClient()
        result = client.get_replication_status("repl-1")

        assert result.success is True
        assert result.status is not None
        assert result.status.policy_id == "repl-1"
        assert result.status.total_objects_synced == 1500
        assert result.status.total_bytes_synced == 10485760


class TestModelValidation:
    """Test model validation for new types."""

    def test_lifecycle_policy_creation(self) -> None:
        """Test creating a lifecycle policy."""
        policy = LifecyclePolicy(
            id="test-policy",
            prefix="logs/",
            retention_seconds=86400,
            action="delete",
        )

        assert policy.id == "test-policy"
        assert policy.prefix == "logs/"
        assert policy.retention_seconds == 86400
        assert policy.action == "delete"
        assert policy.destination_type is None

    def test_replication_policy_creation(self) -> None:
        """Test creating a replication policy."""
        policy = ReplicationPolicy(
            id="repl-policy",
            source_backend="local",
            source_settings={"path": "/data"},
            destination_backend="s3",
            destination_settings={"bucket": "backup"},
            check_interval_seconds=300,
            enabled=True,
        )

        assert policy.id == "repl-policy"
        assert policy.source_backend == "local"
        assert policy.destination_backend == "s3"
        assert policy.check_interval_seconds == 300
        assert policy.enabled is True

    def test_trigger_replication_options(self) -> None:
        """Test creating trigger replication options."""
        opts = TriggerReplicationOptions(
            policy_id="repl-1",
            parallel=True,
            worker_count=4,
        )

        assert opts.policy_id == "repl-1"
        assert opts.parallel is True
        assert opts.worker_count == 4

    def test_sync_result_creation(self) -> None:
        """Test creating a sync result."""
        result = SyncResult(
            policy_id="repl-1",
            synced=150,
            deleted=5,
            failed=2,
            bytes_total=1048576,
            duration_ms=5200,
            errors=["error1", "error2"],
        )

        assert result.policy_id == "repl-1"
        assert result.synced == 150
        assert result.deleted == 5
        assert result.failed == 2
        assert result.bytes_total == 1048576
        assert len(result.errors) == 2

    def test_replication_status_creation(self) -> None:
        """Test creating a replication status."""
        status = ReplicationStatus(
            policy_id="repl-1",
            source_backend="local",
            destination_backend="s3",
            enabled=True,
            total_objects_synced=1500,
            total_objects_deleted=50,
            total_bytes_synced=10485760,
            total_errors=3,
            last_sync_time=datetime.now(),
            average_sync_duration_ms=2500,
            sync_count=10,
        )

        assert status.policy_id == "repl-1"
        assert status.source_backend == "local"
        assert status.destination_backend == "s3"
        assert status.enabled is True
        assert status.total_objects_synced == 1500
