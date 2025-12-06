"""Data models for the go-objstore SDK."""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, ConfigDict


class HealthStatus(str, Enum):
    """Health status enumeration."""

    UNKNOWN = "UNKNOWN"
    SERVING = "SERVING"
    NOT_SERVING = "NOT_SERVING"


class ReplicationMode(str, Enum):
    """Replication mode enumeration."""

    TRANSPARENT = "transparent"
    OPAQUE = "opaque"


class Metadata(BaseModel):
    """Object metadata."""

    model_config = ConfigDict(populate_by_name=True)

    content_type: Optional[str] = Field(None, description="MIME type of the object")
    content_encoding: Optional[str] = Field(None, description="Content encoding")
    size: Optional[int] = Field(None, description="Size in bytes")
    last_modified: Optional[datetime] = Field(None, description="Last modification timestamp")
    etag: Optional[str] = Field(None, description="Entity tag")
    custom: Dict[str, str] = Field(default_factory=dict, description="Custom metadata")


class ObjectInfo(BaseModel):
    """Object information."""

    model_config = ConfigDict(populate_by_name=True)

    key: str = Field(..., description="Object key/path")
    metadata: Optional[Metadata] = Field(None, description="Object metadata")


class LifecyclePolicy(BaseModel):
    """Lifecycle policy for objects."""

    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(..., description="Unique policy identifier")
    prefix: str = Field(..., description="Object key prefix filter")
    retention_seconds: Optional[int] = Field(None, description="Retention duration in seconds")
    action: str = Field(..., description="Action: 'delete' or 'archive'")
    destination_type: Optional[str] = Field(None, description="Destination backend type")
    destination_settings: Dict[str, str] = Field(
        default_factory=dict, description="Destination settings"
    )
    days_after_creation: Optional[int] = Field(
        None,
        description="Days after creation (auto-converts to retention_seconds)",
        exclude=True  # Exclude from serialization
    )
    enabled: bool = Field(True, description="Whether policy is enabled")

    def model_post_init(self, __context: Any) -> None:
        """Post-initialization to convert days_after_creation to retention_seconds."""
        super().model_post_init(__context)
        if self.days_after_creation is not None and self.retention_seconds is None:
            self.retention_seconds = self.days_after_creation * 86400
        elif self.retention_seconds is None:
            # Default to 30 days if neither is provided
            self.retention_seconds = 30 * 86400


class EncryptionConfig(BaseModel):
    """Encryption configuration for a single layer."""

    model_config = ConfigDict(populate_by_name=True)

    enabled: bool = Field(False, description="Whether encryption is enabled")
    provider: str = Field("noop", description="Encryption provider")
    default_key: Optional[str] = Field(None, description="Default encryption key")


class EncryptionPolicy(BaseModel):
    """Encryption policy for all three layers."""

    model_config = ConfigDict(populate_by_name=True)

    backend: Optional[EncryptionConfig] = Field(None, description="Backend encryption")
    source: Optional[EncryptionConfig] = Field(None, description="Source encryption")
    destination: Optional[EncryptionConfig] = Field(None, description="Destination encryption")


class ReplicationPolicy(BaseModel):
    """Replication policy."""

    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(..., description="Unique policy identifier")
    source_backend: str = Field(..., description="Source backend type")
    source_settings: Dict[str, str] = Field(
        default_factory=dict, description="Source backend settings"
    )
    source_prefix: str = Field("", description="Source object prefix filter")
    destination_backend: str = Field(..., description="Destination backend type")
    destination_settings: Dict[str, str] = Field(
        default_factory=dict, description="Destination backend settings"
    )
    check_interval_seconds: int = Field(..., description="Check interval in seconds")
    last_sync_time: Optional[datetime] = Field(None, description="Last sync timestamp")
    enabled: bool = Field(True, description="Whether policy is active")
    encryption: Optional[EncryptionPolicy] = Field(None, description="Encryption configuration")
    replication_mode: ReplicationMode = Field(
        ReplicationMode.TRANSPARENT, description="Replication mode"
    )


class ListResponse(BaseModel):
    """List objects response."""

    model_config = ConfigDict(populate_by_name=True)

    objects: List[ObjectInfo] = Field(default_factory=list, description="List of objects")
    common_prefixes: List[str] = Field(
        default_factory=list, description="Common prefixes when using delimiter"
    )
    next_token: Optional[str] = Field(None, description="Pagination token")
    truncated: bool = Field(False, description="Whether more results are available")


class PutResponse(BaseModel):
    """Put object response."""

    model_config = ConfigDict(populate_by_name=True)

    success: bool = Field(..., description="Whether the operation was successful")
    message: Optional[str] = Field(None, description="Optional message")
    etag: Optional[str] = Field(None, description="ETag of the stored object")


class DeleteResponse(BaseModel):
    """Delete object response."""

    model_config = ConfigDict(populate_by_name=True)

    success: bool = Field(..., description="Whether the operation was successful")
    message: Optional[str] = Field(None, description="Optional message")


class ExistsResponse(BaseModel):
    """Object exists response."""

    model_config = ConfigDict(populate_by_name=True)

    exists: bool = Field(..., description="Whether the object exists")


class HealthResponse(BaseModel):
    """Health check response."""

    model_config = ConfigDict(populate_by_name=True)

    status: HealthStatus = Field(..., description="Health status")
    message: Optional[str] = Field(None, description="Optional message")


class ArchiveResponse(BaseModel):
    """Archive object response."""

    model_config = ConfigDict(populate_by_name=True)

    success: bool = Field(..., description="Whether the operation was successful")
    message: Optional[str] = Field(None, description="Optional message")


class PolicyResponse(BaseModel):
    """Generic policy operation response."""

    model_config = ConfigDict(populate_by_name=True)

    success: bool = Field(..., description="Whether the operation was successful")
    message: Optional[str] = Field(None, description="Optional message")


class GetPoliciesResponse(BaseModel):
    """Get lifecycle policies response."""

    model_config = ConfigDict(populate_by_name=True)

    policies: List[LifecyclePolicy] = Field(default_factory=list, description="List of policies")
    success: bool = Field(..., description="Whether the operation was successful")
    message: Optional[str] = Field(None, description="Optional message")


class ApplyPoliciesResponse(BaseModel):
    """Apply policies response."""

    model_config = ConfigDict(populate_by_name=True)

    success: bool = Field(..., description="Whether the operation was successful")
    policies_count: int = Field(..., description="Number of policies applied")
    objects_processed: int = Field(..., description="Number of objects processed")
    message: Optional[str] = Field(None, description="Optional message")


class GetReplicationPoliciesResponse(BaseModel):
    """Get replication policies response."""

    model_config = ConfigDict(populate_by_name=True)

    policies: List[ReplicationPolicy] = Field(
        default_factory=list, description="List of replication policies"
    )


class SyncResult(BaseModel):
    """Replication sync result."""

    model_config = ConfigDict(populate_by_name=True)

    policy_id: str = Field(..., description="Policy ID")
    synced: int = Field(..., description="Objects synced")
    deleted: int = Field(..., description="Objects deleted")
    failed: int = Field(..., description="Objects failed")
    bytes_total: int = Field(..., description="Total bytes transferred")
    duration_ms: int = Field(..., description="Duration in milliseconds")
    errors: List[str] = Field(default_factory=list, description="Error messages")


class TriggerReplicationResponse(BaseModel):
    """Trigger replication response."""

    model_config = ConfigDict(populate_by_name=True)

    success: bool = Field(..., description="Whether the operation was successful")
    result: Optional[SyncResult] = Field(None, description="Sync result")
    message: Optional[str] = Field(None, description="Optional message")


class ReplicationStatus(BaseModel):
    """Replication status."""

    model_config = ConfigDict(populate_by_name=True)

    policy_id: str = Field(..., description="Policy ID")
    source_backend: str = Field(..., description="Source backend type")
    destination_backend: str = Field(..., description="Destination backend type")
    enabled: bool = Field(..., description="Whether policy is enabled")
    total_objects_synced: int = Field(..., description="Total objects synced")
    total_objects_deleted: int = Field(..., description="Total objects deleted")
    total_bytes_synced: int = Field(..., description="Total bytes synced")
    total_errors: int = Field(..., description="Total errors")
    last_sync_time: Optional[datetime] = Field(None, description="Last sync time")
    average_sync_duration_ms: int = Field(..., description="Average sync duration")
    sync_count: int = Field(..., description="Number of syncs performed")


class TriggerReplicationOptions(BaseModel):
    """Options for triggering replication."""

    model_config = ConfigDict(populate_by_name=True)

    policy_id: str = Field(..., description="Policy ID to trigger")
    parallel: bool = Field(False, description="Whether to run in parallel")
    worker_count: int = Field(1, description="Number of workers for parallel execution")


class GetReplicationStatusResponse(BaseModel):
    """Get replication status response."""

    model_config = ConfigDict(populate_by_name=True)

    success: bool = Field(..., description="Whether the operation was successful")
    status: Optional[ReplicationStatus] = Field(None, description="Replication status")
    message: Optional[str] = Field(None, description="Optional message")
