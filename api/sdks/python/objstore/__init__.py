"""Go-ObjStore Python SDK.

A comprehensive Python SDK for go-objstore with support for REST, gRPC, and QUIC/HTTP3 protocols.
"""

from objstore.client import ObjectStoreClient
from objstore.models import (
    ApplyPoliciesResponse,
    ArchiveResponse,
    DeleteResponse,
    EncryptionConfig,
    EncryptionPolicy,
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
from objstore.exceptions import (
    ObjectStoreError,
    ObjectNotFoundError,
    ConnectionError,
    AuthenticationError,
    ValidationError,
)

__version__ = "0.1.0"
__all__ = [
    "ObjectStoreClient",
    "ApplyPoliciesResponse",
    "ArchiveResponse",
    "AuthenticationError",
    "ConnectionError",
    "DeleteResponse",
    "EncryptionConfig",
    "EncryptionPolicy",
    "ExistsResponse",
    "GetPoliciesResponse",
    "GetReplicationPoliciesResponse",
    "GetReplicationStatusResponse",
    "HealthResponse",
    "HealthStatus",
    "LifecyclePolicy",
    "ListResponse",
    "Metadata",
    "ObjectInfo",
    "ObjectNotFoundError",
    "ObjectStoreError",
    "PolicyResponse",
    "PutResponse",
    "ReplicationMode",
    "ReplicationPolicy",
    "ReplicationStatus",
    "SyncResult",
    "TriggerReplicationOptions",
    "TriggerReplicationResponse",
    "ValidationError",
]
