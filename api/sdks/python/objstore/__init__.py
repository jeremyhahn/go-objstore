"""Go-ObjStore Python SDK.

A comprehensive Python SDK for go-objstore with support for REST, gRPC, QUIC/HTTP3,
MCP (Model Context Protocol HTTP), and Unix domain socket protocols.
"""

from objstore.client import ObjectStoreClient, Protocol
from objstore.mcp_client import McpClient
from objstore.unix_client import UnixClient
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
    AuthorizationError,
    AlreadyExistsError,
    RateLimitError,
    ValidationError,
)

__version__ = "0.2.0"
__all__ = [
    "ObjectStoreClient",
    "Protocol",
    "McpClient",
    "UnixClient",
    "AlreadyExistsError",
    "ApplyPoliciesResponse",
    "ArchiveResponse",
    "AuthenticationError",
    "AuthorizationError",
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
    "RateLimitError",
    "ReplicationMode",
    "ReplicationPolicy",
    "ReplicationStatus",
    "SyncResult",
    "TriggerReplicationOptions",
    "TriggerReplicationResponse",
    "ValidationError",
]
