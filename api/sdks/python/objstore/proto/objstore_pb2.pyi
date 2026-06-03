from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ReplicationMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TRANSPARENT: _ClassVar[ReplicationMode]
    OPAQUE: _ClassVar[ReplicationMode]
TRANSPARENT: ReplicationMode
OPAQUE: ReplicationMode

class Metadata(_message.Message):
    __slots__ = ("content_type", "content_encoding", "size", "last_modified", "etag", "custom")
    class CustomEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_ENCODING_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    LAST_MODIFIED_FIELD_NUMBER: _ClassVar[int]
    ETAG_FIELD_NUMBER: _ClassVar[int]
    CUSTOM_FIELD_NUMBER: _ClassVar[int]
    content_type: str
    content_encoding: str
    size: int
    last_modified: _timestamp_pb2.Timestamp
    etag: str
    custom: _containers.ScalarMap[str, str]
    def __init__(self, content_type: _Optional[str] = ..., content_encoding: _Optional[str] = ..., size: _Optional[int] = ..., last_modified: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., etag: _Optional[str] = ..., custom: _Optional[_Mapping[str, str]] = ...) -> None: ...

class ObjectInfo(_message.Message):
    __slots__ = ("key", "metadata")
    KEY_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    key: str
    metadata: Metadata
    def __init__(self, key: _Optional[str] = ..., metadata: _Optional[_Union[Metadata, _Mapping]] = ...) -> None: ...

class PutRequest(_message.Message):
    __slots__ = ("key", "data", "metadata")
    KEY_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    key: str
    data: bytes
    metadata: Metadata
    def __init__(self, key: _Optional[str] = ..., data: _Optional[bytes] = ..., metadata: _Optional[_Union[Metadata, _Mapping]] = ...) -> None: ...

class PutResponse(_message.Message):
    __slots__ = ("success", "message", "etag")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ETAG_FIELD_NUMBER: _ClassVar[int]
    success: bool
    message: str
    etag: str
    def __init__(self, success: bool = ..., message: _Optional[str] = ..., etag: _Optional[str] = ...) -> None: ...

class GetRequest(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: str
    def __init__(self, key: _Optional[str] = ...) -> None: ...

class GetResponse(_message.Message):
    __slots__ = ("data", "metadata", "is_last")
    DATA_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    IS_LAST_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    metadata: Metadata
    is_last: bool
    def __init__(self, data: _Optional[bytes] = ..., metadata: _Optional[_Union[Metadata, _Mapping]] = ..., is_last: bool = ...) -> None: ...

class DeleteRequest(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: str
    def __init__(self, key: _Optional[str] = ...) -> None: ...

class DeleteResponse(_message.Message):
    __slots__ = ("success", "message")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    success: bool
    message: str
    def __init__(self, success: bool = ..., message: _Optional[str] = ...) -> None: ...

class ListRequest(_message.Message):
    __slots__ = ("prefix", "delimiter", "max_results", "continue_from")
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    DELIMITER_FIELD_NUMBER: _ClassVar[int]
    MAX_RESULTS_FIELD_NUMBER: _ClassVar[int]
    CONTINUE_FROM_FIELD_NUMBER: _ClassVar[int]
    prefix: str
    delimiter: str
    max_results: int
    continue_from: str
    def __init__(self, prefix: _Optional[str] = ..., delimiter: _Optional[str] = ..., max_results: _Optional[int] = ..., continue_from: _Optional[str] = ...) -> None: ...

class ListResponse(_message.Message):
    __slots__ = ("objects", "common_prefixes", "next_token", "truncated")
    OBJECTS_FIELD_NUMBER: _ClassVar[int]
    COMMON_PREFIXES_FIELD_NUMBER: _ClassVar[int]
    NEXT_TOKEN_FIELD_NUMBER: _ClassVar[int]
    TRUNCATED_FIELD_NUMBER: _ClassVar[int]
    objects: _containers.RepeatedCompositeFieldContainer[ObjectInfo]
    common_prefixes: _containers.RepeatedScalarFieldContainer[str]
    next_token: str
    truncated: bool
    def __init__(self, objects: _Optional[_Iterable[_Union[ObjectInfo, _Mapping]]] = ..., common_prefixes: _Optional[_Iterable[str]] = ..., next_token: _Optional[str] = ..., truncated: bool = ...) -> None: ...

class ExistsRequest(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: str
    def __init__(self, key: _Optional[str] = ...) -> None: ...

class ExistsResponse(_message.Message):
    __slots__ = ("exists",)
    EXISTS_FIELD_NUMBER: _ClassVar[int]
    exists: bool
    def __init__(self, exists: bool = ...) -> None: ...

class GetMetadataRequest(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: str
    def __init__(self, key: _Optional[str] = ...) -> None: ...

class MetadataResponse(_message.Message):
    __slots__ = ("metadata", "success", "message")
    METADATA_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    metadata: Metadata
    success: bool
    message: str
    def __init__(self, metadata: _Optional[_Union[Metadata, _Mapping]] = ..., success: bool = ..., message: _Optional[str] = ...) -> None: ...

class UpdateMetadataRequest(_message.Message):
    __slots__ = ("key", "metadata")
    KEY_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    key: str
    metadata: Metadata
    def __init__(self, key: _Optional[str] = ..., metadata: _Optional[_Union[Metadata, _Mapping]] = ...) -> None: ...

class UpdateMetadataResponse(_message.Message):
    __slots__ = ("success", "message")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    success: bool
    message: str
    def __init__(self, success: bool = ..., message: _Optional[str] = ...) -> None: ...

class HealthRequest(_message.Message):
    __slots__ = ("service",)
    SERVICE_FIELD_NUMBER: _ClassVar[int]
    service: str
    def __init__(self, service: _Optional[str] = ...) -> None: ...

class HealthResponse(_message.Message):
    __slots__ = ("status", "message")
    class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        UNKNOWN: _ClassVar[HealthResponse.Status]
        SERVING: _ClassVar[HealthResponse.Status]
        NOT_SERVING: _ClassVar[HealthResponse.Status]
    UNKNOWN: HealthResponse.Status
    SERVING: HealthResponse.Status
    NOT_SERVING: HealthResponse.Status
    STATUS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    status: HealthResponse.Status
    message: str
    def __init__(self, status: _Optional[_Union[HealthResponse.Status, str]] = ..., message: _Optional[str] = ...) -> None: ...

class ArchiveRequest(_message.Message):
    __slots__ = ("key", "destination_type", "destination_settings")
    class DestinationSettingsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    KEY_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_TYPE_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_SETTINGS_FIELD_NUMBER: _ClassVar[int]
    key: str
    destination_type: str
    destination_settings: _containers.ScalarMap[str, str]
    def __init__(self, key: _Optional[str] = ..., destination_type: _Optional[str] = ..., destination_settings: _Optional[_Mapping[str, str]] = ...) -> None: ...

class ArchiveResponse(_message.Message):
    __slots__ = ("success", "message")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    success: bool
    message: str
    def __init__(self, success: bool = ..., message: _Optional[str] = ...) -> None: ...

class LifecyclePolicy(_message.Message):
    __slots__ = ("id", "prefix", "retention_seconds", "action", "destination_type", "destination_settings")
    class DestinationSettingsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    RETENTION_SECONDS_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_TYPE_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_SETTINGS_FIELD_NUMBER: _ClassVar[int]
    id: str
    prefix: str
    retention_seconds: int
    action: str
    destination_type: str
    destination_settings: _containers.ScalarMap[str, str]
    def __init__(self, id: _Optional[str] = ..., prefix: _Optional[str] = ..., retention_seconds: _Optional[int] = ..., action: _Optional[str] = ..., destination_type: _Optional[str] = ..., destination_settings: _Optional[_Mapping[str, str]] = ...) -> None: ...

class AddPolicyRequest(_message.Message):
    __slots__ = ("policy",)
    POLICY_FIELD_NUMBER: _ClassVar[int]
    policy: LifecyclePolicy
    def __init__(self, policy: _Optional[_Union[LifecyclePolicy, _Mapping]] = ...) -> None: ...

class AddPolicyResponse(_message.Message):
    __slots__ = ("success", "message")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    success: bool
    message: str
    def __init__(self, success: bool = ..., message: _Optional[str] = ...) -> None: ...

class RemovePolicyRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class RemovePolicyResponse(_message.Message):
    __slots__ = ("success", "message")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    success: bool
    message: str
    def __init__(self, success: bool = ..., message: _Optional[str] = ...) -> None: ...

class GetPoliciesRequest(_message.Message):
    __slots__ = ("prefix",)
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    prefix: str
    def __init__(self, prefix: _Optional[str] = ...) -> None: ...

class GetPoliciesResponse(_message.Message):
    __slots__ = ("policies", "success", "message")
    POLICIES_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    policies: _containers.RepeatedCompositeFieldContainer[LifecyclePolicy]
    success: bool
    message: str
    def __init__(self, policies: _Optional[_Iterable[_Union[LifecyclePolicy, _Mapping]]] = ..., success: bool = ..., message: _Optional[str] = ...) -> None: ...

class ApplyPoliciesRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ApplyPoliciesResponse(_message.Message):
    __slots__ = ("success", "policies_count", "objects_processed", "message")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    POLICIES_COUNT_FIELD_NUMBER: _ClassVar[int]
    OBJECTS_PROCESSED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    success: bool
    policies_count: int
    objects_processed: int
    message: str
    def __init__(self, success: bool = ..., policies_count: _Optional[int] = ..., objects_processed: _Optional[int] = ..., message: _Optional[str] = ...) -> None: ...

class EncryptionConfig(_message.Message):
    __slots__ = ("enabled", "provider", "default_key")
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_KEY_FIELD_NUMBER: _ClassVar[int]
    enabled: bool
    provider: str
    default_key: str
    def __init__(self, enabled: bool = ..., provider: _Optional[str] = ..., default_key: _Optional[str] = ...) -> None: ...

class EncryptionPolicy(_message.Message):
    __slots__ = ("backend", "source", "destination")
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_FIELD_NUMBER: _ClassVar[int]
    backend: EncryptionConfig
    source: EncryptionConfig
    destination: EncryptionConfig
    def __init__(self, backend: _Optional[_Union[EncryptionConfig, _Mapping]] = ..., source: _Optional[_Union[EncryptionConfig, _Mapping]] = ..., destination: _Optional[_Union[EncryptionConfig, _Mapping]] = ...) -> None: ...

class ReplicationPolicy(_message.Message):
    __slots__ = ("id", "source_backend", "source_settings", "source_prefix", "destination_backend", "destination_settings", "check_interval_seconds", "last_sync_time", "enabled", "encryption", "replication_mode")
    class SourceSettingsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    class DestinationSettingsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_BACKEND_FIELD_NUMBER: _ClassVar[int]
    SOURCE_SETTINGS_FIELD_NUMBER: _ClassVar[int]
    SOURCE_PREFIX_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_BACKEND_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_SETTINGS_FIELD_NUMBER: _ClassVar[int]
    CHECK_INTERVAL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    LAST_SYNC_TIME_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    ENCRYPTION_FIELD_NUMBER: _ClassVar[int]
    REPLICATION_MODE_FIELD_NUMBER: _ClassVar[int]
    id: str
    source_backend: str
    source_settings: _containers.ScalarMap[str, str]
    source_prefix: str
    destination_backend: str
    destination_settings: _containers.ScalarMap[str, str]
    check_interval_seconds: int
    last_sync_time: _timestamp_pb2.Timestamp
    enabled: bool
    encryption: EncryptionPolicy
    replication_mode: ReplicationMode
    def __init__(self, id: _Optional[str] = ..., source_backend: _Optional[str] = ..., source_settings: _Optional[_Mapping[str, str]] = ..., source_prefix: _Optional[str] = ..., destination_backend: _Optional[str] = ..., destination_settings: _Optional[_Mapping[str, str]] = ..., check_interval_seconds: _Optional[int] = ..., last_sync_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., enabled: bool = ..., encryption: _Optional[_Union[EncryptionPolicy, _Mapping]] = ..., replication_mode: _Optional[_Union[ReplicationMode, str]] = ...) -> None: ...

class AddReplicationPolicyRequest(_message.Message):
    __slots__ = ("policy",)
    POLICY_FIELD_NUMBER: _ClassVar[int]
    policy: ReplicationPolicy
    def __init__(self, policy: _Optional[_Union[ReplicationPolicy, _Mapping]] = ...) -> None: ...

class AddReplicationPolicyResponse(_message.Message):
    __slots__ = ("success", "message")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    success: bool
    message: str
    def __init__(self, success: bool = ..., message: _Optional[str] = ...) -> None: ...

class RemoveReplicationPolicyRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class RemoveReplicationPolicyResponse(_message.Message):
    __slots__ = ("success", "message")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    success: bool
    message: str
    def __init__(self, success: bool = ..., message: _Optional[str] = ...) -> None: ...

class GetReplicationPoliciesRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetReplicationPoliciesResponse(_message.Message):
    __slots__ = ("policies",)
    POLICIES_FIELD_NUMBER: _ClassVar[int]
    policies: _containers.RepeatedCompositeFieldContainer[ReplicationPolicy]
    def __init__(self, policies: _Optional[_Iterable[_Union[ReplicationPolicy, _Mapping]]] = ...) -> None: ...

class GetReplicationPolicyRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class GetReplicationPolicyResponse(_message.Message):
    __slots__ = ("policy",)
    POLICY_FIELD_NUMBER: _ClassVar[int]
    policy: ReplicationPolicy
    def __init__(self, policy: _Optional[_Union[ReplicationPolicy, _Mapping]] = ...) -> None: ...

class TriggerReplicationRequest(_message.Message):
    __slots__ = ("policy_id", "parallel", "worker_count")
    POLICY_ID_FIELD_NUMBER: _ClassVar[int]
    PARALLEL_FIELD_NUMBER: _ClassVar[int]
    WORKER_COUNT_FIELD_NUMBER: _ClassVar[int]
    policy_id: str
    parallel: bool
    worker_count: int
    def __init__(self, policy_id: _Optional[str] = ..., parallel: bool = ..., worker_count: _Optional[int] = ...) -> None: ...

class SyncResult(_message.Message):
    __slots__ = ("policy_id", "synced", "deleted", "failed", "bytes_total", "duration_ms", "errors")
    POLICY_ID_FIELD_NUMBER: _ClassVar[int]
    SYNCED_FIELD_NUMBER: _ClassVar[int]
    DELETED_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    BYTES_TOTAL_FIELD_NUMBER: _ClassVar[int]
    DURATION_MS_FIELD_NUMBER: _ClassVar[int]
    ERRORS_FIELD_NUMBER: _ClassVar[int]
    policy_id: str
    synced: int
    deleted: int
    failed: int
    bytes_total: int
    duration_ms: int
    errors: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, policy_id: _Optional[str] = ..., synced: _Optional[int] = ..., deleted: _Optional[int] = ..., failed: _Optional[int] = ..., bytes_total: _Optional[int] = ..., duration_ms: _Optional[int] = ..., errors: _Optional[_Iterable[str]] = ...) -> None: ...

class TriggerReplicationResponse(_message.Message):
    __slots__ = ("success", "result", "message")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    success: bool
    result: SyncResult
    message: str
    def __init__(self, success: bool = ..., result: _Optional[_Union[SyncResult, _Mapping]] = ..., message: _Optional[str] = ...) -> None: ...

class GetReplicationStatusRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class ReplicationStatus(_message.Message):
    __slots__ = ("policy_id", "source_backend", "destination_backend", "enabled", "total_objects_synced", "total_objects_deleted", "total_bytes_synced", "total_errors", "last_sync_time", "average_sync_duration_ms", "sync_count")
    POLICY_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_BACKEND_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_BACKEND_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    TOTAL_OBJECTS_SYNCED_FIELD_NUMBER: _ClassVar[int]
    TOTAL_OBJECTS_DELETED_FIELD_NUMBER: _ClassVar[int]
    TOTAL_BYTES_SYNCED_FIELD_NUMBER: _ClassVar[int]
    TOTAL_ERRORS_FIELD_NUMBER: _ClassVar[int]
    LAST_SYNC_TIME_FIELD_NUMBER: _ClassVar[int]
    AVERAGE_SYNC_DURATION_MS_FIELD_NUMBER: _ClassVar[int]
    SYNC_COUNT_FIELD_NUMBER: _ClassVar[int]
    policy_id: str
    source_backend: str
    destination_backend: str
    enabled: bool
    total_objects_synced: int
    total_objects_deleted: int
    total_bytes_synced: int
    total_errors: int
    last_sync_time: _timestamp_pb2.Timestamp
    average_sync_duration_ms: int
    sync_count: int
    def __init__(self, policy_id: _Optional[str] = ..., source_backend: _Optional[str] = ..., destination_backend: _Optional[str] = ..., enabled: bool = ..., total_objects_synced: _Optional[int] = ..., total_objects_deleted: _Optional[int] = ..., total_bytes_synced: _Optional[int] = ..., total_errors: _Optional[int] = ..., last_sync_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., average_sync_duration_ms: _Optional[int] = ..., sync_count: _Optional[int] = ...) -> None: ...

class GetReplicationStatusResponse(_message.Message):
    __slots__ = ("success", "status", "message")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    success: bool
    status: ReplicationStatus
    message: str
    def __init__(self, success: bool = ..., status: _Optional[_Union[ReplicationStatus, _Mapping]] = ..., message: _Optional[str] = ...) -> None: ...
