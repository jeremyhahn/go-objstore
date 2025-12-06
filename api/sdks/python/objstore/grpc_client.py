"""gRPC client implementation for go-objstore."""

from datetime import datetime
from typing import Iterator, List, Optional, Union

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
    ReplicationPolicy,
    ReplicationStatus,
    SyncResult,
    TriggerReplicationOptions,
    TriggerReplicationResponse,
)

try:
    import grpc
    from google.protobuf.timestamp_pb2 import Timestamp

    # Try to import generated proto files
    try:
        from objstore.proto import objstore_pb2, objstore_pb2_grpc

        GRPC_AVAILABLE = True
    except ImportError:
        GRPC_AVAILABLE = False
except ImportError:
    GRPC_AVAILABLE = False


class GrpcClient:
    """gRPC client for go-objstore.

    Note: Requires gRPC proto files to be generated.
    Run: scripts/generate_grpc.sh
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 50051,
        timeout: int = 30,
        max_retries: int = 3,
    ) -> None:
        """Initialize gRPC client.

        Args:
            host: Server hostname
            port: Server port
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts

        Raises:
            ImportError: If gRPC is not available
        """
        if not GRPC_AVAILABLE:
            raise ImportError(
                "gRPC support requires proto files. Run: scripts/generate_grpc.sh"
            )

        self.host = host
        self.port = port
        self.timeout = timeout
        self.max_retries = max_retries
        self.channel = grpc.insecure_channel(f"{host}:{port}")
        self.stub = objstore_pb2_grpc.ObjectStoreStub(self.channel)

    def _metadata_to_proto(self, metadata: Optional[Metadata]) -> object:
        """Convert Metadata model to protobuf message.

        Args:
            metadata: Metadata model

        Returns:
            Protobuf metadata message
        """
        if not metadata:
            return None

        proto_metadata = objstore_pb2.Metadata()
        if metadata.content_type:
            proto_metadata.content_type = metadata.content_type
        if metadata.content_encoding:
            proto_metadata.content_encoding = metadata.content_encoding
        if metadata.size:
            proto_metadata.size = metadata.size
        if metadata.etag:
            proto_metadata.etag = metadata.etag
        if metadata.custom:
            proto_metadata.custom.update(metadata.custom)

        return proto_metadata

    def _proto_to_metadata(self, proto_metadata: object) -> Metadata:
        """Convert protobuf metadata to Metadata model.

        Args:
            proto_metadata: Protobuf metadata message

        Returns:
            Metadata model
        """
        metadata = Metadata(
            content_type=proto_metadata.content_type or None,
            content_encoding=proto_metadata.content_encoding or None,
            size=proto_metadata.size if proto_metadata.size else None,
            etag=proto_metadata.etag or None,
            custom=dict(proto_metadata.custom) if proto_metadata.custom else {},
        )

        if proto_metadata.HasField("last_modified"):
            metadata.last_modified = datetime.fromtimestamp(
                proto_metadata.last_modified.seconds
            )

        return metadata

    def put(self, key: str, data: bytes, metadata: Optional[Metadata] = None) -> PutResponse:
        """Upload an object.

        Args:
            key: Object key/path
            data: Object data
            metadata: Optional metadata

        Returns:
            PutResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        try:
            request = objstore_pb2.PutRequest(
                key=key, data=data, metadata=self._metadata_to_proto(metadata)
            )

            response = self.stub.Put(request, timeout=self.timeout)

            return PutResponse(
                success=response.success, message=response.message, etag=response.etag
            )

        except grpc.RpcError as e:
            self._handle_grpc_error(e)
            return PutResponse(success=False, message=str(e))

    def get(self, key: str) -> tuple[bytes, Metadata]:
        """Download an object.

        Args:
            key: Object key/path

        Returns:
            Tuple of (data, metadata)

        Raises:
            ObjectStoreError: On failure
        """
        try:
            request = objstore_pb2.GetRequest(key=key)
            responses = self.stub.Get(request, timeout=self.timeout)

            data = b""
            metadata = Metadata()

            for response in responses:
                data += response.data
                if response.HasField("metadata"):
                    metadata = self._proto_to_metadata(response.metadata)

            return data, metadata

        except grpc.RpcError as e:
            self._handle_grpc_error(e)
            return b"", Metadata()

    def get_stream(self, key: str) -> Iterator[bytes]:
        """Download an object as a stream.

        Args:
            key: Object key/path

        Yields:
            Chunks of object data

        Raises:
            ObjectStoreError: On failure
        """
        try:
            request = objstore_pb2.GetRequest(key=key)
            responses = self.stub.Get(request, timeout=self.timeout)

            for response in responses:
                if response.data:
                    yield response.data

        except grpc.RpcError as e:
            self._handle_grpc_error(e)

    def delete(self, key: str) -> DeleteResponse:
        """Delete an object.

        Args:
            key: Object key/path

        Returns:
            DeleteResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        try:
            request = objstore_pb2.DeleteRequest(key=key)
            response = self.stub.Delete(request, timeout=self.timeout)

            return DeleteResponse(success=response.success, message=response.message)

        except grpc.RpcError as e:
            self._handle_grpc_error(e)
            return DeleteResponse(success=False, message=str(e))

    def list(
        self,
        prefix: str = "",
        delimiter: str = "",
        max_results: int = 100,
        continue_from: Optional[str] = None,
    ) -> ListResponse:
        """List objects.

        Args:
            prefix: Filter objects by prefix
            delimiter: Delimiter for hierarchical listing
            max_results: Maximum number of results
            continue_from: Pagination token

        Returns:
            ListResponse with matching objects

        Raises:
            ObjectStoreError: On failure
        """
        try:
            request = objstore_pb2.ListRequest(
                prefix=prefix,
                delimiter=delimiter,
                max_results=max_results,
                continue_from=continue_from or "",
            )

            response = self.stub.List(request, timeout=self.timeout)

            objects = [
                ObjectInfo(key=obj.key, metadata=self._proto_to_metadata(obj.metadata))
                for obj in response.objects
            ]

            return ListResponse(
                objects=objects,
                common_prefixes=list(response.common_prefixes),
                next_token=response.next_token if response.next_token else None,
                truncated=response.truncated,
            )

        except grpc.RpcError as e:
            self._handle_grpc_error(e)
            return ListResponse()

    def exists(self, key: str) -> ExistsResponse:
        """Check if an object exists.

        Args:
            key: Object key/path

        Returns:
            ExistsResponse indicating existence

        Raises:
            ObjectStoreError: On failure
        """
        try:
            request = objstore_pb2.ExistsRequest(key=key)
            response = self.stub.Exists(request, timeout=self.timeout)

            return ExistsResponse(exists=response.exists)

        except grpc.RpcError as e:
            self._handle_grpc_error(e)
            return ExistsResponse(exists=False)

    def get_metadata(self, key: str) -> Metadata:
        """Get object metadata.

        Args:
            key: Object key/path

        Returns:
            Object metadata

        Raises:
            ObjectStoreError: On failure
        """
        try:
            request = objstore_pb2.GetMetadataRequest(key=key)
            response = self.stub.GetMetadata(request, timeout=self.timeout)

            if response.success and response.HasField("metadata"):
                return self._proto_to_metadata(response.metadata)

            return Metadata()

        except grpc.RpcError as e:
            self._handle_grpc_error(e)
            return Metadata()

    def update_metadata(self, key: str, metadata: Metadata) -> PolicyResponse:
        """Update object metadata.

        Args:
            key: Object key/path
            metadata: New metadata

        Returns:
            PolicyResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        try:
            request = objstore_pb2.UpdateMetadataRequest(
                key=key, metadata=self._metadata_to_proto(metadata)
            )

            response = self.stub.UpdateMetadata(request, timeout=self.timeout)

            return PolicyResponse(success=response.success, message=response.message)

        except grpc.RpcError as e:
            self._handle_grpc_error(e)
            return PolicyResponse(success=False, message=str(e))

    def health(self) -> HealthResponse:
        """Check server health.

        Returns:
            HealthResponse with server status

        Raises:
            ObjectStoreError: On failure
        """
        try:
            request = objstore_pb2.HealthRequest()
            response = self.stub.Health(request, timeout=self.timeout)

            status_map = {
                0: HealthStatus.UNKNOWN,
                1: HealthStatus.SERVING,
                2: HealthStatus.NOT_SERVING,
            }

            status = status_map.get(response.status, HealthStatus.UNKNOWN)

            return HealthResponse(status=status, message=response.message)

        except grpc.RpcError as e:
            self._handle_grpc_error(e)
            return HealthResponse(status=HealthStatus.UNKNOWN)

    def archive(self, key: str, destination_type: str, settings: dict[str, str]) -> ArchiveResponse:
        """Archive an object to a different storage backend.

        Args:
            key: Object key/path
            destination_type: Destination backend type
            settings: Backend-specific settings

        Returns:
            ArchiveResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        try:
            request = objstore_pb2.ArchiveRequest(
                key=key, destination_type=destination_type, destination_settings=settings
            )
            response = self.stub.Archive(request, timeout=self.timeout)

            return ArchiveResponse(success=response.success, message=response.message)

        except grpc.RpcError as e:
            self._handle_grpc_error(e)
            return ArchiveResponse(success=False, message=str(e))

    def add_policy(self, policy: LifecyclePolicy) -> PolicyResponse:
        """Add a lifecycle policy.

        Args:
            policy: Lifecycle policy to add

        Returns:
            PolicyResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        try:
            proto_policy = objstore_pb2.LifecyclePolicy(
                id=policy.id,
                prefix=policy.prefix,
                retention_seconds=policy.retention_seconds,
                action=policy.action,
                destination_type=policy.destination_type or "",
                destination_settings=policy.destination_settings,
            )
            request = objstore_pb2.AddPolicyRequest(policy=proto_policy)
            response = self.stub.AddPolicy(request, timeout=self.timeout)

            return PolicyResponse(success=response.success, message=response.message)

        except grpc.RpcError as e:
            self._handle_grpc_error(e)
            return PolicyResponse(success=False, message=str(e))

    def remove_policy(self, policy_id: str) -> PolicyResponse:
        """Remove a lifecycle policy.

        Args:
            policy_id: Policy ID to remove

        Returns:
            PolicyResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        try:
            request = objstore_pb2.RemovePolicyRequest(id=policy_id)
            response = self.stub.RemovePolicy(request, timeout=self.timeout)

            return PolicyResponse(success=response.success, message=response.message)

        except grpc.RpcError as e:
            self._handle_grpc_error(e)
            return PolicyResponse(success=False, message=str(e))

    def get_policies(self, prefix: str = "") -> GetPoliciesResponse:
        """Get lifecycle policies.

        Args:
            prefix: Filter policies by prefix

        Returns:
            GetPoliciesResponse with list of policies

        Raises:
            ObjectStoreError: On failure
        """
        try:
            request = objstore_pb2.GetPoliciesRequest(prefix=prefix)
            response = self.stub.GetPolicies(request, timeout=self.timeout)

            policies = [
                LifecyclePolicy(
                    id=p.id,
                    prefix=p.prefix,
                    retention_seconds=p.retention_seconds,
                    action=p.action,
                    destination_type=p.destination_type or None,
                    destination_settings=dict(p.destination_settings),
                )
                for p in response.policies
            ]

            return GetPoliciesResponse(
                policies=policies, success=response.success, message=response.message
            )

        except grpc.RpcError as e:
            self._handle_grpc_error(e)
            return GetPoliciesResponse(policies=[], success=False, message=str(e))

    def apply_policies(self) -> ApplyPoliciesResponse:
        """Apply all lifecycle policies.

        Returns:
            ApplyPoliciesResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        try:
            request = objstore_pb2.ApplyPoliciesRequest()
            response = self.stub.ApplyPolicies(request, timeout=self.timeout)

            return ApplyPoliciesResponse(
                success=response.success,
                policies_count=response.policies_count,
                objects_processed=response.objects_processed,
                message=response.message,
            )

        except grpc.RpcError as e:
            self._handle_grpc_error(e)
            return ApplyPoliciesResponse(
                success=False, policies_count=0, objects_processed=0, message=str(e)
            )

    def add_replication_policy(self, policy: ReplicationPolicy) -> PolicyResponse:
        """Add a replication policy.

        Args:
            policy: Replication policy to add

        Returns:
            PolicyResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        try:
            proto_policy = objstore_pb2.ReplicationPolicy(
                id=policy.id,
                source_backend=policy.source_backend,
                source_settings=policy.source_settings,
                source_prefix=policy.source_prefix,
                destination_backend=policy.destination_backend,
                destination_settings=policy.destination_settings,
                check_interval_seconds=policy.check_interval_seconds,
                enabled=policy.enabled,
            )
            request = objstore_pb2.AddReplicationPolicyRequest(policy=proto_policy)
            response = self.stub.AddReplicationPolicy(request, timeout=self.timeout)

            return PolicyResponse(success=response.success, message=response.message)

        except grpc.RpcError as e:
            self._handle_grpc_error(e)
            return PolicyResponse(success=False, message=str(e))

    def remove_replication_policy(self, policy_id: str) -> PolicyResponse:
        """Remove a replication policy.

        Args:
            policy_id: Policy ID to remove

        Returns:
            PolicyResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        try:
            request = objstore_pb2.RemoveReplicationPolicyRequest(id=policy_id)
            response = self.stub.RemoveReplicationPolicy(request, timeout=self.timeout)

            return PolicyResponse(success=response.success, message=response.message)

        except grpc.RpcError as e:
            self._handle_grpc_error(e)
            return PolicyResponse(success=False, message=str(e))

    def get_replication_policies(self) -> GetReplicationPoliciesResponse:
        """Get all replication policies.

        Returns:
            GetReplicationPoliciesResponse with list of policies

        Raises:
            ObjectStoreError: On failure
        """
        try:
            request = objstore_pb2.GetReplicationPoliciesRequest()
            response = self.stub.GetReplicationPolicies(request, timeout=self.timeout)

            policies = [
                ReplicationPolicy(
                    id=p.id,
                    source_backend=p.source_backend,
                    source_settings=dict(p.source_settings),
                    source_prefix=p.source_prefix,
                    destination_backend=p.destination_backend,
                    destination_settings=dict(p.destination_settings),
                    check_interval_seconds=p.check_interval_seconds,
                    enabled=p.enabled,
                )
                for p in response.policies
            ]

            return GetReplicationPoliciesResponse(policies=policies)

        except grpc.RpcError as e:
            self._handle_grpc_error(e)
            return GetReplicationPoliciesResponse(policies=[])

    def get_replication_policy(self, policy_id: str) -> ReplicationPolicy:
        """Get a specific replication policy.

        Args:
            policy_id: Policy ID to retrieve

        Returns:
            ReplicationPolicy

        Raises:
            ObjectStoreError: On failure
        """
        try:
            request = objstore_pb2.GetReplicationPolicyRequest(id=policy_id)
            response = self.stub.GetReplicationPolicy(request, timeout=self.timeout)

            if response.success and response.HasField("policy"):
                p = response.policy
                return ReplicationPolicy(
                    id=p.id,
                    source_backend=p.source_backend,
                    source_settings=dict(p.source_settings),
                    source_prefix=p.source_prefix,
                    destination_backend=p.destination_backend,
                    destination_settings=dict(p.destination_settings),
                    check_interval_seconds=p.check_interval_seconds,
                    enabled=p.enabled,
                )

            return ReplicationPolicy(
                id="", source_backend="", destination_backend="", check_interval_seconds=0
            )

        except grpc.RpcError as e:
            self._handle_grpc_error(e)
            return ReplicationPolicy(
                id="", source_backend="", destination_backend="", check_interval_seconds=0
            )

    def trigger_replication(self, opts: TriggerReplicationOptions) -> TriggerReplicationResponse:
        """Trigger replication synchronization.

        Args:
            opts: Trigger options

        Returns:
            TriggerReplicationResponse with sync result

        Raises:
            ObjectStoreError: On failure
        """
        try:
            request = objstore_pb2.TriggerReplicationRequest(
                policy_id=opts.policy_id, parallel=opts.parallel, worker_count=opts.worker_count
            )
            response = self.stub.TriggerReplication(request, timeout=self.timeout)

            sync_result = None
            if response.HasField("result"):
                r = response.result
                sync_result = SyncResult(
                    policy_id=r.policy_id,
                    synced=r.synced,
                    deleted=r.deleted,
                    failed=r.failed,
                    bytes_total=r.bytes_total,
                    duration_ms=r.duration_ms,
                    errors=list(r.errors),
                )

            return TriggerReplicationResponse(
                success=response.success, result=sync_result, message=response.message
            )

        except grpc.RpcError as e:
            self._handle_grpc_error(e)
            return TriggerReplicationResponse(success=False, result=None, message=str(e))

    def get_replication_status(self, policy_id: str) -> GetReplicationStatusResponse:
        """Get replication status for a policy.

        Args:
            policy_id: Policy ID to get status for

        Returns:
            GetReplicationStatusResponse with status

        Raises:
            ObjectStoreError: On failure
        """
        try:
            request = objstore_pb2.GetReplicationStatusRequest(policy_id=policy_id)
            response = self.stub.GetReplicationStatus(request, timeout=self.timeout)

            status = None
            if response.success and response.HasField("status"):
                s = response.status
                last_sync = None
                if s.HasField("last_sync_time"):
                    last_sync = datetime.fromtimestamp(s.last_sync_time.seconds)

                status = ReplicationStatus(
                    policy_id=s.policy_id,
                    source_backend=s.source_backend,
                    destination_backend=s.destination_backend,
                    enabled=s.enabled,
                    total_objects_synced=s.total_objects_synced,
                    total_objects_deleted=s.total_objects_deleted,
                    total_bytes_synced=s.total_bytes_synced,
                    total_errors=s.total_errors,
                    last_sync_time=last_sync,
                    average_sync_duration_ms=s.average_sync_duration_ms,
                    sync_count=s.sync_count,
                )

            return GetReplicationStatusResponse(
                success=response.success, status=status, message=response.message
            )

        except grpc.RpcError as e:
            self._handle_grpc_error(e)
            return GetReplicationStatusResponse(success=False, status=None, message=str(e))

    def _handle_grpc_error(self, error: object) -> None:
        """Handle gRPC errors.

        Args:
            error: gRPC error

        Raises:
            ObjectStoreError: Converted exception
        """
        code = error.code()
        details = error.details()

        if code == grpc.StatusCode.NOT_FOUND:
            raise ObjectNotFoundError(details)
        elif code == grpc.StatusCode.DEADLINE_EXCEEDED:
            raise TimeoutError(details)
        elif code == grpc.StatusCode.UNAVAILABLE:
            raise ConnectionError(details)
        elif code == grpc.StatusCode.INTERNAL:
            raise ServerError(details)
        else:
            raise ObjectStoreError(f"gRPC error: {details}")

    def close(self) -> None:
        """Close the gRPC channel."""
        if self.channel:
            self.channel.close()

    def __enter__(self) -> "GrpcClient":
        """Context manager entry."""
        return self

    def __exit__(self, *args: object) -> None:
        """Context manager exit."""
        self.close()
