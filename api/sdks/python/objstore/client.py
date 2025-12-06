"""Unified ObjectStore client with protocol selection."""

import asyncio
from enum import Enum
from typing import Any, BinaryIO, Coroutine, Iterator, Optional, TypeVar, Union

from objstore.exceptions import ValidationError
from objstore.grpc_client import GrpcClient
from objstore.models import (
    ApplyPoliciesResponse,
    ArchiveResponse,
    DeleteResponse,
    ExistsResponse,
    GetPoliciesResponse,
    GetReplicationPoliciesResponse,
    GetReplicationStatusResponse,
    HealthResponse,
    LifecyclePolicy,
    ListResponse,
    Metadata,
    PolicyResponse,
    PutResponse,
    ReplicationPolicy,
    TriggerReplicationOptions,
    TriggerReplicationResponse,
)
from objstore.quic_client import QuicClient
from objstore.rest_client import RestClient

T = TypeVar("T")


class Protocol(str, Enum):
    """Supported protocols."""

    REST = "rest"
    GRPC = "grpc"
    QUIC = "quic"


class ObjectStoreClient:
    """Unified client for go-objstore with protocol selection.

    This client provides a consistent interface across REST, gRPC, and QUIC/HTTP3
    protocols, allowing you to switch between them without changing your code.

    Example:
        # REST
        client = ObjectStoreClient(protocol=Protocol.REST, base_url="http://localhost:8080")

        # gRPC
        client = ObjectStoreClient(protocol=Protocol.GRPC, host="localhost", port=50051)

        # QUIC/HTTP3
        client = ObjectStoreClient(protocol=Protocol.QUIC, base_url="https://localhost:4433")

        # Use the client
        with client:
            client.put("my-key", b"my-data")
            data, metadata = client.get("my-key")
            client.delete("my-key")
    """

    def __init__(
        self,
        protocol: Protocol = Protocol.REST,
        base_url: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        api_version: str = "v1",
        timeout: int = 30,
        max_retries: int = 3,
        verify_ssl: bool = False,
    ) -> None:
        """Initialize ObjectStore client.

        Args:
            protocol: Protocol to use (REST, gRPC, or QUIC)
            base_url: Base URL for REST/QUIC (e.g., "http://localhost:8080")
            host: Hostname for gRPC (e.g., "localhost")
            port: Port for gRPC (e.g., 50051)
            api_version: API version for REST/QUIC
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            verify_ssl: Whether to verify SSL certificates (QUIC only)

        Raises:
            ValidationError: If configuration is invalid
        """
        self.protocol = protocol
        self.timeout = timeout
        self.max_retries = max_retries
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None

        if protocol == Protocol.REST:
            if not base_url:
                base_url = "http://localhost:8080"
            self._client = RestClient(
                base_url=base_url,
                api_version=api_version,
                timeout=timeout,
                max_retries=max_retries,
            )
        elif protocol == Protocol.GRPC:
            if not host:
                host = "localhost"
            if not port:
                port = 50051
            self._client = GrpcClient(
                host=host, port=port, timeout=timeout, max_retries=max_retries
            )
        elif protocol == Protocol.QUIC:
            if not base_url:
                base_url = "https://localhost:4433"
            self._client = QuicClient(
                base_url=base_url,
                api_version=api_version,
                timeout=timeout,
                verify_ssl=verify_ssl,
            )
        else:
            raise ValidationError(f"Unsupported protocol: {protocol}")

    def _run_async(self, coro: Coroutine[Any, Any, T]) -> T:
        """Run an async coroutine in a sync context.

        This method properly manages the event loop for QUIC operations,
        reusing the same loop across calls to avoid overhead and issues.

        Args:
            coro: Coroutine to run

        Returns:
            Result from the coroutine
        """
        # Try to get the running loop
        try:
            loop = asyncio.get_running_loop()
            # If we're in an async context, we can't use run_until_complete
            # Create a new event loop in a thread instead
            import concurrent.futures
            import threading

            result = None
            exception = None

            def run_in_thread():
                nonlocal result, exception
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                try:
                    result = new_loop.run_until_complete(coro)
                except Exception as e:
                    exception = e
                finally:
                    new_loop.close()

            thread = threading.Thread(target=run_in_thread)
            thread.start()
            thread.join()

            if exception:
                raise exception
            return result
        except RuntimeError:
            # No event loop running, we can use asyncio.run
            # But prefer reusing a loop if we have one
            if self._event_loop is None or self._event_loop.is_closed():
                # Use asyncio.run for cleaner loop management
                return asyncio.run(coro)
            else:
                return self._event_loop.run_until_complete(coro)

    def put(
        self,
        key: str,
        data: Union[bytes, BinaryIO],
        metadata: Optional[Metadata] = None,
    ) -> PutResponse:
        """Upload an object.

        Args:
            key: Object key/path
            data: Object data (bytes or file-like object)
            metadata: Optional metadata

        Returns:
            PutResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        if self.protocol == Protocol.QUIC:
            return self._run_async(self._client.put(key, data, metadata))
        return self._client.put(key, data, metadata)

    def get(self, key: str) -> tuple[bytes, Metadata]:
        """Download an object.

        Args:
            key: Object key/path

        Returns:
            Tuple of (data, metadata)

        Raises:
            ObjectStoreError: On failure
        """
        if self.protocol == Protocol.QUIC:
            return self._run_async(self._client.get(key))
        return self._client.get(key)

    def get_stream(self, key: str) -> Iterator[bytes]:
        """Download an object as a stream.

        Args:
            key: Object key/path

        Yields:
            Chunks of object data

        Raises:
            ObjectStoreError: On failure
        """
        if self.protocol == Protocol.QUIC:
            import asyncio

            async def _stream() -> Iterator[bytes]:
                async for chunk in self._client.get_stream(key):
                    yield chunk

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                async_gen = _stream()
                while True:
                    try:
                        yield loop.run_until_complete(async_gen.__anext__())
                    except StopAsyncIteration:
                        break
            finally:
                loop.close()
        else:
            yield from self._client.get_stream(key)

    def delete(self, key: str) -> DeleteResponse:
        """Delete an object.

        Args:
            key: Object key/path

        Returns:
            DeleteResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        if self.protocol == Protocol.QUIC:
            return self._run_async(self._client.delete(key))
        return self._client.delete(key)

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
        if self.protocol == Protocol.QUIC:
            return self._run_async(
                self._client.list(prefix, delimiter, max_results, continue_from)
            )
        return self._client.list(prefix, delimiter, max_results, continue_from)

    def exists(self, key: str) -> ExistsResponse:
        """Check if an object exists.

        Args:
            key: Object key/path

        Returns:
            ExistsResponse indicating existence

        Raises:
            ObjectStoreError: On failure
        """
        if self.protocol == Protocol.QUIC:
            return self._run_async(self._client.exists(key))
        return self._client.exists(key)

    def get_metadata(self, key: str) -> Metadata:
        """Get object metadata.

        Args:
            key: Object key/path

        Returns:
            Object metadata

        Raises:
            ObjectStoreError: On failure
        """
        if self.protocol == Protocol.QUIC:
            return self._run_async(self._client.get_metadata(key))
        return self._client.get_metadata(key)

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
        if self.protocol == Protocol.QUIC:
            return self._run_async(self._client.update_metadata(key, metadata))
        return self._client.update_metadata(key, metadata)

    def health(self) -> HealthResponse:
        """Check server health.

        Returns:
            HealthResponse with server status

        Raises:
            ObjectStoreError: On failure
        """
        if self.protocol == Protocol.QUIC:
            return self._run_async(self._client.health())
        return self._client.health()

    def archive(self, key: str, destination_type: str, settings: dict[str, str]) -> ArchiveResponse:
        """Archive an object to a different storage backend.

        Args:
            key: Object key/path
            destination_type: Destination backend type (e.g., "s3", "gcs", "azure")
            settings: Backend-specific settings

        Returns:
            ArchiveResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        if self.protocol == Protocol.QUIC:
            return self._run_async(self._client.archive(key, destination_type, settings))
        return self._client.archive(key, destination_type, settings)

    def add_policy(self, policy: LifecyclePolicy) -> PolicyResponse:
        """Add a lifecycle policy.

        Args:
            policy: Lifecycle policy to add

        Returns:
            PolicyResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        if self.protocol == Protocol.QUIC:
            return self._run_async(self._client.add_policy(policy))
        return self._client.add_policy(policy)

    def remove_policy(self, policy_id: str) -> PolicyResponse:
        """Remove a lifecycle policy.

        Args:
            policy_id: Policy ID to remove

        Returns:
            PolicyResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        if self.protocol == Protocol.QUIC:
            return self._run_async(self._client.remove_policy(policy_id))
        return self._client.remove_policy(policy_id)

    def get_policies(self, prefix: str = "") -> GetPoliciesResponse:
        """Get lifecycle policies.

        Args:
            prefix: Filter policies by prefix

        Returns:
            GetPoliciesResponse with list of policies

        Raises:
            ObjectStoreError: On failure
        """
        if self.protocol == Protocol.QUIC:
            return self._run_async(self._client.get_policies(prefix))
        return self._client.get_policies(prefix)

    def apply_policies(self) -> ApplyPoliciesResponse:
        """Apply all lifecycle policies.

        Returns:
            ApplyPoliciesResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        if self.protocol == Protocol.QUIC:
            return self._run_async(self._client.apply_policies())
        return self._client.apply_policies()

    def add_replication_policy(self, policy: ReplicationPolicy) -> PolicyResponse:
        """Add a replication policy.

        Args:
            policy: Replication policy to add

        Returns:
            PolicyResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        if self.protocol == Protocol.QUIC:
            return self._run_async(self._client.add_replication_policy(policy))
        return self._client.add_replication_policy(policy)

    def remove_replication_policy(self, policy_id: str) -> PolicyResponse:
        """Remove a replication policy.

        Args:
            policy_id: Policy ID to remove

        Returns:
            PolicyResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        if self.protocol == Protocol.QUIC:
            return self._run_async(self._client.remove_replication_policy(policy_id))
        return self._client.remove_replication_policy(policy_id)

    def get_replication_policies(self) -> GetReplicationPoliciesResponse:
        """Get all replication policies.

        Returns:
            GetReplicationPoliciesResponse with list of policies

        Raises:
            ObjectStoreError: On failure
        """
        if self.protocol == Protocol.QUIC:
            return self._run_async(self._client.get_replication_policies())
        return self._client.get_replication_policies()

    def get_replication_policy(self, policy_id: str) -> ReplicationPolicy:
        """Get a specific replication policy.

        Args:
            policy_id: Policy ID to retrieve

        Returns:
            ReplicationPolicy

        Raises:
            ObjectStoreError: On failure
        """
        if self.protocol == Protocol.QUIC:
            return self._run_async(self._client.get_replication_policy(policy_id))
        return self._client.get_replication_policy(policy_id)

    def trigger_replication(self, opts: TriggerReplicationOptions) -> TriggerReplicationResponse:
        """Trigger replication synchronization.

        Args:
            opts: Trigger options

        Returns:
            TriggerReplicationResponse with sync result

        Raises:
            ObjectStoreError: On failure
        """
        if self.protocol == Protocol.QUIC:
            return self._run_async(self._client.trigger_replication(opts))
        return self._client.trigger_replication(opts)

    def get_replication_status(self, policy_id: str) -> GetReplicationStatusResponse:
        """Get replication status for a policy.

        Args:
            policy_id: Policy ID to get status for

        Returns:
            GetReplicationStatusResponse with status

        Raises:
            ObjectStoreError: On failure
        """
        if self.protocol == Protocol.QUIC:
            return self._run_async(self._client.get_replication_status(policy_id))
        return self._client.get_replication_status(policy_id)

    def close(self) -> None:
        """Close the underlying client connection."""
        if self.protocol == Protocol.QUIC:
            self._run_async(self._client.close())
            # Clean up event loop if we created one
            if self._event_loop and not self._event_loop.is_closed():
                self._event_loop.close()
                self._event_loop = None
        else:
            self._client.close()

    def __enter__(self) -> "ObjectStoreClient":
        """Context manager entry."""
        return self

    def __exit__(self, *args: object) -> None:
        """Context manager exit."""
        self.close()
