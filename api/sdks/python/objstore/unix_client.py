"""Unix domain socket client implementation for go-objstore.

Wire protocol: newline-delimited JSON-RPC 2.0 over AF_UNIX stream sockets.
Auth: OS peer credentials (server-side only); the client simply connects.
Binary data is base64-encoded in the ``data`` field of put/get params/results.
"""

import base64
import binascii
import json
import socket
import threading
from typing import BinaryIO, Dict, Iterator, List, Optional, Union

from objstore._jsonrpc import raise_rpc_error
from objstore.exceptions import (
    ConnectionError,
    ServerError,
    TimeoutError,
)
from objstore.models import (
    ApplyPoliciesResponse,
    ArchiveResponse,
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

_RECV_CHUNK = 4096


class UnixClient:
    """Unix domain socket JSON-RPC 2.0 client for go-objstore.

    Maintains a single persistent connection to the Unix domain socket and
    sends newline-delimited JSON-RPC 2.0 requests over it. The server keeps
    connections open between requests but closes idle ones after ~30 seconds;
    the client transparently reconnects on the next call after the connection
    drops. Binary data is base64-encoded on the wire. Authentication is
    handled server-side via OS peer credentials; no token is required from
    the client.
    """

    def __init__(
        self,
        socket_path: str = "/tmp/objstore.sock",
        timeout: int = 30,
    ) -> None:
        """Initialize Unix socket client.

        Args:
            socket_path: Path to the Unix domain socket
            timeout: Socket timeout in seconds
        """
        self.socket_path = socket_path
        self.timeout = timeout
        self._id_counter: int = 0
        self._sock: Optional[socket.socket] = None
        self._recv_buf: bytes = b""
        self._lock = threading.Lock()

    def _next_id(self) -> int:
        """Return a monotonically increasing request ID.

        Returns:
            Next request ID
        """
        self._id_counter += 1
        return self._id_counter

    def _connect(self) -> socket.socket:
        """Create and connect a new socket to the configured path.

        Returns:
            Connected socket

        Raises:
            ConnectionError: If the socket cannot be connected
        """
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        try:
            sock.connect(self.socket_path)
        except OSError as exc:
            sock.close()
            raise ConnectionError(f"Cannot connect to Unix socket {self.socket_path!r}: {exc}")
        return sock

    def _close_locked(self) -> None:
        """Close the current socket and discard buffered data.

        Caller must hold ``self._lock``.
        """
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None
        self._recv_buf = b""

    def _read_line_locked(self) -> bytes:
        """Read one newline-terminated response line from the socket.

        Caller must hold ``self._lock``. Bytes received past the newline are
        retained for the next response.

        Returns:
            The response line without its trailing newline

        Raises:
            ConnectionError: If the server closes the connection mid-read
        """
        while b"\n" not in self._recv_buf:
            chunk = self._sock.recv(_RECV_CHUNK)
            if not chunk:
                raise ConnectionError("Connection closed by server")
            self._recv_buf += chunk
        line, self._recv_buf = self._recv_buf.split(b"\n", 1)
        return line

    def _send_request(self, method: str, params: Dict) -> object:
        """Send a JSON-RPC 2.0 request and return the result.

        Reuses a single persistent connection, connecting lazily on first
        use; the server keeps connections open between requests and closes
        idle ones after ~30 seconds. A lock serializes request/response
        pairs so the client is safe to share across threads. On a socket
        error or response ID mismatch the connection is closed and a fresh
        one is established on the next call.

        Args:
            method: JSON-RPC method name
            params: Method parameters dict

        Returns:
            The ``result`` field from the JSON-RPC response

        Raises:
            ObjectStoreError: On any error condition
        """
        with self._lock:
            request = {
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
                "id": self._next_id(),
            }
            raw = json.dumps(request) + "\n"

            if self._sock is None:
                self._sock = self._connect()

            try:
                self._sock.sendall(raw.encode("utf-8"))
                line = self._read_line_locked()
            except socket.timeout:
                self._close_locked()
                raise TimeoutError("Request timed out")
            except ConnectionError:
                self._close_locked()
                raise
            except OSError as exc:
                self._close_locked()
                raise ConnectionError(f"Socket error: {exc}")

            try:
                response = json.loads(line.decode("utf-8"))
            except (ValueError, UnicodeDecodeError) as exc:
                self._close_locked()
                raise ServerError(f"Invalid JSON response: {exc}")

            if response.get("id") != request["id"]:
                self._close_locked()
                raise ServerError(
                    f"JSON-RPC response id {response.get('id')!r} does not "
                    f"match request id {request['id']}"
                )

        if "error" in response and response["error"] is not None:
            raise_rpc_error(response["error"])

        return response.get("result")

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

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
        if isinstance(data, bytes):
            body = data
        else:
            body = data.read()

        params: Dict = {
            "key": key,
            "data": base64.b64encode(body).decode("ascii"),
        }
        if metadata:
            meta_params: Dict = {}
            if metadata.content_type:
                meta_params["content_type"] = metadata.content_type
            if metadata.content_encoding:
                meta_params["content_encoding"] = metadata.content_encoding
            if metadata.custom:
                meta_params["custom"] = metadata.custom
            if meta_params:
                params["metadata"] = meta_params

        self._send_request("put", params)
        return PutResponse(success=True, message="Object uploaded successfully")

    def put_stream(
        self,
        key: str,
        data: Union[Iterator[bytes], BinaryIO],
        metadata: Optional[Metadata] = None,
    ) -> PutResponse:
        """Upload an object from a stream or iterator.

        Buffers the full stream in memory then sends a single put request,
        because the Unix JSON-RPC protocol does not support chunked transfer.

        Args:
            key: Object key/path
            data: Iterable of byte chunks or file-like object
            metadata: Optional metadata

        Returns:
            PutResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        if isinstance(data, bytes):
            body = data
        elif hasattr(data, "read"):
            body = data.read()
        else:
            chunks: List[bytes] = []
            for chunk in data:
                chunks.append(chunk)
            body = b"".join(chunks)
        return self.put(key, body, metadata)

    def get(self, key: str) -> tuple[bytes, Metadata]:
        """Download an object.

        Args:
            key: Object key/path

        Returns:
            Tuple of (data, metadata)

        Raises:
            ObjectStoreError: On failure
        """
        result = self._send_request("get", {"key": key})

        raw_data = result.get("data", "") if result else ""
        if not raw_data:
            body = b""
        else:
            # Object data is base64-encoded on the unix transport; anything
            # else is a protocol violation and must surface as an error.
            try:
                body = base64.b64decode(raw_data, validate=True)
            except (binascii.Error, ValueError) as exc:
                raise ServerError(f"Invalid base64 data in response: {exc}")

        meta_raw = result.get("metadata") if result else None
        metadata = Metadata()
        if isinstance(meta_raw, dict):
            metadata = Metadata(
                content_type=meta_raw.get("content_type"),
                content_encoding=meta_raw.get("content_encoding"),
                custom=meta_raw.get("custom") or {},
            )

        return body, metadata

    def get_stream(self, key: str) -> Iterator[bytes]:
        """Download an object as a single chunk stream.

        The Unix protocol returns the full object in one response. This
        method wraps it in an iterator for API compatibility.

        Args:
            key: Object key/path

        Yields:
            Object data as one chunk

        Raises:
            ObjectStoreError: On failure
        """
        body, _ = self.get(key)
        if body:
            yield body

    def delete(self, key: str) -> DeleteResponse:
        """Delete an object.

        Args:
            key: Object key/path

        Returns:
            DeleteResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        self._send_request("delete", {"key": key})
        return DeleteResponse(success=True, message="Object deleted successfully")

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
        params: Dict = {"max_results": max_results}
        if prefix:
            params["prefix"] = prefix
        if delimiter:
            params["delimiter"] = delimiter
        if continue_from:
            params["continue_from"] = continue_from

        result = self._send_request("list", params)

        objects: List[ObjectInfo] = []
        if result and "objects" in result:
            for obj in result["objects"]:
                objects.append(
                    ObjectInfo(
                        key=obj["key"],
                        metadata=Metadata(
                            size=obj.get("size"),
                            etag=obj.get("etag"),
                        ),
                    )
                )

        next_cursor = result.get("next_cursor") if result else None
        is_truncated = result.get("is_truncated", False) if result else False

        return ListResponse(
            objects=objects,
            next_token=next_cursor or None,
            truncated=is_truncated,
        )

    def exists(self, key: str) -> ExistsResponse:
        """Check if an object exists.

        Args:
            key: Object key/path

        Returns:
            ExistsResponse indicating existence

        Raises:
            ObjectStoreError: On failure
        """
        result = self._send_request("exists", {"key": key})
        exists_val = result.get("exists", False) if result else False
        return ExistsResponse(exists=bool(exists_val))

    def get_metadata(self, key: str) -> Metadata:
        """Get object metadata.

        Args:
            key: Object key/path

        Returns:
            Object metadata

        Raises:
            ObjectStoreError: On failure
        """
        result = self._send_request("get_metadata", {"key": key})
        if not result:
            return Metadata()

        meta_raw = result.get("metadata") if isinstance(result, dict) else None
        if isinstance(meta_raw, dict):
            return Metadata(
                content_type=meta_raw.get("content_type"),
                content_encoding=meta_raw.get("content_encoding"),
                custom=meta_raw.get("custom") or {},
            )

        # Server may return metadata fields at top level
        if isinstance(result, dict):
            return Metadata(
                content_type=result.get("content_type"),
                content_encoding=result.get("content_encoding"),
                custom=result.get("custom") or {},
            )

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
        meta_params: Dict = {}
        if metadata.content_type is not None:
            meta_params["content_type"] = metadata.content_type
        if metadata.content_encoding is not None:
            meta_params["content_encoding"] = metadata.content_encoding
        if metadata.custom:
            meta_params["custom"] = metadata.custom

        self._send_request("update_metadata", {"key": key, "metadata": meta_params})
        return PolicyResponse(success=True, message="Metadata updated successfully")

    def health(self) -> HealthResponse:
        """Check server health.

        Returns:
            HealthResponse with server status

        Raises:
            ObjectStoreError: On failure
        """
        result = self._send_request("health", {})
        status_str = "UNKNOWN"
        if result and "status" in result:
            status_str = str(result["status"]).upper()
            # The Unix server returns "healthy" / "unhealthy" rather than
            # SERVING / NOT_SERVING.  Normalise to HealthStatus values.
            if status_str in ("HEALTHY", "OK"):
                status_str = "SERVING"
            elif status_str in ("UNHEALTHY",):
                status_str = "NOT_SERVING"

        try:
            status = HealthStatus(status_str)
        except ValueError:
            status = HealthStatus.UNKNOWN

        return HealthResponse(status=status, message=result.get("version") if result else None)

    def archive(self, key: str, destination_type: str, settings: Dict[str, str]) -> ArchiveResponse:
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
        params: Dict = {
            "key": key,
            "destination_type": destination_type,
            "destination_settings": settings,
        }
        self._send_request("archive", params)
        return ArchiveResponse(success=True, message="Object archived successfully")

    # ------------------------------------------------------------------
    # Lifecycle policies
    # ------------------------------------------------------------------

    def add_policy(self, policy: LifecyclePolicy) -> PolicyResponse:
        """Add a lifecycle policy.

        Args:
            policy: Lifecycle policy to add

        Returns:
            PolicyResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        params: Dict = {
            "id": policy.id,
            "prefix": policy.prefix,
            "action": policy.action,
            "retention_seconds": policy.retention_seconds or 0,
        }
        self._send_request("add_policy", params)
        return PolicyResponse(success=True, message="Policy added successfully")

    def remove_policy(self, policy_id: str) -> PolicyResponse:
        """Remove a lifecycle policy.

        Args:
            policy_id: Policy ID to remove

        Returns:
            PolicyResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        self._send_request("remove_policy", {"id": policy_id})
        return PolicyResponse(success=True, message="Policy removed successfully")

    def get_policies(self, prefix: str = "") -> GetPoliciesResponse:
        """Get lifecycle policies.

        Args:
            prefix: Filter policies by prefix

        Returns:
            GetPoliciesResponse with list of policies

        Raises:
            ObjectStoreError: On failure
        """
        params: Dict = {}
        if prefix:
            params["prefix"] = prefix
        result = self._send_request("get_policies", params)

        # The unix server returns a bare JSON array; tolerate a dict
        # wrapper with a "policies" key for compatibility.
        items = []
        if result:
            if isinstance(result, list):
                items = result
            elif "policies" in result:
                items = result["policies"]

        policies: List[LifecyclePolicy] = []
        for p in items:
            retention = p.get("retention_seconds")
            if retention is None:
                retention = (p.get("after_days", 0) or 0) * 86400
            policies.append(
                LifecyclePolicy(
                    id=p.get("id", ""),
                    prefix=p.get("prefix", ""),
                    action=p.get("action", "delete"),
                    retention_seconds=retention,
                )
            )

        return GetPoliciesResponse(
            policies=policies,
            success=True,
            message="Policies retrieved successfully",
        )

    def apply_policies(self) -> ApplyPoliciesResponse:
        """Apply all lifecycle policies.

        Returns:
            ApplyPoliciesResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        result = self._send_request("apply_policies", {})
        policies_count = 0
        objects_processed = 0
        if result:
            policies_count = result.get("policies_count", 0)
            objects_processed = result.get("objects_processed", 0)

        return ApplyPoliciesResponse(
            success=True,
            policies_count=policies_count,
            objects_processed=objects_processed,
            message="Policies applied successfully",
        )

    # ------------------------------------------------------------------
    # Replication policies
    # ------------------------------------------------------------------

    def add_replication_policy(self, policy: ReplicationPolicy) -> PolicyResponse:
        """Add a replication policy.

        Args:
            policy: Replication policy to add

        Returns:
            PolicyResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        params: Dict = {
            "id": policy.id,
            "source_prefix": policy.source_prefix or "",
            "destination_type": policy.destination_backend,
            "destination": policy.destination_settings,
            "enabled": policy.enabled,
        }
        self._send_request("add_replication_policy", params)
        return PolicyResponse(success=True, message="Replication policy added successfully")

    def remove_replication_policy(self, policy_id: str) -> PolicyResponse:
        """Remove a replication policy.

        Args:
            policy_id: Policy ID to remove

        Returns:
            PolicyResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        self._send_request("remove_replication_policy", {"id": policy_id})
        return PolicyResponse(success=True, message="Replication policy removed successfully")

    def get_replication_policies(self) -> GetReplicationPoliciesResponse:
        """Get all replication policies.

        Returns:
            GetReplicationPoliciesResponse with list of policies

        Raises:
            ObjectStoreError: On failure
        """
        result = self._send_request("get_replication_policies", {})

        policies: List[ReplicationPolicy] = []
        items = []
        if result:
            if isinstance(result, list):
                items = result
            elif "policies" in result:
                items = result["policies"]

        for p in items:
            policies.append(
                ReplicationPolicy(
                    id=p.get("id", ""),
                    source_backend=p.get("source_backend", p.get("destination_type", "")),
                    destination_backend=p.get("destination_type", p.get("destination_backend", "")),
                    destination_settings=p.get("destination") or p.get("destination_settings") or {},
                    source_prefix=p.get("source_prefix", ""),
                    enabled=p.get("enabled", True),
                )
            )

        return GetReplicationPoliciesResponse(policies=policies)

    def get_replication_policy(self, policy_id: str) -> ReplicationPolicy:
        """Get a specific replication policy.

        Args:
            policy_id: Policy ID to retrieve

        Returns:
            ReplicationPolicy

        Raises:
            ObjectStoreError: On failure
        """
        result = self._send_request("get_replication_policy", {"id": policy_id})
        if not result:
            return ReplicationPolicy(id="", source_backend="", destination_backend="")

        p = result if isinstance(result, dict) else {}
        return ReplicationPolicy(
            id=p.get("id", ""),
            source_backend=p.get("source_backend", p.get("destination_type", "")),
            destination_backend=p.get("destination_type", p.get("destination_backend", "")),
            destination_settings=p.get("destination") or p.get("destination_settings") or {},
            source_prefix=p.get("source_prefix", ""),
            enabled=p.get("enabled", True),
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
        params: Dict = {}
        if opts.policy_id:
            params["id"] = opts.policy_id

        result = self._send_request("trigger_replication", params)

        sync_result = None
        if result:
            r = result if isinstance(result, dict) else {}
            try:
                sync_result = SyncResult(
                    policy_id=opts.policy_id or r.get("policy_id", ""),
                    synced=r.get("objects_synced", 0),
                    deleted=r.get("objects_deleted", 0),
                    failed=r.get("objects_failed", 0),
                    bytes_total=r.get("bytes_transferred", 0),
                    duration_ms=0,
                    errors=r.get("errors") or [],
                )
            except Exception:
                sync_result = None

        return TriggerReplicationResponse(
            success=True,
            result=sync_result,
            message="Replication triggered successfully",
        )

    def get_replication_status(self, policy_id: str) -> GetReplicationStatusResponse:
        """Get replication status for a policy.

        Args:
            policy_id: Policy ID to get status for

        Returns:
            GetReplicationStatusResponse with status

        Raises:
            ObjectStoreError: On failure
        """
        result = self._send_request("get_replication_status", {"id": policy_id})

        rep_status = None
        if result:
            r = result if isinstance(result, dict) else {}
            try:
                rep_status = ReplicationStatus(
                    policy_id=r.get("policy_id", policy_id),
                    source_backend=r.get("source_backend", ""),
                    destination_backend=r.get("destination_backend", ""),
                    enabled=r.get("enabled", True),
                    total_objects_synced=r.get("objects_synced", 0),
                    total_objects_deleted=0,
                    total_bytes_synced=0,
                    total_errors=r.get("objects_failed", 0),
                    last_sync_time=None,
                    average_sync_duration_ms=0,
                    sync_count=0,
                )
            except Exception:
                rep_status = None

        return GetReplicationStatusResponse(
            success=True,
            status=rep_status,
            message="Status retrieved successfully",
        )

    def close(self) -> None:
        """Close the persistent socket connection."""
        with self._lock:
            self._close_locked()

    def __enter__(self) -> "UnixClient":
        """Context manager entry."""
        return self

    def __exit__(self, *args: object) -> None:
        """Context manager exit."""
        self.close()
