"""MCP (Model Context Protocol) client implementation for go-objstore.

Wire protocol: HTTP POST JSON-RPC 2.0 to base URL path "/".
Method: "tools/call", params {"name": "objstore_<op>", "arguments": {...}}.
Result text is in result.content[0].text (JSON string).
Auth: when a token is set the Authorization: Bearer header is added; custom
headers and X-Tenant-ID are also forwarded per SDK convention.
"""

import base64
import binascii
import json
from typing import BinaryIO, Dict, Iterator, List, Optional, Union

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from objstore._http import build_auth_headers, handle_http_error
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


class McpClient:
    """MCP HTTP client for go-objstore.

    Sends JSON-RPC 2.0 requests using the ``tools/call`` method with
    ``objstore_<op>`` tool names as defined in the MCP server's tool registry.
    The server returns results as JSON text in ``result.content[0].text``.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8081",
        timeout: int = 30,
        max_retries: int = 3,
        token: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        tenant_id: Optional[str] = None,
    ) -> None:
        """Initialize MCP client.

        Args:
            base_url: Base URL of the MCP server (e.g., "http://localhost:8081")
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            token: Optional bearer token for Authorization header
            headers: Optional dict of additional request headers
            tenant_id: Optional tenant identifier (sent as X-Tenant-ID)
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.token = token
        self.extra_headers = headers or {}
        self.tenant_id = tenant_id
        self.session = requests.Session()
        self._id_counter: int = 0

    def _next_id(self) -> int:
        """Return a monotonically increasing request ID.

        Returns:
            Next request ID
        """
        self._id_counter += 1
        return self._id_counter

    def _build_headers(self) -> Dict[str, str]:
        """Build the headers for a request.

        Returns:
            Headers dict with auth and custom headers applied
        """
        hdrs: Dict[str, str] = {"Content-Type": "application/json"}
        hdrs.update(build_auth_headers(self.token, self.tenant_id, self.extra_headers))
        return hdrs

    def _call_tool(self, tool_name: str, arguments: Dict) -> Dict:
        """Invoke an MCP tool and return the parsed result dict.

        Args:
            tool_name: Fully-qualified tool name (e.g., ``objstore_put``)
            arguments: Tool argument dict

        Returns:
            Parsed JSON result dict from result.content[0].text

        Raises:
            ObjectStoreError: On any failure
        """
        url = f"{self.base_url}/"
        body = {
            "jsonrpc": "2.0",
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": arguments,
            },
            "id": self._next_id(),
        }

        try:
            response = self.session.post(
                url,
                json=body,
                headers=self._build_headers(),
                timeout=self.timeout,
            )
        except requests.exceptions.Timeout:
            raise TimeoutError("Request timed out")
        except requests.exceptions.ConnectionError as exc:
            raise ConnectionError(f"Connection failed: {exc}")

        if response.status_code != 200:
            handle_http_error(response)

        try:
            rpc_resp = response.json()
        except ValueError as exc:
            raise ServerError(f"Invalid JSON response: {exc}")

        if "error" in rpc_resp and rpc_resp["error"] is not None:
            raise_rpc_error(rpc_resp["error"])

        result = rpc_resp.get("result")
        if result is None:
            raise ServerError("Empty result from MCP server")

        # Extract text from content[0].text
        content = result.get("content") or []
        if not content:
            raise ServerError("No content in MCP result")

        text = content[0].get("text", "")
        try:
            return json.loads(text)
        except (ValueError, TypeError) as exc:
            raise ServerError(f"Could not parse tool result: {exc}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
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

        # Object data travels base64-encoded so the transport is binary-safe.
        args: Dict = {
            "key": key,
            "data": base64.b64encode(body).decode("ascii"),
        }
        if metadata:
            meta: Dict = {}
            if metadata.content_type:
                meta["content_type"] = metadata.content_type
            if metadata.content_encoding:
                meta["content_encoding"] = metadata.content_encoding
            if metadata.custom:
                meta["custom"] = metadata.custom
            if meta:
                args["metadata"] = meta

        result = self._call_tool("objstore_put", args)
        return PutResponse(
            success=result.get("success", True),
            message=result.get("message", "Object uploaded successfully"),
        )

    def put_stream(
        self,
        key: str,
        data: Union[Iterator[bytes], BinaryIO],
        metadata: Optional[Metadata] = None,
    ) -> PutResponse:
        """Upload an object from a stream or iterator.

        Buffers the full stream then issues a single put call, because the
        MCP protocol does not support chunked transfer encoding.

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

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def get(self, key: str) -> tuple[bytes, Metadata]:
        """Download an object.

        Args:
            key: Object key/path

        Returns:
            Tuple of (data, metadata)

        Raises:
            ObjectStoreError: On failure
        """
        result = self._call_tool("objstore_get", {"key": key})
        raw_data = result.get("data", "")
        if not isinstance(raw_data, str) or raw_data == "":
            return b"", Metadata()
        try:
            body = base64.b64decode(raw_data, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ServerError(f"Invalid base64 data in MCP response: {exc}")
        return body, Metadata()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def get_stream(self, key: str) -> Iterator[bytes]:
        """Download an object as a single-chunk stream.

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

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def delete(self, key: str) -> DeleteResponse:
        """Delete an object.

        Args:
            key: Object key/path

        Returns:
            DeleteResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        result = self._call_tool("objstore_delete", {"key": key})
        return DeleteResponse(
            success=result.get("success", True),
            message=result.get("message", "Object deleted successfully"),
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
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
        args: Dict = {"max_results": max_results}
        if prefix:
            args["prefix"] = prefix
        if continue_from:
            args["continue_from"] = continue_from

        result = self._call_tool("objstore_list", args)

        keys: List[str] = result.get("keys") or []
        objects = [ObjectInfo(key=k, metadata=None) for k in keys]

        return ListResponse(
            objects=objects,
            next_token=result.get("next_token"),
            truncated=result.get("truncated", False),
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
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
        result = self._call_tool("objstore_exists", {"key": key})
        return ExistsResponse(exists=bool(result.get("exists", False)))

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def get_metadata(self, key: str) -> Metadata:
        """Get object metadata.

        Args:
            key: Object key/path

        Returns:
            Object metadata

        Raises:
            ObjectStoreError: On failure
        """
        result = self._call_tool("objstore_get_metadata", {"key": key})
        return Metadata(
            content_type=result.get("content_type"),
            content_encoding=result.get("content_encoding"),
            size=result.get("size"),
            etag=result.get("etag"),
            custom=result.get("custom") or {},
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
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
        meta: Dict = {}
        if metadata.content_type is not None:
            meta["content_type"] = metadata.content_type
        if metadata.content_encoding is not None:
            meta["content_encoding"] = metadata.content_encoding
        if metadata.custom:
            meta["custom"] = metadata.custom

        result = self._call_tool("objstore_update_metadata", {"key": key, "metadata": meta})
        return PolicyResponse(
            success=result.get("success", True),
            message=result.get("message", "Metadata updated successfully"),
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def health(self) -> HealthResponse:
        """Check server health.

        Returns:
            HealthResponse with server status

        Raises:
            ObjectStoreError: On failure
        """
        result = self._call_tool("objstore_health", {})
        status_str = str(result.get("status", "UNKNOWN")).upper()
        if status_str in ("HEALTHY", "OK"):
            status_str = "SERVING"
        elif status_str in ("UNHEALTHY",):
            status_str = "NOT_SERVING"
        try:
            status = HealthStatus(status_str)
        except ValueError:
            status = HealthStatus.UNKNOWN

        return HealthResponse(status=status, message=result.get("version"))

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
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
        args: Dict = {
            "key": key,
            "destination_type": destination_type,
            "destination_settings": settings,
        }
        result = self._call_tool("objstore_archive", args)
        return ArchiveResponse(
            success=result.get("success", True),
            message=result.get("message", "Object archived successfully"),
        )

    # ------------------------------------------------------------------
    # Lifecycle policies
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def add_policy(self, policy: LifecyclePolicy) -> PolicyResponse:
        """Add a lifecycle policy.

        Args:
            policy: Lifecycle policy to add

        Returns:
            PolicyResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        args: Dict = {
            "id": policy.id,
            "prefix": policy.prefix,
            "retention_seconds": policy.retention_seconds or 0,
            "action": policy.action,
        }
        if policy.destination_type:
            args["destination_type"] = policy.destination_type
        if policy.destination_settings:
            args["destination_settings"] = policy.destination_settings

        result = self._call_tool("objstore_add_policy", args)
        return PolicyResponse(
            success=result.get("success", True),
            message=result.get("message", "Policy added successfully"),
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def remove_policy(self, policy_id: str) -> PolicyResponse:
        """Remove a lifecycle policy.

        Args:
            policy_id: Policy ID to remove

        Returns:
            PolicyResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        result = self._call_tool("objstore_remove_policy", {"id": policy_id})
        return PolicyResponse(
            success=result.get("success", True),
            message=result.get("message", "Policy removed successfully"),
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def get_policies(self, prefix: str = "") -> GetPoliciesResponse:
        """Get lifecycle policies.

        Args:
            prefix: Filter policies by prefix

        Returns:
            GetPoliciesResponse with list of policies

        Raises:
            ObjectStoreError: On failure
        """
        args: Dict = {}
        if prefix:
            args["prefix"] = prefix

        result = self._call_tool("objstore_get_policies", args)

        policies: List[LifecyclePolicy] = []
        for p in result.get("policies") or []:
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
            success=result.get("success", True),
            message=result.get("message", "Policies retrieved successfully"),
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def apply_policies(self) -> ApplyPoliciesResponse:
        """Apply all lifecycle policies.

        Returns:
            ApplyPoliciesResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        result = self._call_tool("objstore_apply_policies", {})
        return ApplyPoliciesResponse(
            success=result.get("success", True),
            policies_count=result.get("policies_count", 0),
            objects_processed=result.get("objects_processed", 0),
            message=result.get("message", "Policies applied successfully"),
        )

    # ------------------------------------------------------------------
    # Replication policies
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
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
        args: Dict = {
            "id": policy.id,
            "source_backend": policy.source_backend,
            "source_settings": policy.source_settings,
            "source_prefix": policy.source_prefix or "",
            "destination_backend": policy.destination_backend,
            "destination_settings": policy.destination_settings,
            "check_interval": policy.check_interval_seconds,
            "enabled": policy.enabled,
        }
        result = self._call_tool("objstore_add_replication_policy", args)
        return PolicyResponse(
            success=result.get("success", True),
            message=result.get("message", "Replication policy added successfully"),
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def remove_replication_policy(self, policy_id: str) -> PolicyResponse:
        """Remove a replication policy.

        Args:
            policy_id: Policy ID to remove

        Returns:
            PolicyResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        result = self._call_tool("objstore_remove_replication_policy", {"id": policy_id})
        return PolicyResponse(
            success=result.get("success", True),
            message=result.get("message", "Replication policy removed successfully"),
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def get_replication_policies(self) -> GetReplicationPoliciesResponse:
        """Get all replication policies.

        Returns:
            GetReplicationPoliciesResponse with list of policies

        Raises:
            ObjectStoreError: On failure
        """
        result = self._call_tool("objstore_list_replication_policies", {})

        policies: List[ReplicationPolicy] = []
        for p in result.get("policies") or []:
            policies.append(
                ReplicationPolicy(
                    id=p.get("id", ""),
                    source_backend=p.get("source_backend", ""),
                    source_settings=p.get("source_settings") or {},
                    source_prefix=p.get("source_prefix", ""),
                    destination_backend=p.get("destination_backend", ""),
                    destination_settings=p.get("destination_settings") or {},
                    check_interval_seconds=p.get("check_interval", 0),
                    enabled=p.get("enabled", True),
                )
            )

        return GetReplicationPoliciesResponse(policies=policies)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def get_replication_policy(self, policy_id: str) -> ReplicationPolicy:
        """Get a specific replication policy.

        Args:
            policy_id: Policy ID to retrieve

        Returns:
            ReplicationPolicy

        Raises:
            ObjectStoreError: On failure
        """
        result = self._call_tool("objstore_get_replication_policy", {"id": policy_id})
        return ReplicationPolicy(
            id=result.get("id", ""),
            source_backend=result.get("source_backend", ""),
            source_settings=result.get("source_settings") or {},
            source_prefix=result.get("source_prefix", ""),
            destination_backend=result.get("destination_backend", ""),
            destination_settings=result.get("destination_settings") or {},
            check_interval_seconds=result.get("check_interval", 0),
            enabled=result.get("enabled", True),
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
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
        args: Dict = {}
        if opts.policy_id:
            args["policy_id"] = opts.policy_id

        result = self._call_tool("objstore_trigger_replication", args)

        sync_result = None
        r = result.get("result") or {}
        if r:
            try:
                sync_result = SyncResult(
                    policy_id=r.get("policy_id", opts.policy_id or ""),
                    synced=r.get("synced", 0),
                    deleted=r.get("deleted", 0),
                    failed=r.get("failed", 0),
                    bytes_total=r.get("bytes_total", 0),
                    duration=r.get("duration"),
                    duration_ms=r.get("duration_ms"),
                    errors=r.get("errors") or [],
                )
            except Exception:
                sync_result = None

        return TriggerReplicationResponse(
            success=result.get("success", True),
            result=sync_result,
            message=result.get("message", "Replication triggered successfully"),
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
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
        result = self._call_tool("objstore_get_replication_status", {"policy_id": policy_id})

        rep_status = None
        try:
            rep_status = ReplicationStatus(
                policy_id=result.get("policy_id", policy_id),
                source_backend=result.get("source_backend", ""),
                destination_backend=result.get("destination_backend", ""),
                enabled=result.get("enabled", True),
                total_objects_synced=result.get("total_objects_synced", 0),
                total_objects_deleted=result.get("total_objects_deleted", 0),
                total_bytes_synced=result.get("total_bytes_synced", 0),
                total_errors=result.get("total_errors", 0),
                average_sync_duration=result.get("average_sync_duration"),
                average_sync_duration_ms=result.get("average_sync_duration_ms"),
                sync_count=result.get("sync_count", 0),
            )
        except Exception:
            rep_status = None

        return GetReplicationStatusResponse(
            success=result.get("success", True),
            status=rep_status,
            message=result.get("message", "Status retrieved successfully"),
        )

    def close(self) -> None:
        """Close the HTTP session."""
        self.session.close()

    def __enter__(self) -> "McpClient":
        """Context manager entry."""
        return self

    def __exit__(self, *args: object) -> None:
        """Context manager exit."""
        self.close()
