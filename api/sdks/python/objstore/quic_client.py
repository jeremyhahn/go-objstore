"""QUIC/HTTP3 client implementation for go-objstore."""

from email.utils import parsedate_to_datetime
from typing import AsyncIterator, BinaryIO, Dict, Optional, Union

import httpx

from objstore._http import build_auth_headers, handle_http_error
from objstore.exceptions import (
    ConnectionError,
    ObjectStoreError,
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


class QuicClient:
    """QUIC/HTTP3 client for go-objstore.

    This client uses HTTP/3 over QUIC for improved performance
    and reduced latency compared to traditional HTTP/2.
    """

    def __init__(
        self,
        base_url: str = "https://localhost:4433",
        api_version: str = "v1",
        timeout: int = 30,
        verify_ssl: bool = True,
        token: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        tenant_id: Optional[str] = None,
    ) -> None:
        """Initialize QUIC client.

        Args:
            base_url: Base URL of the go-objstore server (must be https)
            api_version: API version to use
            timeout: Request timeout in seconds
            verify_ssl: Whether to verify SSL certificates. Defaults to True;
                disable only for testing against self-signed certificates.
            token: Optional bearer token for Authorization header
            headers: Optional dict of additional request headers
            tenant_id: Optional tenant identifier (sent as X-Tenant-ID)
        """
        self.base_url = base_url.rstrip("/")
        self.api_version = api_version
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self.token = token
        self.extra_headers = headers or {}
        self.tenant_id = tenant_id

        default_headers = build_auth_headers(token, tenant_id, self.extra_headers)

        # Create HTTP/3 client
        try:
            # Try to enable HTTP/3, fall back to HTTP/2 if not available
            self.client = httpx.AsyncClient(
                http2=True,  # Enable HTTP/2 as fallback
                verify=verify_ssl,
                timeout=timeout,
                headers=default_headers,
            )
        except Exception:
            # If HTTP/3 not available, use HTTP/2
            self.client = httpx.AsyncClient(
                http2=True,
                verify=verify_ssl,
                timeout=timeout,
                headers=default_headers,
            )

    def _url(self, path: str) -> str:
        """Construct full URL from path.

        The QUIC server serves RESTful bare paths with no ``/api/v1`` prefix,
        so the path is appended directly to the base URL.

        Args:
            path: API path

        Returns:
            Full URL
        """
        path = path.lstrip("/")
        return f"{self.base_url}/{path}"

    def _handle_error(self, response: httpx.Response) -> None:
        """Handle HTTP error responses.

        Args:
            response: HTTP response

        Raises:
            ObjectStoreError: For various error conditions
        """
        handle_http_error(response)

    def _metadata_from_headers(self, headers: httpx.Headers) -> Metadata:
        """Build a Metadata object from QUIC response headers.

        Args:
            headers: HTTP response headers

        Returns:
            Parsed metadata, with custom entries from ``X-Meta-*`` headers
        """
        custom: Dict[str, str] = {}
        for name, value in headers.items():
            if name.lower().startswith("x-meta-"):
                custom[name[len("x-meta-"):]] = value

        size: Optional[int] = None
        content_length = headers.get("Content-Length")
        if content_length is not None:
            try:
                size = int(content_length)
            except ValueError:
                size = None

        last_modified = None
        lm_header = headers.get("Last-Modified")
        if lm_header:
            try:
                last_modified = parsedate_to_datetime(lm_header)
            except (TypeError, ValueError):
                last_modified = None

        return Metadata(
            content_type=headers.get("Content-Type"),
            content_encoding=headers.get("Content-Encoding"),
            size=size,
            last_modified=last_modified,
            etag=headers.get("ETag"),
            custom=custom,
        )

    @staticmethod
    def _go_duration_to_ms(value: object) -> int:
        """Convert a Go duration string (e.g. "1.5s", "250ms") to milliseconds.

        Accepts ints/floats (treated as milliseconds) for forward compatibility.

        Args:
            value: Duration as a Go-formatted string or a number

        Returns:
            Duration in milliseconds (0 if it cannot be parsed)
        """
        if value is None:
            return 0
        if isinstance(value, (int, float)):
            return int(value)
        if not isinstance(value, str):
            return 0

        text = value.strip()
        if not text:
            return 0

        units = (
            ("ns", 1e-6),
            ("us", 1e-3),
            ("µs", 1e-3),
            ("ms", 1.0),
            ("s", 1000.0),
            ("m", 60_000.0),
            ("h", 3_600_000.0),
        )

        total_ms = 0.0
        i = 0
        matched = False
        while i < len(text):
            j = i
            while j < len(text) and (text[j].isdigit() or text[j] in ".+-"):
                j += 1
            number = text[i:j]
            k = j
            while k < len(text) and not (text[k].isdigit() or text[k] in ".+-"):
                k += 1
            unit = text[j:k]
            try:
                magnitude = float(number)
            except ValueError:
                return 0
            factor = next((f for u, f in units if u == unit), None)
            if factor is None:
                return 0
            total_ms += magnitude * factor
            matched = True
            i = k

        return int(round(total_ms)) if matched else 0

    def _sync_result_from_dict(self, raw: Dict[str, object]) -> SyncResult:
        """Build a SyncResult from a QUIC trigger response result dict.

        The QUIC server returns ``duration`` as a Go duration string; the model
        field is ``duration_ms``.

        Args:
            raw: Result fields as returned by the QUIC server

        Returns:
            Parsed sync result
        """
        data = dict(raw)
        if "duration_ms" not in data:
            data["duration_ms"] = self._go_duration_to_ms(data.pop("duration", None))
        else:
            data.pop("duration", None)
        return SyncResult(**data)

    def _replication_status_from_dict(self, raw: Dict[str, object]) -> ReplicationStatus:
        """Build a ReplicationStatus from a QUIC status dict.

        The QUIC server returns ``average_sync_duration`` as a Go duration
        string; the model field is ``average_sync_duration_ms``.

        Args:
            raw: Status fields as returned by the QUIC server

        Returns:
            Parsed replication status
        """
        data = dict(raw)
        if "average_sync_duration_ms" not in data:
            data["average_sync_duration_ms"] = self._go_duration_to_ms(
                data.pop("average_sync_duration", None)
            )
        else:
            data.pop("average_sync_duration", None)
        return ReplicationStatus(**data)

    def _replication_policy_from_dict(self, raw: Dict[str, object]) -> ReplicationPolicy:
        """Build a ReplicationPolicy from a QUIC policy dict.

        The QUIC server uses ``check_interval`` (seconds) where the model field
        is ``check_interval_seconds``.

        Args:
            raw: Policy fields as returned by the QUIC server

        Returns:
            Parsed replication policy
        """
        data = dict(raw)
        if "check_interval" in data and "check_interval_seconds" not in data:
            data["check_interval_seconds"] = data.pop("check_interval")
        return ReplicationPolicy(**data)

    async def put(
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
        url = self._url(f"objects/{key}")

        try:
            if isinstance(data, bytes):
                body_data = data
            else:
                body_data = data.read()

            headers: Dict[str, str] = {}
            if metadata:
                if metadata.content_type:
                    headers["Content-Type"] = metadata.content_type
                if metadata.content_encoding:
                    headers["Content-Encoding"] = metadata.content_encoding
                for ck, cv in (metadata.custom or {}).items():
                    headers[f"X-Meta-{ck}"] = cv

            response = await self.client.put(url, content=body_data, headers=headers)

            if response.status_code == 201:
                result = response.json()
                return PutResponse(
                    success=True,
                    message=result.get("message", "Object uploaded successfully"),
                    etag=response.headers.get("ETag"),
                )

            self._handle_error(response)
            return PutResponse(success=False, message="Upload failed")

        except httpx.TimeoutException:
            raise TimeoutError("Request timed out")
        except httpx.ConnectError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

    async def get(self, key: str) -> tuple[bytes, Metadata]:
        """Download an object.

        Args:
            key: Object key/path

        Returns:
            Tuple of (data, metadata)

        Raises:
            ObjectStoreError: On failure
        """
        url = self._url(f"objects/{key}")

        try:
            response = await self.client.get(url)

            if response.status_code == 200:
                metadata = self._metadata_from_headers(response.headers)
                return response.content, metadata

            self._handle_error(response)
            return b"", Metadata()

        except httpx.TimeoutException:
            raise TimeoutError("Request timed out")
        except httpx.ConnectError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

    async def get_stream(self, key: str) -> AsyncIterator[bytes]:
        """Download an object as a stream.

        Args:
            key: Object key/path

        Yields:
            Chunks of object data

        Raises:
            ObjectStoreError: On failure
        """
        url = self._url(f"objects/{key}")

        try:
            async with self.client.stream("GET", url) as response:
                if response.status_code == 200:
                    async for chunk in response.aiter_bytes(chunk_size=8192):
                        if chunk:
                            yield chunk
                else:
                    self._handle_error(response)

        except httpx.TimeoutException:
            raise TimeoutError("Request timed out")
        except httpx.ConnectError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

    async def put_stream(
        self,
        key: str,
        data: Union[bytes, BinaryIO],
        metadata: Optional[Metadata] = None,
    ) -> "PutResponse":
        """Upload an object from a stream or file-like object.

        Streams the content directly via httpx without buffering the full
        payload in memory.

        Args:
            key: Object key/path
            data: Byte stream or file-like object to upload
            metadata: Optional metadata

        Returns:
            PutResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        url = self._url(f"objects/{key}")

        try:
            headers: Dict[str, str] = {}
            if metadata:
                if metadata.content_type:
                    headers["Content-Type"] = metadata.content_type
                if metadata.content_encoding:
                    headers["Content-Encoding"] = metadata.content_encoding
                for ck, cv in (metadata.custom or {}).items():
                    headers[f"X-Meta-{ck}"] = cv

            if isinstance(data, bytes):
                content = data
            else:
                content = data.read()

            response = await self.client.put(url, content=content, headers=headers)

            if response.status_code == 201:
                result = response.json()
                from objstore.models import PutResponse
                return PutResponse(
                    success=True,
                    message=result.get("message", "Object uploaded successfully"),
                    etag=response.headers.get("ETag"),
                )

            self._handle_error(response)
            from objstore.models import PutResponse
            return PutResponse(success=False, message="Upload failed")

        except httpx.TimeoutException:
            raise TimeoutError("Request timed out")
        except httpx.ConnectError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

    async def delete(self, key: str) -> DeleteResponse:
        """Delete an object.

        Args:
            key: Object key/path

        Returns:
            DeleteResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        url = self._url(f"objects/{key}")

        try:
            response = await self.client.delete(url)

            if response.status_code in (200, 204):
                return DeleteResponse(
                    success=True, message="Object deleted successfully"
                )

            self._handle_error(response)
            return DeleteResponse(success=False, message="Delete failed")

        except httpx.TimeoutException:
            raise TimeoutError("Request timed out")
        except httpx.ConnectError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

    async def list(
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
        url = self._url("objects")
        params: Dict[str, Union[str, int]] = {"max": max_results}

        if prefix:
            params["prefix"] = prefix
        if delimiter:
            params["delimiter"] = delimiter
        if continue_from:
            params["continue"] = continue_from

        try:
            response = await self.client.get(url, params=params)

            if response.status_code == 200:
                data = response.json()
                objects = [
                    ObjectInfo(
                        key=obj["key"],
                        metadata=Metadata(
                            size=obj.get("size"),
                            etag=obj.get("etag"),
                            custom=obj.get("metadata", {}),
                        ),
                    )
                    for obj in data.get("objects", [])
                ]
                return ListResponse(
                    objects=objects,
                    common_prefixes=data.get("common_prefixes", []),
                    next_token=data.get("next_token"),
                    truncated=data.get("truncated", False),
                )

            self._handle_error(response)
            return ListResponse()

        except httpx.TimeoutException:
            raise TimeoutError("Request timed out")
        except httpx.ConnectError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

    async def exists(self, key: str) -> ExistsResponse:
        """Check if an object exists.

        Args:
            key: Object key/path

        Returns:
            ExistsResponse indicating existence

        Raises:
            ObjectStoreError: On failure
        """
        url = self._url(f"objects/{key}")

        try:
            response = await self.client.get(url, params={"exists": "1"})

            if response.status_code == 200:
                try:
                    data = response.json()
                    return ExistsResponse(exists=bool(data.get("exists", True)))
                except Exception:
                    return ExistsResponse(exists=True)

            if response.status_code == 404:
                return ExistsResponse(exists=False)

            self._handle_error(response)
            return ExistsResponse(exists=False)

        except httpx.TimeoutException:
            raise TimeoutError("Request timed out")
        except httpx.ConnectError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

    async def get_metadata(self, key: str) -> Metadata:
        """Get object metadata.

        Args:
            key: Object key/path

        Returns:
            Object metadata

        Raises:
            ObjectStoreError: On failure
        """
        url = self._url(f"objects/{key}")

        try:
            response = await self.client.head(url)

            if response.status_code == 200:
                return self._metadata_from_headers(response.headers)

            self._handle_error(response)
            return Metadata()

        except httpx.TimeoutException:
            raise TimeoutError("Request timed out")
        except httpx.ConnectError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

    async def update_metadata(self, key: str, metadata: Metadata) -> PolicyResponse:
        """Update object metadata.

        Args:
            key: Object key/path
            metadata: New metadata

        Returns:
            PolicyResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        url = self._url(f"objects/{key}")

        payload: Dict[str, object] = {}
        if metadata.content_type is not None:
            payload["content_type"] = metadata.content_type
        if metadata.content_encoding is not None:
            payload["content_encoding"] = metadata.content_encoding
        if metadata.custom:
            payload["custom"] = metadata.custom

        try:
            response = await self.client.patch(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
            )

            if response.status_code == 200:
                result = response.json()
                return PolicyResponse(
                    success=True, message=result.get("message", "Metadata updated successfully")
                )

            self._handle_error(response)
            return PolicyResponse(success=False, message="Update failed")

        except httpx.TimeoutException:
            raise TimeoutError("Request timed out")
        except httpx.ConnectError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

    async def health(self) -> HealthResponse:
        """Check server health.

        Returns:
            HealthResponse with server status

        Raises:
            ObjectStoreError: On failure
        """
        url = self._url("health")

        try:
            response = await self.client.get(url)

            if response.status_code == 200:
                data = response.json()
                status_str = data.get("status", "UNKNOWN").upper()
                try:
                    status = HealthStatus(status_str)
                except ValueError:
                    status = HealthStatus.UNKNOWN

                return HealthResponse(status=status, message=data.get("message"))

            self._handle_error(response)
            return HealthResponse(status=HealthStatus.UNKNOWN)

        except httpx.TimeoutException:
            raise TimeoutError("Request timed out")
        except httpx.ConnectError as e:
            raise ConnectionError(f"Connection failed: All connection attempts failed. {str(e)}")
        except ObjectStoreError:
            # Errors raised by _handle_error (e.g. ServerError) must propagate
            # unchanged rather than being masked as a connection failure.
            raise
        except Exception as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

    async def archive(
        self, key: str, destination_type: str, settings: Dict[str, str]
    ) -> ArchiveResponse:
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
        url = self._url("archive")

        try:
            request_data = {
                "key": key,
                "destination_type": destination_type,
                "destination_settings": settings,
            }

            response = await self.client.post(url, json=request_data)

            if response.status_code == 200:
                data = response.json()
                return ArchiveResponse(
                    success=True, message=data.get("message", "Object archived successfully")
                )

            self._handle_error(response)
            return ArchiveResponse(success=False, message="Archive failed")

        except httpx.TimeoutException:
            raise TimeoutError("Request timed out")
        except httpx.ConnectError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

    async def add_policy(self, policy: LifecyclePolicy) -> PolicyResponse:
        """Add a lifecycle policy.

        Args:
            policy: Lifecycle policy to add

        Returns:
            PolicyResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        url = self._url("policies")

        try:
            response = await self.client.post(url, json=policy.model_dump(exclude_none=True))

            if response.status_code in (200, 201):
                data = response.json()
                return PolicyResponse(
                    success=True, message=data.get("message", "Policy added successfully")
                )

            self._handle_error(response)
            return PolicyResponse(success=False, message="Add policy failed")

        except httpx.TimeoutException:
            raise TimeoutError("Request timed out")
        except httpx.ConnectError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

    async def remove_policy(self, policy_id: str) -> PolicyResponse:
        """Remove a lifecycle policy.

        Args:
            policy_id: Policy ID to remove

        Returns:
            PolicyResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        url = self._url(f"policies/{policy_id}")

        try:
            response = await self.client.delete(url)

            if response.status_code == 200:
                data = response.json()
                return PolicyResponse(
                    success=True, message=data.get("message", "Policy removed successfully")
                )

            self._handle_error(response)
            return PolicyResponse(success=False, message="Remove policy failed")

        except httpx.TimeoutException:
            raise TimeoutError("Request timed out")
        except httpx.ConnectError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

    async def get_policies(self, prefix: str = "") -> GetPoliciesResponse:
        """Get lifecycle policies.

        Args:
            prefix: Filter policies by prefix

        Returns:
            GetPoliciesResponse with list of policies

        Raises:
            ObjectStoreError: On failure
        """
        url = self._url("policies")
        params = {}
        if prefix:
            params["prefix"] = prefix

        try:
            response = await self.client.get(url, params=params)

            if response.status_code == 200:
                data = response.json()
                policies = [
                    LifecyclePolicy(**policy_data) for policy_data in data.get("policies", [])
                ]
                return GetPoliciesResponse(
                    policies=policies,
                    success=True,
                    message=data.get("message", "Policies retrieved successfully"),
                )

            self._handle_error(response)
            return GetPoliciesResponse(policies=[], success=False, message="Get policies failed")

        except httpx.TimeoutException:
            raise TimeoutError("Request timed out")
        except httpx.ConnectError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

    async def apply_policies(self) -> ApplyPoliciesResponse:
        """Apply all lifecycle policies.

        Returns:
            ApplyPoliciesResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        url = self._url("policies/apply")

        try:
            response = await self.client.post(url)

            if response.status_code == 200:
                data = response.json()
                return ApplyPoliciesResponse(
                    success=True,
                    policies_count=data.get("policies_count", 0),
                    objects_processed=data.get("objects_processed", 0),
                    message=data.get("message", "Policies applied successfully"),
                )

            self._handle_error(response)
            return ApplyPoliciesResponse(
                success=False, policies_count=0, objects_processed=0, message="Apply policies failed"
            )

        except httpx.TimeoutException:
            raise TimeoutError("Request timed out")
        except httpx.ConnectError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

    async def add_replication_policy(self, policy: ReplicationPolicy) -> PolicyResponse:
        """Add a replication policy.

        Args:
            policy: Replication policy to add

        Returns:
            PolicyResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        url = self._url("replication/policies")

        try:
            payload = policy.model_dump(exclude_none=True)
            # QUIC server expects `check_interval` (seconds), not `check_interval_seconds`.
            if "check_interval_seconds" in payload:
                payload["check_interval"] = payload.pop("check_interval_seconds")

            response = await self.client.post(url, json=payload)

            if response.status_code in (200, 201):
                data = response.json()
                return PolicyResponse(
                    success=True,
                    message=data.get("message", "Replication policy added successfully"),
                )

            self._handle_error(response)
            return PolicyResponse(success=False, message="Add replication policy failed")

        except httpx.TimeoutException:
            raise TimeoutError("Request timed out")
        except httpx.ConnectError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

    async def remove_replication_policy(self, policy_id: str) -> PolicyResponse:
        """Remove a replication policy.

        Args:
            policy_id: Policy ID to remove

        Returns:
            PolicyResponse with operation result

        Raises:
            ObjectStoreError: On failure
        """
        url = self._url(f"replication/policies/{policy_id}")

        try:
            response = await self.client.delete(url)

            if response.status_code == 200:
                data = response.json()
                return PolicyResponse(
                    success=True,
                    message=data.get("message", "Replication policy removed successfully"),
                )

            self._handle_error(response)
            return PolicyResponse(success=False, message="Remove replication policy failed")

        except httpx.TimeoutException:
            raise TimeoutError("Request timed out")
        except httpx.ConnectError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

    async def get_replication_policies(self) -> GetReplicationPoliciesResponse:
        """Get all replication policies.

        Returns:
            GetReplicationPoliciesResponse with list of policies

        Raises:
            ObjectStoreError: On failure
        """
        url = self._url("replication/policies")

        try:
            response = await self.client.get(url)

            if response.status_code == 200:
                data = response.json()
                policies = [
                    self._replication_policy_from_dict(policy_data)
                    for policy_data in data.get("policies", [])
                ]
                return GetReplicationPoliciesResponse(policies=policies)

            self._handle_error(response)
            return GetReplicationPoliciesResponse(policies=[])

        except httpx.TimeoutException:
            raise TimeoutError("Request timed out")
        except httpx.ConnectError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

    async def get_replication_policy(self, policy_id: str) -> ReplicationPolicy:
        """Get a specific replication policy.

        Args:
            policy_id: Policy ID to retrieve

        Returns:
            ReplicationPolicy

        Raises:
            ObjectStoreError: On failure
        """
        url = self._url(f"replication/policies/{policy_id}")

        try:
            response = await self.client.get(url)

            if response.status_code == 200:
                data = response.json()
                # QUIC returns the policy fields flat at the top level (with a
                # `success` wrapper key), not nested under `policy`.
                policy_data = {k: v for k, v in data.items() if k != "success"}
                return self._replication_policy_from_dict(policy_data)

            self._handle_error(response)
            return ReplicationPolicy(
                id="", source_backend="", destination_backend="", check_interval_seconds=0
            )

        except httpx.TimeoutException:
            raise TimeoutError("Request timed out")
        except httpx.ConnectError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

    async def trigger_replication(
        self, opts: TriggerReplicationOptions
    ) -> TriggerReplicationResponse:
        """Trigger replication synchronization.

        Args:
            opts: Trigger options

        Returns:
            TriggerReplicationResponse with sync result

        Raises:
            ObjectStoreError: On failure
        """
        url = self._url("replication/trigger")
        params: Dict[str, str] = {}
        if opts.policy_id:
            params["policy_id"] = opts.policy_id

        try:
            response = await self.client.post(url, params=params)

            if response.status_code == 200:
                data = response.json()
                result_data = data.get("result")
                sync_result = self._sync_result_from_dict(result_data) if result_data else None
                return TriggerReplicationResponse(
                    success=True,
                    result=sync_result,
                    message=data.get("message", "Replication triggered successfully"),
                )

            self._handle_error(response)
            return TriggerReplicationResponse(
                success=False, result=None, message="Trigger replication failed"
            )

        except httpx.TimeoutException:
            raise TimeoutError("Request timed out")
        except httpx.ConnectError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

    async def get_replication_status(self, policy_id: str) -> GetReplicationStatusResponse:
        """Get replication status for a policy.

        Args:
            policy_id: Policy ID to get status for

        Returns:
            GetReplicationStatusResponse with status

        Raises:
            ObjectStoreError: On failure
        """
        url = self._url(f"replication/status/{policy_id}")

        try:
            response = await self.client.get(url)

            if response.status_code == 200:
                data = response.json()
                # QUIC returns the status fields flat at the top level (with a
                # `success` wrapper key), not nested under `status`.
                status_data = {
                    k: v for k, v in data.items() if k not in ("success", "message")
                }
                status = (
                    self._replication_status_from_dict(status_data) if status_data else None
                )
                return GetReplicationStatusResponse(
                    success=True,
                    status=status,
                    message=data.get("message", "Status retrieved successfully"),
                )

            self._handle_error(response)
            return GetReplicationStatusResponse(
                success=False, status=None, message="Get replication status failed"
            )

        except httpx.TimeoutException:
            raise TimeoutError("Request timed out")
        except httpx.ConnectError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

    async def close(self) -> None:
        """Close the HTTP client."""
        await self.client.aclose()

    async def __aenter__(self) -> "QuicClient":
        """Async context manager entry."""
        return self

    async def __aexit__(self, *args: object) -> None:
        """Async context manager exit."""
        await self.close()
