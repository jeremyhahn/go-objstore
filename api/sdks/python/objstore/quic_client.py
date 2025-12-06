"""QUIC/HTTP3 client implementation for go-objstore."""

import json
from io import BytesIO
from typing import AsyncIterator, BinaryIO, Dict, Iterator, Optional, Union

import httpx

from objstore.exceptions import (
    AuthenticationError,
    ConnectionError,
    ObjectNotFoundError,
    ObjectStoreError,
    ServerError,
    TimeoutError,
    ValidationError,
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
        verify_ssl: bool = False,
    ) -> None:
        """Initialize QUIC client.

        Args:
            base_url: Base URL of the go-objstore server (must be https)
            api_version: API version to use
            timeout: Request timeout in seconds
            verify_ssl: Whether to verify SSL certificates
        """
        self.base_url = base_url.rstrip("/")
        self.api_version = api_version
        self.timeout = timeout
        self.verify_ssl = verify_ssl

        # Create HTTP/3 client
        try:
            # Try to enable HTTP/3, fall back to HTTP/2 if not available
            self.client = httpx.AsyncClient(
                http2=True,  # Enable HTTP/2 as fallback
                verify=verify_ssl,
                timeout=timeout,
            )
        except Exception:
            # If HTTP/3 not available, use HTTP/2
            self.client = httpx.AsyncClient(
                http2=True,
                verify=verify_ssl,
                timeout=timeout,
            )

    def _url(self, path: str) -> str:
        """Construct full URL from path.

        Args:
            path: API path

        Returns:
            Full URL
        """
        path = path.lstrip("/")
        if self.api_version and not path.startswith(self.api_version):
            return f"{self.base_url}/api/{self.api_version}/{path}"
        return f"{self.base_url}/{path}"

    def _handle_error(self, response: httpx.Response) -> None:
        """Handle HTTP error responses.

        Args:
            response: HTTP response

        Raises:
            ObjectStoreError: For various error conditions
        """
        if response.status_code == 404:
            raise ObjectNotFoundError("Object not found")
        elif response.status_code == 401:
            raise AuthenticationError("Authentication failed")
        elif response.status_code == 400:
            try:
                error_data = response.json()
                message = error_data.get("message", "Validation error")
            except Exception:
                message = response.text or "Validation error"
            raise ValidationError(message)
        elif response.status_code >= 500:
            try:
                error_data = response.json()
                message = error_data.get("message", "Server error")
            except Exception:
                message = response.text or "Server error"
            raise ServerError(message, status_code=response.status_code)
        else:
            raise ObjectStoreError(
                f"HTTP {response.status_code}: {response.text}",
                status_code=response.status_code,
            )

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
                files = {"file": (key, BytesIO(data))}
            else:
                files = {"file": (key, data)}

            data_dict = {}
            if metadata:
                data_dict["metadata"] = json.dumps(metadata.model_dump(exclude_none=True))

            response = await self.client.put(url, files=files, data=data_dict)

            if response.status_code == 201:
                result = response.json()
                return PutResponse(
                    success=True,
                    message=result.get("message", "Object uploaded successfully"),
                    etag=result.get("data", {}).get("etag"),
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
                metadata = Metadata(
                    content_type=response.headers.get("Content-Type"),
                    size=int(response.headers.get("Content-Length", 0)),
                    etag=response.headers.get("ETag"),
                )
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

            if response.status_code == 200:
                result = response.json()
                return DeleteResponse(
                    success=True, message=result.get("message", "Object deleted successfully")
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
        params: Dict[str, Union[str, int]] = {"limit": max_results}

        if prefix:
            params["prefix"] = prefix
        if delimiter:
            params["delimiter"] = delimiter
        if continue_from:
            params["token"] = continue_from

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
            response = await self.client.head(url)
            return ExistsResponse(exists=response.status_code == 200)

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
        url = self._url(f"objects/{key}/metadata")

        try:
            response = await self.client.get(url)

            if response.status_code == 200:
                data = response.json()
                return Metadata(
                    content_type=data.get("metadata", {}).get("content_type"),
                    content_encoding=data.get("metadata", {}).get("content_encoding"),
                    size=data.get("size"),
                    etag=data.get("etag"),
                    custom=data.get("metadata", {}),
                )

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
        url = self._url(f"objects/{key}/metadata")

        try:
            response = await self.client.put(
                url,
                json=metadata.model_dump(exclude_none=True),
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
            response = await self.client.post(url, json=policy.model_dump(exclude_none=True))

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
                    ReplicationPolicy(**policy_data) for policy_data in data.get("policies", [])
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
                return ReplicationPolicy(**data.get("policy", {}))

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

        try:
            response = await self.client.post(url, json=opts.model_dump(exclude_none=True))

            if response.status_code == 200:
                data = response.json()
                result_data = data.get("result")
                sync_result = SyncResult(**result_data) if result_data else None
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
                status_data = data.get("status")
                status = ReplicationStatus(**status_data) if status_data else None
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
