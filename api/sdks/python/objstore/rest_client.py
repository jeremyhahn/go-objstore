"""REST client implementation for go-objstore."""

import json
from io import BytesIO
from typing import BinaryIO, Dict, Iterator, List, Optional, Union

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

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
    SyncResult,
    TriggerReplicationOptions,
    TriggerReplicationResponse,
)


class RestClient:
    """REST client for go-objstore."""

    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        api_version: str = "v1",
        timeout: int = 30,
        max_retries: int = 3,
    ) -> None:
        """Initialize REST client.

        Args:
            base_url: Base URL of the go-objstore server
            api_version: API version to use
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
        """
        self.base_url = base_url.rstrip("/")
        self.api_version = api_version
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()

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

    def _handle_error(self, response: requests.Response) -> None:
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
        url = self._url(f"objects/{key}")

        try:
            # Convert file-like objects to bytes
            if isinstance(data, bytes):
                body_data = data
            else:
                body_data = data.read()

            headers = {}
            if metadata:
                if metadata.content_type:
                    headers["Content-Type"] = metadata.content_type
                if metadata.content_encoding:
                    headers["Content-Encoding"] = metadata.content_encoding
                if metadata.custom:
                    headers["X-Object-Metadata"] = json.dumps(metadata.custom)

            response = self.session.put(
                url, data=body_data, headers=headers, timeout=self.timeout
            )

            if response.status_code == 201:
                result = response.json()
                return PutResponse(
                    success=True,
                    message=result.get("message", "Object uploaded successfully"),
                    etag=result.get("data", {}).get("etag"),
                )

            self._handle_error(response)
            return PutResponse(success=False, message="Upload failed")

        except requests.exceptions.Timeout:
            raise TimeoutError("Request timed out")
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

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
        url = self._url(f"objects/{key}")

        try:
            response = self.session.get(url, timeout=self.timeout)

            if response.status_code == 200:
                metadata = Metadata(
                    content_type=response.headers.get("Content-Type"),
                    size=int(response.headers.get("Content-Length", 0)),
                    etag=response.headers.get("ETag"),
                )
                return response.content, metadata

            self._handle_error(response)
            return b"", Metadata()

        except requests.exceptions.Timeout:
            raise TimeoutError("Request timed out")
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def get_stream(self, key: str) -> Iterator[bytes]:
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
            response = self.session.get(url, stream=True, timeout=self.timeout)

            if response.status_code == 200:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        yield chunk
            else:
                self._handle_error(response)

        except requests.exceptions.Timeout:
            raise TimeoutError("Request timed out")
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

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
        url = self._url(f"objects/{key}")

        try:
            response = self.session.delete(url, timeout=self.timeout)

            if response.status_code == 200:
                result = response.json()
                return DeleteResponse(
                    success=True, message=result.get("message", "Object deleted successfully")
                )

            # Handle server returning 500 for non-existent objects (should be 404)
            if response.status_code == 500:
                # Check if it's a "not found" error
                try:
                    error_data = response.json()
                    message = error_data.get("message", "").lower()
                    if "not found" in message or "does not exist" in message:
                        raise ObjectNotFoundError("Object not found")
                except (ValueError, KeyError):
                    pass

            self._handle_error(response)
            return DeleteResponse(success=False, message="Delete failed")

        except requests.exceptions.Timeout:
            raise TimeoutError("Request timed out")
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

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
        url = self._url("objects")
        params: Dict[str, Union[str, int]] = {"limit": max_results}

        if prefix:
            params["prefix"] = prefix
        if delimiter:
            params["delimiter"] = delimiter
        if continue_from:
            params["token"] = continue_from

        try:
            response = self.session.get(url, params=params, timeout=self.timeout)

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

        except requests.exceptions.Timeout:
            raise TimeoutError("Request timed out")
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

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
        url = self._url(f"objects/{key}")

        try:
            response = self.session.head(url, timeout=self.timeout)
            return ExistsResponse(exists=response.status_code == 200)

        except requests.exceptions.Timeout:
            raise TimeoutError("Request timed out")
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

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
        url = self._url(f"metadata/{key}")

        try:
            response = self.session.get(url, timeout=self.timeout)

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

        except requests.exceptions.Timeout:
            raise TimeoutError("Request timed out")
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

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
        url = self._url(f"metadata/{key}")

        try:
            response = self.session.put(
                url,
                json=metadata.model_dump(exclude_none=True),
                headers={"Content-Type": "application/json"},
                timeout=self.timeout,
            )

            if response.status_code == 200:
                result = response.json()
                return PolicyResponse(
                    success=True, message=result.get("message", "Metadata updated successfully")
                )

            # Server may return 201 if it creates a metadata object instead of updating
            # This is an API inconsistency that should be fixed server-side
            if response.status_code == 201:
                # Check if the object exists first
                # For now, treat this as success but it's not ideal
                result = response.json()
                return PolicyResponse(
                    success=True, message=result.get("message", "Metadata updated successfully")
                )

            self._handle_error(response)
            return PolicyResponse(success=False, message="Update failed")

        except requests.exceptions.Timeout:
            raise TimeoutError("Request timed out")
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

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
        # Health endpoint doesn't use API version prefix
        url = f"{self.base_url}/health"

        try:
            response = self.session.get(url, timeout=self.timeout)

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

        except requests.exceptions.Timeout:
            raise TimeoutError("Request timed out")
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def archive(self, key: str, destination_type: str, settings: Dict[str, str]) -> ArchiveResponse:
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
        url = self._url("archive")

        try:
            request_data = {
                "key": key,
                "destination_type": destination_type,
                "destination_settings": settings,
            }

            response = self.session.post(
                url,
                json=request_data,
                headers={"Content-Type": "application/json"},
                timeout=self.timeout,
            )

            if response.status_code == 200:
                result = response.json()
                return ArchiveResponse(
                    success=True, message=result.get("message", "Object archived successfully")
                )

            self._handle_error(response)
            return ArchiveResponse(success=False, message="Archive failed")

        except requests.exceptions.Timeout:
            raise TimeoutError("Request timed out")
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

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
        url = self._url("policies")

        try:
            response = self.session.post(
                url,
                json=policy.model_dump(exclude_none=True),
                headers={"Content-Type": "application/json"},
                timeout=self.timeout,
            )

            if response.status_code in (200, 201):
                result = response.json()
                return PolicyResponse(
                    success=True, message=result.get("message", "Policy added successfully")
                )

            self._handle_error(response)
            return PolicyResponse(success=False, message="Add policy failed")

        except requests.exceptions.Timeout:
            raise TimeoutError("Request timed out")
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

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
        url = self._url(f"policies/{policy_id}")

        try:
            response = self.session.delete(url, timeout=self.timeout)

            if response.status_code == 200:
                result = response.json()
                return PolicyResponse(
                    success=True, message=result.get("message", "Policy removed successfully")
                )

            self._handle_error(response)
            return PolicyResponse(success=False, message="Remove policy failed")

        except requests.exceptions.Timeout:
            raise TimeoutError("Request timed out")
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

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
        url = self._url("policies")
        params = {}
        if prefix:
            params["prefix"] = prefix

        try:
            response = self.session.get(url, params=params, timeout=self.timeout)

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

        except requests.exceptions.Timeout:
            raise TimeoutError("Request timed out")
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

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
        url = self._url("policies/apply")

        try:
            response = self.session.post(url, timeout=self.timeout)

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

        except requests.exceptions.Timeout:
            raise TimeoutError("Request timed out")
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

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
        url = self._url("replication/policies")

        try:
            response = self.session.post(
                url,
                json=policy.model_dump(exclude_none=True),
                headers={"Content-Type": "application/json"},
                timeout=self.timeout,
            )

            if response.status_code in (200, 201):
                result = response.json()
                return PolicyResponse(
                    success=True, message=result.get("message", "Replication policy added successfully")
                )

            self._handle_error(response)
            return PolicyResponse(success=False, message="Add replication policy failed")

        except requests.exceptions.Timeout:
            raise TimeoutError("Request timed out")
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

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
        url = self._url(f"replication/policies/{policy_id}")

        try:
            response = self.session.delete(url, timeout=self.timeout)

            if response.status_code == 200:
                result = response.json()
                return PolicyResponse(
                    success=True, message=result.get("message", "Replication policy removed successfully")
                )

            self._handle_error(response)
            return PolicyResponse(success=False, message="Remove replication policy failed")

        except requests.exceptions.Timeout:
            raise TimeoutError("Request timed out")
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

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
        url = self._url("replication/policies")

        try:
            response = self.session.get(url, timeout=self.timeout)

            if response.status_code == 200:
                data = response.json()
                policies = [
                    ReplicationPolicy(**policy_data) for policy_data in data.get("policies", [])
                ]
                return GetReplicationPoliciesResponse(policies=policies)

            self._handle_error(response)
            return GetReplicationPoliciesResponse(policies=[])

        except requests.exceptions.Timeout:
            raise TimeoutError("Request timed out")
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

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
        url = self._url(f"replication/policies/{policy_id}")

        try:
            response = self.session.get(url, timeout=self.timeout)

            if response.status_code == 200:
                data = response.json()
                return ReplicationPolicy(**data.get("policy", {}))

            self._handle_error(response)
            return ReplicationPolicy(
                id="",
                source_backend="",
                destination_backend="",
                check_interval_seconds=0,
            )

        except requests.exceptions.Timeout:
            raise TimeoutError("Request timed out")
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

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
        url = self._url("replication/trigger")

        try:
            response = self.session.post(
                url,
                json=opts.model_dump(exclude_none=True),
                headers={"Content-Type": "application/json"},
                timeout=self.timeout,
            )

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

        except requests.exceptions.Timeout:
            raise TimeoutError("Request timed out")
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

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
        url = self._url(f"replication/status/{policy_id}")

        try:
            response = self.session.get(url, timeout=self.timeout)

            if response.status_code == 200:
                data = response.json()
                from objstore.models import ReplicationStatus

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

        except requests.exceptions.Timeout:
            raise TimeoutError("Request timed out")
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Connection failed: {str(e)}")

    def close(self) -> None:
        """Close the HTTP session."""
        self.session.close()

    def __enter__(self) -> "RestClient":
        """Context manager entry."""
        return self

    def __exit__(self, *args: object) -> None:
        """Context manager exit."""
        self.close()
