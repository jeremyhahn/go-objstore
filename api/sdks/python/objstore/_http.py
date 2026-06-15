"""Shared HTTP helpers for the REST, QUIC, and MCP clients.

Centralizes Bearer/X-Tenant-ID header assembly and HTTP status-code to
exception mapping so the HTTP-based transports stay in sync.
"""

from typing import Any, Dict, Optional, Protocol

from objstore.exceptions import (
    AlreadyExistsError,
    AuthenticationError,
    AuthorizationError,
    ObjectNotFoundError,
    ObjectStoreError,
    RateLimitError,
    ServerError,
    ValidationError,
)


class HttpResponse(Protocol):
    """Minimal response surface shared by requests and httpx responses."""

    status_code: int
    text: str

    def json(self) -> Any:
        """Return the response body parsed as JSON."""
        ...


def build_auth_headers(
    token: Optional[str],
    tenant_id: Optional[str],
    extra_headers: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    """Build authentication and custom request headers.

    Args:
        token: Optional bearer token for the Authorization header
        tenant_id: Optional tenant identifier sent as X-Tenant-ID
        extra_headers: Optional additional headers, applied last so they
            may override the generated entries

    Returns:
        Headers dict
    """
    headers: Dict[str, str] = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if tenant_id:
        headers["X-Tenant-ID"] = tenant_id
    if extra_headers:
        headers.update(extra_headers)
    return headers


def handle_http_error(response: HttpResponse) -> None:
    """Translate an HTTP error response into an SDK exception.

    Args:
        response: HTTP response (requests or httpx)

    Raises:
        ObjectStoreError: For various error conditions
    """
    if response.status_code == 404:
        raise ObjectNotFoundError("Object not found")
    if response.status_code == 401:
        raise AuthenticationError("Authentication failed")
    if response.status_code == 403:
        raise AuthorizationError("Access denied")
    if response.status_code == 409:
        try:
            message = response.json().get("message", "Already exists")
        except Exception:
            message = response.text or "Already exists"
        raise AlreadyExistsError(message)
    if response.status_code == 429:
        raise RateLimitError("Rate limit exceeded")
    if response.status_code == 400:
        try:
            message = response.json().get("message", "Validation error")
        except Exception:
            message = response.text or "Validation error"
        raise ValidationError(message)
    if response.status_code >= 500:
        try:
            message = response.json().get("message", "Server error")
        except Exception:
            message = response.text or "Server error"
        raise ServerError(message, status_code=response.status_code)
    raise ObjectStoreError(
        f"HTTP {response.status_code}: {response.text}",
        status_code=response.status_code,
    )
