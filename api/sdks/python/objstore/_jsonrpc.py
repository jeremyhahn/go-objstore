"""Shared JSON-RPC 2.0 error mapping for the Unix and MCP transports.

Error codes mirror the server contract in pkg/server/jsonrpc: classification
is by code, never by message text.
"""

from typing import Dict

from objstore.exceptions import (
    AlreadyExistsError,
    AuthenticationError,
    AuthorizationError,
    ObjectNotFoundError,
    RateLimitError,
    ServerError,
    ValidationError,
)

# Implementation-defined server error codes (pkg/server/jsonrpc).
ERR_FORBIDDEN = -32001
ERR_UNAUTHENTICATED = -32002
ERR_NOT_FOUND = -32004
ERR_ALREADY_EXISTS = -32005
ERR_RATE_LIMITED = -32029

# Standard JSON-RPC 2.0 error codes.
ERR_PARSE_ERROR = -32700
ERR_INVALID_REQUEST = -32600
ERR_METHOD_NOT_FOUND = -32601
ERR_INVALID_PARAMS = -32602
ERR_INTERNAL = -32603


def raise_rpc_error(error: Dict) -> None:
    """Translate a JSON-RPC error object into an SDK exception.

    Args:
        error: JSON-RPC error dict with ``code`` and ``message`` fields

    Raises:
        ObjectStoreError: Converted exception
    """
    code: int = error.get("code", 0)
    message: str = error.get("message", "unknown error")

    if code == ERR_NOT_FOUND:
        raise ObjectNotFoundError(message)
    if code == ERR_UNAUTHENTICATED:
        raise AuthenticationError(message)
    if code == ERR_FORBIDDEN:
        raise AuthorizationError(message)
    if code == ERR_ALREADY_EXISTS:
        raise AlreadyExistsError(message)
    if code == ERR_RATE_LIMITED:
        raise RateLimitError(message)
    if code in (ERR_INVALID_PARAMS, ERR_INVALID_REQUEST):
        raise ValidationError(message)
    if code == ERR_INTERNAL:
        raise ServerError(message)
    raise ServerError(f"RPC error {code}: {message}")
