"""Exceptions for the go-objstore SDK."""

from typing import Optional


class ObjectStoreError(Exception):
    """Base exception for all go-objstore errors."""

    def __init__(self, message: str, status_code: Optional[int] = None) -> None:
        """Initialize ObjectStoreError.

        Args:
            message: Error message
            status_code: HTTP status code if applicable
        """
        super().__init__(message)
        self.message = message
        self.status_code = status_code


class ObjectNotFoundError(ObjectStoreError):
    """Raised when an object is not found in the storage backend."""

    def __init__(self, key: str) -> None:
        """Initialize ObjectNotFoundError.

        Args:
            key: Object key that was not found
        """
        super().__init__(f"Object not found: {key}", status_code=404)
        self.key = key


class ConnectionError(ObjectStoreError):
    """Raised when connection to the server fails."""

    pass


class AuthenticationError(ObjectStoreError):
    """Raised when authentication fails."""

    def __init__(self, message: str = "Authentication failed") -> None:
        """Initialize AuthenticationError.

        Args:
            message: Error message
        """
        super().__init__(message, status_code=401)


class AuthorizationError(ObjectStoreError):
    """Raised when the caller is authenticated but not permitted."""

    def __init__(self, message: str = "Access denied") -> None:
        """Initialize AuthorizationError.

        Args:
            message: Error message
        """
        super().__init__(message, status_code=403)


class AlreadyExistsError(ObjectStoreError):
    """Raised when an object or resource already exists."""

    def __init__(self, message: str = "Already exists") -> None:
        """Initialize AlreadyExistsError.

        Args:
            message: Error message
        """
        super().__init__(message, status_code=409)


class RateLimitError(ObjectStoreError):
    """Raised when the server rejects a request due to rate limiting."""

    def __init__(self, message: str = "Rate limit exceeded") -> None:
        """Initialize RateLimitError.

        Args:
            message: Error message
        """
        super().__init__(message, status_code=429)


class ValidationError(ObjectStoreError):
    """Raised when request validation fails."""

    def __init__(self, message: str) -> None:
        """Initialize ValidationError.

        Args:
            message: Error message
        """
        super().__init__(message, status_code=400)


class ServerError(ObjectStoreError):
    """Raised when server returns an error."""

    pass


class TimeoutError(ObjectStoreError):
    """Raised when a request times out."""

    pass
