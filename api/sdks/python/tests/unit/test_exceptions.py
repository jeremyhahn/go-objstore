"""Unit tests for exceptions."""

import pytest

from objstore.exceptions import (
    AlreadyExistsError,
    AuthenticationError,
    AuthorizationError,
    ConnectionError,
    ObjectNotFoundError,
    ObjectStoreError,
    RateLimitError,
    ServerError,
    TimeoutError,
    ValidationError,
)


class TestExceptions:
    """Test exception classes."""

    def test_object_store_error(self) -> None:
        """Test base ObjectStoreError."""
        error = ObjectStoreError("test error", status_code=500)
        assert str(error) == "test error"
        assert error.message == "test error"
        assert error.status_code == 500

    def test_object_not_found_error(self) -> None:
        """Test ObjectNotFoundError."""
        error = ObjectNotFoundError("my-key")
        assert "my-key" in str(error)
        assert error.key == "my-key"
        assert error.status_code == 404

    def test_connection_error(self) -> None:
        """Test ConnectionError."""
        error = ConnectionError("connection failed")
        assert str(error) == "connection failed"

    def test_authentication_error(self) -> None:
        """Test AuthenticationError."""
        error = AuthenticationError("invalid token")
        assert str(error) == "invalid token"
        assert error.status_code == 401

    def test_authorization_error(self) -> None:
        """Test AuthorizationError."""
        error = AuthorizationError("access denied")
        assert str(error) == "access denied"
        assert error.status_code == 403

    def test_already_exists_error(self) -> None:
        """Test AlreadyExistsError."""
        error = AlreadyExistsError("object already exists")
        assert str(error) == "object already exists"
        assert error.status_code == 409

    def test_rate_limit_error(self) -> None:
        """Test RateLimitError."""
        error = RateLimitError("too many requests")
        assert str(error) == "too many requests"
        assert error.status_code == 429

    def test_validation_error(self) -> None:
        """Test ValidationError."""
        error = ValidationError("invalid input")
        assert str(error) == "invalid input"
        assert error.status_code == 400

    def test_server_error(self) -> None:
        """Test ServerError."""
        error = ServerError("internal error", status_code=503)
        assert str(error) == "internal error"
        assert error.status_code == 503

    def test_timeout_error(self) -> None:
        """Test TimeoutError."""
        error = TimeoutError("request timeout")
        assert str(error) == "request timeout"

    def test_exception_inheritance(self) -> None:
        """Test exception inheritance."""
        assert issubclass(ObjectNotFoundError, ObjectStoreError)
        assert issubclass(ConnectionError, ObjectStoreError)
        assert issubclass(AuthenticationError, ObjectStoreError)
        assert issubclass(AuthorizationError, ObjectStoreError)
        assert issubclass(AlreadyExistsError, ObjectStoreError)
        assert issubclass(RateLimitError, ObjectStoreError)
        assert issubclass(ValidationError, ObjectStoreError)
        assert issubclass(ServerError, ObjectStoreError)
        assert issubclass(TimeoutError, ObjectStoreError)
