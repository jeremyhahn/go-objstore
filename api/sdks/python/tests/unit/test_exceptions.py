"""Unit tests for exceptions."""

import pytest

from objstore.exceptions import (
    AuthenticationError,
    ConnectionError,
    ObjectNotFoundError,
    ObjectStoreError,
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
        assert issubclass(ValidationError, ObjectStoreError)
        assert issubclass(ServerError, ObjectStoreError)
        assert issubclass(TimeoutError, ObjectStoreError)
