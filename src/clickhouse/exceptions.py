"""Custom exceptions for ClickHouse operations."""

from typing import Optional


class StorageError(Exception):
    """Base exception for ClickHouse storage errors."""

    def __init__(self, message: str, original_error: Optional[Exception] = None):
        super().__init__(message)
        self.message = message
        self.original_error = original_error

    def __str__(self) -> str:
        return f"{self.message}: {self.original_error}" if self.original_error else self.message


class ConnectionError(StorageError):
    """Raised when unable to connect to ClickHouse."""
    pass


class InsertError(StorageError):
    """Raised when bulk insert fails."""
    pass


class QueryError(StorageError):
    """Raised when query execution fails."""
    pass
