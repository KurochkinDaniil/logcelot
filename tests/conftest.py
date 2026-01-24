"""Pytest configuration and shared fixtures.

Global fixtures for unit and integration tests.
"""

import pytest


@pytest.fixture
def sample_log_data():
    """Sample log data for testing.
    
    Returns:
        Dict with valid log fields.
    """
    return {
        "source": "http",
        "source_service": "test-api",
        "level": "info",
        "message": "Test log message",
        "metadata": {
            "user_id": "12345",
            "action": "test",
        },
    }


@pytest.fixture
def sample_error_log_data():
    """Sample error log data for testing.
    
    Returns:
        Dict with error log fields.
    """
    return {
        "source": "kafka",
        "source_service": "worker",
        "level": "error",
        "message": "Processing failed",
        "metadata": {
            "error_code": "ERR_001",
            "trace_id": "abc-123-xyz",
        },
        "error_type": "ProcessingError",
        "error_stack": "Traceback (most recent call last)...",
    }


@pytest.fixture
def sample_http_log_data():
    """Sample HTTP access log data for testing.
    
    Returns:
        Dict with HTTP log fields.
    """
    return {
        "source": "http",
        "source_service": "nginx",
        "level": "info",
        "message": "GET /api/users 200",
        "http_method": "GET",
        "http_path": "/api/users",
        "http_status": 200,
        "http_response_time_ms": 45,
        "metadata": {
            "client_ip": "192.168.1.100",
        },
    }

