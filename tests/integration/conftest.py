"""Pytest configuration for integration tests.

Provides shared fixtures for testcontainers and test setup.
"""

import pytest


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "integration: Integration tests requiring Docker"
    )
    config.addinivalue_line(
        "markers", "slow: Slow tests (>5 seconds)"
    )

