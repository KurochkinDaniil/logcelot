"""Unit tests for main application module."""
import pytest
from unittest.mock import Mock, AsyncMock, patch

from src.main import app


class TestHealthEndpoint:
    """Test health check endpoint."""

    def test_health_returns_status(self):
        """Test that health endpoint returns status info."""
        from fastapi.testclient import TestClient
        
        client = TestClient(app)
        response = client.get("/health")
        
        # Should return 200 or 503 depending on dependencies
        assert response.status_code in [200, 503]
        data = response.json()
        assert "status" in data
        assert "kafka" in data
        assert "clickhouse" in data


class TestMetricsEndpoint:
    """Test Prometheus metrics endpoint."""

    def test_metrics_endpoint_exists(self):
        """Test that metrics endpoint is available."""
        from fastapi.testclient import TestClient
        
        client = TestClient(app)
        response = client.get("/metrics")
        
        # Metrics endpoint should return prometheus format
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/plain")
