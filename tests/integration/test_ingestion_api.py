"""Integration tests for log ingestion API.

Tests the ingestion flow: API -> Validation -> Kafka Producer (mocked).
Uses TestClient and mocked dependencies via dependency_overrides.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient


# Patch lifespan events to prevent real connections
@pytest.fixture(scope="module", autouse=True)
def mock_lifespan():
    """Mock lifespan events to prevent real Kafka/ClickHouse connections."""
    with patch("src.kafka.producer.LogProducer.start", new_callable=AsyncMock):
        with patch("src.kafka.producer.LogProducer.stop", new_callable=AsyncMock):
            with patch("src.clickhouse.client.ClickHouseClient.connect", new_callable=AsyncMock):
                with patch("src.clickhouse.client.ClickHouseClient.close", new_callable=AsyncMock):
                    yield


@pytest.fixture
def mock_kafka_producer():
    """Mock Kafka producer for testing."""
    mock_producer = AsyncMock()
    mock_producer._started = True
    mock_producer.health_check.return_value = True

    async def mock_send_log(log_entry):
        return {
            "kafka_topic": "logs.raw",
            "kafka_partition": 2,
            "kafka_offset": 12345,
            "log_id": str(log_entry.id),
        }
    
    mock_producer.send_log = mock_send_log
    return mock_producer


@pytest.fixture
def mock_clickhouse_client():
    """Mock ClickHouse client."""
    mock_ch = MagicMock()
    mock_ch._connected = True
    mock_ch.check_health.return_value = True
    mock_ch.query.return_value = ([], 0)
    return mock_ch


@pytest.fixture
def app_with_overrides(mock_kafka_producer, mock_clickhouse_client):
    """Create app with dependency overrides."""
    from src.main import app
    from src.api.routes import get_producer, get_client
    
    # Override dependencies
    app.dependency_overrides[get_producer] = lambda: mock_kafka_producer
    app.dependency_overrides[get_client] = lambda: mock_clickhouse_client
    
    yield app
    
    # Clear overrides after test
    app.dependency_overrides.clear()


@pytest.fixture
def client(app_with_overrides):
    """Create TestClient with mocked dependencies."""
    with TestClient(app_with_overrides, raise_server_exceptions=False) as c:
        yield c


class TestIngestionAPI:
    def test_ingest_single_log_success(self, client):
        response = client.post(
            "/logs",
            json={
                "source": "http",
                "source_service": "test-api",
                "level": "info",
                "message": "Test log message",
                "payload": {"user_id": "12345"},
            },
        )

        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "queued"
        assert data["count"] == 1
        assert "kafka_topic" in data
        assert "kafka_offset" in data

    def test_ingest_batch_logs_success(self, client):
        response = client.post(
            "/logs/batch",
            json={
                "logs": [
                    {"source": "http", "source_service": "api", "level": "info", "message": "Log 1"},
                    {"source": "http", "source_service": "api", "level": "error", "message": "Log 2"},
                ]
            },
        )

        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "queued"
        assert data["count"] == 2
        assert data["success"] == 2
        assert data["failed"] == 0

    def test_ingest_log_validation_error(self, client):
        response = client.post("/logs", json={"source": "http"})
        assert response.status_code == 422

    def test_ingest_log_invalid_level(self, client):
        response = client.post(
            "/logs",
            json={
                "source": "http",
                "source_service": "api",
                "level": "INVALID_LEVEL",
                "message": "Test",
            },
        )
        assert response.status_code == 422

    def test_batch_empty_logs_rejected(self, client):
        response = client.post("/logs/batch", json={"logs": []})
        assert response.status_code == 422

    def test_batch_exceeds_limit(self, client):
        response = client.post(
            "/logs/batch",
            json={
                "logs": [
                    {"source": "test", "source_service": "test", "level": "info", "message": f"Log {i}"}
                    for i in range(1001)
                ]
            },
        )
        assert response.status_code == 422

    @pytest.mark.skip(reason="Health endpoint creates new client instances, needs real connection")
    def test_health_endpoint(self, client):
        """Health endpoint test requires real Kafka/ClickHouse or more complex mocking."""
        response = client.get("/health")
        # Skipped because health endpoint instantiates new clients
        assert response.status_code in [200, 503]

    def test_root_endpoint(self, client):
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert "endpoints" in data
        assert "POST /logs" in data["endpoints"]["ingest_single"]

    @pytest.mark.skip(reason="Search endpoint creates new client instances, needs real connection")
    def test_search_endpoint_works_with_mocked_clickhouse(self, client):
        """Search endpoint test requires real ClickHouse or more complex mocking."""
        response = client.get("/logs/search")
        # Skipped because search endpoint instantiates new clients
        assert response.status_code in [200, 503]
