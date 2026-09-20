"""Unit tests for Kafka consumer service.

Tests the consumer batching logic, flush strategy, and error handling.
"""

import asyncio
from datetime import datetime
from uuid import uuid4
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from src.services.consumer_service import LogConsumerService, run_consumer
from src.models.log_entry import LogEntry
from src.clickhouse.exceptions import StorageError


@pytest.fixture
def mock_consumer():
    """Mock AIOKafkaConsumer."""
    consumer = AsyncMock()
    consumer.start = AsyncMock()
    consumer.stop = AsyncMock()
    consumer.commit = AsyncMock()
    return consumer


@pytest.fixture
def mock_producer():
    """Mock AIOKafkaProducer (DLQ producer)."""
    producer = AsyncMock()
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    producer.send_and_wait = AsyncMock()
    return producer


@pytest.fixture
def mock_clickhouse_client():
    """Mock ClickHouse client returned by get_client()."""
    with patch("src.services.consumer_service.get_client") as mock_get_client:
        mock_client = MagicMock()
        mock_client.insert_logs.return_value = {"duration_ms": 100}
        mock_get_client.return_value = mock_client
        yield mock_client


@pytest.fixture
def sample_log_entry() -> LogEntry:
    """Sample LogEntry for testing."""
    return LogEntry(
        id=uuid4(),
        created_at=datetime.utcnow(),
        source="http",
        source_host="localhost",
        source_service="test-service",
        level="INFO",
        message="Test log message",
        metadata={"key": "value"},
        user_id="user123",
    )


@pytest.fixture
def mock_kafka_message(sample_log_entry):
    """Mock Kafka message."""
    message = Mock()
    message.value = sample_log_entry.model_dump_json()
    message.offset = 12345
    message.partition = 0
    message.timestamp = int(datetime.utcnow().timestamp() * 1000)
    return message


class TestLogConsumerService:
    def test_consumer_initialization(self):
        """Test consumer service initialization."""
        service = LogConsumerService(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            group_id="test-group",
            batch_size=100,
            flush_interval=5,
        )

        assert service.bootstrap_servers == "localhost:9092"
        assert service.topic == "test-topic"
        assert service.group_id == "test-group"
        assert service.batch_size == 100
        assert service.flush_interval == 5
        assert service.buffer == []
        assert not service.running

    @pytest.mark.asyncio
    async def test_start_consumer(self, mock_consumer, mock_producer):
        """Test starting the consumer."""
        with patch("src.services.consumer_service.AIOKafkaConsumer", return_value=mock_consumer):
            with patch("src.services.consumer_service.AIOKafkaProducer", return_value=mock_producer):
                service = LogConsumerService(
                    bootstrap_servers="localhost:9092",
                    topic="test-topic",
                    group_id="test-group",
                )

                await service.start()

                mock_consumer.start.assert_called_once()
                mock_producer.start.assert_called_once()

                assert service.running
                assert service.consumer is not None
                assert service.dlq_producer is not None

    @pytest.mark.asyncio
    async def test_stop_consumer_flushes_buffer(
        self, mock_consumer, mock_producer, mock_clickhouse_client
    ):
        """Test that stopping consumer flushes remaining logs."""
        with patch("src.services.consumer_service.AIOKafkaConsumer", return_value=mock_consumer):
            with patch("src.services.consumer_service.AIOKafkaProducer", return_value=mock_producer):
                service = LogConsumerService(
                    bootstrap_servers="localhost:9092",
                    topic="test-topic",
                    group_id="test-group",
                )

                await service.start()

                # Add logs to buffer
                log1 = LogEntry(source="test", source_service="svc", level="INFO", message="msg1")
                log2 = LogEntry(source="test", source_service="svc", level="INFO", message="msg2")
                service.buffer = [log1, log2]

                await service.stop()

                # Should flush buffer before stopping
                mock_clickhouse_client.insert_logs.assert_called_once()
                service.consumer.commit.assert_called_once()

                mock_consumer.stop.assert_called_once()
                mock_producer.stop.assert_called_once()

                assert not service.running
                assert len(service.buffer) == 0

    @pytest.mark.asyncio
    async def test_parse_message_success(self, mock_kafka_message, sample_log_entry):
        """Test parsing valid Kafka message."""
        service = LogConsumerService(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            group_id="test-group",
        )

        log = service._parse_message(mock_kafka_message)

        assert isinstance(log, LogEntry)
        assert log.source_service == sample_log_entry.source_service
        assert log.level == sample_log_entry.level
        assert log.message == sample_log_entry.message

    @pytest.mark.asyncio
    async def test_parse_message_invalid_json(self):
        """Test parsing invalid JSON message."""
        service = LogConsumerService(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            group_id="test-group",
        )

        message = Mock()
        message.value = "invalid json {{"
        message.offset = 123
        message.partition = 0

        with pytest.raises(ValueError, match="Failed to parse message"):
            service._parse_message(message)

    @pytest.mark.asyncio
    async def test_flush_buffer_on_size_threshold(self, mock_clickhouse_client):
        """Test buffer flush when size threshold is reached."""
        service = LogConsumerService(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            group_id="test-group",
            batch_size=10,
            flush_interval=100,
        )

        service.consumer = AsyncMock()
        service.consumer.commit = AsyncMock()
        service.running = True

        # Add logs to buffer (reach batch_size)
        for i in range(10):
            service.buffer.append(
                LogEntry(source="test", source_service="svc", level="INFO", message=f"msg{i}")
            )

        await service._flush_buffer()

        mock_clickhouse_client.insert_logs.assert_called_once()

        # Verify insert was called with correct number of logs
        call_args, call_kwargs = mock_clickhouse_client.insert_logs.call_args
        inserted_logs = call_args[0]
        assert len(inserted_logs) == 10
        assert call_kwargs == {}

        service.consumer.commit.assert_called_once()
        assert len(service.buffer) == 0

    @pytest.mark.asyncio
    async def test_flush_buffer_empty(self, mock_clickhouse_client):
        """Test flushing empty buffer does nothing."""
        service = LogConsumerService(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            group_id="test-group",
        )

        service.consumer = AsyncMock()
        service.consumer.commit = AsyncMock()
        service.running = True

        await service._flush_buffer()

        mock_clickhouse_client.insert_logs.assert_not_called()
        service.consumer.commit.assert_not_called()

    @pytest.mark.asyncio
    async def test_flush_buffer_retry_on_storage_error(self, mock_clickhouse_client):
        """Test retry logic when ClickHouse insert fails."""
        service = LogConsumerService(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            group_id="test-group",
        )

        service.consumer = AsyncMock()
        service.consumer.commit = AsyncMock()
        service.running = True

        service.buffer.append(LogEntry(source="test", source_service="svc", level="INFO", message="msg"))

        # First call fails, second succeeds
        mock_clickhouse_client.insert_logs.side_effect = [
            StorageError("Connection failed"),
            {"duration_ms": 100},
        ]

        await service._flush_buffer()

        assert mock_clickhouse_client.insert_logs.call_count == 2
        service.consumer.commit.assert_called_once()
        assert len(service.buffer) == 0

    @pytest.mark.asyncio
    async def test_flush_buffer_max_retries_clears_buffer(self, mock_clickhouse_client):
        """Test buffer is cleared after max retries."""
        service = LogConsumerService(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            group_id="test-group",
        )

        service.consumer = AsyncMock()
        service.consumer.commit = AsyncMock()
        service.running = True

        service.buffer.append(LogEntry(source="test", source_service="svc", level="INFO", message="msg"))

        # All attempts fail
        mock_clickhouse_client.insert_logs.side_effect = StorageError("Connection failed")

        await service._flush_buffer()

        assert mock_clickhouse_client.insert_logs.call_count == 3
        # On max retries it commits offsets too (skip poisoned batch)
        service.consumer.commit.assert_called_once()
        assert len(service.buffer) == 0

    @pytest.mark.asyncio
    async def test_request_shutdown(self):
        """Test graceful shutdown request."""
        service = LogConsumerService(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            group_id="test-group",
        )

        assert not service._shutdown_requested
        service.request_shutdown()
        assert service._shutdown_requested

    @pytest.mark.asyncio
    async def test_batching_strategy_metrics(self, mock_clickhouse_client):
        """Test that batching metrics are tracked correctly."""
        service = LogConsumerService(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            group_id="test-group",
            batch_size=5,
        )

        service.consumer = AsyncMock()
        service.consumer.commit = AsyncMock()
        service.running = True

        for i in range(5):
            service.buffer.append(
                LogEntry(source="test", source_service="svc", level="INFO", message=f"msg{i}")
            )

        await service._flush_buffer()

        assert service.total_flushed == 5
        assert service.flush_count == 1
        assert len(service.buffer) == 0


class TestConsumerIntegration:
    @pytest.mark.asyncio
    @pytest.mark.skipif(
        not hasattr(asyncio.get_event_loop(), "add_signal_handler"),
        reason="Signal handlers not supported on Windows",
    )
    async def test_run_consumer_with_mock_settings(self, mock_consumer, mock_producer, mock_clickhouse_client):
        """Test run_consumer function with mocked dependencies."""
        mock_settings = MagicMock()
        mock_settings.kafka_bootstrap_servers = "localhost:9092"
        mock_settings.kafka_topic_logs = "logs.raw"
        mock_settings.kafka_consumer_group = "test-group"
        mock_settings.kafka_topic_dlq = "logs.dead_letter"

        with patch("src.services.consumer_service.get_settings", return_value=mock_settings):
            with patch("src.services.consumer_service.AIOKafkaConsumer", return_value=mock_consumer):
                with patch("src.services.consumer_service.AIOKafkaProducer", return_value=mock_producer):
                    with patch("src.services.consumer_service.LogConsumerService.consume_loop") as mock_loop:
                        mock_loop.return_value = None

                        await run_consumer()

                        mock_consumer.start.assert_called_once()
                        mock_consumer.stop.assert_called_once()
                        mock_producer.start.assert_called_once()
                        mock_producer.stop.assert_called_once()
