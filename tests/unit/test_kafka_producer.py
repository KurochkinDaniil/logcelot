"""Unit tests for Kafka producer.

Tests cover producer initialization, message sending, and error handling.
Uses mocks to avoid requiring actual Kafka broker.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from aiokafka.errors import KafkaConnectionError, KafkaError

from src.kafka.producer import LogProducer
from src.models.log_entry import LogEntry


@pytest.mark.asyncio
class TestLogProducer:
    """Test suite for LogProducer."""
    
    async def test_producer_initialization(self):
        """Test producer initialization with config."""
        producer = LogProducer(
            bootstrap_servers="localhost:9092",
            topic="logs.raw",
            dlq_topic="logs.dead_letter",
        )
        
        assert producer.bootstrap_servers == "localhost:9092"
        assert producer.topic == "logs.raw"
        assert producer.dlq_topic == "logs.dead_letter"
        assert producer.producer is None
        assert producer._started is False
    
    @patch('src.kafka.producer.AIOKafkaProducer')
    async def test_producer_start_success(self, mock_producer_class):
        """Test successful producer startup."""
        # Mock AIOKafkaProducer
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        producer = LogProducer(
            bootstrap_servers="localhost:9092",
            topic="logs.raw",
            dlq_topic="logs.dead_letter",
        )
        
        await producer.start(max_retries=1)
        
        assert producer._started is True
        assert producer.producer is not None
        mock_producer.start.assert_called_once()
    
    @patch('src.kafka.producer.AIOKafkaProducer')
    async def test_producer_start_with_retry(self, mock_producer_class):
        """Test producer startup with connection retry."""
        # Mock AIOKafkaProducer that fails first time, succeeds second time
        mock_producer = AsyncMock()
        mock_producer.start.side_effect = [
            KafkaConnectionError("Connection refused"),
            None,  # Success on second try
        ]
        mock_producer_class.return_value = mock_producer
        
        producer = LogProducer(
            bootstrap_servers="localhost:9092",
            topic="logs.raw",
            dlq_topic="logs.dead_letter",
        )
        
        await producer.start(max_retries=2, retry_delay=0)
        
        assert producer._started is True
        assert mock_producer.start.call_count == 2
    
    @patch('src.kafka.producer.AIOKafkaProducer')
    async def test_producer_start_max_retries_exceeded(self, mock_producer_class):
        """Test producer startup fails after max retries."""
        # Mock AIOKafkaProducer that always fails
        mock_producer = AsyncMock()
        mock_producer.start.side_effect = KafkaConnectionError("Connection refused")
        mock_producer_class.return_value = mock_producer
        
        producer = LogProducer(
            bootstrap_servers="localhost:9092",
            topic="logs.raw",
            dlq_topic="logs.dead_letter",
        )
        
        with pytest.raises(KafkaConnectionError):
            await producer.start(max_retries=2, retry_delay=0)
        
        assert producer._started is False
    
    @patch('src.kafka.producer.AIOKafkaProducer')
    async def test_send_log_success(self, mock_producer_class):
        """Test successful log sending."""
        # Mock AIOKafkaProducer
        mock_producer = AsyncMock()
        mock_future = MagicMock()
        mock_future.topic = "logs.raw"
        mock_future.partition = 2
        mock_future.offset = 12345
        mock_producer.send_and_wait.return_value = mock_future
        mock_producer_class.return_value = mock_producer
        
        producer = LogProducer(
            bootstrap_servers="localhost:9092",
            topic="logs.raw",
            dlq_topic="logs.dead_letter",
        )
        await producer.start(max_retries=1)
        
        # Create log entry
        log = LogEntry(
            source="test",
            source_service="test-service",
            level="info",
            message="Test message",
        )
        
        # Send log
        result = await producer.send_log(log)
        
        assert result["kafka_topic"] == "logs.raw"
        assert result["kafka_partition"] == 2
        assert result["kafka_offset"] == 12345
        assert "log_id" in result
        
        mock_producer.send_and_wait.assert_called_once()
    
    async def test_send_log_producer_not_started(self):
        """Test sending log when producer not started raises error."""
        producer = LogProducer(
            bootstrap_servers="localhost:9092",
            topic="logs.raw",
            dlq_topic="logs.dead_letter",
        )
        
        log = LogEntry(
            source="test",
            source_service="test-service",
            level="info",
            message="Test",
        )
        
        with pytest.raises(RuntimeError, match="Producer not started"):
            await producer.send_log(log)
    
    @patch('src.kafka.producer.AIOKafkaProducer')
    async def test_send_to_dlq(self, mock_producer_class):
        """Test sending to Dead Letter Queue."""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        producer = LogProducer(
            bootstrap_servers="localhost:9092",
            topic="logs.raw",
            dlq_topic="logs.dead_letter",
        )
        await producer.start(max_retries=1)
        
        # Send to DLQ
        await producer.send_to_dlq(
            raw_payload='{"invalid": "json}',
            error_message="JSON parse error",
            error_type="JSONDecodeError",
            source="http",
        )
        
        # Verify DLQ send was called
        assert mock_producer.send_and_wait.call_count == 1
        call_args = mock_producer.send_and_wait.call_args
        assert call_args[0][0] == "logs.dead_letter"
    
    @patch('src.kafka.producer.AIOKafkaProducer')
    async def test_health_check(self, mock_producer_class):
        """Test health check returns correct status."""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        producer = LogProducer(
            bootstrap_servers="localhost:9092",
            topic="logs.raw",
            dlq_topic="logs.dead_letter",
        )
        
        # Before start
        assert await producer.health_check() is False
        
        # After start
        await producer.start(max_retries=1)
        assert await producer.health_check() is True
    
    @patch('src.kafka.producer.AIOKafkaProducer')
    async def test_graceful_shutdown(self, mock_producer_class):
        """Test graceful producer shutdown."""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        producer = LogProducer(
            bootstrap_servers="localhost:9092",
            topic="logs.raw",
            dlq_topic="logs.dead_letter",
        )
        await producer.start(max_retries=1)
        
        # Stop producer
        await producer.stop()
        
        assert producer._started is False
        mock_producer.stop.assert_called_once()

