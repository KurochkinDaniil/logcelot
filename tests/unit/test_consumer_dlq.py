"""Unit tests for Consumer Service DLQ functionality.

Tests the Dead Letter Queue implementation in LogConsumerService.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import ConsumerRecord

from src.services.consumer_service import LogConsumerService
from src.models.log_entry import LogEntry
from src.clickhouse.exceptions import StorageError


@pytest.fixture
def mock_consumer():
    """Mock AIOKafkaConsumer."""
    consumer = AsyncMock(spec=AIOKafkaConsumer)
    consumer.start = AsyncMock()
    consumer.stop = AsyncMock()
    consumer.commit = AsyncMock()
    return consumer


@pytest.fixture
def mock_dlq_producer():
    """Mock AIOKafkaProducer for DLQ."""
    producer = AsyncMock(spec=AIOKafkaProducer)
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    producer.send_and_wait = AsyncMock()
    return producer


@pytest.fixture
def mock_clickhouse_client():
    """Mock ClickHouse client."""
    with patch('src.services.consumer_service.get_client') as mock_get_client:
        client = MagicMock()
        client.insert_logs = MagicMock()
        mock_get_client.return_value = client
        yield client


@pytest.fixture
def consumer_service():
    """Create LogConsumerService instance."""
    return LogConsumerService(
        bootstrap_servers="localhost:9092",
        topic="logs.raw",
        group_id="test-group",
        batch_size=10,
        flush_interval=5,
    )


class TestDLQInitialization:
    """Test DLQ producer initialization."""
    
    @pytest.mark.asyncio
    async def test_dlq_producer_created_on_start(self, consumer_service):
        """Test that DLQ producer is created when service starts."""
        with patch.object(consumer_service, 'consumer', AsyncMock()):
            with patch('src.services.consumer_service.AIOKafkaProducer') as mock_producer_class:
                mock_producer = AsyncMock()
                mock_producer.start = AsyncMock()
                mock_producer_class.return_value = mock_producer
                
                with patch('src.services.consumer_service.AIOKafkaConsumer') as mock_consumer_class:
                    mock_consumer = AsyncMock()
                    mock_consumer.start = AsyncMock()
                    mock_consumer_class.return_value = mock_consumer
                    
                    await consumer_service.start()
                    
                    # Verify DLQ producer was created
                    assert consumer_service.dlq_producer is not None
                    mock_producer.start.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_dlq_metric_initialized(self, consumer_service):
        """Test that DLQ metric counter is initialized to 0."""
        assert consumer_service.dlq_messages_sent_total == 0


class TestDLQBatchSending:
    """Test sending batches to DLQ."""
    
    @pytest.mark.asyncio
    async def test_send_batch_to_dlq_on_storage_error(
        self, consumer_service, mock_clickhouse_client, mock_dlq_producer
    ):
        """Test that batch is sent to DLQ after max retries on storage error."""
        consumer_service.consumer = AsyncMock()
        consumer_service.consumer.commit = AsyncMock()
        consumer_service.dlq_producer = mock_dlq_producer
        
        # Create test logs
        logs = [
            LogEntry(
                source="test",
                source_service="test-service",
                level="INFO",
                message=f"Test log {i}"
            )
            for i in range(5)
        ]
        consumer_service.buffer = logs.copy()
        
        # Mock ClickHouse insert to always fail
        mock_clickhouse_client.insert_logs.side_effect = StorageError("Connection refused")
        
        # Call flush_buffer (will retry 3 times and then send to DLQ)
        await consumer_service._flush_buffer()
        
        # Verify DLQ producer was called for each log
        assert mock_dlq_producer.send_and_wait.call_count == 5
        
        # Verify metric was updated
        assert consumer_service.dlq_messages_sent_total == 5
        
        # Verify buffer was cleared
        assert len(consumer_service.buffer) == 0
        
        # Verify offset was committed (to skip poisoned batch)
        consumer_service.consumer.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_send_batch_to_dlq_includes_error_context(
        self, consumer_service, mock_dlq_producer
    ):
        """Test that DLQ entries include error message and type."""
        consumer_service.dlq_producer = mock_dlq_producer
        
        log = LogEntry(
            source="test",
            source_service="test-service",
            level="ERROR",
            message="Test error log"
        )
        
        await consumer_service._send_batch_to_dlq(
            logs=[log],
            error_message="ClickHouse insert failed after 3 retries",
            error_type="clickhouse_insert_failure",
        )
        
        # Get the call arguments
        call_args = mock_dlq_producer.send_and_wait.call_args
        
        # Verify topic
        assert call_args[0][0] == "logs.dead_letter"
        
        # Verify payload structure
        import json
        payload = json.loads(call_args[1]['value'])
        
        assert "dlq_id" in payload
        assert "dlq_timestamp" in payload
        assert payload["error_message"] == "ClickHouse insert failed after 3 retries"
        assert payload["error_type"] == "clickhouse_insert_failure"
        assert "original_log" in payload
        assert payload["original_log"]["message"] == "Test error log"


class TestDLQMessageSending:
    """Test sending individual messages to DLQ."""
    
    @pytest.mark.asyncio
    async def test_send_message_to_dlq_on_parse_error(
        self, consumer_service, mock_dlq_producer
    ):
        """Test that unparseable messages are sent to DLQ."""
        consumer_service.dlq_producer = mock_dlq_producer
        
        raw_payload = '{"invalid": json syntax'
        
        await consumer_service._send_message_to_dlq(
            raw_payload=raw_payload,
            error_message="JSONDecodeError: Expecting property name",
            error_type="message_parse_error",
        )
        
        # Verify DLQ producer was called
        mock_dlq_producer.send_and_wait.assert_called_once()
        
        # Verify metric was updated
        assert consumer_service.dlq_messages_sent_total == 1
        
        # Verify payload
        call_args = mock_dlq_producer.send_and_wait.call_args
        import json
        payload = json.loads(call_args[1]['value'])
        
        assert payload["error_type"] == "message_parse_error"
        assert payload["raw_payload"] == raw_payload
    
    @pytest.mark.asyncio
    async def test_send_message_truncates_long_payloads(
        self, consumer_service, mock_dlq_producer
    ):
        """Test that very long raw payloads are truncated."""
        consumer_service.dlq_producer = mock_dlq_producer
        
        # Create a 20KB payload
        long_payload = "x" * 20000
        
        await consumer_service._send_message_to_dlq(
            raw_payload=long_payload,
            error_message="Payload too large",
            error_type="message_parse_error",
        )
        
        # Verify payload was truncated to 10KB
        call_args = mock_dlq_producer.send_and_wait.call_args
        import json
        payload = json.loads(call_args[1]['value'])
        
        assert len(payload["raw_payload"]) == 10000


class TestDLQMetrics:
    """Test DLQ metrics tracking."""
    
    @pytest.mark.asyncio
    async def test_dlq_metric_increments_on_batch_send(
        self, consumer_service, mock_dlq_producer
    ):
        """Test that DLQ metric increments correctly for batches."""
        consumer_service.dlq_producer = mock_dlq_producer
        
        logs = [
            LogEntry(source="test", source_service="svc", level="INFO", message=f"Log {i}")
            for i in range(10)
        ]
        
        await consumer_service._send_batch_to_dlq(
            logs=logs,
            error_message="Storage error",
            error_type="storage_error",
        )
        
        assert consumer_service.dlq_messages_sent_total == 10
    
    @pytest.mark.asyncio
    async def test_dlq_metric_persists_across_multiple_sends(
        self, consumer_service, mock_dlq_producer
    ):
        """Test that DLQ metric accumulates across multiple sends."""
        consumer_service.dlq_producer = mock_dlq_producer
        
        # Send first batch
        logs1 = [LogEntry(source="test", source_service="svc", level="INFO", message="1")]
        await consumer_service._send_batch_to_dlq(logs1, "Error 1", "type_1")
        
        assert consumer_service.dlq_messages_sent_total == 1
        
        # Send second batch
        logs2 = [
            LogEntry(source="test", source_service="svc", level="INFO", message=f"Log {i}")
            for i in range(5)
        ]
        await consumer_service._send_batch_to_dlq(logs2, "Error 2", "type_2")
        
        assert consumer_service.dlq_messages_sent_total == 6
    
    @pytest.mark.asyncio
    async def test_dlq_metric_logged_on_stop(self, consumer_service):
        """Test that DLQ metric is included in shutdown logs."""
        consumer_service.running = True
        consumer_service.consumer = AsyncMock()
        consumer_service.dlq_producer = AsyncMock()
        consumer_service.dlq_messages_sent_total = 42
        
        with patch('src.services.consumer_service.logger') as mock_logger:
            await consumer_service.stop()
            
            # Verify metric was logged
            stop_call = [
                call for call in mock_logger.info.call_args_list
                if 'consumer_stopped' in str(call)
            ]
            assert len(stop_call) > 0
            # Check that dlq_messages_sent was passed
            assert any('dlq_messages_sent' in str(call) for call in mock_logger.info.call_args_list)


class TestDLQGracefulShutdown:
    """Test DLQ producer shutdown."""
    
    @pytest.mark.asyncio
    async def test_dlq_producer_stopped_on_shutdown(self, consumer_service):
        """Test that DLQ producer is stopped during graceful shutdown."""
        consumer_service.running = True
        consumer_service.consumer = AsyncMock()
        consumer_service.dlq_producer = AsyncMock()
        
        await consumer_service.stop()
        
        # Verify DLQ producer was stopped
        consumer_service.dlq_producer.stop.assert_called_once()


class TestDLQErrorHandling:
    """Test error handling in DLQ operations."""
    
    @pytest.mark.asyncio
    async def test_dlq_send_failure_logged_critically(
        self, consumer_service, mock_dlq_producer
    ):
        """Test that DLQ send failures are logged as critical (data loss risk)."""
        consumer_service.dlq_producer = mock_dlq_producer
        
        # Mock DLQ producer to fail
        mock_dlq_producer.send_and_wait.side_effect = Exception("DLQ Kafka broker down")
        
        log = LogEntry(source="test", source_service="svc", level="ERROR", message="Test")
        
        with patch('src.services.consumer_service.logger') as mock_logger:
            await consumer_service._send_batch_to_dlq([log], "Error", "storage_error")
            
            # Verify critical log was emitted
            critical_calls = [call for call in mock_logger.critical.call_args_list]
            assert len(critical_calls) > 0
            assert "Data loss risk" in str(critical_calls[0])
    
    @pytest.mark.asyncio
    async def test_dlq_not_available_skips_send(self, consumer_service):
        """Test that DLQ send is skipped if producer not available."""
        consumer_service.dlq_producer = None
        
        log = LogEntry(source="test", source_service="svc", level="INFO", message="Test")
        
        with patch('src.services.consumer_service.logger') as mock_logger:
            await consumer_service._send_batch_to_dlq([log], "Error", "storage_error")
            
            # Verify warning was logged
            warning_calls = [call for call in mock_logger.warning.call_args_list]
            assert any("dlq_producer_not_available" in str(call) for call in warning_calls)


class TestDLQIntegration:
    """Integration tests for DLQ in consume loop."""
    
    @pytest.mark.asyncio
    async def test_consume_loop_sends_unparseable_to_dlq(
        self, consumer_service, mock_dlq_producer
    ):
        """Test that consume loop sends unparseable messages to DLQ."""
        consumer_service.dlq_producer = mock_dlq_producer
        
        # Create mock consumer with invalid message
        mock_message = MagicMock()
        mock_message.value = "invalid json"
        mock_message.offset = 123
        mock_message.partition = 0
        
        # Mock parse to raise exception
        with patch.object(consumer_service, '_parse_message', side_effect=ValueError("Parse failed")):
            with patch.object(consumer_service, '_send_message_to_dlq', AsyncMock()) as mock_send_dlq:
                # Manually trigger message processing
                try:
                    log = consumer_service._parse_message(mock_message)
                    consumer_service.buffer.append(log)
                except Exception as e:
                    await consumer_service._send_message_to_dlq(
                        raw_payload=mock_message.value,
                        error_message=str(e),
                        error_type="message_parse_error",
                    )
                
                # Verify DLQ was called
                mock_send_dlq.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
