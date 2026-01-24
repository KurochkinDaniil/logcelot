"""Async Kafka producer for log ingestion.

This module implements the Kafka producer following the singleton pattern
for connection reuse across FastAPI lifecycle.

References:
    - Course Requirement: Message Queues & Streaming
    - Implementation Constraint: Async-first, no blocking I/O
"""

import asyncio
from typing import Any, Optional

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError, KafkaConnectionError
import structlog

from src.models.log_entry import LogEntry
from src.core.config import get_settings

logger = structlog.get_logger()


class LogProducer:
    """Async Kafka producer for log ingestion.
    
    Implements singleton pattern for connection reuse.
    Handles retries and graceful shutdown.
    
    Architecture Notes:
        - Pull model: Producer pushes to Kafka, Consumer pulls
        - At-least-once delivery: acks=1, retries enabled
        - Compression: LZ4 for balance between speed and ratio
    
    Attributes:
        bootstrap_servers: Kafka broker addresses.
        topic: Main logs topic.
        dlq_topic: Dead Letter Queue topic.
        producer: AIOKafkaProducer instance (lazy-initialized).
    """
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        dlq_topic: str,
        compression_type: str = "lz4",
        acks: int = 1,
        retries: int = 3,
        max_batch_size: int = 16384,
        linger_ms: int = 10,
    ):
        """Initialize producer configuration.
        
        Args:
            bootstrap_servers: Comma-separated broker addresses.
            topic: Main logs topic name.
            dlq_topic: Dead Letter Queue topic name.
            compression_type: Compression algorithm (lz4, gzip, snappy).
            acks: Acknowledgment level (0, 1, all).
            retries: Number of retries for failed sends.
            max_batch_size: Maximum batch size in bytes.
            linger_ms: Time to wait before sending batch (batching).
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.dlq_topic = dlq_topic
        self.compression_type = compression_type
        self.acks = acks
        self.retries = retries
        self.max_batch_size = max_batch_size
        self.linger_ms = linger_ms
        
        self.producer: Optional[AIOKafkaProducer] = None
        self._started = False
        
        logger.info(
            "kafka_producer_initialized",
            bootstrap_servers=bootstrap_servers,
            topic=topic,
        )
    
    async def start(self, max_retries: int = 5, retry_delay: int = 2) -> None:
        """Start Kafka producer with retry logic.
        
        Implements exponential backoff for connection retries.
        This handles cases where Kafka broker is not yet ready.
        
        Args:
            max_retries: Maximum number of connection attempts.
            retry_delay: Initial delay between retries (seconds).
            
        Raises:
            KafkaConnectionError: If unable to connect after max_retries.
        """
        if self._started:
            logger.warning("kafka_producer_already_started")
            return
        
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(
                    "kafka_producer_connecting",
                    attempt=attempt,
                    max_retries=max_retries,
                )
                
                # Initialize producer
                self.producer = AIOKafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    # Serialization
                    value_serializer=lambda v: v.encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    # Performance tuning (Course: Kafka Best Practices)
                    compression_type=self.compression_type,
                    acks=self.acks,
                    max_batch_size=self.max_batch_size,
                    linger_ms=self.linger_ms,
                    # Timeouts
                    request_timeout_ms=30000,
                )
                
                await self.producer.start()
                self._started = True
                
                logger.info(
                    "kafka_producer_started",
                    bootstrap_servers=self.bootstrap_servers,
                    topic=self.topic,
                )
                return
                
            except KafkaConnectionError as e:
                logger.warning(
                    "kafka_connection_failed",
                    attempt=attempt,
                    error=str(e),
                    retry_in=retry_delay,
                )
                
                if attempt == max_retries:
                    logger.error(
                        "kafka_producer_start_failed",
                        max_retries=max_retries,
                        error=str(e),
                    )
                    raise
                
                # Exponential backoff
                await asyncio.sleep(retry_delay)
                retry_delay *= 2  # 2, 4, 8, 16 seconds
    
    async def stop(self) -> None:
        """Gracefully stop producer.
        
        Flushes pending messages and closes connection.
        This is called during FastAPI shutdown.
        """
        if not self._started or not self.producer:
            return
        
        try:
            logger.info("kafka_producer_stopping")
            await self.producer.stop()
            self._started = False
            logger.info("kafka_producer_stopped")
        except Exception as e:
            logger.error("kafka_producer_stop_error", error=str(e))
    
    async def send_log(self, log: LogEntry) -> dict[str, Any]:
        """Send log entry to Kafka topic.
        
        Serializes LogEntry to JSON and sends to configured topic.
        Uses source_service as partition key for load distribution.
        
        Args:
            log: Validated log entry to send.
            
        Returns:
            Dict with Kafka metadata (topic, partition, offset).
            
        Raises:
            RuntimeError: If producer not started.
            KafkaError: If send fails after retries.
            
        Example:
            log = LogEntry(source="http", source_service="api", level="INFO", message="Test")
            result = await producer.send_log(log)
            print(result)
            ...
            {
                "kafka_topic": "logs.raw",
                "kafka_partition": 2,
                "kafka_offset": 12345,
                "log_id": "550e8400-e29b-41d4-a716-446655440000"
            }
        """
        if not self._started or not self.producer:
            raise RuntimeError("Producer not started. Call start() first.")
        
        try:
            # Serialize to JSON using Pydantic
            value = log.model_dump_json()
            
            # Use source_service as partition key
            # This ensures logs from same service go to same partition
            key = log.source_service
            
            # Send to Kafka (async, returns Future)
            future = await self.producer.send_and_wait(
                self.topic,
                value=value,
                key=key,
            )
            
            logger.info(
                "log_sent_to_kafka",
                log_id=str(log.id),
                topic=future.topic,
                partition=future.partition,
                offset=future.offset,
                source_service=log.source_service,
            )
            
            return {
                "kafka_topic": future.topic,
                "kafka_partition": future.partition,
                "kafka_offset": future.offset,
                "log_id": str(log.id),
            }
            
        except KafkaError as e:
            logger.error(
                "kafka_send_failed",
                log_id=str(log.id),
                error=str(e),
                error_type=type(e).__name__,
            )
            raise
    
    async def send_to_dlq(
        self,
        raw_payload: str,
        error_message: str,
        error_type: str,
        source: str = "unknown",
    ) -> None:
        """Send invalid log to Dead Letter Queue.
        
        This implements the DLQ pattern for Data Quality.
        Logs that fail validation are not lost, but isolated.
        
        Args:
            raw_payload: Original (invalid) log data.
            error_message: Error description.
            error_type: Error classification.
            source: Source that sent the invalid log.
        """
        if not self._started or not self.producer:
            logger.warning("dlq_send_skipped_producer_not_started")
            return
        
        try:
            import json
            from datetime import datetime
            from uuid import uuid4
            
            dlq_entry = {
                "id": str(uuid4()),
                "created_at": datetime.utcnow().isoformat(),
                "raw_payload": raw_payload,
                "error_message": error_message,
                "error_type": error_type,
                "source": source,
            }
            
            value = json.dumps(dlq_entry)
            
            await self.producer.send_and_wait(
                self.dlq_topic,
                value=value,
            )
            
            logger.warning(
                "log_sent_to_dlq",
                dlq_id=dlq_entry["id"],
                error_type=error_type,
                source=source,
            )
            
        except Exception as e:
            logger.error("dlq_send_failed", error=str(e))
    
    async def health_check(self) -> bool:
        """Check if producer is healthy.
        
        Returns:
            True if producer is connected and operational.
        """
        return self._started and self.producer is not None


# Global producer instance (Singleton)
_producer_instance: Optional[LogProducer] = None


async def get_producer() -> LogProducer:
    """Get or create global producer instance (Singleton).
    
    This is used as a FastAPI dependency for endpoint injection.
    
    Returns:
        Singleton LogProducer instance.
        
    Example:
        @app.post("/logs")
        async def ingest_logs(producer: LogProducer = Depends(get_producer)):
        ...     await producer.send_log(log)
    """
    global _producer_instance
    
    if _producer_instance is None:
        settings = get_settings()
        _producer_instance = LogProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic=settings.kafka_topic_logs,
            dlq_topic=settings.kafka_topic_dlq,
            compression_type=settings.kafka_compression_type,
            acks=settings.kafka_acks,
            retries=settings.kafka_retries,
            max_batch_size=settings.kafka_max_batch_size,
            linger_ms=settings.kafka_linger_ms,
        )
        
        # Start producer if not already started
        if not _producer_instance._started:
            await _producer_instance.start()
    
    return _producer_instance


async def shutdown_producer() -> None:
    """Shutdown global producer instance.
    
    Called during FastAPI application shutdown.
    """
    global _producer_instance
    
    if _producer_instance is not None:
        await _producer_instance.stop()
        _producer_instance = None

