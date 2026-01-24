"""Kafka Consumer service for log processing.

Implements batching strategy and at-least-once delivery guarantees.
Reads from Kafka, buffers logs, and bulk inserts into ClickHouse.
"""

import asyncio
import signal
import time
from typing import Any, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import structlog

from src.models.log_entry import LogEntry
from src.clickhouse.client import get_client
from src.clickhouse.exceptions import StorageError
from src.core.config import get_settings
from src.services.metrics import (
    KAFKA_MESSAGES_CONSUMED,
    CLICKHOUSE_BATCH_SIZE,
    CLICKHOUSE_WRITE_DURATION,
    CLICKHOUSE_WRITES_TOTAL,
    CLICKHOUSE_ROWS_WRITTEN,
    DLQ_MESSAGES_SENT,
    BUFFER_SIZE,
    BUFFER_FLUSH_DURATION,
    WORKER_TOTAL_CONSUMED,
    WORKER_TOTAL_FLUSHED,
)

logger = structlog.get_logger()


class LogConsumerService:
    """Kafka consumer service for log processing."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        batch_size: int = 1000,
        flush_interval: int = 5,
        dlq_topic: str = "logs.dead_letter",
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.dlq_topic = dlq_topic

        self.consumer: Optional[AIOKafkaConsumer] = None
        self.dlq_producer: Optional[AIOKafkaProducer] = None
        self.buffer: list[LogEntry] = []
        self.last_flush_time = time.time()
        self.running = False
        self._shutdown_requested = False

        # Statistics
        self.total_consumed = 0
        self.total_flushed = 0
        self.flush_count = 0
        self.dlq_messages_sent_total = 0

        logger.info(
            "consumer_service_initialized",
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            group_id=group_id,
            batch_size=batch_size,
            flush_interval=flush_interval,
            dlq_topic=dlq_topic,
        )

    async def start(self) -> None:
        """Start Kafka consumer and DLQ producer."""
        try:
            logger.info("consumer_starting")

            self.consumer = AIOKafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                value_deserializer=lambda m: m.decode("utf-8"),
                max_poll_records=self.batch_size * 2,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
            )
            await self.consumer.start()

            # DLQ producer
            self.dlq_producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
                compression_type="gzip",
                acks=1,
                request_timeout_ms=30000,
            )
            await self.dlq_producer.start()

            self.running = True

            logger.info(
                "consumer_started",
                topic=self.topic,
                group_id=self.group_id,
                dlq_enabled=True,
                dlq_topic=self.dlq_topic,
            )

        except Exception as e:
            logger.error("consumer_start_failed", error=str(e))
            raise

    async def stop(self) -> None:
        """Stop consumer and cleanup resources."""
        if not self.running:
            return

        logger.info("consumer_stopping", buffered_logs=len(self.buffer))

        try:
            if self.buffer:
                await self._flush_buffer()

            if self.consumer:
                await self.consumer.stop()

            if self.dlq_producer:
                await self.dlq_producer.stop()

            self.running = False

            logger.info(
                "consumer_stopped",
                total_consumed=self.total_consumed,
                total_flushed=self.total_flushed,
                flush_count=self.flush_count,
                dlq_messages_sent=self.dlq_messages_sent_total,
            )

        except Exception as e:
            logger.error("consumer_stop_error", error=str(e))

    async def consume_loop(self) -> None:
        """Main consumption loop."""
        logger.info("consumer_loop_started")

        if not self.consumer:
            raise RuntimeError("Consumer is not started")

        try:
            while self.running and not self._shutdown_requested:
                # Fetch messages with timeout to allow periodic buffer flushing
                messages = await self.consumer.getmany(
                    timeout_ms=1000,  # 1 second timeout
                    max_records=self.batch_size,
                )
                
                # Process messages
                for topic_partition, records in messages.items():
                    for message in records:
                        try:
                            log = self._parse_message(message)
                            self.buffer.append(log)
                            self.total_consumed += 1
                            
                            # Update metrics
                            KAFKA_MESSAGES_CONSUMED.labels(topic=self.topic).inc()
                            WORKER_TOTAL_CONSUMED.inc()
                            BUFFER_SIZE.set(len(self.buffer))

                        except Exception as e:
                            logger.error(
                                "message_processing_error",
                                error=str(e),
                                offset=message.offset,
                                partition=message.partition,
                            )
                            await self._send_message_to_dlq(
                                raw_payload=message.value,
                                error_message=str(e),
                                error_type="message_parse_error",
                            )
                            continue
                
                # Check if buffer should be flushed (after processing batch or by time)
                should_flush = (
                    len(self.buffer) >= self.batch_size
                    or (len(self.buffer) > 0 and (time.time() - self.last_flush_time) >= self.flush_interval)
                )
                if should_flush:
                    await self._flush_buffer()

        except Exception as e:
            logger.error("consume_loop_error", error=str(e))
            raise
        finally:
            logger.info("consume_loop_ended")

    def _parse_message(self, message: Any) -> LogEntry:
        """Parse Kafka message into LogEntry."""
        import json

        try:
            log_data = json.loads(message.value)
            log = LogEntry(**log_data)

            logger.debug(
                "message_parsed",
                log_id=str(log.id),
                partition=message.partition,
                offset=message.offset,
            )
            return log

        except Exception as e:
            logger.error(
                "message_parse_failed",
                error=str(e),
                raw_value=message.value[:200] if message.value else None,
            )
            raise ValueError(f"Failed to parse message: {e}") from e

    async def _flush_buffer(self) -> None:
        """Flush buffered logs to ClickHouse."""
        if not self.buffer:
            return

        if not self.consumer:
            raise RuntimeError("Consumer is not started")

        batch_size = len(self.buffer)
        flush_start = time.time()

        logger.info(
            "flush_starting",
            batch_size=batch_size,
            time_since_last_flush=flush_start - self.last_flush_time,
        )

        max_retries = 3
        retry_delay = 1

        for attempt in range(1, max_retries + 1):
            try:
                client = get_client()
                
                # Measure ClickHouse write duration
                ch_write_start = time.time()
                result = client.insert_logs(self.buffer.copy())
                ch_write_duration = time.time() - ch_write_start

                # Commit only after successful insert() call
                await self.consumer.commit()

                self.total_flushed += batch_size
                self.flush_count += 1
                flush_duration = time.time() - flush_start

                # Update metrics
                CLICKHOUSE_BATCH_SIZE.observe(batch_size)
                CLICKHOUSE_WRITE_DURATION.observe(ch_write_duration)
                CLICKHOUSE_WRITES_TOTAL.labels(status="success").inc()
                CLICKHOUSE_ROWS_WRITTEN.inc(batch_size)
                BUFFER_FLUSH_DURATION.observe(flush_duration)
                WORKER_TOTAL_FLUSHED.inc(batch_size)

                logger.info(
                    "flush_success",
                    batch_size=batch_size,
                    flush_duration_ms=int(flush_duration * 1000),
                    clickhouse_duration_ms=result.get("duration_ms", 0) if isinstance(result, dict) else 0,
                    total_flushed=self.total_flushed,
                    flush_count=self.flush_count,
                    throughput_per_sec=int(batch_size / flush_duration) if flush_duration > 0 else 0,
                )

                self.buffer.clear()
                BUFFER_SIZE.set(0)
                self.last_flush_time = time.time()
                return

            except StorageError as e:
                CLICKHOUSE_WRITES_TOTAL.labels(status="error").inc()
                
                logger.error(
                    "flush_failed_storage_error",
                    attempt=attempt,
                    max_retries=max_retries,
                    error=str(e),
                    batch_size=batch_size,
                )

                if attempt == max_retries:
                    logger.error(
                        "flush_failed_max_retries_sending_to_dlq",
                        batch_size=batch_size,
                        error=str(e),
                        error_type="storage_error",
                    )

                    await self._send_batch_to_dlq(
                        logs=self.buffer,
                        error_message=f"Failed to insert into ClickHouse after {max_retries} retries: {e}",
                        error_type="clickhouse_insert_failure",
                    )

                    # Skip this poisoned batch
                    await self.consumer.commit()

                    self.buffer.clear()
                    BUFFER_SIZE.set(0)
                    self.last_flush_time = time.time()
                    return

                await asyncio.sleep(retry_delay)
                retry_delay *= 2

            except Exception as e:
                logger.error(
                    "flush_failed_unexpected",
                    attempt=attempt,
                    error=str(e),
                    error_type=type(e).__name__,
                )

                if attempt == max_retries:
                    logger.error(
                        "flush_failed_unexpected_error_sending_to_dlq",
                        batch_size=batch_size,
                        error=str(e),
                        error_type=type(e).__name__,
                    )

                    await self._send_batch_to_dlq(
                        logs=self.buffer,
                        error_message=f"Unexpected error during flush: {e}",
                        error_type=f"unexpected_{type(e).__name__}",
                    )

                    await self.consumer.commit()

                    self.buffer.clear()
                    BUFFER_SIZE.set(0)
                    self.last_flush_time = time.time()
                    return

                await asyncio.sleep(retry_delay)
                retry_delay *= 2

    async def _send_batch_to_dlq(
        self,
        logs: list[LogEntry],
        error_message: str,
        error_type: str,
    ) -> None:
        """Send batch of logs to Dead Letter Queue."""
        if not self.dlq_producer:
            logger.warning("dlq_producer_not_available_skipping_dlq_send")
            return

        try:
            import json
            from datetime import datetime
            from uuid import uuid4

            for log in logs:
                dlq_entry = {
                    "dlq_id": str(uuid4()),
                    "dlq_timestamp": datetime.utcnow().isoformat(),
                    "error_message": error_message,
                    "error_type": error_type,
                    "original_log": log.model_dump(mode="json"),
                }
                await self.dlq_producer.send_and_wait(self.dlq_topic, value=json.dumps(dlq_entry))

            self.dlq_messages_sent_total += len(logs)
            
            # Update DLQ metrics
            DLQ_MESSAGES_SENT.labels(error_type=error_type).inc(len(logs))

            logger.error(
                "batch_sent_to_dlq",
                batch_size=len(logs),
                error_type=error_type,
                dlq_topic=self.dlq_topic,
                dlq_messages_sent_total=self.dlq_messages_sent_total,
                reason="Batch sent to DLQ due to storage failure",
            )

        except Exception as e:
            logger.critical(
                "dlq_send_failed",
                error=str(e),
                batch_size=len(logs),
                error_type=error_type,
                consequence="Data loss risk: logs not stored in ClickHouse or DLQ",
            )

    async def _send_message_to_dlq(
        self,
        raw_payload: str,
        error_message: str,
        error_type: str,
    ) -> None:
        """Send single unparseable message to Dead Letter Queue."""
        if not self.dlq_producer:
            logger.warning("dlq_producer_not_available_skipping_dlq_send")
            return

        try:
            import json
            from datetime import datetime
            from uuid import uuid4

            dlq_entry = {
                "dlq_id": str(uuid4()),
                "dlq_timestamp": datetime.utcnow().isoformat(),
                "error_message": error_message,
                "error_type": error_type,
                "raw_payload": raw_payload[:10000] if raw_payload else None,
            }

            await self.dlq_producer.send_and_wait(self.dlq_topic, value=json.dumps(dlq_entry))

            self.dlq_messages_sent_total += 1
            
            # Update DLQ metrics
            DLQ_MESSAGES_SENT.labels(error_type=error_type).inc()

            logger.warning(
                "message_sent_to_dlq",
                error_type=error_type,
                dlq_topic=self.dlq_topic,
                dlq_messages_sent_total=self.dlq_messages_sent_total,
            )

        except Exception as e:
            logger.error("dlq_send_failed", error=str(e), error_type=error_type)

    def request_shutdown(self) -> None:
        logger.info("shutdown_requested")
        self._shutdown_requested = True


async def run_consumer() -> None:
    """Run consumer service."""
    settings = get_settings()

    service = LogConsumerService(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_topic_logs,
        group_id=settings.kafka_consumer_group,
        batch_size=1000,
        flush_interval=5,
        dlq_topic=settings.kafka_topic_dlq,  # FIX: no hardcode
    )

    loop = asyncio.get_running_loop()

    def signal_handler():
        logger.info("signal_received_initiating_shutdown")
        service.request_shutdown()

    try:
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, signal_handler)
    except NotImplementedError:
        logger.warning("signal_handlers_not_supported_using_keyboard_interrupt_only")

    try:
        await service.start()
        await service.consume_loop()
    except KeyboardInterrupt:
        logger.info("keyboard_interrupt_received")
    except Exception as e:
        logger.error("consumer_error", error=str(e))
        raise
    finally:
        await service.stop()


if __name__ == "__main__":
    asyncio.run(run_consumer())
