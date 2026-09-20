"""Prometheus metrics for worker service.

Exports metrics for Kafka consumption, ClickHouse writes, and DLQ.
"""

from prometheus_client import Counter, Histogram, Gauge, start_http_server
import structlog

logger = structlog.get_logger()

# Kafka consumer metrics
KAFKA_MESSAGES_CONSUMED = Counter(
    "logcelot_worker_kafka_messages_consumed_total",
    "Total number of messages consumed from Kafka",
    ["topic"]
)

KAFKA_MESSAGES_PROCESSING_TIME = Histogram(
    "logcelot_worker_message_processing_seconds",
    "Time spent processing a single message",
    ["topic"]
)

KAFKA_CONSUMER_LAG = Gauge(
    "logcelot_worker_kafka_consumer_lag",
    "Current consumer lag (messages behind)",
    ["topic", "partition"]
)

# ClickHouse metrics
CLICKHOUSE_BATCH_SIZE = Histogram(
    "logcelot_worker_clickhouse_batch_size",
    "Size of batches written to ClickHouse",
    buckets=[10, 50, 100, 250, 500, 1000, 2500, 5000]
)

CLICKHOUSE_WRITE_DURATION = Histogram(
    "logcelot_worker_clickhouse_write_duration_seconds",
    "Duration of ClickHouse batch writes",
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
)

CLICKHOUSE_WRITES_TOTAL = Counter(
    "logcelot_worker_clickhouse_writes_total",
    "Total number of writes to ClickHouse",
    ["status"]
)

CLICKHOUSE_ROWS_WRITTEN = Counter(
    "logcelot_worker_clickhouse_rows_written_total",
    "Total number of rows written to ClickHouse"
)

# DLQ metrics
DLQ_MESSAGES_SENT = Counter(
    "logcelot_worker_dlq_messages_sent_total",
    "Total number of messages sent to DLQ",
    ["error_type"]
)

# Buffer metrics
BUFFER_SIZE = Gauge(
    "logcelot_worker_buffer_size",
    "Current size of the message buffer"
)

BUFFER_FLUSH_DURATION = Histogram(
    "logcelot_worker_buffer_flush_duration_seconds",
    "Duration of buffer flush operations",
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
)

# Worker health
WORKER_UP = Gauge(
    "logcelot_worker_up",
    "Worker service is up (1) or down (0)"
)

WORKER_TOTAL_CONSUMED = Counter(
    "logcelot_worker_total_consumed",
    "Total messages consumed since startup"
)

WORKER_TOTAL_FLUSHED = Counter(
    "logcelot_worker_total_flushed",
    "Total messages flushed to ClickHouse since startup"
)


def start_metrics_server(port: int = 8000) -> None:
    """Start Prometheus metrics HTTP server.
    
    Args:
        port: Port to bind metrics server to (default: 8000)
    """
    try:
        start_http_server(port)
        logger.info("metrics_server_started", port=port, endpoint=f"http://0.0.0.0:{port}/metrics")
        WORKER_UP.set(1)
    except Exception as e:
        logger.error("metrics_server_start_failed", port=port, error=str(e))
        raise
