"""Kafka-specific configuration and utilities."""

from dataclasses import dataclass


@dataclass
class KafkaConfig:
    """Kafka producer/consumer configuration.
    
    This is a convenience wrapper around Settings for Kafka-specific config.
    
    Attributes:
        bootstrap_servers: Kafka broker addresses.
        topic_logs: Main logs topic.
        topic_dlq: Dead Letter Queue topic.
        compression_type: Compression algorithm.
        acks: Acknowledgment level.
        retries: Number of retries.
        retry_backoff_ms: Backoff between retries.
        request_timeout_ms: Request timeout.
        max_batch_size: Maximum batch size.
        linger_ms: Batch linger time.
    """
    
    bootstrap_servers: str
    topic_logs: str
    topic_dlq: str
    compression_type: str
    acks: str
    retries: int
    retry_backoff_ms: int
    request_timeout_ms: int
    max_batch_size: int
    linger_ms: int

