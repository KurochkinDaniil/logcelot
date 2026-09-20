"""Application configuration using Pydantic Settings."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Application
    app_env: str = "development"
    log_level: str = "INFO"

    # Kafka
    kafka_bootstrap_servers: str = "kafka:29092"
    kafka_topic_logs: str = "logs.raw"
    kafka_topic_dlq: str = "logs.dead_letter"
    kafka_consumer_group: str = "logcelot-consumer"
    kafka_compression_type: str = "gzip"
    kafka_acks: int = 1
    kafka_retries: int = 3
    kafka_retry_backoff_ms: int = 100
    kafka_request_timeout_ms: int = 30000
    kafka_max_batch_size: int = 16384
    kafka_linger_ms: int = 10

    # ClickHouse
    # В HA окружении сюда передаётся clickhouse-lb и порт 8123 (HTTP). [web:240][web:244]
    clickhouse_host: str = "clickhouse"
    clickhouse_port: int = 9000
    clickhouse_http_port: int = 8123
    clickhouse_database: str = "logcelot"
    clickhouse_user: str = "logcelot_user"
    clickhouse_password: str = "logcelot_pass"

    # Client-side insert behavior.
    # Рекомендуется: async_insert=1 и wait_for_async_insert=1. [web:138][web:142]
    clickhouse_async_insert: bool = True
    clickhouse_wait_for_async_insert: bool = True

    # Target table names (HA schema: logs = Distributed)
    clickhouse_logs_table: str = "logs"

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_workers: int = 4
    api_reload: bool = True

    # Monitoring
    prometheus_enabled: bool = True
    prometheus_port: int = 9090

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


@lru_cache()
def get_settings() -> Settings:
    return Settings()
