"""Configuration for log generator service."""

from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class GeneratorSettings(BaseSettings):
    """Generator service configuration."""

    # Kafka
    kafka_bootstrap_servers: str = "kafka-1:29092,kafka-2:29092,kafka-3:29092"
    kafka_topic: str = "logs.raw"
    
    # Generation rates
    generator_rps: int = 100
    generator_scenario: str = "normal"
    
    # HTTP API
    generator_api_host: str = "0.0.0.0"
    generator_api_port: int = 8080
    
    # Scenario configs
    error_spike_count: int = 500
    error_spike_duration: int = 30
    dead_service_duration: int = 300
    parse_error_ratio: float = 0.1
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


def get_generator_settings() -> GeneratorSettings:
    """Get generator settings singleton."""
    return GeneratorSettings()
