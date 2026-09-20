"""Pydantic models for DLQ triage reports.

Course Requirement: Type safety and data validation (Senior-level practice).
"""

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class DlqErrorClass(str, Enum):
    """Classification of DLQ errors."""

    INFRA_TRANSIENT = "infra/transient"
    STORAGE_SCHEMA = "storage/schema/config"
    DATA_PARSING = "data/parsing"
    UNKNOWN = "unknown"


class DlqMessage(BaseModel):
    """Sampled message from DLQ topic."""

    partition: int
    offset: int
    key: Optional[str] = None
    timestamp: datetime
    headers: dict[str, str] = Field(default_factory=dict)
    value_preview: str = Field(..., max_length=4096, description="First 4KB of message")
    value_json: Optional[dict[str, Any]] = Field(None, description="Parsed JSON if valid")
    error_class: DlqErrorClass = DlqErrorClass.UNKNOWN
    error_reason: Optional[str] = None


class DlqStats(BaseModel):
    """Statistics about DLQ topic."""

    topic: str
    partitions: int
    total_lag_estimate: int
    consumer_group: str


class ClassificationStats(BaseModel):
    """Statistics per error class."""

    error_class: DlqErrorClass
    count: int
    percentage: float
    top_errors: list[str] = Field(default_factory=list, max_length=5)
    sample_messages: list[DlqMessage] = Field(default_factory=list, max_length=3)


class TriageReport(BaseModel):
    """DLQ triage classification report."""

    sampled_count: int
    classification_stats: list[ClassificationStats]
    dominant_class: DlqErrorClass
    dominant_class_percentage: float


class ClickHouseSignals(BaseModel):
    """ClickHouse data quality signals."""

    parse_errors_last_10min: int
    max_ingested_at: Optional[datetime]
    ingestion_lag_seconds: Optional[float]
    total_logs_count: int


class ReplayReport(BaseModel):
    """Report on replayed messages."""

    replayed: bool
    replayed_count: int
    replay_topic: str
    offsets_committed: bool
    error: Optional[str] = None


class FinalReport(BaseModel):
    """Final consolidated report from DLQ triage DAG."""

    dag_run_id: str
    execution_date: datetime
    dlq_stats: DlqStats
    triage_report: TriageReport
    clickhouse_signals: ClickHouseSignals
    replay_report: ReplayReport
    recommendation: str
