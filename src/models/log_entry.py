"""
Pydantic models for log entry validation.

Implements Data Quality validation as per course requirements.
All logs are SCD-0 (immutable facts) as defined in the architecture.
"""

import json
from datetime import datetime
from typing import Any, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator, model_validator
import structlog

logger = structlog.get_logger()


def _utcnow() -> datetime:
    return datetime.utcnow()


class LogEntry(BaseModel):
    """Validated log entry model for ingestion pipeline."""

    # Primary Identifiers
    id: UUID = Field(default_factory=uuid4, description="Unique log identifier")

    # Event time (original timestamp if present, else fallback to ingested_at)
    created_at: Optional[datetime] = Field(
        default=None,
        description="Event timestamp (original if available). If missing/invalid, will fallback to ingested_at."
    )

    # Ingestion time (always set at the system boundary)
    ingested_at: datetime = Field(
        default_factory=_utcnow,
        description="Ingestion timestamp (UTC)."
    )

    # Data quality: 1 if created_at was missing/invalid and replaced with ingested_at
    event_time_missing: bool = Field(
        default=False,
        description="True if event timestamp is missing/invalid and created_at was set to ingested_at."
    )

    # Source Information
    source: str = Field(..., min_length=1, max_length=50, description="Log source type: http, kafka, file")
    source_host: Optional[str] = Field(None, max_length=255, description="Hostname or IP address")
    source_service: str = Field(..., min_length=1, max_length=100, description="Service name (e.g., auth-api, nginx)")

    # Log Level (Course Requirement: Validation)
    level: str = Field(..., max_length=20, description="Log severity: DEBUG, INFO, WARN, ERROR, FATAL")

    # Message Content
    message: str = Field(..., min_length=1, max_length=10000, description="Raw log message (max 10KB)")
    parsed_message: Optional[str] = Field(None, max_length=10000, description="Normalized message after parsing")

    # Format Information
    format: str = Field(default="json", max_length=20, description="Original format: json, syslog, clf")

    # Metadata (Course Requirement: Flexible schema)
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional structured data as JSON")

    # Extracted Common Fields (OpenTelemetry compatible)
    user_id: Optional[str] = Field(None, max_length=255)
    request_id: Optional[str] = Field(None, max_length=255)
    trace_id: Optional[str] = Field(None, max_length=255)
    span_id: Optional[str] = Field(None, max_length=255)

    # HTTP-specific fields (for access logs)
    http_method: Optional[str] = Field(None, max_length=10)
    http_path: Optional[str] = Field(None, max_length=2000)
    http_status: Optional[int] = Field(None, ge=100, le=599)
    http_response_time_ms: Optional[int] = Field(None, ge=0)

    # Error Tracking
    error_type: Optional[str] = Field(None, max_length=100)
    error_stack: Optional[str] = Field(None, max_length=50000)

    # Data Quality Flags
    is_parsed: bool = Field(default=True, description="Whether log was successfully parsed")
    parse_errors: Optional[str] = Field(None, description="Parsing error messages (sent to DLQ if present)")

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            UUID: lambda v: str(v),
        }

    @field_validator("level", mode="before")
    @classmethod
    def normalize_level(cls, v: str) -> str:
        v_upper = v.upper().strip()

        level_mapping = {
            "DEBUG": "DEBUG",
            "TRACE": "DEBUG",
            "INFO": "INFO",
            "INFORMATION": "INFO",
            "WARN": "WARN",
            "WARNING": "WARN",
            "ERROR": "ERROR",
            "ERR": "ERROR",
            "FATAL": "FATAL",
            "CRITICAL": "FATAL",
            "PANIC": "FATAL",
        }

        if v_upper not in level_mapping:
            logger.warning("invalid_log_level", level=v, allowed=list(level_mapping.keys()))
            raise ValueError(
                f"Invalid log level '{v}'. Allowed: DEBUG, INFO, WARN/WARNING, ERROR, FATAL/CRITICAL"
            )

        return level_mapping[v_upper]

    @field_validator("created_at", mode="before")
    @classmethod
    def parse_timestamp_or_none(cls, v: Any) -> Optional[datetime]:
        """Parse created_at, but allow None / invalid values (we'll fallback later)."""
        if v is None:
            return None

        if isinstance(v, datetime):
            return v

        if isinstance(v, (int, float)):
            try:
                return datetime.utcfromtimestamp(v)
            except (ValueError, OSError):
                return None

        if isinstance(v, str):
            s = v.strip()
            if not s or s == "-":
                return None

            # ISO8601 string
            try:
                s_clean = s.replace("Z", "+00:00") if s.endswith("Z") else s
                return datetime.fromisoformat(s_clean)
            except ValueError:
                formats = [
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                    "%Y-%m-%dT%H:%M:%SZ",
                    "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%d %H:%M:%S",
                ]
                for fmt in formats:
                    try:
                        return datetime.strptime(s, fmt)
                    except ValueError:
                        continue
                return None

        return None

    @field_validator("metadata", mode="before")
    @classmethod
    def parse_metadata(cls, v: Any) -> dict[str, Any]:
        if v is None:
            return {}
        if isinstance(v, dict):
            return v
        if isinstance(v, str):
            try:
                return json.loads(v)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in metadata: {e}") from e
        raise ValueError(f"Metadata must be dict or JSON string, got {type(v)}")

    @model_validator(mode="after")
    def apply_event_time_fallback(self) -> "LogEntry":
        """
        If created_at is missing/invalid -> set it to ingested_at and mark flag.

        This keeps created_at non-null for ClickHouse partitioning / TTL,
        while preserving the fact that original event time was missing.
        """
        if self.created_at is None:
            self.created_at = self.ingested_at
            self.event_time_missing = True

        return self

    @model_validator(mode="after")
    def extract_fields_from_metadata(self) -> "LogEntry":
        if not self.metadata:
            return self

        if not self.trace_id and "trace_id" in self.metadata:
            self.trace_id = str(self.metadata["trace_id"])
        if not self.span_id and "span_id" in self.metadata:
            self.span_id = str(self.metadata["span_id"])
        if not self.request_id and "request_id" in self.metadata:
            self.request_id = str(self.metadata["request_id"])
        if not self.user_id and "user_id" in self.metadata:
            self.user_id = str(self.metadata["user_id"])

        if not self.http_method and "http_method" in self.metadata:
            self.http_method = str(self.metadata["http_method"])

        if not self.http_status and "http_status" in self.metadata:
            try:
                self.http_status = int(self.metadata["http_status"])
            except (ValueError, TypeError):
                pass

        if not self.http_response_time_ms and "response_time_ms" in self.metadata:
            try:
                self.http_response_time_ms = int(self.metadata["response_time_ms"])
            except (ValueError, TypeError):
                pass

        return self

    def to_clickhouse_dict(self) -> dict[str, Any]:
        """Convert log entry to ClickHouse INSERT format."""
        return {
            "id": str(self.id),
            "created_at": self.created_at,
            "ingested_at": self.ingested_at,
            "event_time_missing": int(self.event_time_missing),

            "source": self.source,
            "source_host": self.source_host or "",
            "source_service": self.source_service,
            "level": self.level,
            "message": self.message,
            "parsed_message": self.parsed_message or "",
            "format": self.format,
            "metadata": json.dumps(self.metadata),

            "user_id": self.user_id,
            "request_id": self.request_id,
            "trace_id": self.trace_id,
            "span_id": self.span_id,

            "http_method": self.http_method,
            "http_path": self.http_path,
            "http_status": self.http_status,
            "http_response_time_ms": self.http_response_time_ms,

            "error_type": self.error_type,
            "error_stack": self.error_stack,

            "is_parsed": int(self.is_parsed),  # UInt8 in CH schema
            "parse_errors": self.parse_errors,
        }


class LogIngestionRequest(BaseModel):
    """API request model for POST /logs endpoint."""

    source: str = Field(..., min_length=1, max_length=50)
    source_service: str = Field(..., min_length=1, max_length=100)
    level: str = Field(..., min_length=1, max_length=10)
    message: str = Field(..., min_length=1, max_length=10000)
    payload: Optional[dict[str, Any]] = Field(None, description="Optional structured data")

    source_host: Optional[str] = Field(None, max_length=255)
    trace_id: Optional[str] = Field(None, max_length=255)

    # Optional: allow client to send event time; if absent, we'll fallback
    created_at: Optional[Any] = Field(
        None,
        description="Optional event timestamp (ISO8601 / Unix seconds). If missing/invalid -> fallback to ingested_at."
    )

    def to_log_entry(self) -> LogEntry:
        return LogEntry(
            source=self.source,
            source_host=self.source_host,
            source_service=self.source_service,
            level=self.level,
            message=self.message,
            metadata=self.payload or {},
            trace_id=self.trace_id,
            format="json",
            is_parsed=True,
            created_at=self.created_at,
        )


class LogBatchRequest(BaseModel):
    logs: list[LogIngestionRequest] = Field(..., min_length=1, max_length=1000)


class SearchRequest(BaseModel):
    source: Optional[str] = Field(None, max_length=50)
    source_service: Optional[str] = Field(None, max_length=100)
    level: Optional[str] = Field(None, max_length=10)

    time_from: Optional[datetime] = Field(None, description="Start time (inclusive). Defaults to 24h ago.")
    time_to: Optional[datetime] = Field(None, description="End time (inclusive). Defaults to now.")

    query: Optional[str] = Field(None, max_length=1000, description="Full-text search in message field")
    trace_id: Optional[str] = Field(None, max_length=255)
    user_id: Optional[str] = Field(None, max_length=255)

    event_time_missing: Optional[bool] = Field(
        None,
        description="If true/false, filter by missing event time flag."
    )

    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=100, ge=1, le=1000)

    @field_validator("level")
    @classmethod
    def normalize_search_level(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        return v.upper()

    @model_validator(mode="after")
    def set_default_time_range(self) -> "SearchRequest":
        from datetime import timedelta

        if self.time_to is None:
            self.time_to = datetime.utcnow()
        if self.time_from is None:
            self.time_from = self.time_to - timedelta(hours=24)
        return self


class LogSearchResponse(BaseModel):
    total: int = Field(..., ge=0)
    offset: int = Field(..., ge=0)
    limit: int = Field(..., ge=1)
    items: list[dict[str, Any]] = Field(...)
    query_time_ms: int = Field(..., ge=0)
