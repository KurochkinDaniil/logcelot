"""Unit tests for LogEntry Pydantic models.

Tests cover Data Quality validation requirements from course checklist.

References:
    - Course Requirement: Data Quality & Observability
    - src/models/log_entry.py
"""

import json
from datetime import datetime
from uuid import UUID

import pytest
from pydantic import ValidationError

from src.models.log_entry import (
    LogEntry,
    LogIngestionRequest,
    LogBatchRequest,
    SearchRequest,
)


class TestLogEntryValidation:
    """Test suite for LogEntry validation."""
    
    def test_minimal_valid_log(self):
        """Test creation with minimal required fields."""
        log = LogEntry(
            source="http",
            source_service="test-service",
            level="info",
            message="Test message",
        )
        
        assert log.source == "http"
        assert log.source_service == "test-service"
        assert log.level == "INFO"  # Normalized to uppercase
        assert log.message == "Test message"
        assert isinstance(log.id, UUID)
        assert isinstance(log.created_at, datetime)
    
    def test_level_normalization_uppercase(self):
        """Test that level is normalized to uppercase."""
        log = LogEntry(
            source="http",
            source_service="test",
            level="error",
            message="Test",
        )
        assert log.level == "ERROR"
    
    def test_level_normalization_variations(self):
        """Test that common level variations are accepted."""
        variations = {
            "debug": "DEBUG",
            "DEBUG": "DEBUG",
            "trace": "DEBUG",  # Maps to DEBUG
            "info": "INFO",
            "INFO": "INFO",
            "information": "INFO",
            "warn": "WARN",
            "warning": "WARN",
            "WARN": "WARN",
            "error": "ERROR",
            "ERROR": "ERROR",
            "err": "ERROR",
            "fatal": "FATAL",
            "FATAL": "FATAL",
            "critical": "FATAL",  # Maps to FATAL
            "panic": "FATAL",
        }
        
        for input_level, expected_level in variations.items():
            log = LogEntry(
                source="test",
                source_service="test",
                level=input_level,
                message="Test",
            )
            assert log.level == expected_level, f"Failed for input: {input_level}"
    
    def test_invalid_level_raises_error(self):
        """Test that invalid log level raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            LogEntry(
                source="test",
                source_service="test",
                level="INVALID_LEVEL",
                message="Test",
            )
        
        # Check that ValidationError was raised with appropriate message
        assert "Invalid log level" in str(exc_info.value)
    
    def test_timestamp_iso8601_parsing(self):
        """Test parsing ISO8601 timestamp strings."""
        timestamps = [
            "2024-01-15T10:30:00.123Z",
            "2024-01-15T10:30:00Z",
            "2024-01-15T10:30:00",
            "2024-01-15 10:30:00",
        ]
        
        for ts_str in timestamps:
            log = LogEntry(
                source="test",
                source_service="test",
                level="info",
                message="Test",
                created_at=ts_str,
            )
            assert isinstance(log.created_at, datetime)
    
    def test_timestamp_unix_parsing(self):
        """Test parsing Unix timestamp (int and float)."""
        # 2024-01-15 10:30:00 UTC
        unix_timestamp = 1705318200
        
        log = LogEntry(
            source="test",
            source_service="test",
            level="info",
            message="Test",
            created_at=unix_timestamp,
        )
        
        assert log.created_at.year == 2024
        assert log.created_at.month == 1
        assert log.created_at.day == 15
    
    def test_timestamp_datetime_passthrough(self):
        """Test that datetime objects are passed through."""
        dt = datetime(2024, 1, 15, 10, 30, 0)
        
        log = LogEntry(
            source="test",
            source_service="test",
            level="info",
            message="Test",
            created_at=dt,
        )
        
        assert log.created_at == dt
    
    def test_invalid_timestamp_fallback_to_ingested(self):
        """Test that invalid timestamps fallback to ingested_at."""
        log = LogEntry(
            source="test",
            source_service="test",
            level="info",
            message="Test",
            created_at="invalid-timestamp",
        )
        # Invalid timestamp should result in created_at being set to ingested_at
        assert log.created_at is not None
        assert log.event_time_missing is True
    
    def test_metadata_dict_accepted(self):
        """Test that dict metadata is accepted."""
        metadata = {"user_id": "12345", "action": "login"}
        
        log = LogEntry(
            source="test",
            source_service="test",
            level="info",
            message="Test",
            metadata=metadata,
        )
        
        assert log.metadata == metadata
    
    def test_metadata_json_string_parsed(self):
        """Test that JSON string metadata is parsed."""
        metadata_json = '{"user_id": "12345", "action": "login"}'
        
        log = LogEntry(
            source="test",
            source_service="test",
            level="info",
            message="Test",
            metadata=metadata_json,
        )
        
        assert log.metadata == {"user_id": "12345", "action": "login"}
    
    def test_metadata_invalid_json_raises_error(self):
        """Test that invalid JSON in metadata raises error."""
        with pytest.raises(ValidationError):
            LogEntry(
                source="test",
                source_service="test",
                level="info",
                message="Test",
                metadata='{"invalid": json}',  # Missing quotes
            )
    
    def test_field_extraction_from_metadata(self):
        """Test auto-extraction of common fields from metadata."""
        log = LogEntry(
            source="test",
            source_service="test",
            level="info",
            message="Test",
            metadata={
                "trace_id": "abc-123-xyz",
                "user_id": "12345",
                "request_id": "req-999",
                "http_method": "POST",
                "http_status": 200,
                "response_time_ms": 150,
            },
        )
        
        # Verify fields were extracted
        assert log.trace_id == "abc-123-xyz"
        assert log.user_id == "12345"
        assert log.request_id == "req-999"
        assert log.http_method == "POST"
        assert log.http_status == 200
        assert log.http_response_time_ms == 150
    
    def test_explicit_fields_not_overwritten(self):
        """Test that explicitly set fields are not overwritten by metadata."""
        log = LogEntry(
            source="test",
            source_service="test",
            level="info",
            message="Test",
            trace_id="explicit-trace-id",
            metadata={
                "trace_id": "metadata-trace-id",
            },
        )
        
        # Explicit field should take precedence
        assert log.trace_id == "explicit-trace-id"
    
    def test_http_status_validation(self):
        """Test that HTTP status is validated (100-599)."""
        # Valid status
        log = LogEntry(
            source="test",
            source_service="test",
            level="info",
            message="Test",
            http_status=200,
        )
        assert log.http_status == 200
        
        # Invalid status (too low)
        with pytest.raises(ValidationError):
            LogEntry(
                source="test",
                source_service="test",
                level="info",
                message="Test",
                http_status=99,
            )
        
        # Invalid status (too high)
        with pytest.raises(ValidationError):
            LogEntry(
                source="test",
                source_service="test",
                level="info",
                message="Test",
                http_status=600,
            )


class TestToClickHouseDict:
    """Test suite for to_clickhouse_dict() method."""
    
    def test_basic_conversion(self):
        """Test conversion to ClickHouse dict format."""
        log = LogEntry(
            source="http",
            source_service="api",
            level="info",
            message="Test message",
        )
        
        ch_dict = log.to_clickhouse_dict()
        
        # Check UUID is converted to string
        assert isinstance(ch_dict["id"], str)
        assert len(ch_dict["id"]) == 36  # UUID string length
        
        # Check datetime is kept as datetime object (for clickhouse-connect)
        from datetime import datetime
        assert isinstance(ch_dict["created_at"], datetime)
        
        # Check basic fields
        assert ch_dict["source"] == "http"
        assert ch_dict["source_service"] == "api"
        assert ch_dict["level"] == "INFO"
        assert ch_dict["message"] == "Test message"
    
    def test_metadata_serialized_to_json(self):
        """Test that metadata dict is serialized to JSON string."""
        log = LogEntry(
            source="test",
            source_service="test",
            level="info",
            message="Test",
            metadata={"user_id": "12345", "nested": {"key": "value"}},
        )
        
        ch_dict = log.to_clickhouse_dict()
        
        # Metadata should be JSON string
        assert isinstance(ch_dict["metadata"], str)
        
        # Verify it can be parsed back
        parsed = json.loads(ch_dict["metadata"])
        assert parsed["user_id"] == "12345"
        assert parsed["nested"]["key"] == "value"
    
    def test_optional_fields_handling(self):
        """Test that optional fields are handled correctly."""
        log = LogEntry(
            source="test",
            source_service="test",
            level="info",
            message="Test",
            trace_id="abc-123",
            http_status=404,
        )
        
        ch_dict = log.to_clickhouse_dict()
        
        # Set fields should be present
        assert ch_dict["trace_id"] == "abc-123"
        assert ch_dict["http_status"] == 404
        
        # Unset optional fields should be None
        assert ch_dict["user_id"] is None
        assert ch_dict["error_stack"] is None


class TestLogIngestionRequest:
    """Test suite for LogIngestionRequest model."""
    
    def test_valid_request(self):
        """Test valid ingestion request."""
        request = LogIngestionRequest(
            source="http",
            source_service="api",
            level="error",
            message="Test error",
            payload={"user_id": "12345"},
        )
        
        assert request.source == "http"
        assert request.source_service == "api"
        assert request.level == "error"
        assert request.message == "Test error"
        assert request.payload == {"user_id": "12345"}
    
    def test_to_log_entry_conversion(self):
        """Test conversion to LogEntry."""
        request = LogIngestionRequest(
            source="http",
            source_service="api",
            level="info",
            message="Test",
            payload={"key": "value"},
            trace_id="trace-123",
        )
        
        log = request.to_log_entry()
        
        assert isinstance(log, LogEntry)
        assert log.source == "http"
        assert log.source_service == "api"
        assert log.level == "INFO"  # Normalized
        assert log.message == "Test"
        assert log.metadata == {"key": "value"}
        assert log.trace_id == "trace-123"
        assert log.format == "json"
        assert log.is_parsed is True


class TestLogBatchRequest:
    """Test suite for LogBatchRequest model."""
    
    def test_valid_batch(self):
        """Test valid batch request."""
        batch = LogBatchRequest(
            logs=[
                LogIngestionRequest(
                    source="http",
                    source_service="api",
                    level="info",
                    message="Log 1",
                ),
                LogIngestionRequest(
                    source="http",
                    source_service="api",
                    level="info",
                    message="Log 2",
                ),
            ]
        )
        
        assert len(batch.logs) == 2
        assert batch.logs[0].message == "Log 1"
        assert batch.logs[1].message == "Log 2"
    
    def test_empty_batch_rejected(self):
        """Test that empty batch is rejected."""
        with pytest.raises(ValidationError):
            LogBatchRequest(logs=[])
    
    def test_batch_size_limit(self):
        """Test that batch size is limited to 1000."""
        # Valid: exactly 1000
        batch = LogBatchRequest(
            logs=[
                LogIngestionRequest(
                    source="test",
                    source_service="test",
                    level="info",
                    message=f"Log {i}",
                )
                for i in range(1000)
            ]
        )
        assert len(batch.logs) == 1000
        
        # Invalid: 1001
        with pytest.raises(ValidationError):
            LogBatchRequest(
                logs=[
                    LogIngestionRequest(
                        source="test",
                        source_service="test",
                        level="info",
                        message=f"Log {i}",
                    )
                    for i in range(1001)
                ]
            )


class TestSearchRequest:
    """Test suite for SearchRequest model."""
    
    def test_valid_search_request(self):
        """Test valid search request."""
        search = SearchRequest(
            source_service="api",
            level="error",
            time_from=datetime(2024, 1, 15, 10, 0, 0),
            time_to=datetime(2024, 1, 15, 11, 0, 0),
            limit=50,
        )
        
        assert search.source_service == "api"
        assert search.level == "ERROR"  # Normalized
        assert search.limit == 50
    
    def test_default_pagination(self):
        """Test default pagination values."""
        search = SearchRequest()
        
        assert search.offset == 0
        assert search.limit == 100
    
    def test_limit_boundaries(self):
        """Test limit validation (1-1000)."""
        # Valid: 1
        search = SearchRequest(limit=1)
        assert search.limit == 1
        
        # Valid: 1000
        search = SearchRequest(limit=1000)
        assert search.limit == 1000
        
        # Invalid: 0
        with pytest.raises(ValidationError):
            SearchRequest(limit=0)
        
        # Invalid: 1001
        with pytest.raises(ValidationError):
            SearchRequest(limit=1001)
    
    def test_negative_offset_rejected(self):
        """Test that negative offset is rejected."""
        with pytest.raises(ValidationError):
            SearchRequest(offset=-1)


class TestDataQualityRequirements:
    """Test suite for Data Quality requirements from course checklist."""
    
    def test_invalid_log_creates_parse_error(self):
        """Test that invalid logs can be marked with parse errors."""
        log = LogEntry(
            source="kafka",
            source_service="unknown",
            level="error",
            message="Unparseable log",
            is_parsed=False,
            parse_errors="JSON decode error: Expecting value at line 1",
        )
        
        assert log.is_parsed is False
        assert "JSON decode error" in log.parse_errors
    
    def test_valid_log_has_no_parse_errors(self):
        """Test that valid logs have no parse errors."""
        log = LogEntry(
            source="http",
            source_service="api",
            level="info",
            message="Valid log",
        )
        
        assert log.is_parsed is True
        assert log.parse_errors is None
    
    def test_scd_type_0_immutability(self):
        """Test that LogEntry represents immutable events (SCD-0).
        
        Note: This is architectural - logs should never be updated in ClickHouse.
        This test documents the design decision.
        """
        log = LogEntry(
            source="test",
            source_service="test",
            level="info",
            message="Immutable log event",
        )
        
        # Log has a unique ID and timestamp
        assert log.id is not None
        assert log.created_at is not None
        
        # In production: No UPDATE statements should exist for logs table
        # Only INSERT (append-only) and DELETE (via TTL)

