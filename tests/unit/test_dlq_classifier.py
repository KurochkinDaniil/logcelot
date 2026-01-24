"""Unit tests for DLQ error classification.

Course Requirement: Testing demonstrates code quality and reliability.
"""

import pytest

# Import from Airflow DAG utils
import sys
from pathlib import Path

# Add Airflow DAGs directory to Python path
airflow_dags_path = Path(__file__).parent.parent.parent / "infrastructure" / "airflow" / "dags"
sys.path.insert(0, str(airflow_dags_path))

from utils.classifier import classify_error, should_replay
from utils.models import DlqErrorClass


class TestClassifyError:
    """Test suite for classify_error function."""

    def test_infra_timeout_error(self) -> None:
        """Test classification of timeout errors as INFRA_TRANSIENT."""
        error_text = "Connection timeout to ClickHouse server"
        error_class, reason = classify_error(error_text)

        assert error_class == DlqErrorClass.INFRA_TRANSIENT
        assert "timeout" in reason.lower()

    def test_infra_connection_refused(self) -> None:
        """Test classification of connection errors as INFRA_TRANSIENT."""
        error_text = "Connection refused by host clickhouse-01:8123"
        error_class, reason = classify_error(error_text)

        assert error_class == DlqErrorClass.INFRA_TRANSIENT
        assert "connection" in reason.lower()

    def test_infra_http_503(self) -> None:
        """Test classification of HTTP 503 as INFRA_TRANSIENT."""
        error_text = "Upstream returned 503 Service Unavailable"
        error_class, reason = classify_error(error_text)

        assert error_class == DlqErrorClass.INFRA_TRANSIENT
        assert "503" in reason

    def test_infra_network_error(self) -> None:
        """Test classification of network errors as INFRA_TRANSIENT."""
        error_text = "Network error while sending batch to ClickHouse"
        error_class, reason = classify_error(error_text)

        assert error_class == DlqErrorClass.INFRA_TRANSIENT
        assert "network" in reason.lower()

    def test_storage_unrecognized_column(self) -> None:
        """Test classification of schema errors as STORAGE_SCHEMA."""
        error_text = "Unrecognized column 'ingested_at' in table logs"
        error_class, reason = classify_error(error_text)

        assert error_class == DlqErrorClass.STORAGE_SCHEMA
        assert "unrecognized column" in reason.lower()

    def test_storage_table_not_exist(self) -> None:
        """Test classification of missing table as STORAGE_SCHEMA."""
        error_text = "Table logcelot.logs_backup doesn't exist"
        error_class, reason = classify_error(error_text)

        assert error_class == DlqErrorClass.STORAGE_SCHEMA
        assert "doesn't exist" in reason.lower()

    def test_storage_access_denied(self) -> None:
        """Test classification of auth errors as STORAGE_SCHEMA."""
        error_text = "Access denied for user 'logcelot_readonly'"
        error_class, reason = classify_error(error_text)

        assert error_class == DlqErrorClass.STORAGE_SCHEMA
        assert "access denied" in reason.lower()

    def test_data_invalid_json(self) -> None:
        """Test classification of JSON errors as DATA_PARSING."""
        error_text = "Invalid JSON: Expecting value at line 1 column 5"
        error_class, reason = classify_error(error_text)

        assert error_class == DlqErrorClass.DATA_PARSING
        assert "invalid json" in reason.lower()

    def test_data_malformed_message(self) -> None:
        """Test classification of malformed data as DATA_PARSING."""
        error_text = "Malformed log entry: missing required field 'level'"
        error_class, reason = classify_error(error_text)

        assert error_class == DlqErrorClass.DATA_PARSING
        assert "malformed" in reason.lower()

    def test_data_parse_error(self) -> None:
        """Test classification of parse errors as DATA_PARSING."""
        error_text = "Parse error: cannot decode syslog format"
        error_class, reason = classify_error(error_text)

        assert error_class == DlqErrorClass.DATA_PARSING
        assert "parse error" in reason.lower()

    def test_unknown_error(self) -> None:
        """Test classification of unrecognized errors as UNKNOWN."""
        error_text = "Something went terribly wrong"
        error_class, reason = classify_error(error_text)

        assert error_class == DlqErrorClass.UNKNOWN
        assert reason is None

    def test_empty_error_text(self) -> None:
        """Test classification with no error text."""
        error_class, reason = classify_error(None)

        assert error_class == DlqErrorClass.UNKNOWN
        assert reason is None

    def test_error_in_headers(self) -> None:
        """Test classification using error from headers."""
        headers = {"error": "Timeout while waiting for ClickHouse response"}
        error_class, reason = classify_error(None, headers=headers)

        assert error_class == DlqErrorClass.INFRA_TRANSIENT
        assert "timeout" in reason.lower()

    def test_invalid_json_in_message_value(self) -> None:
        """Test classification of invalid JSON in message value."""
        message_value = "{not valid json at all}"
        error_class, reason = classify_error(None, message_value=message_value)

        assert error_class == DlqErrorClass.DATA_PARSING
        assert "json decode error" in reason.lower()

    def test_priority_infra_over_storage(self) -> None:
        """Test that INFRA errors have priority over STORAGE."""
        # Error text contains both patterns
        error_text = "Connection timeout while accessing table logs"
        error_class, reason = classify_error(error_text)

        # Should classify as INFRA (higher priority)
        assert error_class == DlqErrorClass.INFRA_TRANSIENT
        assert "timeout" in reason.lower()


class TestShouldReplay:
    """Test suite for should_replay decision logic."""

    def test_replay_when_dominant_infra(self) -> None:
        """Test replay decision when dominant class is INFRA_TRANSIENT."""
        triage_report = {
            "dominant_class": DlqErrorClass.INFRA_TRANSIENT,
            "dominant_class_percentage": 0.85,
            "classification_stats": [
                {"error_class": DlqErrorClass.INFRA_TRANSIENT, "percentage": 0.85},
                {"error_class": DlqErrorClass.STORAGE_SCHEMA, "percentage": 0.05},
            ],
        }

        should, reason = should_replay(triage_report)

        assert should is True
        assert "safe to replay" in reason.lower()

    def test_no_replay_when_high_storage_errors(self) -> None:
        """Test no replay when storage errors are too high."""
        triage_report = {
            "dominant_class": DlqErrorClass.INFRA_TRANSIENT,
            "dominant_class_percentage": 0.70,
            "classification_stats": [
                {"error_class": DlqErrorClass.INFRA_TRANSIENT, "percentage": 0.70},
                {"error_class": DlqErrorClass.STORAGE_SCHEMA, "percentage": 0.25},
            ],
        }

        should, reason = should_replay(triage_report)

        assert should is False
        assert "fix schema first" in reason.lower()

    def test_no_replay_when_dominant_storage(self) -> None:
        """Test no replay when dominant class is STORAGE_SCHEMA."""
        triage_report = {
            "dominant_class": DlqErrorClass.STORAGE_SCHEMA,
            "dominant_class_percentage": 0.80,
            "classification_stats": [
                {"error_class": DlqErrorClass.STORAGE_SCHEMA, "percentage": 0.80},
            ],
        }

        should, reason = should_replay(triage_report)

        assert should is False
        assert "fix schema" in reason.lower()

    def test_no_replay_when_dominant_data_parsing(self) -> None:
        """Test no replay when dominant class is DATA_PARSING."""
        triage_report = {
            "dominant_class": DlqErrorClass.DATA_PARSING,
            "dominant_class_percentage": 0.75,
            "classification_stats": [
                {"error_class": DlqErrorClass.DATA_PARSING, "percentage": 0.75},
            ],
        }

        should, reason = should_replay(triage_report)

        assert should is False
        assert "bad data" in reason.lower()

    def test_no_replay_when_dominant_unknown(self) -> None:
        """Test no replay when dominant class is UNKNOWN."""
        triage_report = {
            "dominant_class": DlqErrorClass.UNKNOWN,
            "dominant_class_percentage": 0.60,
            "classification_stats": [
                {"error_class": DlqErrorClass.UNKNOWN, "percentage": 0.60},
            ],
        }

        should, reason = should_replay(triage_report)

        assert should is False
        assert "manual triage" in reason.lower()
