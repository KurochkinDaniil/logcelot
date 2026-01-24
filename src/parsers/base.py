"""Base parser interface for log parsing.

Defines the protocol/interface that all log parsers must implement.
This follows the Strategy pattern for parser selection.

Semantics (project-wide):
- `format` describes how the log line is encoded (syslog, clf, json).
- `source` describes where the log arrived from (http, kafka, file).
- `created_at` is the event timestamp if available; it may be None.
  If None, LogEntry will set created_at=ingested_at and event_time_missing=1. [file:178]
"""

from typing import Any, Dict, Protocol


class ParserError(Exception):
    """Base exception for parser-related errors.

    Raised when log parsing fails due to invalid format or data.
    """

    def __init__(
        self,
        message: str,
        original_content: str = "",
        format_type: str = "unknown",
    ):
        super().__init__(message)
        self.message = message
        self.original_content = original_content
        self.format_type = format_type


class LogParser(Protocol):
    """Protocol/interface for log parsers."""

    def parse(self, content: str) -> Dict[str, Any]:
        """Parse raw log content into structured data.

        The resulting dict must be suitable for creating a `LogEntry` model.

        Expected keys (minimum):
        - source_service: Service name (or "unknown")
        - level: Log level (DEBUG, INFO, WARN, ERROR, FATAL)
        - message: Log message
        - format: One of "syslog" / "clf"
        - created_at: Event timestamp (datetime or ISO8601 string) or None

        Optional keys:
        - source: "http" / "kafka" / "file" (can be set by ParserManager)
        - source_host, parsed_message, metadata, user_id, request_id, trace_id, span_id,
          http_method, http_path, http_status, http_response_time_ms,
          error_type, error_stack, is_parsed, parse_errors, etc.

        Raises:
            ParserError: If content cannot be parsed.
        """
        ...
