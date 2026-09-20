"""Parser manager/factory for log format selection.

Provides a centralized way to select and use log parsers based on format type.
Implements the Factory pattern for parser instantiation.

Notes (project semantics):
- `format` = how the log line is encoded (syslog, clf, json).
- `source` = where the log came from (http, kafka, file).
  In ClickHouse schema, `source` is part of ORDER BY, so it must not be "syslog/clf". [file:178]
"""

from typing import Dict, Type

import structlog

from src.parsers.base import LogParser, ParserError
from src.parsers.syslog import SyslogParser
from src.parsers.clf import CLFParser
from src.models.log_entry import LogEntry

logger = structlog.get_logger()


class ParserManager:
    """Manager for log parsers."""

    PARSER_REGISTRY: Dict[str, Type[LogParser]] = {
        "syslog": SyslogParser,
        "clf": CLFParser,
    }

    def __init__(self) -> None:
        self._parser_cache: Dict[str, LogParser] = {}

    def get_parser(self, format_type: str) -> LogParser:
        format_lower = format_type.lower().strip()

        if format_lower in self._parser_cache:
            return self._parser_cache[format_lower]

        if format_lower not in self.PARSER_REGISTRY:
            supported = ", ".join(self.PARSER_REGISTRY.keys())
            raise ParserError(
                f"Unsupported format '{format_type}'. Supported formats: {supported}, json",
                format_type=format_type,
            )

        parser_class = self.PARSER_REGISTRY[format_lower]
        parser = parser_class()
        self._parser_cache[format_lower] = parser

        logger.debug("parser_created", format=format_lower)
        return parser

    def parse_log(
        self,
        content: str,
        format_type: str,
        source_service: str = "unknown",
        source: str = "http",
    ) -> LogEntry:
        """Parse raw log content into LogEntry model.

        Args:
            content: Raw log content.
            format_type: Log format (syslog, clf, json).
            source_service: Service name (optional override).
            source: Source/transport (http, file, kafka). This maps to ClickHouse `source`. [file:178]
        """
        format_lower = format_type.lower().strip()

        if format_lower == "json":
            raise ParserError(
                "JSON format should be handled directly by API, not via parser",
                format_type="json",
            )

        try:
            parser = self.get_parser(format_lower)
            parsed_data = parser.parse(content)

            # Normalize semantics across parsers
            parsed_data["format"] = format_lower
            parsed_data["source"] = source

            if source_service and source_service != "unknown":
                parsed_data["source_service"] = source_service

            log = LogEntry(**parsed_data)

            logger.info(
                "log_parsed_successfully",
                format=format_lower,
                source=source,
                source_service=log.source_service,
                level=log.level,
            )

            return log

        except ParserError as e:
            logger.warning(
                "log_parse_failed",
                format=format_lower,
                source=source,
                error=str(e),
                content_preview=content[:200] if len(content) > 200 else content,
            )
            # Re-raise to send to DLQ instead of storing unparsed
            raise

        except Exception as e:
            logger.error(
                "log_parse_unexpected_error",
                format=format_lower,
                source=source,
                error=str(e),
                error_type=type(e).__name__,
            )
            # Re-raise to send to DLQ instead of storing unparsed
            raise ParserError(f"Unexpected parsing error: {str(e)}") from e

    def list_supported_formats(self) -> list[str]:
        return list(self.PARSER_REGISTRY.keys()) + ["json"]


# Global parser manager instance
_parser_manager: ParserManager = ParserManager()


def get_parser(format_type: str) -> LogParser:
    return _parser_manager.get_parser(format_type)


def parse_log(
    content: str,
    format_type: str,
    source_service: str = "unknown",
    source: str = "http",
) -> LogEntry:
    return _parser_manager.parse_log(content, format_type, source_service, source)
