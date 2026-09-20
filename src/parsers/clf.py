"""Common Log Format (CLF) parser implementation.

Parses Apache/Nginx access logs in Common Log Format or Combined Log Format.

Apache common:
    %h %l %u %t "%r" %>s %b

Apache combined:
    %h %l %u %t "%r" %>s %b "%{Referer}i" "%{User-agent}i"

Notes:
- %b is "-" if no bytes are sent (use %B to log 0) in Apache. [Apache docs]
- Some servers emit "-" for request when it is unavailable.

References:
    - Apache CLF: https://httpd.apache.org/docs/current/logs.html#common
    - Nginx access_log: http://nginx.org/en/docs/http/ngx_http_log_module.html
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import structlog

from src.parsers.base import LogParser, ParserError

logger = structlog.get_logger()


class CLFParser:
    """Parser for Common Log Format (Apache/Nginx access logs).

    Tolerant behavior goals:
    - Parse common and combined.
    - Accept request '-' (unknown).
    - Accept bytes '-' (unknown).
    - Convert '-' in user/referer/agent to None.
    - Do not silently accept trailing garbage (anchor with $).
    - If timestamp can't be parsed: set created_at=None (not "now").
    """

    # Anchored to end-of-line to avoid accepting trailing junk.
    # request is allowed to be "-" or any string not containing quote.
    CLF_PATTERN = re.compile(
        r'^(?P<remote_addr>\S+)\s+'          # %h
        r'(?P<ident>\S+)\s+'                 # %l (often '-')
        r'(?P<remote_user>\S+)\s+'           # %u (often '-')
        r'\[(?P<timestamp>[^\]]+)\]\s+'      # %t  [10/Oct/2000:13:55:36 -0700]
        r'"(?P<request>[^"]*)"\s+'           # "%r" (can be "-" or empty)
        r'(?P<status>\d{3})\s+'              # %>s
        r'(?P<body_bytes_sent>\d+|-)'        # %b (can be '-')
        r'(?:\s+"(?P<http_referer>[^"]*)"\s+'
        r'"(?P<http_user_agent>[^"]*)")?'
        r'$'
    )

    # Strict request-line parsing: METHOD SP request-target [SP HTTP-version]
    # - We use fullmatch() to avoid silently ignoring tail.
    REQUEST_PATTERN = re.compile(
        r'(?P<method>[A-Z!#$%&\'*+\-.^_`|~0-9]+)\s+'
        r'(?P<path>\S+)'
        r'(?:\s+(?P<protocol>HTTP/\d(?:\.\d)?))?'
        r'$'
    )

    STATUS_TO_LEVEL = {
        range(100, 200): "DEBUG",
        range(200, 300): "INFO",
        range(300, 400): "INFO",
        range(400, 500): "WARN",
        range(500, 600): "ERROR",
    }

    def __init__(self):
        self.pattern = self.CLF_PATTERN
        self.request_pattern = self.REQUEST_PATTERN

    def parse(self, content: str) -> Dict[str, Any]:
        if not content or not isinstance(content, str):
            raise ParserError(
                "CLF content must be a non-empty string",
                original_content=str(content),
                format_type="clf",
            )

        line = content.strip()
        match = self.pattern.match(line)

        if not match:
            logger.warning(
                "clf_parse_failed",
                content_preview=line[:200] if len(line) > 200 else line,
            )
            raise ParserError(
                "Content does not match Common/Combined Log Format. "
                'Expected: remote_addr ident user [timestamp] "request" status bytes ["referer" "user-agent"]',
                original_content=content,
                format_type="clf",
            )

        try:
            g = match.groupdict()

            http_status = int(g["status"])
            level = self._status_to_level(http_status)

            created_at = self._parse_timestamp_or_none(g["timestamp"])

            request_line = g["request"]
            http_method: Optional[str] = None
            http_path: Optional[str] = None
            http_protocol: Optional[str] = None

            # Treat "-" (or empty) as unknown request line
            if request_line and request_line != "-":
                rm = self.request_pattern.fullmatch(request_line)
                if rm:
                    rp = rm.groupdict()
                    http_method = rp.get("method")
                    http_path = rp.get("path")
                    http_protocol = rp.get("protocol")
                else:
                    # Tolerant: leave method/path as None and keep the request in message/metadata
                    pass

            # bytes
            bytes_sent_str = g["body_bytes_sent"]
            bytes_sent = int(bytes_sent_str) if bytes_sent_str != "-" else None

            # user
            remote_user = g["remote_user"]
            user_id = remote_user if remote_user != "-" else None

            # optional combined fields: keep even if empty string, but normalize "-" to None
            referer = g.get("http_referer")
            if referer == "-":
                referer = None

            user_agent = g.get("http_user_agent")
            if user_agent == "-":
                user_agent = None

            metadata: Dict[str, Any] = {
                "remote_addr": g["remote_addr"],
                "ident": None if g["ident"] == "-" else g["ident"],
                "request_raw": request_line if request_line else None,
                "http_protocol": http_protocol,
                "body_bytes_sent": bytes_sent,
            }

            if created_at is None:
                metadata["event_timestamp_missing"] = True
                metadata["event_timestamp_raw"] = g["timestamp"]

            if referer is not None:
                metadata["http_referer"] = referer
            if user_agent is not None:
                metadata["http_user_agent"] = user_agent

            # message
            if http_method and http_path:
                message = f"HTTP {http_status} {http_method} {http_path}"
            elif request_line == "-" or request_line == "":
                message = f"HTTP {http_status} <no-request>"
            else:
                message = f"HTTP {http_status} <unparsed-request>"

            result = {
                "source": "http",
                "source_host": g["remote_addr"],
                "source_service": "access-log",
                "level": level,
                "message": message,
                "created_at": created_at,
                "format": "clf",
                "user_id": user_id,
                "http_method": http_method,
                "http_path": http_path,
                "http_status": http_status,
                "metadata": metadata,
                "is_parsed": True,
            }

            logger.debug(
                "clf_parsed",
                method=http_method,
                path=http_path,
                status=http_status,
                level=level,
            )
            return result

        except Exception as e:
            logger.error(
                "clf_parse_error",
                error=str(e),
                error_type=type(e).__name__,
                content_preview=line[:200],
            )
            raise ParserError(
                f"Failed to parse CLF content: {str(e)}",
                original_content=content,
                format_type="clf",
            ) from e

    def _parse_timestamp_or_none(self, timestamp_str: str) -> Optional[datetime]:
        try:
            dt = datetime.strptime(timestamp_str, "%d/%b/%Y:%H:%M:%S %z")
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
        except ValueError:
            try:
                dt = datetime.strptime(timestamp_str, "%d/%b/%Y:%H:%M:%S")
                return dt
            except ValueError:
                logger.warning("clf_timestamp_parse_failed", timestamp=timestamp_str)
                return None

    def _status_to_level(self, status_code: int) -> str:
        for status_range, level in self.STATUS_TO_LEVEL.items():
            if status_code in status_range:
                return level
        return "INFO"
