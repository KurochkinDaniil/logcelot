"""
Syslog RFC5424 parser implementation.

Parses syslog messages following RFC5424 format:

    <PRI>VERSION TIMESTAMP HOSTNAME APP-NAME PROCID MSGID STRUCTURED-DATA [MSG]

RFC5424 ABNF:
    SYSLOG-MSG = HEADER SP STRUCTURED-DATA [SP MSG]
    HEADER = PRI VERSION SP TIMESTAMP SP HOSTNAME SP APP-NAME SP PROCID SP MSGID
    STRUCTURED-DATA = "-" / 1*SD-ELEMENT
    SD-ELEMENT = "[" SD-ID *(SP SD-PARAM) "]"
    SD-PARAM = PARAM-NAME "=" %d34 PARAM-VALUE %d34
    Inside PARAM-VALUE, characters '"', '\' and ']' MUST be escaped.

References:
    - RFC5424 (RFC Editor): https://www.rfc-editor.org/rfc/rfc5424
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import structlog

from src.parsers.base import LogParser, ParserError

logger = structlog.get_logger()


class SyslogParser:
    """Parser for RFC5424 syslog format.

    This implementation aims to be robust for real-world RFC5424-ish logs:
    - Correct HEADER parsing.
    - Correct STRUCTURED-DATA extraction including spaces inside quoted values.
    - Optional tolerant handling of a common deviation: space between SD-ELEMENTs ('] [').
    - MSG part is optional per RFC.

    Args:
        strict: If True, enforce more RFC correctness. If False, be tolerant.
        allow_sd_element_separator_space: If True, treat `] [` as continuing STRUCTURED-DATA
            (common in some producers). If False, treat it as end of SD and start of MSG
            (RFC-accurate).
    """

    # Parse only the header part with a regex, then parse STRUCTURED-DATA+MSG with a small scanner.
    # We intentionally keep this regex simple and deterministic.
    _HEADER_RE = re.compile(
        r"^<(?P<priority>\d{1,3})>"
        r"(?P<version>\d{1,3})\s+"
        r"(?P<timestamp>\S+)\s+"
        r"(?P<hostname>\S+)\s+"
        r"(?P<app_name>\S+)\s+"
        r"(?P<proc_id>\S+)\s+"
        r"(?P<msg_id>\S+)\s+"
        r"(?P<rest>.*)$"
    )

    SEVERITY_TO_LEVEL = {
        0: "FATAL",  # Emergency
        1: "FATAL",  # Alert
        2: "FATAL",  # Critical
        3: "ERROR",  # Error
        4: "WARN",   # Warning
        5: "INFO",   # Notice
        6: "INFO",   # Informational
        7: "DEBUG",  # Debug
    }

    def __init__(
        self,
        strict: bool = False,
        allow_sd_element_separator_space: bool = True,
    ):
        self.strict = strict
        self.allow_sd_element_separator_space = allow_sd_element_separator_space

    def parse(self, content: str) -> Dict[str, Any]:
        if not content or not isinstance(content, str):
            raise ParserError(
                "Syslog content must be a non-empty string",
                original_content=str(content),
                format_type="syslog",
            )

        line = content.strip()

        m = self._HEADER_RE.match(line)
        if not m:
            logger.warning("syslog_parse_failed", content_preview=line[:200])
            raise ParserError(
                "Content does not match RFC5424 syslog format header. "
                "Expected: <priority>version timestamp hostname app-name proc-id msg-id ...",
                original_content=content,
                format_type="syslog",
            )

        groups = m.groupdict()

        try:
            priority = int(groups["priority"])
            version = int(groups["version"])
        except ValueError as e:
            raise ParserError(
                f"Invalid numeric fields in syslog header: {e}",
                original_content=content,
                format_type="syslog",
            ) from e

        # RFC5424 says PRIVAL range is 0..191.
        # If strict -> error; if not -> still parse but keep metadata.
        if self.strict and not (0 <= priority <= 191):
            raise ParserError(
                f"PRIVAL out of range (0..191): {priority}",
                original_content=content,
                format_type="syslog",
            )

        # RFC5424 uses VERSION=1. In tolerant mode keep it as metadata.
        if self.strict and version != 1:
            raise ParserError(
                f"Unsupported RFC5424 VERSION={version} (expected 1)",
                original_content=content,
                format_type="syslog",
            )

        severity = priority % 8
        facility = priority // 8
        level = self.SEVERITY_TO_LEVEL.get(severity, "INFO")

        timestamp_str = groups["timestamp"]
        created_at = self._parse_timestamp_or_none(timestamp_str)

        rest = groups["rest"]

        # According to RFC: after MSGID must be SP STRUCTURED-DATA [SP MSG]
        # We parse rest into structured_data_raw + msg (optional).
        structured_data_raw, msg, sd_parse_error = self._split_structured_data_and_msg(rest)

        result: Dict[str, Any] = {
            "source": "syslog",
            "source_host": groups["hostname"] if groups["hostname"] != "-" else None,
            "source_service": groups["app_name"] if groups["app_name"] != "-" else "unknown",
            "level": level,
            "message": (msg or "").strip(),
            "created_at": created_at,
            "format": "syslog",
            "metadata": {
                "syslog_priority": priority,
                "syslog_severity": severity,
                "syslog_facility": facility,
                "syslog_version": version,
                "proc_id": groups["proc_id"] if groups["proc_id"] != "-" else None,
                "msg_id": groups["msg_id"] if groups["msg_id"] != "-" else None,
                "structured_data": None if structured_data_raw == "-" else structured_data_raw,
            },
            "is_parsed": True,
        }

        if created_at is None:
            result["metadata"]["event_timestamp_missing"] = True
            result["metadata"]["event_timestamp_raw"] = timestamp_str

        if sd_parse_error:
            # In tolerant mode we keep the log, but mark metadata.
            result["metadata"]["structured_data_parse_error"] = sd_parse_error

            if self.strict:
                raise ParserError(
                    f"Malformed STRUCTURED-DATA: {sd_parse_error}",
                    original_content=content,
                    format_type="syslog",
                )

        logger.debug(
            "syslog_parsed",
            app_name=groups["app_name"],
            level=level,
            severity=severity,
        )
        return result

    def _parse_timestamp_or_none(self, timestamp_str: str) -> Optional[datetime]:
        # RFC5424: TIMESTAMP can be NILVALUE "-"
        if timestamp_str == "-":
            return None

        try:
            return self._parse_timestamp(timestamp_str)
        except ValueError as e:
            logger.warning("syslog_timestamp_parse_failed", timestamp=timestamp_str, error=str(e))
            if self.strict:
                raise
            return None

    def _parse_timestamp(self, timestamp_str: str) -> datetime:
        """
        Parse RFC5424 timestamp to datetime.
        """
        if timestamp_str.endswith("Z"):
            timestamp_str = timestamp_str[:-1] + "+00:00"

        dt = datetime.fromisoformat(timestamp_str)

        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)

        return dt

    def _split_structured_data_and_msg(self, rest: str) -> Tuple[str, str, Optional[str]]:
        """
        Split `rest` into (structured_data_raw, msg, sd_parse_error).

        `rest` is what comes after MSGID + space.
        It must begin with '-' or '[' per RFC.

        This function:
        - Reads '-' as NILVALUE: no structured-data, MSG starts after optional SP.
        - Reads one or multiple SD-ELEMENTs: [id a="b" c="d"][id2 x="y"]
        - In tolerant mode, also allows `] [` (space between elements), and treats it as still SD.
        """
        s = rest.lstrip()
        if s == "":
            # Not valid RFC (must have STRUCTURED-DATA), but in tolerant mode treat as empty SD+MSG.
            if self.strict:
                return "-", "", "Missing STRUCTURED-DATA field"
            return "-", "", "Missing STRUCTURED-DATA field"

        if s[0] == "-":
            # STRUCTURED-DATA = NILVALUE
            # After that may be SP MSG (optional).
            after = s[1:]
            if after.startswith(" "):
                return "-", after.lstrip(), None
            # No MSG
            return "-", "", None

        if s[0] != "[":
            # RFC says it must be '-' or '['
            return "-", s, "STRUCTURED-DATA must start with '-' or '['"

        i = 0
        in_quotes = False
        escape = False
        open_brackets = 0
        end_idx = None

        # We'll scan until we finish the last SD-ELEMENT.
        # SD-ELEMENT ends at ']' that closes the last opened '[' (open_brackets back to 0).
        # Inside PARAM-VALUE quotes, ']' may appear escaped, and quotes may contain spaces.
        while i < len(s):
            ch = s[i]

            if escape:
                escape = False
                i += 1
                continue

            if ch == "\\":
                # escape allowed inside PARAM-VALUE; we don't validate escapes strictly here.
                escape = True
                i += 1
                continue

            if ch == '"':
                in_quotes = not in_quotes
                i += 1
                continue

            if not in_quotes:
                if ch == "[":
                    open_brackets += 1
                elif ch == "]":
                    open_brackets -= 1
                    if open_brackets == 0:
                        # We closed a SD-ELEMENT and (maybe) the whole STRUCTURED-DATA sequence.
                        # Decide if another SD-ELEMENT follows.
                        j = i + 1
                        if j >= len(s):
                            end_idx = j
                            break

                        if s[j] == "[":
                            # RFC-correct: immediately next SD-ELEMENT
                            i = j
                            continue

                        if self.allow_sd_element_separator_space:
                            # Tolerant: allow `] [` to be treated as still structured-data.
                            # i points to ']' of last element. We look ahead for optional spaces then '['.
                            k = j
                            while k < len(s) and s[k] == " ":
                                k += 1
                            if k < len(s) and s[k] == "[":
                                # Treat spaces as part of SD (store raw SD exactly as received).
                                i = k
                                continue

                        # Otherwise SD ends here, MSG begins after optional spaces.
                        end_idx = j
                        break

            if open_brackets < 0:
                return "-", s, "Unexpected ']' in STRUCTURED-DATA"

            i += 1

        if end_idx is None:
            return "-", s, "Unterminated STRUCTURED-DATA (missing closing ']')"

        structured_data_raw = s[:end_idx].rstrip()
        msg = s[end_idx:].lstrip()
        return structured_data_raw, msg, None
