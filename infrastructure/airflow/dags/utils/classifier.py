"""Error classification logic for DLQ triage.

Course Requirement: Data Quality monitoring and error categorization.
"""

import re
from typing import Optional

from .models import DlqErrorClass


# Classification patterns (compiled for performance)
INFRA_PATTERNS = [
    re.compile(r"timeout", re.IGNORECASE),
    re.compile(r"timed out", re.IGNORECASE),
    re.compile(r"connection", re.IGNORECASE),
    re.compile(r"temporarily disabled", re.IGNORECASE),
    re.compile(r"\b502\b"),
    re.compile(r"\b503\b"),
    re.compile(r"\b504\b"),
    re.compile(r"upstream timed out", re.IGNORECASE),
    re.compile(r"connection refused", re.IGNORECASE),
    re.compile(r"connection reset", re.IGNORECASE),
    re.compile(r"network error", re.IGNORECASE),
]

STORAGE_PATTERNS = [
    re.compile(r"unrecognized column", re.IGNORECASE),
    re.compile(r"unknown column", re.IGNORECASE),
    re.compile(r"table.*doesn't exist", re.IGNORECASE),
    re.compile(r"access denied", re.IGNORECASE),
    re.compile(r"authentication", re.IGNORECASE),
    re.compile(r"permission denied", re.IGNORECASE),
    re.compile(r"schema", re.IGNORECASE),
    re.compile(r"type mismatch", re.IGNORECASE),
    re.compile(r"cannot parse", re.IGNORECASE),
]

DATA_PATTERNS = [
    re.compile(r"invalid json", re.IGNORECASE),
    re.compile(r"json decode", re.IGNORECASE),
    re.compile(r"malformed", re.IGNORECASE),
    re.compile(r"validation error", re.IGNORECASE),
    re.compile(r"parse error", re.IGNORECASE),
    re.compile(r"decode error", re.IGNORECASE),
]


def classify_error(
    error_text: Optional[str],
    headers: Optional[dict[str, str]] = None,
    message_value: Optional[str] = None,
) -> tuple[DlqErrorClass, Optional[str]]:
    """Classify DLQ error into categories.

    Uses pattern matching on error text, headers, and message value
    to determine the root cause of DLQ entry.

    Args:
        error_text: Error message or reason from DLQ headers/metadata.
        headers: Kafka message headers (may contain error info).
        message_value: Original message value (for data validation).

    Returns:
        Tuple of (error_class, reason_snippet).

    Example:
        >>> classify_error("Connection timeout to ClickHouse")
        (DlqErrorClass.INFRA_TRANSIENT, "Connection timeout")

        >>> classify_error("Unrecognized column 'ingested_at'")
        (DlqErrorClass.STORAGE_SCHEMA, "Unrecognized column")
    """
    # Combine all text sources for classification
    text_parts: list[str] = []

    if error_text:
        text_parts.append(error_text)

    if headers:
        # Check common error headers
        for key in ["error", "exception", "reason", "error_message"]:
            if key in headers:
                text_parts.append(headers[key])

    combined_text = " ".join(text_parts)

    # Classify by patterns (priority: infra -> storage -> data)
    # Infrastructure errors (transient, retryable)
    for pattern in INFRA_PATTERNS:
        match = pattern.search(combined_text)
        if match:
            return DlqErrorClass.INFRA_TRANSIENT, match.group(0)

    # Storage/Schema errors (configuration, schema mismatch)
    for pattern in STORAGE_PATTERNS:
        match = pattern.search(combined_text)
        if match:
            return DlqErrorClass.STORAGE_SCHEMA, match.group(0)

    # Data parsing errors (bad input data)
    for pattern in DATA_PATTERNS:
        match = pattern.search(combined_text)
        if match:
            return DlqErrorClass.DATA_PARSING, match.group(0)

    # Additional check: try to parse message_value as JSON
    if message_value:
        import json

        try:
            json.loads(message_value)
        except json.JSONDecodeError as e:
            return DlqErrorClass.DATA_PARSING, f"JSON decode error: {str(e)[:50]}"

    # Unknown if no patterns matched
    return DlqErrorClass.UNKNOWN, None


def should_replay(
    triage_report_dict: dict,
    min_infra_percentage: float = 0.70,
    max_storage_percentage: float = 0.10,
) -> tuple[bool, str]:
    """Decide whether to replay messages based on triage report.

    Args:
        triage_report_dict: Dictionary from TriageReport.model_dump().
        min_infra_percentage: Minimum % of infra errors to consider replay.
        max_storage_percentage: Maximum % of storage errors to allow replay.

    Returns:
        Tuple of (should_replay, reason).

    Example:
        >>> report = {"dominant_class": "infra/transient", "dominant_class_percentage": 0.85}
        >>> should_replay(report)
        (True, "Dominant class is infra/transient (85.0%), safe to replay")
    """
    dominant_class = triage_report_dict.get("dominant_class")
    dominant_pct = triage_report_dict.get("dominant_class_percentage", 0.0)

    # Find storage error percentage
    storage_pct = 0.0
    for stats in triage_report_dict.get("classification_stats", []):
        if stats["error_class"] == DlqErrorClass.STORAGE_SCHEMA:
            storage_pct = stats["percentage"]
            break

    # Decision logic
    if dominant_class == DlqErrorClass.INFRA_TRANSIENT and dominant_pct >= min_infra_percentage:
        if storage_pct <= max_storage_percentage:
            return True, (
                f"Dominant class is {dominant_class} ({dominant_pct:.1%}), "
                f"storage errors only {storage_pct:.1%}. Safe to replay."
            )
        else:
            return False, (
                f"Dominant class is {dominant_class} but storage errors are {storage_pct:.1%} "
                f"(> {max_storage_percentage:.1%}). Fix schema first."
            )

    elif dominant_class == DlqErrorClass.STORAGE_SCHEMA:
        return False, f"Dominant class is {dominant_class}. Fix schema/config before replay."

    elif dominant_class == DlqErrorClass.DATA_PARSING:
        return False, f"Dominant class is {dominant_class}. Bad data, cannot replay."

    else:
        return False, f"Dominant class is {dominant_class} or unknown. Manual triage required."
