"""FastAPI routes for log ingestion and search.

Implements the ingestion API following async-first principles.

References:
    - Course Requirement: REST API for ingestion
    - Implementation Constraint: Non-blocking I/O only
"""

from typing import Any, Optional, Union
from fastapi import File, Form, Header, UploadFile
import json
import uuid

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status
from pydantic import ValidationError
import structlog

from src.api.file_helpers import _read_limited, MAX_UPLOAD_BYTES, _has_allowed_ext, ALLOWED_JSONL_CONTENT_TYPES, \
    ALLOWED_TEXT_CONTENT_TYPES
from src.models.log_entry import (
    LogEntry,
    LogIngestionRequest,
    LogBatchRequest,
    SearchRequest,
    LogSearchResponse,
)
from src.kafka.producer import get_producer, LogProducer
from src.clickhouse.client import get_client
from src.parsers.manager import parse_log
from src.parsers.base import ParserError

logger = structlog.get_logger()

# Create router
router = APIRouter(
    prefix="",
    tags=["logs"],
)


@router.post(
    "/logs",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=dict[str, Any],
    summary="Ingest log entries",
    description=(
        "Accepts single or batch log entries and queues them for processing. "
        "Returns immediately with 202 Accepted (async processing)."
    ),
)
async def ingest_logs(
    request: LogIngestionRequest,
    producer: LogProducer = Depends(get_producer),
) -> dict[str, Any]:
    """Ingest a single log entry.
    
    Validates the log entry and sends it to Kafka for processing.
    This endpoint is async and returns immediately (202 Accepted).
    
    Args:
        request: Log ingestion request with log data.
        producer: Kafka producer (injected dependency).
        
    Returns:
        Response with status and Kafka metadata.
        
    Raises:
        HTTPException 400: If validation fails.
        HTTPException 503: If Kafka is unavailable.
        
    Example:
        > curl -X POST http://localhost:8000/logs \
        ...   -H "Content-Type: application/json" \
        ...   -d '{
        ...     "source": "http",
        ...     "source_service": "auth-api",
        ...     "level": "error",
        ...     "message": "Login failed",
        ...     "payload": {"user_id": "12345"}
        ...   }'
        
        Response (202):
        {
            "status": "queued",
            "count": 1,
            "kafka_topic": "logs.raw",
            "kafka_partition": 2,
            "kafka_offset": 12345,
            "log_id": "550e8400-..."
        }
    """
    try:
        # Convert request to validated LogEntry
        log = request.to_log_entry()
        
        # Send to Kafka (async)
        kafka_metadata = await producer.send_log(log)
        
        logger.info(
            "log_ingested",
            log_id=str(log.id),
            source=log.source,
            source_service=log.source_service,
            level=log.level,
        )
        
        return {
            "status": "queued",
            "count": 1,
            **kafka_metadata,
        }
        
    except ValidationError as e:
        # Validation error (should not happen as request is already validated)
        logger.warning(
            "log_validation_failed",
            error=str(e),
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "Validation failed", "details": e.errors()},
        )
        
    except RuntimeError as e:
        # Producer not started (Kafka unavailable)
        logger.error(
            "kafka_unavailable",
            error=str(e),
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "Service temporarily unavailable",
                "message": "Kafka broker is not available. Please try again later.",
            },
        )
        
    except Exception as e:
        # Unexpected error
        logger.error(
            "log_ingestion_error",
            error=str(e),
            error_type=type(e).__name__,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "Internal server error"},
        )


@router.post(
    "/logs/batch",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=dict[str, Any],
    summary="Batch ingest log entries",
    description=(
        "Accepts multiple log entries (1-1000) and queues them for processing. "
        "More efficient than individual requests for high-volume ingestion."
    ),
)
async def ingest_logs_batch(
    request: LogBatchRequest,
    producer: LogProducer = Depends(get_producer),
) -> dict[str, Any]:
    """Ingest multiple log entries in a batch.
    
    Accepts 1-1000 log entries and sends them to Kafka.
    This is more efficient than individual requests for bulk ingestion.
    
    Args:
        request: Batch ingestion request with multiple logs.
        producer: Kafka producer (injected dependency).
        
    Returns:
        Response with status and aggregated metadata.
        
    Raises:
        HTTPException 400: If validation fails.
        HTTPException 503: If Kafka is unavailable.
        
    Example:
        > curl -X POST http://localhost:8000/logs/batch \
        ...   -H "Content-Type: application/json" \
        ...   -d '{
        ...     "logs": [
        ...       {"source": "http", "source_service": "api", "level": "info", "message": "Log 1"},
        ...       {"source": "http", "source_service": "api", "level": "info", "message": "Log 2"}
        ...     ]
        ...   }'
        
        Response (202):
        {
            "status": "queued",
            "count": 2,
            "success": 2,
            "failed": 0
        }
    """
    try:
        success_count = 0
        failed_count = 0
        errors = []
        
        # Process each log in batch
        for idx, log_request in enumerate(request.logs):
            try:
                # Convert to LogEntry
                log = log_request.to_log_entry()
                
                # Send to Kafka
                await producer.send_log(log)
                success_count += 1
                
            except Exception as e:
                failed_count += 1
                errors.append({
                    "index": idx,
                    "error": str(e),
                })
                
                logger.warning(
                    "batch_log_failed",
                    index=idx,
                    error=str(e),
                )
        
        logger.info(
            "batch_ingested",
            total=len(request.logs),
            success=success_count,
            failed=failed_count,
        )
        
        response = {
            "status": "queued",
            "count": len(request.logs),
            "success": success_count,
            "failed": failed_count,
        }
        
        # Include errors if any
        if errors:
            response["errors"] = errors
        
        # Return 503 if all logs failed
        if failed_count == len(request.logs):
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "error": "All logs failed to queue",
                    "details": errors,
                },
            )
        
        return response
        
    except HTTPException:
        raise
        
    except Exception as e:
        logger.error(
            "batch_ingestion_error",
            error=str(e),
            error_type=type(e).__name__,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "Internal server error"},
        )


@router.post(
    "/logs/raw",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=dict[str, Any],
    summary="Ingest raw log (Syslog/CLF)",
    description=(
        "Accepts raw log content in Syslog RFC5424 or CLF format. "
        "The log is parsed, validated, and queued for processing."
    ),
)
async def ingest_raw_log(
    content: str = Body(
        ...,
        media_type="text/plain",
        description="Raw log content",
        examples={
            "syslog": {
                "value": "<134>1 2024-01-15T10:30:00Z web-server nginx 1234 ID47 - GET /api/users"
            },
            "clf": {
                "value": '192.168.1.1 - user123 [15/Jan/2024:10:30:00 +0000] "GET /api/users HTTP/1.1" 200 1234'
            }
        }
    ),
    format: str = Query(
        ...,
        description="Log format: syslog, clf",
        pattern="^(syslog|clf)$"
    ),
    source_service: str = Query(
        "unknown",
        description="Service name (optional)"
    ),
    producer: LogProducer = Depends(get_producer),
) -> dict[str, Any]:
    """Ingest raw log content (Syslog or CLF).
    
    Parses raw log formats and converts to LogEntry before queuing.
    Supports Syslog RFC5424 and Common Log Format (Apache/Nginx).
    
    Args:
        content: Raw log content as plain text.
        format: Log format (syslog, clf).
        source_service: Optional service name override.
        producer: Kafka producer (injected dependency).
        
    Returns:
        Response with status and Kafka metadata.
        
    Raises:
        HTTPException 400: If parsing fails.
        HTTPException 503: If Kafka is unavailable.
        
    Example (Syslog):
        > curl -X POST http://localhost:8000/logs/raw?format=syslog \
        ...   -H "Content-Type: text/plain" \
        ...   -d "<134>1 2024-01-15T10:30:00Z web-server nginx 1234 ID47 - GET /api/users"
        
    Example (CLF):
        > curl -X POST http://localhost:8000/logs/raw?format=clf \
        ...   -H "Content-Type: text/plain" \
        ...   -d '192.168.1.1 - user [15/Jan/2024:10:30:00 +0000] "GET /api/users HTTP/1.1" 200 1234'
    """
    try:
        # Parse raw content using appropriate parser
        log = parse_log(content, format, source_service)
        
        # Send to Kafka
        kafka_metadata = await producer.send_log(log)
        
        logger.info(
            "raw_log_ingested",
            log_id=str(log.id),
            format=format,
            source_service=log.source_service,
            level=log.level,
        )
        
        return {
            "status": "queued",
            "count": 1,
            "format": format,
            "parsed": True,
            **kafka_metadata,
        }
        
    except ParserError as e:
        # Parser error (invalid format) - send to DLQ
        logger.warning(
            "raw_log_parser_error_sending_to_dlq",
            format=format,
            error=str(e),
            content_preview=content[:200] if len(content) > 200 else content
        )
        
        # Send to DLQ
        await producer.send_to_dlq(
            raw_payload=content,
            error_message=str(e),
            error_type="parse_error",
            source="http",
        )
        
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "Log parsing failed - sent to DLQ",
                "message": str(e),
                "format": format,
            }
        )
        
    except RuntimeError as e:
        # Producer not started
        logger.error("kafka_unavailable_raw_log", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "Service temporarily unavailable",
                "message": "Kafka broker is not available. Please try again later.",
            }
        )
        
    except Exception as e:
        # Unexpected error
        logger.error(
            "raw_log_ingestion_error",
            error=str(e),
            error_type=type(e).__name__
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "Internal server error"},
        )


@router.get(
    "/logs/search",
    status_code=status.HTTP_200_OK,
    response_model=LogSearchResponse,
    summary="Search logs",
    description=(
        "Search logs with multiple filters and pagination. "
        "Default time range is last 24 hours for optimal performance."
    ),
)
async def search_logs(
    source: Optional[str] = None,
    source_service: Optional[str] = None,
    level: Optional[str] = None,
    time_from: Optional[str] = None,
    time_to: Optional[str] = None,
    query: Optional[str] = None,
    trace_id: Optional[str] = None,
    user_id: Optional[str] = None,
    offset: int = 0,
    limit: int = 100,
) -> LogSearchResponse:
    """Search logs with filters and pagination.
    
    Queries ClickHouse with optimized filters and pagination.
    Default time range is last 24 hours to optimize partition scanning.
    
    Performance Notes:
        - Time range filter enables partition pruning
        - Sorting key (source, level, created_at) optimizes common queries
        - Full-text search uses ILIKE (case-insensitive)
        - Results ordered by created_at DESC (most recent first)
    
    Args:
        source: Filter by source type (http, kafka, file).
        source_service: Filter by service name.
        level: Filter by log level (DEBUG, INFO, WARN, ERROR, FATAL).
        time_from: Start time (ISO8601, e.g., "2024-01-15T10:00:00Z").
        time_to: End time (ISO8601).
        query: Full-text search in message field.
        trace_id: Filter by distributed trace ID.
        user_id: Filter by user ID.
        offset: Pagination offset (default: 0).
        limit: Max results per page (1-1000, default: 100).
        
    Returns:
        LogSearchResponse with results and metadata.
        
    Raises:
        HTTPException 400: If validation fails.
        HTTPException 503: If ClickHouse is unavailable.
        HTTPException 500: If query execution fails.
        
    Example:
        > # Search for errors in auth-api service
        > curl "http://localhost:8000/logs/search?source_service=auth-api&level=ERROR&limit=50"
        
        > # Full-text search with time range
        > curl "http://localhost:8000/logs/search?query=database&time_from=2024-01-15T00:00:00Z&time_to=2024-01-16T00:00:00Z"
        
        > # Trace all logs for a request
        > curl "http://localhost:8000/logs/search?trace_id=550e8400-e29b-41d4-a716-446655440000"
        
        Response (200):
        {
            "total": 1500,
            "offset": 0,
            "limit": 100,
            "items": [
                {
                    "id": "550e8400-...",
                    "created_at": "2024-01-15T10:30:00Z",
                    "source": "http",
                    "source_service": "auth-api",
                    "level": "ERROR",
                    "message": "Database connection failed",
                    ...
                }
            ],
            "query_time_ms": 45
        }
    """
    import time as time_module
    start_time = time_module.time()
    
    try:
        # Validate search parameters using Pydantic model
        search_request = SearchRequest(
            source=source,
            source_service=source_service,
            level=level,
            time_from=time_from,
            time_to=time_to,
            query=query,
            trace_id=trace_id,
            user_id=user_id,
            offset=offset,
            limit=limit,
        )
        
        # Get ClickHouse client
        client = get_client()
        
        # Execute search
        results, total = client.search_logs(
            source=search_request.source,
            source_service=search_request.source_service,
            level=search_request.level,
            time_from=search_request.time_from.isoformat() if search_request.time_from else None,
            time_to=search_request.time_to.isoformat() if search_request.time_to else None,
            query=search_request.query,
            trace_id=search_request.trace_id,
            user_id=search_request.user_id,
            offset=search_request.offset,
            limit=search_request.limit,
        )
        
        end_time = time_module.time()
        query_time_ms = int((end_time - start_time) * 1000)
        
        logger.info(
            "search_request_completed",
            total_found=total,
            returned=len(results),
            offset=offset,
            limit=limit,
            query_time_ms=query_time_ms,
        )
        
        return LogSearchResponse(
            total=total,
            offset=search_request.offset,
            limit=search_request.limit,
            items=results,
            query_time_ms=query_time_ms,
        )
        
    except ValidationError as e:
        logger.warning(
            "search_validation_failed",
            error=str(e),
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "Validation failed", "details": e.errors()},
        )
        
    except Exception as e:
        logger.error(
            "search_request_failed",
            error=str(e),
            error_type=type(e).__name__,
        )
        
        # Check if ClickHouse is unavailable
        if "not connected" in str(e).lower() or "connection" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "error": "Service temporarily unavailable",
                    "message": "Database is not available. Please try again later.",
                },
            )
        
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "Search failed", "message": str(e)},
        )

@router.post(
    "/logs/file/raw",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=dict[str, Any],
    summary="Upload raw logs file (Syslog/CLF)",
    description=(
        "Upload a text file where each line is a raw log record. "
        "Each line is parsed as Syslog RFC5424 or CLF and queued to Kafka."
    ),
)
async def ingest_raw_file(
    file: UploadFile = File(..., description="Text file with one log per line"),
    format: str = Form(..., description="Log format: syslog, clf", pattern="^(syslog|clf)$"),
    source_service: str = Form("unknown", description="Service name override"),
    source: str = Form("file", description="Source type marker"),
    content_length: Union[int, None] = Header(default=None, alias="Content-Length"),
    producer: LogProducer = Depends(get_producer),
) -> dict[str, Any]:
    if content_length is not None and content_length > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail={"error": "File too large", "max_bytes": MAX_UPLOAD_BYTES},
        )

    if file.content_type and file.content_type not in ALLOWED_TEXT_CONTENT_TYPES:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail={"error": "Unsupported content type", "content_type": file.content_type},
        )

    if not _has_allowed_ext(file.filename, {".log", ".txt", ".raw", ".clf", ".syslog"}):
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail={"error": "Unsupported file extension", "filename": file.filename},
        )

    raw = await _read_limited(file, MAX_UPLOAD_BYTES)

    text = raw.decode("utf-8", errors="replace")
    lines = [ln for ln in text.splitlines() if ln.strip()]

    if not lines:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "Empty file"},
        )

    success = 0
    failed = 0
    errors: list[dict[str, Any]] = []

    for idx, line in enumerate(lines):
        try:
            log = parse_log(line, format, source_service)
            log.source = source
            await producer.send_log(log)
            success += 1
        except Exception as e:
            # Send unparseable log to DLQ
            failed += 1
            errors.append({"line": idx + 1, "error": str(e)})
            
            # Send to DLQ directly from API
            try:
                await producer.send_to_dlq(
                    raw_payload=line,
                    error_message=str(e),
                    error_type="parse_error",
                    source=source,
                )
            except Exception as send_err:
                logger.error("failed_to_send_parse_error_to_dlq", error=str(send_err))

    resp: dict[str, Any] = {
        "status": "queued",
        "count": len(lines),
        "success": success,
        "failed": failed,
        "format": format,
        "source_service": source_service,
        "filename": file.filename,
    }
    if errors:
        resp["errors"] = errors[:20]  # не раздуваем ответ

    # Если вообще ничего не поставилось в очередь — считаем это проблемой Kafka/валидации
    if success == 0:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": "All lines failed to queue", "details": resp.get("errors", [])},
        )

    return resp

@router.post(
    "/logs/file/jsonl",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=dict[str, Any],
    summary="Upload JSONL logs file",
    description=(
        "Upload a JSONL file (one JSON object per line). "
        "Each line is validated and queued to Kafka."
    ),
)
async def ingest_jsonl_file(
    file: UploadFile = File(..., description="JSONL file: one JSON object per line"),
    default_source: str = Form("file", description="Default 'source' if missing in JSON"),
    default_source_service: str = Form("unknown", description="Default service if missing"),
    content_length: Union[int, None] = Header(default=None, alias="Content-Length"),
    producer: LogProducer = Depends(get_producer),
) -> dict[str, Any]:
    if content_length is not None and content_length > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail={"error": "File too large", "max_bytes": MAX_UPLOAD_BYTES},
        )

    if file.content_type and file.content_type not in ALLOWED_JSONL_CONTENT_TYPES:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail={"error": "Unsupported content type", "content_type": file.content_type},
        )

    if not _has_allowed_ext(file.filename, {".jsonl", ".json", ".txt"}):
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail={"error": "Unsupported file extension", "filename": file.filename},
        )

    raw = await _read_limited(file, MAX_UPLOAD_BYTES)
    text = raw.decode("utf-8", errors="replace")
    lines = [ln for ln in text.splitlines() if ln.strip()]

    if not lines:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "Empty file"},
        )

    success = 0
    failed = 0
    errors: list[dict[str, Any]] = []

    for idx, line in enumerate(lines):
        try:
            obj = json.loads(line)

            # Поддержим два стиля:
            # 1) формат как LogIngestionRequest (source/source_service/level/message/payload)
            # 2) формат сразу LogEntry-подобный (если у вас такое есть)
            # Здесь: приводим к LogIngestionRequest и используем ваш же .to_log_entry()
            if "source" not in obj:
                obj["source"] = default_source
            if "source_service" not in obj:
                obj["source_service"] = default_source_service

            req = LogIngestionRequest(**obj)
            log = req.to_log_entry()
            await producer.send_log(log)
            success += 1

        except Exception as e:
            failed += 1
            errors.append({"line": idx + 1, "error": str(e), "preview": line[:200]})

    resp: dict[str, Any] = {
        "status": "queued",
        "count": len(lines),
        "success": success,
        "failed": failed,
        "filename": file.filename,
    }
    if errors:
        resp["errors"] = errors[:20]

    if success == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "No valid JSONL records", "details": resp.get("errors", [])},
        )

    return resp
