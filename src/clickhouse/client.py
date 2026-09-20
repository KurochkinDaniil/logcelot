"""ClickHouse client for log storage.

HA-first:
- INSERT and SELECT go to Distributed table `logcelot.logs` (routes to logs_local on shards). [web:248]
- In HA compose, connection is usually via clickhouse-lb:8123 (HTTP interface). [web:240][web:244]

Reliability:
- If async inserts are enabled, prefer wait_for_async_insert=1 (avoid fire-and-forget). [web:144]
"""

import time
from datetime import datetime
from typing import Any, Optional

import clickhouse_connect
from clickhouse_connect.driver import Client
from clickhouse_connect.driver.exceptions import ClickHouseError, DatabaseError, ProgrammingError
import structlog

from src.models.log_entry import LogEntry
from src.core.config import get_settings
from src.clickhouse.exceptions import ConnectionError, InsertError, QueryError

logger = structlog.get_logger()


class ClickHouseClient:
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        connect_timeout: int = 10,
        send_receive_timeout: int = 300,
        logs_table: str = "logs",
        # Async insert behavior (client-side session settings)
        async_insert: bool = True,
        wait_for_async_insert: bool = True,
    ):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connect_timeout = connect_timeout
        self.send_receive_timeout = send_receive_timeout

        # HA: should be Distributed table, default "logs"
        self.logs_table = logs_table

        self.async_insert = async_insert
        self.wait_for_async_insert = wait_for_async_insert

        self.client: Optional[Client] = None
        self._connected = False

        logger.info(
            "clickhouse_client_initialized",
            host=host,
            port=port,
            database=database,
            logs_table=logs_table,
            async_insert=async_insert,
            wait_for_async_insert=wait_for_async_insert,
        )

    def connect(self) -> None:
        if self._connected and self.client:
            logger.warning("clickhouse_already_connected")
            return

        try:
            logger.info("clickhouse_connecting", host=self.host, port=self.port)

            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
                database=self.database,
                connect_timeout=self.connect_timeout,
                send_receive_timeout=self.send_receive_timeout,
                compress=True,
            )

            self.check_health()
            self._connected = True

            logger.info("clickhouse_connected", host=self.host, database=self.database)

        except Exception as e:
            logger.error("clickhouse_connection_failed", host=self.host, error=str(e))
            raise ConnectionError(
                f"Failed to connect to ClickHouse at {self.host}:{self.port}",
                original_error=e,
            )

    def close(self) -> None:
        if not self.client:
            return
        try:
            self.client.close()
            self._connected = False
            logger.info("clickhouse_connection_closed")
        except Exception as e:
            logger.warning("clickhouse_close_error", error=str(e))

    def check_health(self) -> bool:
        if not self.client:
            raise QueryError("Client not connected. Call connect() first.")

        try:
            self.client.command("SELECT 1")
            logger.debug("clickhouse_health_check_passed")
            return True
        except Exception as e:
            logger.error("clickhouse_health_check_failed", error=str(e))
            raise QueryError("Health check failed", original_error=e)

    def _insert_settings(self) -> dict[str, Any]:
        if not self.async_insert:
            return {}

        # Recommended production mode for async inserts. [web:144]
        return {
            "async_insert": 1,
            "wait_for_async_insert": 1 if self.wait_for_async_insert else 0,
        }

    def insert_logs(self, logs: list[LogEntry]) -> dict[str, Any]:
        if not self.client:
            raise InsertError("Client not connected. Call connect() first.")

        if not logs:
            logger.warning("insert_logs_called_with_empty_list")
            return {"rows_inserted": 0, "duration_ms": 0, "table": self.logs_table}

        start_time = time.time()

        try:
            rows_dicts = [log.to_clickhouse_dict() for log in logs]

            # Stable column order
            column_names = list(rows_dicts[0].keys())
            rows_data = [[row.get(col) for col in column_names] for row in rows_dicts]

            logger.info(
                "clickhouse_insert_starting",
                rows=len(rows_data),
                columns=len(column_names),
                table=self.logs_table,
                async_insert=self.async_insert,
                wait_for_async_insert=self.wait_for_async_insert if self.async_insert else None,
            )

            self.client.insert(
                table=self.logs_table,
                data=rows_data,
                column_names=column_names,
                settings=self._insert_settings(),
            )

            duration_ms = int((time.time() - start_time) * 1000)

            logger.info(
                "clickhouse_insert_success",
                rows_inserted=len(logs),
                duration_ms=duration_ms,
                table=self.logs_table,
            )

            return {"rows_inserted": len(logs), "duration_ms": duration_ms, "table": self.logs_table}

        except (ClickHouseError, DatabaseError, ProgrammingError) as e:
            logger.error(
                "clickhouse_insert_failed",
                rows=len(logs),
                error=str(e),
                error_type=type(e).__name__,
                table=self.logs_table,
            )
            raise InsertError(f"Failed to insert {len(logs)} rows into ClickHouse", original_error=e)

        except Exception as e:
            logger.error(
                "clickhouse_insert_unexpected_error",
                rows=len(logs),
                error=str(e),
                error_type=type(e).__name__,
                table=self.logs_table,
            )
            raise InsertError(f"Unexpected error during insert: {str(e)}", original_error=e)

    def query(self, query: str, parameters: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
        if not self.client:
            raise QueryError("Client not connected. Call connect() first.")

        try:
            result = self.client.query(query, parameters=parameters or {})
            return [dict(zip(result.column_names, row)) for row in result.result_rows]
        except Exception as e:
            logger.error("clickhouse_query_failed", query=query[:200], error=str(e))
            raise QueryError(f"Query execution failed: {str(e)}", original_error=e)

    def search_logs(
        self,
        source: Optional[str] = None,
        source_service: Optional[str] = None,
        level: Optional[str] = None,
        time_from: Optional[datetime] = None,
        time_to: Optional[datetime] = None,
        query: Optional[str] = None,
        trace_id: Optional[str] = None,
        user_id: Optional[str] = None,
        event_time_missing: Optional[bool] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> tuple[list[dict[str, Any]], int]:
        if not self.client:
            raise QueryError("Client not connected. Call connect() first.")

        import time as time_module
        started = time_module.time()

        try:
            where: list[str] = []
            params: dict[str, Any] = {}

            if time_from is not None:
                where.append("created_at >= %(time_from)s")
                params["time_from"] = time_from
            if time_to is not None:
                where.append("created_at <= %(time_to)s")
                params["time_to"] = time_to

            if source:
                where.append("source = %(source)s")
                params["source"] = source
            if source_service:
                where.append("source_service = %(source_service)s")
                params["source_service"] = source_service
            if level:
                where.append("level = %(level)s")
                params["level"] = level
            if trace_id:
                where.append("trace_id = %(trace_id)s")
                params["trace_id"] = trace_id
            if user_id:
                where.append("user_id = %(user_id)s")
                params["user_id"] = user_id
            if event_time_missing is not None:
                where.append("event_time_missing = %(event_time_missing)s")
                params["event_time_missing"] = 1 if event_time_missing else 0
            if query:
                where.append("message ILIKE %(query_pattern)s")
                params["query_pattern"] = f"%{query}%"

            where_clause = f"WHERE {' AND '.join(where)}" if where else ""

            count_sql = f"SELECT count() FROM {self.logs_table} {where_clause}"
            count_res = self.client.query(count_sql, parameters=params)
            total = int(count_res.result_rows[0][0]) if count_res.result_rows else 0

            data_sql = f"""
                SELECT
                    id,
                    created_at,
                    ingested_at,
                    event_time_missing,
                    source,
                    source_host,
                    source_service,
                    level,
                    message,
                    parsed_message,
                    format,
                    metadata,
                    user_id,
                    request_id,
                    trace_id,
                    span_id,
                    http_method,
                    http_path,
                    http_status,
                    http_response_time_ms,
                    error_type,
                    error_stack,
                    is_parsed,
                    parse_errors
                FROM {self.logs_table}
                {where_clause}
                ORDER BY created_at DESC
                LIMIT %(limit)s OFFSET %(offset)s
            """
            params["limit"] = limit
            params["offset"] = offset

            res = self.client.query(data_sql, parameters=params)

            rows: list[dict[str, Any]] = []
            for r in res.result_rows:
                d = dict(zip(res.column_names, r))
                if d.get("created_at"):
                    d["created_at"] = d["created_at"].isoformat()
                if d.get("ingested_at"):
                    d["ingested_at"] = d["ingested_at"].isoformat()
                rows.append(d)

            logger.info(
                "search_logs_executed",
                total_found=total,
                returned=len(rows),
                duration_ms=int((time_module.time() - started) * 1000),
            )

            return rows, total

        except (ClickHouseError, DatabaseError, ProgrammingError) as e:
            logger.error("search_logs_failed", error=str(e), error_type=type(e).__name__)
            raise QueryError(f"Failed to search logs: {str(e)}", original_error=e)
        except Exception as e:
            logger.error("search_logs_unexpected_error", error=str(e), error_type=type(e).__name__)
            raise QueryError(f"Unexpected error during log search: {str(e)}", original_error=e)


_client_instance: Optional[ClickHouseClient] = None


def get_client() -> ClickHouseClient:
    global _client_instance

    if _client_instance is None:
        settings = get_settings()

        _client_instance = ClickHouseClient(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            database=settings.clickhouse_database,
            user=settings.clickhouse_user,
            password=settings.clickhouse_password,
            logs_table=settings.clickhouse_logs_table,
            async_insert=settings.clickhouse_async_insert,
            wait_for_async_insert=settings.clickhouse_wait_for_async_insert,
        )
        _client_instance.connect()

    return _client_instance


def shutdown_client() -> None:
    global _client_instance
    if _client_instance is not None:
        _client_instance.close()
        _client_instance = None
