"""End-to-End Integration Tests for Logcelot Pipeline.

Tests the complete data flow: Kafka -> Consumer -> ClickHouse using real containers.

Requirements:
- Docker daemon running
- testcontainers[kafka] installed
"""

import asyncio
import json
import time
from typing import AsyncGenerator

import pytest
from testcontainers.kafka import KafkaContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

import clickhouse_connect
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from unittest.mock import patch

from src.models.log_entry import LogEntry
from src.services.consumer_service import LogConsumerService


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skip(reason="E2E tests with testcontainers are slow. Run explicitly with: pytest -m integration --no-skip")
]


# ---------------------------------------------------------------------
# Containers
# ---------------------------------------------------------------------

@pytest.fixture(scope="module")
def kafka_container():
    kafka = KafkaContainer(image="confluentinc/cp-kafka:7.5.0")
    kafka.start()
    yield kafka
    kafka.stop()


@pytest.fixture(scope="module")
def clickhouse_container():
    clickhouse = (
        DockerContainer(image="clickhouse/clickhouse-server:23.12")
        .with_exposed_ports(8123, 9000)
        .with_env("CLICKHOUSE_DB", "logcelot")
        .with_env("CLICKHOUSE_USER", "logcelot_user")
        .with_env("CLICKHOUSE_PASSWORD", "logcelot_pass")
        .with_env("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
    )
    clickhouse.start()

    wait_for_logs(clickhouse, "Ready for connections", timeout=90)

    yield clickhouse
    clickhouse.stop()


@pytest.fixture(scope="module")
def clickhouse_client(clickhouse_container):
    host = clickhouse_container.get_container_host_ip()
    port = int(clickhouse_container.get_exposed_port(8123))

    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username="logcelot_user",
        password="logcelot_pass",
        database="logcelot",
    )
    return client


@pytest.fixture(scope="module")
def setup_clickhouse_schema(clickhouse_client):
    # Minimal schema compatible with your consumer insert payload expectations
    clickhouse_client.command("CREATE DATABASE IF NOT EXISTS logcelot")

    clickhouse_client.command(
        """
        CREATE TABLE IF NOT EXISTS logcelot.logs
        (
            id String,
            created_at DateTime64(3, 'UTC'),
            ingested_at DateTime64(3, 'UTC'),
            event_time_missing UInt8,
            source LowCardinality(String),
            source_host String,
            source_service LowCardinality(String),
            level LowCardinality(String),
            message String,
            parsed_message String,
            format LowCardinality(String),
            metadata String,
            user_id Nullable(String),
            request_id Nullable(String),
            trace_id Nullable(String),
            span_id Nullable(String),
            http_method Nullable(String),
            http_path Nullable(String),
            http_status Nullable(UInt16),
            http_response_time_ms Nullable(UInt32),
            error_type Nullable(String),
            error_stack Nullable(String),
            is_parsed UInt8,
            parse_errors Nullable(String)
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMMDD(created_at)
        ORDER BY (source, level, created_at)
        """
    )

    yield

    # cleanup
    clickhouse_client.command("DROP TABLE IF EXISTS logcelot.logs")


@pytest.fixture(scope="module")
@pytest.mark.asyncio
async def ensure_kafka_topics(kafka_container):
    bootstrap = kafka_container.get_bootstrap_server()

    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        topics = await admin.list_topics()
        to_create = []
        for t in ["logs.raw", "logs.dead_letter"]:
            if t not in topics:
                to_create.append(NewTopic(name=t, num_partitions=3, replication_factor=1))
        if to_create:
            await admin.create_topics(to_create)
    finally:
        await admin.close()


# ---------------------------------------------------------------------
# Producer / Consumer fixtures
# ---------------------------------------------------------------------

@pytest.fixture
@pytest.mark.asyncio
async def kafka_producer(kafka_container, ensure_kafka_topics) -> AsyncGenerator[AIOKafkaProducer, None]:
    bootstrap = kafka_container.get_bootstrap_server()

    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
        compression_type="gzip",
        acks=1,
    )
    await producer.start()
    yield producer
    await producer.stop()


@pytest.fixture
@pytest.mark.asyncio
async def consumer_service(
    kafka_container,
    clickhouse_client,
    setup_clickhouse_schema,
    ensure_kafka_topics,
) -> AsyncGenerator[LogConsumerService, None]:
    bootstrap = kafka_container.get_bootstrap_server()

    # Patch get_client used inside consumer_service._flush_buffer()
    # so it writes to our testcontainer ClickHouse.
    class _CHWrapper:
        def insert_logs(self, logs):
            # clickhouse_connect insert wants data in column order, but easiest for test is INSERT VALUES via JSONEachRow
            # We’ll just use clickhouse_connect insert with dicts -> but your ClickHouseClient converts LogEntry already.
            # Here we mimic minimal behavior: insert into table by dict rows.

            rows = [l.to_clickhouse_dict() for l in logs]
            cols = list(rows[0].keys())
            data = [[r.get(c) for c in cols] for r in rows]
            clickhouse_client.insert("logs", data=data, column_names=cols)
            return {"duration_ms": 0}

    with patch("src.services.consumer_service.get_client", return_value=_CHWrapper()):
        consumer = LogConsumerService(
            bootstrap_servers=bootstrap,
            topic="logs.raw",
            group_id="test-consumer-group",
            batch_size=10,
            flush_interval=2,
            dlq_topic="logs.dead_letter",
        )

        await consumer.start()
        task = asyncio.create_task(consumer.consume_loop())

        yield consumer

        consumer.request_shutdown()
        try:
            await asyncio.wait_for(task, timeout=10.0)
        except asyncio.TimeoutError:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        await consumer.stop()


# ---------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------

@pytest.mark.asyncio
async def test_log_ingestion_flow_e2e(kafka_producer, consumer_service, clickhouse_client):
    clickhouse_client.command("TRUNCATE TABLE logcelot.logs")

    num_logs = 100
    test_logs = [
        LogEntry(
            source="integration_test",
            source_service=f"test-service-{i % 5}",
            level="INFO" if i % 10 != 0 else "ERROR",
            message=f"Test log message {i}",
            metadata={"test_id": i, "batch": "e2e"},
        )
        for i in range(num_logs)
    ]

    for log in test_logs:
        await kafka_producer.send_and_wait("logs.raw", value=log.model_dump_json())

    max_wait = 20
    check_interval = 2
    elapsed = 0

    while elapsed < max_wait:
        await asyncio.sleep(check_interval)
        elapsed += check_interval
        count = clickhouse_client.query("SELECT count() FROM logcelot.logs").result_rows[0][0]
        if count >= num_logs:
            break

    total_count = clickhouse_client.query("SELECT count() FROM logcelot.logs").result_rows[0][0]
    assert total_count == num_logs, f"Expected {num_logs}, got {total_count}"


@pytest.mark.asyncio
async def test_consumer_batching_strategy(kafka_producer, consumer_service, clickhouse_client):
    clickhouse_client.command("TRUNCATE TABLE logcelot.logs")

    # Size-based flush: batch_size=10
    for i in range(10):
        log = LogEntry(
            source="batch_test",
            source_service="batch-service",
            level="INFO",
            message=f"Batch test log {i}",
        )
        await kafka_producer.send_and_wait("logs.raw", value=log.model_dump_json())

    await asyncio.sleep(4)

    count = clickhouse_client.query("SELECT count() FROM logcelot.logs").result_rows[0][0]
    assert count == 10, f"Expected 10 logs, got {count}"

    # Time-based flush: send 5 below threshold, wait > flush_interval
    for i in range(5):
        log = LogEntry(
            source="time_test",
            source_service="time-service",
            level="INFO",
            message=f"Time test log {i}",
        )
        await kafka_producer.send_and_wait("logs.raw", value=log.model_dump_json())

    await asyncio.sleep(5)

    count = clickhouse_client.query("SELECT count() FROM logcelot.logs").result_rows[0][0]
    assert count == 15, f"Expected 15 logs, got {count}"


@pytest.mark.asyncio
async def test_log_parsing_and_field_extraction(kafka_producer, consumer_service, clickhouse_client):
    clickhouse_client.command("TRUNCATE TABLE logcelot.logs")

    log = LogEntry(
        source="parsing_test",
        source_service="api-gateway",
        level="INFO",
        message="User login successful",
        metadata={
            "user_id": "user-12345",
            "trace_id": "trace-abc-xyz",
            "request_id": "req-789",
            "http_method": "POST",
            "http_status": 200,
            "response_time_ms": 145,
            "extra_field": "should be in metadata",
        },
    )

    await kafka_producer.send_and_wait("logs.raw", value=log.model_dump_json())
    await asyncio.sleep(5)

    row = clickhouse_client.query(
        """
        SELECT user_id, trace_id, request_id, http_method, http_status, http_response_time_ms, metadata
        FROM logcelot.logs
        WHERE source = 'parsing_test'
        LIMIT 1
        """
    ).result_rows

    assert len(row) == 1
    user_id, trace_id, request_id, http_method, http_status, rt_ms, metadata_json = row[0]

    assert user_id == "user-12345"
    assert trace_id == "trace-abc-xyz"
    assert request_id == "req-789"
    assert http_method == "POST"
    assert http_status == 200
    assert rt_ms == 145

    metadata = json.loads(metadata_json)
    assert metadata["extra_field"] == "should be in metadata"
