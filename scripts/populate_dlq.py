#!/usr/bin/env python3
"""
Utility script to populate DLQ with different types of messages for DAG demonstration.

Usage:
    python scripts/populate_dlq.py

This creates various DLQ entries:
- Transient errors (can be replayed) - 70%
- Parse errors (cannot be replayed) - 20%
- Schema errors (cannot be replayed) - 10%
"""

import json
import asyncio
from datetime import datetime
from uuid import uuid4

from aiokafka import AIOKafkaProducer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094"
DLQ_TOPIC = "logs.dead_letter"


async def send_dlq_message(producer: AIOKafkaProducer, dlq_entry: dict):
    """Send a message to DLQ topic."""
    value = json.dumps(dlq_entry)
    await producer.send_and_wait(DLQ_TOPIC, value=value.encode('utf-8'))


async def populate_dlq():
    """Populate DLQ with test messages."""
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v if isinstance(v, bytes) else v.encode('utf-8'),
    )
    
    await producer.start()
    
    try:
        print(f"Populating DLQ topic: {DLQ_TOPIC}")
        print("-" * 60)
        
        # 1. INFRA/TRANSIENT errors (70% - can be replayed)
        infra_errors = [
            "Connection timeout to ClickHouse",
            "Network error: connection refused",
            "Upstream service 503 temporarily unavailable",
            "Connection reset by peer",
            "ClickHouse timeout after 30s",
            "Network timeout",
            "Connection to database timed out",
        ]
        
        print(f"\n1. Creating {len(infra_errors)} INFRA/TRANSIENT errors (replayable)...")
        for i, error_msg in enumerate(infra_errors):
            # Send VALID LogEntry directly - this is what gets replayed
            # The error_message is stored in Kafka headers for classification
            valid_log = {
                "id": str(uuid4()),
                "created_at": datetime.utcnow().isoformat() + "Z",
                "ingested_at": datetime.utcnow().isoformat() + "Z",
                "event_time_missing": 0,
                "source": "http",
                "source_host": "test-host",
                "source_service": f"replayable-service-{i}",
                "level": "INFO",
                "message": f"Replayable log #{i} (simulated transient error: {error_msg})",
                "parsed_message": "",
                "format": "json",
                "metadata": json.dumps({"replay_test": True, "original_error": error_msg}),
                "is_parsed": 1,
                "parse_errors": None,
            }
            
            # Send LogEntry directly, with error context in headers
            await producer.send(
                DLQ_TOPIC,
                value=json.dumps(valid_log).encode('utf-8'),
                headers=[
                    ("error_message", error_msg.encode('utf-8')),
                    ("error_type", b"clickhouse_insert_error"),
                    ("dlq_timestamp", datetime.utcnow().isoformat().encode('utf-8')),
                ]
            )
            print(f"  ✓ Sent: {error_msg}")
        
        # 2. DATA/PARSING errors (20% - cannot be replayed, but send as valid logs for demo)
        parse_errors = [
            "Invalid JSON: expecting property name",
            "Parse error: malformed syslog format",
        ]
        
        print(f"\n2. Creating {len(parse_errors)} DATA/PARSING errors (demo - will replay as valid)...")
        for i, error_msg in enumerate(parse_errors):
            # Even parsing errors - send valid log for demo purposes
            valid_log = {
                "id": str(uuid4()),
                "created_at": datetime.utcnow().isoformat() + "Z",
                "ingested_at": datetime.utcnow().isoformat() + "Z",
                "event_time_missing": 0,
                "source": "http",
                "source_host": "test-host",
                "source_service": f"parse-error-demo-{i}",
                "level": "WARN",
                "message": f"Parse error demo #{i} (was: {error_msg})",
                "parsed_message": "",
                "format": "json",
                "metadata": json.dumps({"parse_error_demo": True}),
                "is_parsed": 1,
                "parse_errors": None,
            }
            
            await producer.send(
                DLQ_TOPIC,
                value=json.dumps(valid_log).encode('utf-8'),
                headers=[
                    ("error_message", error_msg.encode('utf-8')),
                    ("error_type", b"message_parse_error"),
                    ("dlq_timestamp", datetime.utcnow().isoformat().encode('utf-8')),
                ]
            )
            print(f"  ✓ Sent: {error_msg}")
        
        # 3. STORAGE/SCHEMA errors (10% - demo as valid log)
        schema_error = "Unrecognized column 'old_field_name' in table logs_local"
        
        print(f"\n3. Creating 1 STORAGE/SCHEMA error (demo - will replay as valid)...")
        valid_log = {
            "id": str(uuid4()),
            "created_at": datetime.utcnow().isoformat() + "Z",
            "ingested_at": datetime.utcnow().isoformat() + "Z",
            "event_time_missing": 0,
            "source": "http",
            "source_host": "test-host",
            "source_service": "schema-error-demo",
            "level": "ERROR",
            "message": "Schema error demo (was: column mismatch)",
            "parsed_message": "",
            "format": "json",
            "metadata": json.dumps({"schema_error_demo": True}),
            "is_parsed": 1,
            "parse_errors": None,
        }
        
        await producer.send(
            DLQ_TOPIC,
            value=json.dumps(valid_log).encode('utf-8'),
            headers=[
                ("error_message", schema_error.encode('utf-8')),
                ("error_type", b"clickhouse_schema_error"),
                ("dlq_timestamp", datetime.utcnow().isoformat().encode('utf-8')),
            ]
        )
        print(f"  Sent: {schema_error}")
        
        print(f" Successfully added 10 DLQ messages:")
        print(f"   - 7 INFRA/TRANSIENT (70%) Should trigger replay")
        print(f"   - 2 DATA/PARSING (20%)")
        print(f"   - 1 STORAGE/SCHEMA (10%)")
        print(f"\nNow check:")
        print(f"- Kafka UI: http://localhost:8080 Topics {DLQ_TOPIC}")
        print(f"- Airflow: http://localhost:8088  DAG: dlq_triage_replay")

        
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(populate_dlq())
