"""Airflow DAG: DLQ Triage and Replay

Regularly monitors Kafka DLQ (logs.dead_letter), classifies error causes,
and optionally replays messages back to logs.raw if safe.

Course Requirement: ETL pipeline with data quality monitoring.

Schedule: Every 5 minutes (configurable)
Owner: Logcelot Team
"""

import sys
import os
import json
from datetime import datetime, timedelta
from typing import Any

# Add dags directory to Python path for imports
sys.path.insert(0, os.path.dirname(__file__))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import structlog

from operators.kafka_dlq_sample_operator import KafkaDlqSampleOperator
from utils.models import (
    TriageReport,
    ClassificationStats,
    ClickHouseSignals,
    FinalReport,
    DlqErrorClass,
    ReplayReport,
)
from utils.classifier import should_replay

logger = structlog.get_logger()

# ============================================================
# DAG Configuration (using Airflow Variables with defaults)
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = Variable.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    default_var="kafka-1:29092,kafka-2:29092,kafka-3:29092",
)
DLQ_TOPIC = Variable.get("DLQ_TOPIC", default_var="logs.dead_letter")
REPLAY_TOPIC = Variable.get("REPLAY_TOPIC", default_var="logs.raw")
DLQ_SAMPLE_SIZE = int(Variable.get("DLQ_SAMPLE_SIZE", default_var="50"))
# DRY_RUN removed - always replay DLQ messages

CLICKHOUSE_HTTP_URL = Variable.get(
    "CLICKHOUSE_HTTP_URL", default_var="http://clickhouse-lb:8123"
)
CLICKHOUSE_DATABASE = Variable.get("CLICKHOUSE_DATABASE", default_var="logcelot")
CLICKHOUSE_USER = Variable.get("CLICKHOUSE_USER", default_var="logcelot_user")
CLICKHOUSE_PASSWORD = Variable.get("CLICKHOUSE_PASSWORD", default_var="logcelot_pass")

# ============================================================
# Task Functions
# ============================================================


def classify_sample(**context: Any) -> dict[str, Any]:
    """Classify sampled DLQ messages into error categories.

    Args:
        context: Airflow context with XCom data.

    Returns:
        TriageReport dictionary.
    """
    ti = context["ti"]

    # Pull sampled messages from previous task
    sample_data = ti.xcom_pull(task_ids="dlq_sample_messages")
    messages = sample_data.get("messages", [])

    if not messages:
        logger.warning("classify_sample_no_messages")
        return TriageReport(
            sampled_count=0,
            classification_stats=[],
            dominant_class=DlqErrorClass.UNKNOWN,
            dominant_class_percentage=0.0,
        ).model_dump()

    # Count errors by class
    class_counts: dict[DlqErrorClass, int] = {
        DlqErrorClass.INFRA_TRANSIENT: 0,
        DlqErrorClass.STORAGE_SCHEMA: 0,
        DlqErrorClass.DATA_PARSING: 0,
        DlqErrorClass.UNKNOWN: 0,
    }

    # Collect top errors and sample messages per class
    class_errors: dict[DlqErrorClass, list[str]] = {cls: [] for cls in class_counts}
    class_samples: dict[DlqErrorClass, list[dict]] = {cls: [] for cls in class_counts}

    for msg in messages:
        error_class = DlqErrorClass(msg["error_class"])
        class_counts[error_class] += 1

        # Store error reason
        if msg.get("error_reason"):
            class_errors[error_class].append(msg["error_reason"])

        # Store sample message (max 3 per class)
        if len(class_samples[error_class]) < 3:
            class_samples[error_class].append(msg)

    # Calculate statistics
    total = len(messages)
    classification_stats = []

    for error_class, count in class_counts.items():
        if count == 0:
            continue

        percentage = count / total

        # Get top 5 unique errors
        top_errors = list(dict.fromkeys(class_errors[error_class]))[:5]

        stats = ClassificationStats(
            error_class=error_class,
            count=count,
            percentage=percentage,
            top_errors=top_errors,
            sample_messages=class_samples[error_class],
        )
        classification_stats.append(stats)

    # Find dominant class
    dominant_class = max(class_counts, key=class_counts.get)  # type: ignore
    dominant_percentage = class_counts[dominant_class] / total

    report = TriageReport(
        sampled_count=total,
        classification_stats=classification_stats,
        dominant_class=dominant_class,
        dominant_class_percentage=dominant_percentage,
    )

    logger.info("classification_complete", report=report.model_dump())

    return report.model_dump()


def replay_messages_from_dlq(**context: Any) -> dict[str, Any]:
    """Replay all sampled DLQ messages back to logs.raw.
    
    Args:
        context: Airflow context with XCom data.
    
    Returns:
        ReplayReport dictionary.
    """
    ti = context["ti"]
    
    # Get messages from sample task
    sample_data = ti.xcom_pull(task_ids="dlq_sample_messages") or {}
    messages = sample_data.get("messages", [])
    
    if not messages:
        logger.info("replay_skipped_no_messages")
        return ReplayReport(
            replayed=False,
            replayed_count=0,
            replay_topic=REPLAY_TOPIC,
            offsets_committed=False,
            error="No messages to replay",
        ).model_dump()
    
    logger.info("replay_start", message_count=len(messages), replay_topic=REPLAY_TOPIC)
    
    # Import Kafka libraries
    from kafka import KafkaProducer, KafkaConsumer, TopicPartition
    from kafka.errors import KafkaError
    
    try:
        # Step 1: Publish messages to replay topic
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8") if isinstance(v, dict) else v if isinstance(v, bytes) else str(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k and isinstance(k, str) else k,
            acks="all",
            retries=3,
        )
        
        published_count = 0
        
        for msg_dict in messages:
            # Get original message value (prefer JSON, fallback to preview)
            # This is already a valid LogEntry
            value = msg_dict.get("value_json")
            key = msg_dict.get("key")
            
            if not value:
                logger.warning("message_skipped_no_value", offset=msg_dict.get("offset"))
                continue
            
            # Send to replay topic (value will be serialized to JSON by producer)
            future = producer.send(REPLAY_TOPIC, value=value, key=key)
            record_metadata = future.get(timeout=10)
            
            logger.debug(
                "message_replayed",
                partition=record_metadata.partition,
                offset=record_metadata.offset,
            )
            
            published_count += 1
        
        producer.flush()
        producer.close()
        
        logger.info("replay_publish_complete", published_count=published_count)
        
        # Step 2: Commit DLQ offsets to remove messages
        # We need to use the SAME consumer that sampled messages, but we can't access it
        # Solution: Create a consumer, subscribe to topic (creates consumer group), 
        # then manually commit specific offsets
        
        # Build map of partition -> MAX offset (we need to commit max+1 per partition)
        partition_max_offset = {}
        for msg_dict in messages:
            partition = msg_dict.get("partition")
            offset = msg_dict.get("offset")
            if partition is not None and offset is not None:
                # Track maximum offset per partition
                if partition not in partition_max_offset:
                    partition_max_offset[partition] = offset
                else:
                    partition_max_offset[partition] = max(partition_max_offset[partition], offset)
        
        if partition_max_offset:
            # Create consumer with subscribe (registers consumer group)
            consumer = KafkaConsumer(
                DLQ_TOPIC,  # Subscribe to topic (not assign!)
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                group_id="airflow-dlq-triage",
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                consumer_timeout_ms=1000,  # Quick timeout
            )
            
            # Build OffsetAndMetadata dict for commit
            from kafka import OffsetAndMetadata
            
            offsets_to_commit = {}
            for partition, max_offset in partition_max_offset.items():
                tp = TopicPartition(DLQ_TOPIC, partition)
                # Commit max_offset + 1 (next message to read)
                offsets_to_commit[tp] = OffsetAndMetadata(max_offset + 1, None)
            
            # Explicitly commit offsets for consumer group
            consumer.commit(offsets=offsets_to_commit)
            offsets_committed = True
            
            logger.info(
                "dlq_offsets_committed",
                partition_count=len(partition_max_offset),
                committed_offsets={p: off+1 for p, off in partition_max_offset.items()},
            )
            
            consumer.close()
        else:
            offsets_committed = False
            logger.warning("no_offsets_to_commit")
        
        logger.info(
            "replay_complete",
            replayed_count=published_count,
            offsets_committed=offsets_committed,
        )
        
        return ReplayReport(
            replayed=True,
            replayed_count=published_count,
            replay_topic=REPLAY_TOPIC,
            offsets_committed=offsets_committed,
        ).model_dump()
        
    except Exception as e:
        logger.error("replay_failed", error=str(e), error_type=type(e).__name__)
        return ReplayReport(
            replayed=False,
            replayed_count=0,
            replay_topic=REPLAY_TOPIC,
            offsets_committed=False,
            error=str(e),
        ).model_dump()


def clickhouse_signal_checks(**context: Any) -> dict[str, Any]:
    """Query ClickHouse for data quality signals.

    Args:
        context: Airflow context.

    Returns:
        ClickHouseSignals dictionary.
    """
    import clickhouse_connect

    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HTTP_URL.replace("http://", "").split(":")[0],
            port=int(CLICKHOUSE_HTTP_URL.split(":")[-1]),
            database=CLICKHOUSE_DATABASE,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
        )

        # Query 1: Parse errors in last 10 minutes
        query_parse_errors = """
            SELECT count() as cnt
            FROM logcelot.logs
            WHERE is_parsed = 0
              AND created_at >= now() - INTERVAL 10 MINUTE
        """
        result = client.query(query_parse_errors)
        parse_errors_count = result.result_rows[0][0] if result.result_rows else 0

        # Query 2: Max ingested_at (freshness check)
        query_max_ingested = """
            SELECT max(ingested_at) as max_time
            FROM logcelot.logs
        """
        result = client.query(query_max_ingested)
        max_ingested_at = result.result_rows[0][0] if result.result_rows else None

        # Query 3: Ingestion lag (seconds)
        ingestion_lag_seconds = None
        if max_ingested_at:
            lag_delta = datetime.utcnow() - max_ingested_at
            ingestion_lag_seconds = lag_delta.total_seconds()

        # Query 4: Total logs count
        query_total_logs = "SELECT count() FROM logcelot.logs"
        result = client.query(query_total_logs)
        total_logs_count = result.result_rows[0][0] if result.result_rows else 0

        client.close()

        signals = ClickHouseSignals(
            parse_errors_last_10min=parse_errors_count,
            max_ingested_at=max_ingested_at,
            ingestion_lag_seconds=ingestion_lag_seconds,
            total_logs_count=total_logs_count,
        )

        logger.info("clickhouse_signals_complete", signals=signals.model_dump())

        return signals.model_dump()

    except Exception as e:
        logger.error("clickhouse_signals_failed", error=str(e))
        # Return empty signals on error
        return ClickHouseSignals(
            parse_errors_last_10min=0,
            max_ingested_at=None,
            ingestion_lag_seconds=None,
            total_logs_count=0,
        ).model_dump()


def final_report(**context: Any) -> dict[str, Any]:
    """Generate final consolidated report.

    Args:
        context: Airflow context with XCom data.

    Returns:
        FinalReport dictionary.
    """
    ti = context["ti"]
    dag_run = context["dag_run"]

    # Pull all reports from XCom
    sample_data = ti.xcom_pull(task_ids="dlq_sample_messages") or {}
    triage_report_dict = ti.xcom_pull(task_ids="classify_sample") or {}
    ch_signals_dict = ti.xcom_pull(task_ids="clickhouse_signal_checks") or {}
    replay_report_dict = ti.xcom_pull(task_ids="replay_dlq") or {}

    # Parse into Pydantic models
    from utils.models import DlqStats, ReplayReport

    dlq_stats = DlqStats(**sample_data.get("dlq_stats", {}))
    triage_report = TriageReport(**triage_report_dict)
    ch_signals = ClickHouseSignals(**ch_signals_dict)
    replay_report = ReplayReport(**replay_report_dict) if replay_report_dict else ReplayReport(
        replayed=False,
        replayed_count=0,
        replay_topic=REPLAY_TOPIC,
        offsets_committed=False,
        error="Replay failed or no data",
    )

    # Generate recommendation
    recommendation = _generate_recommendation(triage_report, ch_signals, replay_report)

    report = FinalReport(
        dag_run_id=dag_run.run_id,
        execution_date=dag_run.execution_date,
        dlq_stats=dlq_stats,
        triage_report=triage_report,
        clickhouse_signals=ch_signals,
        replay_report=replay_report,
        recommendation=recommendation,
    )

    # Log final report as single JSON (for easy parsing)
    logger.info("final_report", report=report.model_dump())

    # Also log as pretty-printed JSON for human readability
    print("=" * 80)
    print("DLQ TRIAGE FINAL REPORT")
    print("=" * 80)
    print(json.dumps(report.model_dump(), indent=2, default=str))
    print("=" * 80)

    return report.model_dump()


def _generate_recommendation(
    triage: TriageReport, ch_signals: ClickHouseSignals, replay: ReplayReport
) -> str:
    """Generate human-readable recommendation based on reports."""
    recommendations = []

    # DLQ lag check
    if triage.sampled_count > 100:
        recommendations.append(
            f"⚠️ DLQ has {triage.sampled_count} messages (sample). Investigate root cause."
        )

    # Error class recommendations
    if triage.dominant_class == DlqErrorClass.INFRA_TRANSIENT:
        recommendations.append(
            f"✅ Dominant errors are transient ({triage.dominant_class_percentage:.1%}). "
            "Consider increasing retry limits or checking infrastructure."
        )
    elif triage.dominant_class == DlqErrorClass.STORAGE_SCHEMA:
        recommendations.append(
            f"❌ Schema/storage errors detected ({triage.dominant_class_percentage:.1%}). "
            "Fix ClickHouse schema before replaying."
        )
    elif triage.dominant_class == DlqErrorClass.DATA_PARSING:
        recommendations.append(
            f"❌ Data parsing errors ({triage.dominant_class_percentage:.1%}). "
            "Review input data sources and parsers."
        )

    # ClickHouse signals
    if ch_signals.parse_errors_last_10min > 0:
        recommendations.append(
            f"⚠️ {ch_signals.parse_errors_last_10min} parse errors in last 10 min. "
            "Check data quality."
        )

    if ch_signals.ingestion_lag_seconds and ch_signals.ingestion_lag_seconds > 300:
        recommendations.append(
            f"⚠️ Ingestion lag is {ch_signals.ingestion_lag_seconds:.0f}s. "
            "Check worker health."
        )

    # Replay status
    if replay.replayed:
        recommendations.append(
            f"✅ Replayed {replay.replayed_count} messages to {replay.replay_topic}."
        )
    else:
        recommendations.append(f"ℹ️ No replay performed: {replay.error}")

    return " ".join(recommendations)


# ============================================================
# DAG Definition
# ============================================================

default_args = {
    "owner": "logcelot",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="dlq_triage_replay",
    default_args=default_args,
    description="Monitor Kafka DLQ, classify errors, and replay if safe",
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["logcelot", "dlq", "data-quality"],
) as dag:

    # Task 1: Sample messages from DLQ
    dlq_sample = KafkaDlqSampleOperator(
        task_id="dlq_sample_messages",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        dlq_topic=DLQ_TOPIC,
        sample_size=DLQ_SAMPLE_SIZE,
        commit_offsets=False,  # Don't commit during sampling
    )

    # Task 2: Classify sampled messages
    classify = PythonOperator(
        task_id="classify_sample",
        python_callable=classify_sample,
    )

    # Task 3: Check ClickHouse signals
    ch_signals = PythonOperator(
        task_id="clickhouse_signal_checks",
        python_callable=clickhouse_signal_checks,
    )

    # Task 4: Replay ALL messages from DLQ (simplified - no branching)
    replay = PythonOperator(
        task_id="replay_dlq",
        python_callable=replay_messages_from_dlq,
    )

    # Task 5: Generate final report
    report = PythonOperator(
        task_id="final_report",
        python_callable=final_report,
    )

    # Define task dependencies (simplified)
    dlq_sample >> classify >> ch_signals >> replay >> report
