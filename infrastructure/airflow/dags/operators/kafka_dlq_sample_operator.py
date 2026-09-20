"""Custom Airflow operator to sample messages from Kafka DLQ.

Course Requirement: Custom operators demonstrate senior-level Airflow knowledge.
"""

import sys
import os
import json
from datetime import datetime
from typing import Any, Optional

# Add parent directory (dags) to path for utils import
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from kafka import KafkaConsumer, TopicPartition, KafkaAdminClient
from kafka.errors import KafkaError
import structlog

from utils.models import DlqMessage, DlqStats
from utils.classifier import classify_error

logger = structlog.get_logger()


class KafkaDlqSampleOperator(BaseOperator):
    """Sample messages from Kafka DLQ topic for triage.

    Reads up to `sample_size` messages from DLQ without committing offsets
    (unless `commit_offsets=True`). Classifies each message using error
    classification logic.

    Args:
        bootstrap_servers: Kafka broker addresses.
        dlq_topic: Dead Letter Queue topic name.
        consumer_group: Consumer group ID for offset tracking.
        sample_size: Maximum number of messages to sample.
        commit_offsets: Whether to commit offsets after sampling.
        timeout_ms: Consumer poll timeout in milliseconds.

    Returns:
        Dictionary with 'dlq_stats' and 'messages' keys.

    Example:
        ```python
        sample_task = KafkaDlqSampleOperator(
            task_id='dlq_sample_messages',
            bootstrap_servers='kafka-1:29092',
            dlq_topic='logs.dead_letter',
            sample_size=50,
        )
        ```
    """

    template_fields = ("bootstrap_servers", "dlq_topic", "sample_size")

    @apply_defaults
    def __init__(
        self,
        bootstrap_servers: str,
        dlq_topic: str,
        consumer_group: str = "airflow-dlq-triage",
        sample_size: int = 50,
        commit_offsets: bool = False,
        timeout_ms: int = 10000,
        max_value_bytes: int = 4096,
        *args: Any,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        self.bootstrap_servers = bootstrap_servers
        self.dlq_topic = dlq_topic
        self.consumer_group = consumer_group
        self.sample_size = sample_size
        self.commit_offsets = commit_offsets
        self.timeout_ms = timeout_ms
        self.max_value_bytes = max_value_bytes

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        """Execute DLQ sampling.

        Returns:
            Dictionary with DLQ stats and sampled messages.
        """
        logger.info(
            "dlq_sample_start",
            dlq_topic=self.dlq_topic,
            sample_size=self.sample_size,
            consumer_group=self.consumer_group,
        )

        # Step 1: Get DLQ stats (partition count, lag estimate)
        dlq_stats = self._get_dlq_stats()

        # Step 2: Sample messages
        messages = self._sample_messages()

        logger.info(
            "dlq_sample_complete",
            dlq_stats=dlq_stats.model_dump(),
            sampled_count=len(messages),
        )

        return {
            "dlq_stats": dlq_stats.model_dump(),
            "messages": [msg.model_dump() for msg in messages],
        }

    def _get_dlq_stats(self) -> DlqStats:
        """Get DLQ topic statistics using AdminClient."""
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers.split(","),
                request_timeout_ms=self.timeout_ms,
            )

            # Get partition metadata using consumer instead
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers.split(","),
                group_id=self.consumer_group,
                enable_auto_commit=False,
                consumer_timeout_ms=1000,
            )

            # Get partitions for the DLQ topic
            partitions_info = consumer.partitions_for_topic(self.dlq_topic)
            partitions = len(partitions_info) if partitions_info else 0

            if partitions == 0:
                logger.warning("dlq_topic_not_found", topic=self.dlq_topic)
                consumer.close()
                admin_client.close()
                return DlqStats(
                    topic=self.dlq_topic,
                    partitions=0,
                    total_lag_estimate=0,
                    consumer_group=self.consumer_group,
                )

            topic_partitions = [
                TopicPartition(self.dlq_topic, p) for p in partitions_info
            ]
            consumer.assign(topic_partitions)

            end_offsets = consumer.end_offsets(topic_partitions)
            committed_offsets = {
                tp: consumer.committed(tp) or 0 for tp in topic_partitions
            }

            total_lag = sum(
                end_offsets[tp] - committed_offsets[tp] for tp in topic_partitions
            )

            consumer.close()
            admin_client.close()

            return DlqStats(
                topic=self.dlq_topic,
                partitions=partitions,
                total_lag_estimate=total_lag,
                consumer_group=self.consumer_group,
            )

        except KafkaError as e:
            logger.error("dlq_stats_failed", error=str(e))
            # Return empty stats on error
            return DlqStats(
                topic=self.dlq_topic,
                partitions=0,
                total_lag_estimate=0,
                consumer_group=self.consumer_group,
            )

    def _sample_messages(self) -> list[DlqMessage]:
        """Sample messages from DLQ topic."""
        try:
            consumer = KafkaConsumer(
                self.dlq_topic,
                bootstrap_servers=self.bootstrap_servers.split(","),
                group_id=self.consumer_group,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                max_poll_records=self.sample_size,
                consumer_timeout_ms=self.timeout_ms,
            )

            messages: list[DlqMessage] = []

            for record in consumer:
                if len(messages) >= self.sample_size:
                    break

                # Extract headers
                headers_dict = {}
                if record.headers:
                    for key, value in record.headers:
                        try:
                            headers_dict[key] = value.decode("utf-8") if value else ""
                        except Exception:
                            headers_dict[key] = str(value)

                # Extract value (truncate to max_value_bytes)
                value_preview = ""
                value_json: Optional[dict[str, Any]] = None

                if record.value:
                    try:
                        value_str = record.value.decode("utf-8")
                        value_preview = value_str[:self.max_value_bytes]

                        # Try to parse as JSON
                        value_json = json.loads(value_str)
                    except Exception:
                        value_preview = str(record.value)[:self.max_value_bytes]

                # Classify error
                error_text = (
                    headers_dict.get("error_message")
                    or headers_dict.get("error")
                    or headers_dict.get("exception")
                )
                
                if not error_text and value_json and isinstance(value_json, dict):
                    error_text = value_json.get("error_message") or value_json.get("error_type")
                
                error_class, error_reason = classify_error(
                    error_text=error_text,
                    headers=headers_dict,
                    message_value=value_preview,
                )

                msg = DlqMessage(
                    partition=record.partition,
                    offset=record.offset,
                    key=record.key.decode("utf-8") if record.key else None,
                    timestamp=datetime.fromtimestamp(record.timestamp / 1000.0),
                    headers=headers_dict,
                    value_preview=value_preview,
                    value_json=value_json,
                    error_class=error_class,
                    error_reason=error_reason,
                )

                messages.append(msg)

            if self.commit_offsets and messages:
                consumer.commit()
                logger.info("dlq_offsets_committed", consumer_group=self.consumer_group)

            consumer.close()

            return messages

        except KafkaError as e:
            logger.error("dlq_sample_failed", error=str(e))
            return []
