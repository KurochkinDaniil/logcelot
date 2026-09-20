"""Custom Airflow operator to replay DLQ messages to target topic.

Course Requirement: Demonstrates idempotency and error recovery patterns.
"""

import sys
import os
import json
from typing import Any

# Add parent directory (dags) to path for utils import
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import structlog

from utils.models import ReplayReport

logger = structlog.get_logger()


class KafkaReplayOperator(BaseOperator):
    """Replay sampled DLQ messages to target topic.

    Publishes messages from DLQ sample to a replay topic (default: logs.raw).
    Only commits DLQ consumer offsets after successful publish.

    Args:
        bootstrap_servers: Kafka broker addresses.
        dlq_topic: Source DLQ topic.
        replay_topic: Target topic for replayed messages.
        consumer_group: Consumer group for DLQ offset tracking.
        messages: List of DlqMessage dicts to replay.
        commit_offsets: Whether to commit DLQ offsets after replay.
        dry_run: If True, skip actual publishing (for testing).

    Returns:
        ReplayReport dictionary.

    Example:
        ```python
        replay_task = KafkaReplayOperator(
            task_id='replay_messages',
            bootstrap_servers='kafka-1:29092',
            dlq_topic='logs.dead_letter',
            replay_topic='logs.raw',
            messages="{{ task_instance.xcom_pull(task_ids='dlq_sample')['messages'] }}",
        )
        ```
    """

    template_fields = ("bootstrap_servers", "messages", "dry_run")

    @apply_defaults
    def __init__(
        self,
        bootstrap_servers: str,
        dlq_topic: str,
        replay_topic: str,
        consumer_group: str = "airflow-dlq-triage",
        messages: list[dict[str, Any]] | None = None,
        commit_offsets: bool = True,
        dry_run: bool = True,
        *args: Any,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        self.bootstrap_servers = bootstrap_servers
        self.dlq_topic = dlq_topic
        self.replay_topic = replay_topic
        self.consumer_group = consumer_group
        self.messages = messages or []
        self.commit_offsets = commit_offsets
        self.dry_run = dry_run

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        """Execute message replay.

        Returns:
            ReplayReport dictionary.
        """
        logger.info(
            "replay_start",
            dlq_topic=self.dlq_topic,
            replay_topic=self.replay_topic,
            message_count=len(self.messages),
            dry_run=self.dry_run,
        )

        if self.dry_run:
            logger.info("replay_skipped_dry_run", message_count=len(self.messages))
            return ReplayReport(
                replayed=False,
                replayed_count=0,
                replay_topic=self.replay_topic,
                offsets_committed=False,
                error="DRY_RUN mode enabled",
            ).model_dump()

        if not self.messages:
            logger.info("replay_skipped_no_messages")
            return ReplayReport(
                replayed=False,
                replayed_count=0,
                replay_topic=self.replay_topic,
                offsets_committed=False,
                error="No messages to replay",
            ).model_dump()

        try:
            # Step 1: Publish messages to replay topic
            replayed_count = self._publish_messages()

            # Step 2: Commit DLQ offsets (only if requested and successful)
            offsets_committed = False
            if self.commit_offsets and replayed_count > 0:
                offsets_committed = self._commit_dlq_offsets()

            logger.info(
                "replay_complete",
                replayed_count=replayed_count,
                offsets_committed=offsets_committed,
            )

            return ReplayReport(
                replayed=True,
                replayed_count=replayed_count,
                replay_topic=self.replay_topic,
                offsets_committed=offsets_committed,
            ).model_dump()

        except Exception as e:
            logger.error("replay_failed", error=str(e), error_type=type(e).__name__)
            return ReplayReport(
                replayed=False,
                replayed_count=0,
                replay_topic=self.replay_topic,
                offsets_committed=False,
                error=str(e),
            ).model_dump()

    def _publish_messages(self) -> int:
        """Publish messages to replay topic.

        Returns:
            Number of successfully published messages.
        """
        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",  # Wait for all replicas
            retries=3,
        )

        published_count = 0

        try:
            for msg_dict in self.messages:
                # Reconstruct message value
                value = msg_dict.get("value_json") or msg_dict.get("value_preview")
                key = msg_dict.get("key")

                # Publish to replay topic
                future = producer.send(
                    self.replay_topic,
                    value=value,
                    key=key,
                )

                # Wait for confirmation (synchronous for reliability)
                record_metadata = future.get(timeout=10)

                logger.debug(
                    "message_replayed",
                    partition=record_metadata.partition,
                    offset=record_metadata.offset,
                )

                published_count += 1

        except KafkaError as e:
            logger.error("publish_failed", error=str(e), published_count=published_count)
            raise

        finally:
            producer.close()

        return published_count

    def _commit_dlq_offsets(self) -> bool:
        """Commit DLQ consumer offsets for processed messages.

        Returns:
            True if offsets committed successfully.
        """
        try:
            # Create consumer to commit offsets
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers.split(","),
                group_id=self.consumer_group,
                enable_auto_commit=False,
            )

            # Commit current offsets (assumes messages were consumed in order)
            consumer.commit()

            logger.info("dlq_offsets_committed", consumer_group=self.consumer_group)

            consumer.close()

            return True

        except KafkaError as e:
            logger.error("commit_failed", error=str(e))
            return False
