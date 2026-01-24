"""Worker entrypoint for Kafka consumer.

Runs the consumer service as a standalone process.

Usage:
    python -m src.worker
"""

import asyncio
import sys
import os

import structlog

from src.services.consumer_service import run_consumer
from src.services.metrics import start_metrics_server

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)

logger = structlog.get_logger()


def main() -> None:
    logger.info(
        "worker_starting",
        service="logcelot-consumer",
        python_version=sys.version,
    )

    # Start Prometheus metrics server
    metrics_port = int(os.getenv("METRICS_PORT", "8000"))
    try:
        start_metrics_server(metrics_port)
    except Exception as e:
        logger.error("failed_to_start_metrics_server", error=str(e))

    try:
        asyncio.run(run_consumer())

    except asyncio.CancelledError:
        # Common path when asyncio.run() cancels tasks on SIGINT/SIGTERM. [web:322]
        logger.info("worker_cancelled")

    except KeyboardInterrupt:
        logger.info("worker_interrupted")

    except Exception as e:
        logger.critical(
            "worker_fatal_error",
            error=str(e),
            error_type=type(e).__name__,
        )
        sys.exit(1)

    finally:
        logger.info("worker_shutdown_complete")


if __name__ == "__main__":
    main()
