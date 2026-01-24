"""Main entrypoint for log generator service."""

import asyncio
import signal
import sys
from contextlib import asynccontextmanager

import structlog
import uvicorn
from fastapi import FastAPI

from src.generator.api import app as api_app
from src.generator.service import get_generator
from src.generator.config import get_generator_settings

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for generator service."""
    logger.info("log_generator_starting")
    
    # Start generator service
    generator = get_generator()
    await generator.start()
    
    logger.info("log_generator_started")
    
    yield
    
    logger.info("log_generator_shutting_down")
    
    # Stop generator service
    await generator.stop()
    
    logger.info("log_generator_shutdown_complete")


# Apply lifespan to API app
api_app.router.lifespan_context = lifespan


def main():
    """Run the generator service."""
    settings = get_generator_settings()
    
    logger.info(
        "generator_main_starting",
        kafka_brokers=settings.kafka_bootstrap_servers,
        kafka_topic=settings.kafka_topic,
        target_rps=settings.generator_rps,
        api_port=settings.generator_api_port,
    )
    
    try:
        uvicorn.run(
            api_app,
            host=settings.generator_api_host,
            port=settings.generator_api_port,
            log_level="info",
        )
    except KeyboardInterrupt:
        logger.info("generator_interrupted")
    except Exception as e:
        logger.error(
            "generator_fatal_error",
            error=str(e),
            error_type=type(e).__name__,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
