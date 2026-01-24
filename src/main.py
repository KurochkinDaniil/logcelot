"""Logcelot API Entrypoint.

FastAPI application for log ingestion and search.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Histogram, Gauge, make_asgi_app
import structlog

from src.api.routes import router
from src.kafka.producer import get_producer, shutdown_producer
from src.clickhouse.client import get_client, shutdown_client

APP_VERSION = "0.2.0"

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)

logger = structlog.get_logger()

# Prometheus metrics
REQUESTS_TOTAL = Counter(
    "logcelot_api_requests_total",
    "Total number of API requests",
    ["method", "endpoint", "status"]
)

REQUESTS_DURATION = Histogram(
    "logcelot_api_request_duration_seconds",
    "API request duration in seconds",
    ["method", "endpoint"]
)

KAFKA_MESSAGES_SENT = Counter(
    "logcelot_kafka_messages_sent_total",
    "Total messages sent to Kafka",
    ["topic", "status"]
)

KAFKA_CONNECTION_STATUS = Gauge(
    "logcelot_kafka_connection_status",
    "Kafka connection status (1=connected, 0=disconnected)"
)

CLICKHOUSE_CONNECTION_STATUS = Gauge(
    "logcelot_clickhouse_connection_status",
    "ClickHouse connection status (1=connected, 0=disconnected)"
)

LOGS_INGESTED_TOTAL = Counter(
    "logcelot_logs_ingested_total",
    "Total number of logs ingested",
    ["source", "level"]
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("application_starting", service="logcelot-api")

    # Kafka producer
    try:
        producer = await get_producer()
        logger.info("kafka_producer_initialized", healthy=await producer.health_check())
    except Exception as e:
        logger.error("kafka_producer_init_failed", error=str(e))

    # ClickHouse client (HA: through clickhouse-lb)
    try:
        ch = get_client()
        ch.check_health()
        logger.info("clickhouse_client_initialized", healthy=True)
    except Exception as e:
        logger.error("clickhouse_client_init_failed", error=str(e))

    logger.info("application_started", service="logcelot-api")
    yield

    logger.info("application_shutting_down", service="logcelot-api")

    try:
        await shutdown_producer()
        logger.info("kafka_producer_shutdown_complete")
    except Exception as e:
        logger.error("kafka_producer_shutdown_error", error=str(e))

    try:
        shutdown_client()
        logger.info("clickhouse_client_shutdown_complete")
    except Exception as e:
        logger.error("clickhouse_client_shutdown_error", error=str(e))

    logger.info("application_shutdown_complete", service="logcelot-api")


app = FastAPI(
    title="Logcelot API",
    description="Logcelot Log System",
    version=APP_VERSION,
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

app.include_router(router)

# Mount Prometheus metrics endpoint
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


@app.get("/health")
async def health_check() -> JSONResponse:
    # Kafka
    try:
        producer = await get_producer()
        kafka_healthy = await producer.health_check()
    except Exception:
        kafka_healthy = False

    # ClickHouse
    try:
        get_client().check_health()
        clickhouse_healthy = True
    except Exception:
        clickhouse_healthy = False

    overall_ok = kafka_healthy and clickhouse_healthy

    # Update Prometheus gauges
    KAFKA_CONNECTION_STATUS.set(1 if kafka_healthy else 0)
    CLICKHOUSE_CONNECTION_STATUS.set(1 if clickhouse_healthy else 0)

    return JSONResponse(
        status_code=200 if overall_ok else 503,
        content={
            "status": "healthy" if overall_ok else "degraded",
            "service": "logcelot-api",
            "version": APP_VERSION,
            "kafka": "connected" if kafka_healthy else "disconnected",
            "clickhouse": "connected" if clickhouse_healthy else "disconnected",
        },
    )


@app.get("/")
async def root() -> JSONResponse:
    return JSONResponse(
        content={
            "message": "Welcome to Logcelot API",
            "version": APP_VERSION,
            "docs": "/docs",
            "health": "/health",
            "endpoints": {
                "ingest_single": "POST /logs",
                "ingest_batch": "POST /logs/batch",
                "search": "GET /logs/search",
            },
        }
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
    )
