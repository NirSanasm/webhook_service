"""
Health, readiness, and metrics endpoints.
"""
import time

import aio_pika
from fastapi import APIRouter
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.responses import Response

from app.core.config import get_settings
from app.db.session import engine
from app.schemas import HealthResponse, ReadinessResponse

router = APIRouter()
settings = get_settings()

_start_time = time.time()


@router.get("/health", response_model=HealthResponse, tags=["system"])
async def health() -> HealthResponse:
    """Basic liveness probe. Returns 200 if the process is running."""
    return HealthResponse(
        status="ok",
        service=settings.app_name,
        environment=settings.environment,
    )


@router.get("/ready", response_model=ReadinessResponse, tags=["system"])
async def readiness() -> ReadinessResponse:
    """
    Readiness probe. Checks PostgreSQL and RabbitMQ connectivity.
    Returns 200 only when all dependencies are available.
    """
    checks: dict[str, str] = {}

    # Check PostgreSQL
    try:
        async with engine.connect() as conn:
            await conn.execute(
                __import__("sqlalchemy", fromlist=["text"]).text("SELECT 1")
            )
        checks["postgresql"] = "ok"
    except Exception as e:
        checks["postgresql"] = f"error: {e}"

    # Check RabbitMQ
    try:
        connection = await aio_pika.connect_robust(
            settings.rabbitmq_url, timeout=5
        )
        await connection.close()
        checks["rabbitmq"] = "ok"
    except Exception as e:
        checks["rabbitmq"] = f"error: {e}"

    all_ok = all(v == "ok" for v in checks.values())
    status_code = 200 if all_ok else 503

    from fastapi.responses import JSONResponse
    return JSONResponse(
        content=ReadinessResponse(
            status="ready" if all_ok else "not_ready",
            checks=checks,
        ).model_dump(),
        status_code=status_code,
    )


@router.get("/metrics", tags=["system"])
async def metrics() -> Response:
    """Expose Prometheus metrics in text format."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

