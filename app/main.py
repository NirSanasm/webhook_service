"""
FastAPI application entry point.
"""
import logging
import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.routes import events, system
from app.core.config import get_settings
from app.core.exceptions import WebhookLabError
from app.observability.logging import configure_logging
from app.observability.metrics import app_info
from app.observability.tracing import configure_tracing, get_tracer, instrument_fastapi

settings = get_settings()
configure_logging(service_name=settings.app_name)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application startup and shutdown lifecycle."""
    logger.info(
        "Starting webhook delivery lab API",
        extra={"service": settings.app_name, "environment": settings.environment},
    )

    # Configure OpenTelemetry
    configure_tracing(
        service_name="webhook-api",
        otel_endpoint=settings.otel_endpoint,
        enabled=settings.otel_enabled,
    )
    instrument_fastapi(app)

    # Set Prometheus info metric
    app_info.info(
        {
            "version": "0.1.0",
            "environment": settings.environment,
            "service": settings.app_name,
        }
    )

    logger.info("API startup complete")
    yield
    logger.info("API shutdown complete")


app = FastAPI(
    title="Webhook Delivery Lab — API",
    description=(
        "A complete webhook delivery and observability laboratory. "
        "Submit events, track deliveries, observe retries and dead-letter routing."
    ),
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------


@app.middleware("http")
async def correlation_id_middleware(request: Request, call_next):
    """
    Attach a correlation ID to every request.
    Uses X-Correlation-ID header if present, otherwise generates one.
    """
    import uuid

    correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))

    # Store in request state for use in route handlers
    request.state.correlation_id = correlation_id

    start = time.perf_counter()
    response = await call_next(request)
    duration = time.perf_counter() - start

    response.headers["X-Correlation-ID"] = correlation_id

    # Record API request duration metric
    from app.observability.metrics import api_request_duration_seconds

    api_request_duration_seconds.labels(
        service=settings.app_name,
        method=request.method,
        endpoint=request.url.path,
        status_code=str(response.status_code),
    ).observe(duration)

    return response


# ---------------------------------------------------------------------------
# Exception handlers
# ---------------------------------------------------------------------------


@app.exception_handler(WebhookLabError)
async def webhook_lab_error_handler(request: Request, exc: WebhookLabError):
    return JSONResponse(
        status_code=500,
        content={"error": exc.message, "details": exc.details},
    )


# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------

app.include_router(events.router, prefix="/api/v1", tags=["events"])
app.include_router(system.router, tags=["system"])

