"""
OpenTelemetry tracing setup.

Instruments FastAPI, HTTPX, SQLAlchemy, and asyncpg.
Propagates W3C Trace Context across HTTP and RabbitMQ message boundaries.
Exports spans to OpenTelemetry Collector via gRPC.
"""
import logging
from contextlib import contextmanager
from typing import Generator

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
from opentelemetry.trace import NonRecordingSpan, Span, SpanKind, Status, StatusCode

logger = logging.getLogger(__name__)


def configure_tracing(service_name: str, otel_endpoint: str, enabled: bool = True) -> None:
    """
    Set up the global TracerProvider.
    Call once at application startup before any instrumentation is used.
    """
    if not enabled:
        logger.info("OpenTelemetry tracing disabled")
        return

    resource = Resource.create(
        {
            "service.name": service_name,
            "service.version": "0.1.0",
            "deployment.environment": "development",
        }
    )

    exporter = OTLPSpanExporter(
        endpoint=otel_endpoint,
        insecure=True,
    )

    provider = TracerProvider(
        resource=resource,
        sampler=ParentBased(root=TraceIdRatioBased(1.0)),  # 100% sampling in lab
    )
    provider.add_span_processor(BatchSpanProcessor(exporter))

    trace.set_tracer_provider(provider)
    logger.info("OpenTelemetry tracing configured", extra={"otel_endpoint": otel_endpoint})


def instrument_fastapi(app: object) -> None:
    FastAPIInstrumentor.instrument_app(app)  # type: ignore[arg-type]


def instrument_httpx() -> None:
    HTTPXClientInstrumentor().instrument()


def instrument_sqlalchemy(engine: object) -> None:
    SQLAlchemyInstrumentor().instrument(engine=engine)


def instrument_asyncpg() -> None:
    AsyncPGInstrumentor().instrument()


def get_tracer(name: str) -> trace.Tracer:
    return trace.get_tracer(name)


def inject_trace_context(headers: dict[str, str]) -> dict[str, str]:
    """
    Inject the current W3C Trace Context into a headers dict.
    Use when publishing to RabbitMQ so the consumer can restore the trace.
    """
    inject(headers)
    return headers


def extract_trace_context(headers: dict[str, str]) -> object:
    """
    Extract W3C Trace Context from headers (e.g. from a RabbitMQ message).
    Returns an OTel Context object suitable for use as a parent span.
    """
    return extract(headers)


@contextmanager
def start_span(
    tracer: trace.Tracer,
    name: str,
    kind: SpanKind = SpanKind.INTERNAL,
    attributes: dict | None = None,
    parent_context: object | None = None,
) -> Generator[Span, None, None]:
    """
    Context manager that creates a span and automatically records exceptions.
    """
    with tracer.start_as_current_span(
        name,
        kind=kind,
        attributes=attributes or {},
        context=parent_context,
    ) as span:
        try:
            yield span
        except Exception as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            raise

