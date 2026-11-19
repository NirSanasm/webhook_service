"""
Delivery Worker — consumes webhook delivery jobs from RabbitMQ.

Architecture:
  - One long-running asyncio task per worker slot (concurrency controlled by semaphore).
  - Each job gets its own AsyncSession (never share sessions between concurrent tasks).
  - Uses manual acknowledgement: ack only after the job outcome is safely persisted.
  - Handles redelivered messages (RabbitMQ redelivers on worker crash/reconnect).
  - Publisher confirms before acking the original message.

At-least-once delivery explanation:
  Consider this sequence:
    1. Worker receives message for delivery ID D.
    2. Worker sends HTTP request → receiver returns 200.
    3. Worker updates DB: status = 'delivered'.
    4. Worker crashes BEFORE sending ack to RabbitMQ.
    5. RabbitMQ redelivers the message (redelivered=True).
    6. Worker receives the same message again.

  Without idempotency: the webhook fires twice (bad for financial events, etc.)
  With idempotency: the receiver checks its idempotency table using delivery_id
    and returns 200 without side effects if it already processed this delivery.

  The worker also checks the delivery status in PostgreSQL before sending:
  if status == 'delivered', skip the HTTP request and just ack the message.
"""
import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone

import aio_pika
import httpx
from aio_pika.abc import AbstractIncomingMessage

from app.core.config import get_settings
from app.db.models import DeliveryStatus, WebhookDelivery
from app.db.session import AsyncSessionLocal
from app.messaging.connection import get_connection
from app.messaging.publishers import publish_dead_letter, publish_retry
from app.messaging.retry import RetryDecision, classify_http_status, evaluate_retry
from app.messaging.topology import (
    EXCHANGE_WEBHOOK_DIRECT,
    QUEUE_DELIVERY,
    ROUTING_KEY_DELIVER,
    declare_topology,
)
from app.observability.logging import get_logger
from app.observability.metrics import (
    webhook_dead_lettered_total,
    webhook_deliveries_failed_total,
    webhook_deliveries_succeeded_total,
    webhook_delivery_duration_seconds,
    webhook_processing_duration_seconds,
    webhook_retries_total,
    worker_active_jobs,
)
from app.observability.tracing import (
    extract_trace_context,
    get_tracer,
    start_span,
)
from sqlalchemy import select

logger = get_logger(__name__)
settings = get_settings()
tracer = get_tracer("webhook.worker")


async def process_delivery(message: AbstractIncomingMessage) -> None:
    """
    Process a single delivery job message.

    This is the core of the worker. Steps:
    1. Parse message body.
    2. Extract trace context from message headers.
    3. Load delivery record from PostgreSQL.
    4. Skip if already delivered (idempotency guard).
    5. Mark as 'processing'.
    6. Send HTTP webhook request.
    7. On success: mark delivered, ack message.
    8. On failure: evaluate retry policy.
       a. If retryable and not exhausted: publish to retry queue (with confirm),
          then ack the original message.
       b. If non-retryable or exhausted: publish to DLQ (with confirm),
          then ack the original message.

    IMPORTANT: Never ack before the retry/DLQ publish is confirmed.
    If publish_retry() raises, we nack (without requeue) and log the error.
    The original message stays in the queue for redelivery.

    Why nack(requeue=False) instead of nack(requeue=True):
      nack(requeue=True) puts the message back at the head of the queue
      immediately, creating a tight loop if the error is persistent.
      nack(requeue=False) sends it to the delivery queue's dead-letter
      routing (x-dead-letter-routing-key = webhook.dead), so it goes
      to the DLQ and we can inspect it.

      In this worker we generally prefer to ack the original message and
      publish explicitly to the retry or DLQ exchange so we have full control.
    """
    async with message.process(ignore_processed=True):
        start_time = asyncio.get_event_loop().time()
        delivery_id: str | None = None

        try:
            body = json.loads(message.body.decode())
            delivery_id = body["delivery_id"]
            endpoint_url = body["endpoint_url"]
            event_type = body.get("event_type", "unknown")
            correlation_id = str(
                message.headers.get("x-correlation-id", uuid.uuid4())
            )

            # Extract trace context from message headers for span parenting
            parent_context = extract_trace_context(
                {k: v for k, v in (message.headers or {}).items()}
            )

            logger.info(
                "Processing delivery",
                extra={
                    "delivery_id": delivery_id,
                    "endpoint_url": endpoint_url,
                    "redelivered": message.redelivered,
                    "worker_id": settings.worker_id,
                    "correlation_id": correlation_id,
                },
            )

        except (json.JSONDecodeError, KeyError) as e:
            logger.error("Malformed delivery message, discarding", exc_info=e)
            # Don't requeue malformed messages
            return

        endpoint_category = _categorize_endpoint(endpoint_url)
        worker_active_jobs.labels(
            service=settings.app_name, worker_id=settings.worker_id
        ).inc()

        try:
            with tracer.start_as_current_span(
                "webhook.delivery.process",
                context=parent_context,
                attributes={
                    "delivery.id": delivery_id,
                    "delivery.endpoint": endpoint_category,
                    "delivery.redelivered": str(message.redelivered),
                },
            ) as span:
                await _do_process_delivery(
                    message=message,
                    delivery_id=delivery_id,
                    endpoint_url=endpoint_url,
                    endpoint_category=endpoint_category,
                    event_type=event_type,
                    correlation_id=correlation_id,
                    span=span,
                )
        except Exception as exc:
            logger.error(
                "Unexpected error in delivery processor",
                exc_info=exc,
                extra={"delivery_id": delivery_id},
            )
            # Let the context manager handle the nack via process()
            raise
        finally:
            elapsed = asyncio.get_event_loop().time() - start_time
            worker_active_jobs.labels(
                service=settings.app_name, worker_id=settings.worker_id
            ).dec()


async def _do_process_delivery(
    message: AbstractIncomingMessage,
    delivery_id: str,
    endpoint_url: str,
    endpoint_category: str,
    event_type: str,
    correlation_id: str,
    span: object,
) -> None:
    """Inner delivery processing with its own database session."""
    start = asyncio.get_event_loop().time()

    async with AsyncSessionLocal() as db:
        try:
            # Load delivery record
            result = await db.execute(
                select(WebhookDelivery).where(
                    WebhookDelivery.id == uuid.UUID(delivery_id)
                )
            )
            delivery: WebhookDelivery | None = result.scalar_one_or_none()

            if delivery is None:
                logger.error(
                    "Delivery record not found, acking to prevent infinite loop",
                    extra={"delivery_id": delivery_id},
                )
                await message.ack()
                return

            # Idempotency guard: already delivered by a previous attempt
            if delivery.status == DeliveryStatus.DELIVERED:
                logger.info(
                    "Delivery already succeeded (redelivered message), acking without re-sending",
                    extra={"delivery_id": delivery_id},
                )
                await message.ack()
                return

            # Mark as processing
            delivery.status = DeliveryStatus.PROCESSING
            delivery.attempts += 1
            await db.commit()

            # Send the HTTP webhook
            attempt = delivery.attempts
            http_start = asyncio.get_event_loop().time()

            try:
                async with httpx.AsyncClient(timeout=settings.delivery_timeout_seconds) as client:
                    response = await client.post(
                        endpoint_url,
                        json={
                            "delivery_id": delivery_id,
                            "event_id": str(delivery.event_id),
                            "event_type": event_type,
                        },
                        headers={
                            "X-Delivery-ID": delivery_id,
                            "X-Webhook-Attempt": str(attempt),
                            "Content-Type": "application/json",
                        },
                    )
                http_duration = asyncio.get_event_loop().time() - http_start
                status_code = response.status_code

            except httpx.TimeoutException as e:
                http_duration = asyncio.get_event_loop().time() - http_start
                status_code = None
                is_retryable = True
                error_msg = f"Timeout: {e}"
                logger.warning(
                    "Webhook delivery timed out",
                    extra={"delivery_id": delivery_id, "attempt": attempt, "error": error_msg},
                )
                await _handle_failure(
                    db=db,
                    message=message,
                    delivery=delivery,
                    endpoint_category=endpoint_category,
                    event_type=event_type,
                    correlation_id=correlation_id,
                    http_status_code=None,
                    is_retryable=True,
                    error_msg=error_msg,
                )
                elapsed = asyncio.get_event_loop().time() - start
                webhook_processing_duration_seconds.labels(
                    service=settings.app_name, status="timeout"
                ).observe(elapsed)
                return

            except httpx.RequestError as e:
                http_duration = asyncio.get_event_loop().time() - http_start
                error_msg = f"Connection error: {e}"
                logger.warning(
                    "Webhook delivery connection error",
                    extra={"delivery_id": delivery_id, "attempt": attempt},
                )
                await _handle_failure(
                    db=db,
                    message=message,
                    delivery=delivery,
                    endpoint_category=endpoint_category,
                    event_type=event_type,
                    correlation_id=correlation_id,
                    http_status_code=None,
                    is_retryable=True,
                    error_msg=error_msg,
                )
                elapsed = asyncio.get_event_loop().time() - start
                webhook_processing_duration_seconds.labels(
                    service=settings.app_name, status="connection_error"
                ).observe(elapsed)
                return

            # Record HTTP duration metric
            webhook_delivery_duration_seconds.labels(
                service=settings.app_name,
                endpoint_category=endpoint_category,
                status=str(status_code),
            ).observe(http_duration)

            is_retryable, reason = classify_http_status(status_code)

            if 200 <= status_code < 300:
                # Success
                delivery.status = DeliveryStatus.DELIVERED
                delivery.response_status_code = status_code
                delivery.delivered_at = datetime.now(timezone.utc)
                delivery.last_error = None
                await db.commit()

                await message.ack()

                webhook_deliveries_succeeded_total.labels(
                    service=settings.app_name,
                    endpoint_category=endpoint_category,
                ).inc()

                elapsed = asyncio.get_event_loop().time() - start
                webhook_processing_duration_seconds.labels(
                    service=settings.app_name, status="success"
                ).observe(elapsed)

                logger.info(
                    "Webhook delivered successfully",
                    extra={
                        "delivery_id": delivery_id,
                        "attempt": attempt,
                        "status_code": status_code,
                        "duration_seconds": round(elapsed, 3),
                    },
                )
            else:
                await _handle_failure(
                    db=db,
                    message=message,
                    delivery=delivery,
                    endpoint_category=endpoint_category,
                    event_type=event_type,
                    correlation_id=correlation_id,
                    http_status_code=status_code,
                    is_retryable=is_retryable,
                    error_msg=f"HTTP {status_code}: {reason}",
                )
                elapsed = asyncio.get_event_loop().time() - start
                webhook_processing_duration_seconds.labels(
                    service=settings.app_name, status="failed"
                ).observe(elapsed)

        except Exception:
            await db.rollback()
            raise


async def _handle_failure(
    db,
    message: AbstractIncomingMessage,
    delivery: WebhookDelivery,
    endpoint_category: str,
    event_type: str,
    correlation_id: str,
    http_status_code: int | None,
    is_retryable: bool,
    error_msg: str,
) -> None:
    """
    Evaluate retry policy and route the message appropriately.
    """
    delivery_id = str(delivery.id)

    retry_result = evaluate_retry(
        attempts=delivery.attempts,
        max_attempts=delivery.max_attempts,
        http_status_code=http_status_code,
        is_retryable=is_retryable,
    )

    # Update delivery record
    delivery.last_error = error_msg
    delivery.response_status_code = http_status_code

    # Get a fresh channel for publishing (confirms per-channel)
    connection = await get_connection()
    async with connection.channel() as channel:
        await channel.set_qos(prefetch_count=1)
        await channel.confirm_delivery()  # Enable publisher confirms

        webhook_exchange = await channel.declare_exchange(
            EXCHANGE_WEBHOOK_DIRECT,
            aio_pika.ExchangeType.DIRECT,
            durable=True,
        )

        if retry_result.decision == RetryDecision.DEAD_LETTER:
            delivery.status = DeliveryStatus.DEAD_LETTERED
            await db.commit()

            # Publish to DLQ with confirm BEFORE acking original
            await publish_dead_letter(
                exchange=webhook_exchange,
                delivery_id=delivery_id,
                event_id=str(delivery.event_id),
                endpoint_url=delivery.endpoint_url,
                event_type=event_type,
                last_error=error_msg,
                correlation_id=correlation_id,
            )
            await message.ack()

            webhook_dead_lettered_total.labels(service=settings.app_name).inc()
            webhook_deliveries_failed_total.labels(
                service=settings.app_name,
                endpoint_category=endpoint_category,
                failure_type="dead_lettered",
            ).inc()

            logger.warning(
                "Delivery dead-lettered after exhausting retries",
                extra={
                    "delivery_id": delivery_id,
                    "attempts": delivery.attempts,
                    "last_error": error_msg,
                },
            )

        else:  # RETRY
            delivery.status = DeliveryStatus.FAILED
            await db.commit()

            # Publish retry with confirm BEFORE acking original
            await publish_retry(
                exchange=webhook_exchange,
                delivery_id=delivery_id,
                event_id=str(delivery.event_id),
                endpoint_url=delivery.endpoint_url,
                event_type=event_type,
                attempt=delivery.attempts + 1,
                max_attempts=delivery.max_attempts,
                correlation_id=correlation_id,
            )
            await message.ack()

            webhook_retries_total.labels(
                service=settings.app_name,
                attempt_number=str(delivery.attempts),
            ).inc()
            webhook_deliveries_failed_total.labels(
                service=settings.app_name,
                endpoint_category=endpoint_category,
                failure_type="retry_scheduled",
            ).inc()

            logger.info(
                "Delivery failed, retry scheduled",
                extra={
                    "delivery_id": delivery_id,
                    "attempt": delivery.attempts,
                    "next_routing_key": retry_result.routing_key,
                    "delay_seconds": retry_result.delay_seconds,
                    "error": error_msg,
                },
            )


def _categorize_endpoint(url: str) -> str:
    """Low-cardinality endpoint category for Prometheus labels."""
    for category in ("success", "fail", "flaky", "slow", "rate-limit", "timeout", "invalid"):
        if category in url:
            return category.replace("-", "_")
    return "unknown"


async def run_worker() -> None:
    """
    Start the delivery worker.
    Creates a single channel and sets up a consumer.
    Worker concurrency is controlled by WORKER_CONCURRENCY semaphore.
    """
    from app.observability.logging import configure_logging
    from app.observability.tracing import configure_tracing, instrument_httpx

    configure_logging(service_name="webhook-worker")
    configure_tracing(
        service_name="webhook-worker",
        otel_endpoint=settings.otel_endpoint,
        enabled=settings.otel_enabled,
    )
    instrument_httpx()

    logger.info(
        "Starting delivery worker",
        extra={
            "worker_id": settings.worker_id,
            "concurrency": settings.worker_concurrency,
            "prefetch": settings.rabbitmq_prefetch_count,
        },
    )

    semaphore = asyncio.Semaphore(settings.worker_concurrency)
    connection = await get_connection()

    async with connection.channel() as channel:
        # Prefetch: broker will send at most N unacked messages per consumer.
        # This prevents one worker from monopolizing the queue.
        await channel.set_qos(prefetch_count=settings.rabbitmq_prefetch_count)

        # Declare topology (idempotent)
        await declare_topology(channel)

        queue = await channel.declare_queue(QUEUE_DELIVERY, durable=True, passive=True)

        logger.info("Worker ready, consuming from queue", extra={"queue": QUEUE_DELIVERY})

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                # Acquire semaphore to limit concurrent jobs
                await semaphore.acquire()

                async def process_with_semaphore(msg: AbstractIncomingMessage) -> None:
                    try:
                        await process_delivery(msg)
                    finally:
                        semaphore.release()

                asyncio.create_task(process_with_semaphore(message))

