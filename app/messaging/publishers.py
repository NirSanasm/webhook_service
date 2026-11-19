"""
RabbitMQ publisher with publisher confirms.

Publisher Confirms:
  When you enable publisher confirms on a channel, the broker sends back
  an ACK (or NACK) for every message you publish. This tells you whether
  the message was accepted and persisted by the broker.

  Without confirms: a published message could be silently dropped if the
  broker restarts before it is flushed to disk.
  With confirms: you know whether the broker received the message before
  proceeding. This is how the outbox publisher guarantees delivery.

  In aio_pika, use channel.confirm_delivery() to enable confirms.
  Then await exchange.publish(...) — it raises if the broker NACKs.

Persistence:
  delivery_mode=PERSISTENT tells the broker to write the message to disk.
  Combined with a durable queue, the message survives a broker restart.
  Without persistence, a message in a durable queue is still lost on restart.
"""
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

import aio_pika
from aio_pika.abc import AbstractExchange

from app.core.config import get_settings
from app.messaging.topology import (
    EXCHANGE_WEBHOOK_DIRECT,
    ROUTING_KEY_DEAD,
    ROUTING_KEY_DELIVER,
    ROUTING_KEY_RETRY_10S,
    ROUTING_KEY_RETRY_60S,
    ROUTING_KEY_RETRY_300S,
)
from app.observability.tracing import inject_trace_context

logger = logging.getLogger(__name__)
settings = get_settings()


def _build_message(
    body: dict[str, Any],
    delivery_id: str | None = None,
    correlation_id: str | None = None,
) -> aio_pika.Message:
    """
    Build a persistent aio_pika Message with trace context headers injected.

    Headers also carry the W3C traceparent/tracestate for distributed tracing
    across the async RabbitMQ boundary.
    """
    headers: dict[str, str] = {}
    if delivery_id:
        headers["x-delivery-id"] = delivery_id
    if correlation_id:
        headers["x-correlation-id"] = correlation_id

    # Inject current OpenTelemetry span context into headers
    inject_trace_context(headers)

    return aio_pika.Message(
        body=json.dumps(body, default=str).encode(),
        content_type="application/json",
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        message_id=str(uuid.uuid4()),
        timestamp=datetime.now(timezone.utc),
        headers=headers,
        correlation_id=correlation_id or str(uuid.uuid4()),
    )


async def publish_delivery(
    exchange: AbstractExchange,
    delivery_id: str,
    event_id: str,
    endpoint_url: str,
    event_type: str,
    max_attempts: int,
    correlation_id: str | None = None,
) -> None:
    """
    Publish a delivery job to the main delivery queue.
    Awaiting this call blocks until the broker confirms receipt.

    If the broker NACKs or the connection drops, aio_pika raises an exception.
    The caller (outbox publisher) must handle this and NOT mark the outbox
    record as published.
    """
    body = {
        "delivery_id": delivery_id,
        "event_id": event_id,
        "endpoint_url": endpoint_url,
        "event_type": event_type,
        "max_attempts": max_attempts,
    }
    message = _build_message(body, delivery_id=delivery_id, correlation_id=correlation_id)

    await exchange.publish(message, routing_key=ROUTING_KEY_DELIVER)
    logger.debug(
        "Delivery job published",
        extra={"delivery_id": delivery_id, "routing_key": ROUTING_KEY_DELIVER},
    )


async def publish_retry(
    exchange: AbstractExchange,
    delivery_id: str,
    event_id: str,
    endpoint_url: str,
    event_type: str,
    attempt: int,
    max_attempts: int,
    correlation_id: str | None = None,
) -> None:
    """
    Publish a delivery to the appropriate retry queue based on attempt number.

    Attempt 2 → webhook.retry.10s  (expires in 10s, redelivered to main queue)
    Attempt 3 → webhook.retry.60s
    Attempt 4 → webhook.retry.300s
    Attempt 5+ → dead letter (caller should use publish_dead_letter instead)

    Why confirm before acking the original message:
      The caller must await this publish (which includes the broker confirm)
      BEFORE acknowledging the original delivery message. If publish_retry()
      raises, the caller should NACK the original message so it is redelivered,
      rather than silently losing it.
    """
    body = {
        "delivery_id": delivery_id,
        "event_id": event_id,
        "endpoint_url": endpoint_url,
        "event_type": event_type,
        "attempt": attempt,
        "max_attempts": max_attempts,
    }
    message = _build_message(body, delivery_id=delivery_id, correlation_id=correlation_id)

    retry_routing_key_map = {
        2: ROUTING_KEY_RETRY_10S,
        3: ROUTING_KEY_RETRY_60S,
        4: ROUTING_KEY_RETRY_300S,
    }
    routing_key = retry_routing_key_map.get(attempt, ROUTING_KEY_RETRY_300S)

    await exchange.publish(message, routing_key=routing_key)
    logger.info(
        "Retry scheduled",
        extra={
            "delivery_id": delivery_id,
            "attempt": attempt,
            "routing_key": routing_key,
        },
    )


async def publish_dead_letter(
    exchange: AbstractExchange,
    delivery_id: str,
    event_id: str,
    endpoint_url: str,
    event_type: str,
    last_error: str,
    correlation_id: str | None = None,
) -> None:
    """
    Send a delivery to the dead-letter queue after all retries are exhausted.
    """
    body = {
        "delivery_id": delivery_id,
        "event_id": event_id,
        "endpoint_url": endpoint_url,
        "event_type": event_type,
        "last_error": last_error,
        "dead_lettered": True,
    }
    message = _build_message(body, delivery_id=delivery_id, correlation_id=correlation_id)

    await exchange.publish(message, routing_key=ROUTING_KEY_DEAD)
    logger.warning(
        "Delivery dead-lettered",
        extra={"delivery_id": delivery_id, "last_error": last_error},
    )

