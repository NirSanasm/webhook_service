"""
Transactional Outbox Publisher Worker.

The Dual-Write Problem:
  Without the outbox pattern, a service that writes to the DB AND publishes
  to a message broker faces a fundamental reliability issue:

    # UNSAFE: two separate writes
    await db.commit()       # Step 1 — if crash here:
    await broker.publish()  # Step 2 — message is LOST

  If the process crashes between steps, the database change is durable but
  the RabbitMQ message is lost. The event is never processed.

The Outbox Solution:
  1. The API writes to both the domain table AND the outbox table in ONE
     transaction. Either both succeed or both fail.
  2. This publisher reads unpublished outbox records.
  3. It publishes each record to RabbitMQ with publisher confirms.
  4. Only after the broker confirms the message does it mark the record published.

Crash scenarios and their outcomes:
  - Crash after API commit, before outbox poll:
    → Outbox record exists and will be published on next poll. Safe.

  - Crash after publish, before marking published:
    → Outbox record will be published again on next poll.
    → Downstream consumer receives duplicate. Must handle via idempotency.
    → This is the at-least-once delivery trade-off.

  - Crash after marking published:
    → No duplicate. Safe.

  - RabbitMQ unavailable during publish:
    → publish_delivery() raises. We record the error and try again next poll.
    → The outbox record remains unpublished. No data is lost.

SELECT FOR UPDATE SKIP LOCKED:
  If two publisher instances run simultaneously, they both query for unpublished
  records. Without locking, they would claim and publish the same records.
  SKIP LOCKED lets each publisher claim a batch exclusively without blocking.
"""
import asyncio
import logging
import uuid
from datetime import datetime, timezone

import aio_pika
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.db.models import OutboxEvent
from app.db.session import AsyncSessionLocal
from app.messaging.connection import get_connection
from app.messaging.publishers import publish_delivery
from app.messaging.topology import EXCHANGE_WEBHOOK_DIRECT, declare_topology
from app.observability.logging import get_logger
from app.observability.metrics import (
    outbox_pending_records,
    outbox_publish_duration_seconds,
    outbox_publish_failures_total,
    outbox_published_total,
)
from app.observability.tracing import get_tracer

logger = get_logger(__name__)
settings = get_settings()
tracer = get_tracer("webhook.outbox")


async def poll_and_publish() -> int:
    """
    One iteration of the outbox poll loop.

    Returns the number of records successfully published.
    """
    async with AsyncSessionLocal() as db:
        try:
            # Claim a batch of unpublished records using SELECT FOR UPDATE SKIP LOCKED.
            # This safely handles concurrent publisher instances.
            stmt = (
                select(OutboxEvent)
                .where(OutboxEvent.published == False)  # noqa: E712
                .where(OutboxEvent.attempts < settings.outbox_max_publish_attempts)
                .order_by(OutboxEvent.created_at.asc())
                .limit(settings.outbox_batch_size)
                .with_for_update(skip_locked=True)
            )
            result = await db.execute(stmt)
            records = result.scalars().all()

            if not records:
                return 0

            logger.debug(
                "Outbox poll found unpublished records",
                extra={"count": len(records)},
            )

            # Update pending gauge
            outbox_pending_records.labels(service=settings.app_name).set(len(records))

            # Get RabbitMQ channel with publisher confirms
            connection = await get_connection()
            async with connection.channel() as channel:
                await channel.confirm_delivery()
                webhook_exchange = await channel.declare_exchange(
                    EXCHANGE_WEBHOOK_DIRECT,
                    aio_pika.ExchangeType.DIRECT,
                    durable=True,
                )

                published_count = 0
                for record in records:
                    await _publish_record(db, record, webhook_exchange)
                    published_count += 1

                await db.commit()
                return published_count

        except Exception as exc:
            await db.rollback()
            logger.error("Outbox poll iteration failed", exc_info=exc)
            return 0


async def _publish_record(
    db: AsyncSession,
    record: OutboxEvent,
    exchange: object,
) -> None:
    """
    Publish a single outbox record to RabbitMQ.
    Mark as published only after broker confirm.
    """
    delivery_id = record.payload.get("delivery_id")
    event_id = record.payload.get("event_id")
    endpoint_url = record.payload.get("endpoint_url")
    event_type = record.payload.get("event_type", record.event_type)
    max_attempts = record.payload.get("max_attempts", settings.delivery_max_attempts)

    if not all([delivery_id, event_id, endpoint_url]):
        logger.error(
            "Outbox record missing required payload fields, marking as published to skip",
            extra={"outbox_id": str(record.id), "payload": record.payload},
        )
        record.published = True
        record.published_at = datetime.now(timezone.utc)
        return

    import time

    start = time.perf_counter()

    try:
        with tracer.start_as_current_span(
            "outbox.publish",
            attributes={
                "outbox.id": str(record.id),
                "delivery.id": delivery_id,
                "event.type": event_type,
            },
        ):
            await publish_delivery(
                exchange=exchange,
                delivery_id=delivery_id,
                event_id=event_id,
                endpoint_url=endpoint_url,
                event_type=event_type,
                max_attempts=max_attempts,
            )

        elapsed = time.perf_counter() - start
        outbox_publish_duration_seconds.labels(service=settings.app_name).observe(elapsed)

        # Mark published ONLY after broker confirms
        record.published = True
        record.published_at = datetime.now(timezone.utc)
        record.attempts += 1

        outbox_published_total.labels(service=settings.app_name).inc()
        logger.info(
            "Outbox record published",
            extra={"outbox_id": str(record.id), "delivery_id": delivery_id},
        )

    except Exception as exc:
        elapsed = time.perf_counter() - start
        # Do NOT mark as published — will be retried on next poll
        record.attempts += 1
        record.last_error = str(exc)

        outbox_publish_failures_total.labels(service=settings.app_name).inc()
        logger.error(
            "Failed to publish outbox record",
            exc_info=exc,
            extra={"outbox_id": str(record.id), "delivery_id": delivery_id},
        )


async def run_outbox_publisher() -> None:
    """
    Main loop for the outbox publisher.
    Runs continuously, polling at a configurable interval.
    """
    from app.observability.logging import configure_logging
    from app.observability.tracing import configure_tracing

    configure_logging(service_name="outbox-publisher")
    configure_tracing(
        service_name="outbox-publisher",
        otel_endpoint=settings.otel_endpoint,
        enabled=settings.otel_enabled,
    )

    logger.info(
        "Starting outbox publisher",
        extra={
            "poll_interval": settings.outbox_poll_interval_seconds,
            "batch_size": settings.outbox_batch_size,
        },
    )

    # Wait for infrastructure to be ready
    await asyncio.sleep(5)

    while True:
        try:
            count = await poll_and_publish()
            if count > 0:
                logger.info("Outbox batch published", extra={"count": count})
        except asyncio.CancelledError:
            logger.info("Outbox publisher shutting down")
            break
        except Exception as exc:
            logger.error("Outbox publisher error", exc_info=exc)

        await asyncio.sleep(settings.outbox_poll_interval_seconds)

