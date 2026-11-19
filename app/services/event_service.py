"""
Business logic services: event creation, delivery management, SSRF protection.
"""
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.exceptions import (
    DeliveryAlreadySucceededError,
    NotFoundError,
    SSRFProtectionError,
)
from app.db.models import DeliveryStatus, OutboxEvent, WebhookDelivery, WebhookEvent
from app.observability.logging import get_logger
from app.observability.metrics import (
    webhook_deliveries_total,
    webhook_events_received_total,
)
from app.schemas import (
    CreateEventRequest,
    CreateEventResponse,
    DeliveryResponse,
    EventResponse,
    PaginatedDeliveriesResponse,
    ReplayResponse,
)

logger = get_logger(__name__)
settings = get_settings()


def validate_endpoint_url(url: str) -> None:
    """
    SSRF protection: reject URLs not in the configured allowlist.

    This prevents a malicious actor from using the webhook system as a
    proxy to scan internal networks or hit unintended endpoints.
    """
    normalized = url.lower()
    for prefix in settings.allowed_endpoint_prefixes:
        if normalized.startswith(prefix.lower()):
            return
    raise SSRFProtectionError(
        f"Endpoint URL '{url}' is not in the allowed destinations list. "
        "Configure ALLOWED_ENDPOINT_PREFIXES to add new destinations.",
        details={"url": url, "allowed": settings.allowed_endpoint_prefixes},
    )


def categorize_endpoint(url: str) -> str:
    """
    Return a low-cardinality label for Prometheus (avoid raw URL as label).
    Examples: "success", "fail", "flaky", "slow", "rate_limit", "unknown"
    """
    for category in ("success", "fail", "flaky", "slow", "rate-limit", "timeout", "invalid"):
        if category in url:
            return category.replace("-", "_")
    return "unknown"


async def create_event_with_delivery(
    db: AsyncSession,
    request: CreateEventRequest,
) -> CreateEventResponse:
    """
    Atomically create a WebhookEvent, a WebhookDelivery, and an OutboxEvent.

    All three records are written in a single database transaction. If the
    transaction is committed successfully, the outbox publisher will eventually
    publish the delivery job to RabbitMQ — even if the process crashes before
    publishing.

    This is the core of the Transactional Outbox Pattern.
    """
    validate_endpoint_url(request.endpoint_url)

    event_id = uuid.uuid4()
    delivery_id = uuid.uuid4()

    # 1. Create the domain event
    event = WebhookEvent(
        id=event_id,
        event_type=request.event_type,
        payload=request.payload,
    )
    db.add(event)

    # 2. Create the delivery record
    delivery = WebhookDelivery(
        id=delivery_id,
        event_id=event_id,
        endpoint_url=request.endpoint_url,
        status=DeliveryStatus.PENDING,
        attempts=0,
        max_attempts=request.max_attempts,
    )
    db.add(delivery)

    # 3. Create the outbox record (same transaction!)
    outbox = OutboxEvent(
        id=uuid.uuid4(),
        aggregate_id=delivery_id,
        event_type="webhook.delivery.requested",
        payload={
            "delivery_id": str(delivery_id),
            "event_id": str(event_id),
            "endpoint_url": request.endpoint_url,
            "event_type": request.event_type,
            "max_attempts": request.max_attempts,
        },
        published=False,
    )
    db.add(outbox)

    # All three writes committed atomically by the caller (via get_db dependency)
    await db.flush()  # assign IDs without committing

    logger.info(
        "Event and delivery created",
        extra={
            "event_id": str(event_id),
            "delivery_id": str(delivery_id),
            "event_type": request.event_type,
            "endpoint_url": request.endpoint_url,
        },
    )

    # Update metrics
    webhook_events_received_total.labels(
        service=settings.app_name,
        event_type=request.event_type,
    ).inc()
    webhook_deliveries_total.labels(service=settings.app_name).inc()

    return CreateEventResponse(event_id=event_id, delivery_id=delivery_id)


async def get_event(db: AsyncSession, event_id: uuid.UUID) -> EventResponse:
    result = await db.execute(
        select(WebhookEvent).where(WebhookEvent.id == event_id)
    )
    event = result.scalar_one_or_none()
    if event is None:
        raise NotFoundError(f"Event {event_id} not found")
    return EventResponse.model_validate(event)


async def list_deliveries(
    db: AsyncSession,
    status: str | None = None,
    page: int = 1,
    page_size: int = 20,
) -> PaginatedDeliveriesResponse:
    query = select(WebhookDelivery)
    count_query = select(func.count()).select_from(WebhookDelivery)

    if status:
        query = query.where(WebhookDelivery.status == status)
        count_query = count_query.where(WebhookDelivery.status == status)

    offset = (page - 1) * page_size
    query = query.order_by(WebhookDelivery.created_at.desc()).offset(offset).limit(page_size)

    result = await db.execute(query)
    deliveries = result.scalars().all()

    total_result = await db.execute(count_query)
    total = total_result.scalar_one()

    return PaginatedDeliveriesResponse(
        items=[DeliveryResponse.model_validate(d) for d in deliveries],
        total=total,
        page=page,
        page_size=page_size,
        has_next=(page * page_size) < total,
    )


async def get_delivery(db: AsyncSession, delivery_id: uuid.UUID) -> DeliveryResponse:
    result = await db.execute(
        select(WebhookDelivery).where(WebhookDelivery.id == delivery_id)
    )
    delivery = result.scalar_one_or_none()
    if delivery is None:
        raise NotFoundError(f"Delivery {delivery_id} not found")
    return DeliveryResponse.model_validate(delivery)


async def replay_delivery(
    db: AsyncSession,
    delivery_id: uuid.UUID,
    force: bool = False,
) -> ReplayResponse:
    """
    Re-queue a failed or dead-lettered delivery by inserting a new outbox record.

    Safety:
    - If status is 'delivered' and force=False, raise DeliveryAlreadySucceededError.
    - Creates a fresh outbox record pointing to the existing delivery ID.
    - The worker uses the delivery ID as an idempotency key to avoid re-delivering
      if the delivery was actually completed by a racing worker.
    """
    result = await db.execute(
        select(WebhookDelivery).where(WebhookDelivery.id == delivery_id)
    )
    delivery = result.scalar_one_or_none()
    if delivery is None:
        raise NotFoundError(f"Delivery {delivery_id} not found")

    if delivery.status == DeliveryStatus.DELIVERED and not force:
        raise DeliveryAlreadySucceededError(
            f"Delivery {delivery_id} already succeeded. "
            "Pass force=true to replay anyway (not idempotent)."
        )

    # Reset the delivery to pending so the worker picks it up
    delivery.status = DeliveryStatus.PENDING
    delivery.last_error = None
    delivery.next_retry_at = None

    # Insert a new outbox record to trigger re-publishing
    outbox = OutboxEvent(
        id=uuid.uuid4(),
        aggregate_id=delivery_id,
        event_type="webhook.delivery.replayed",
        payload={
            "delivery_id": str(delivery_id),
            "event_id": str(delivery.event_id),
            "endpoint_url": delivery.endpoint_url,
            "max_attempts": delivery.max_attempts,
        },
        published=False,
    )
    db.add(outbox)
    await db.flush()

    logger.info("Delivery replay queued", extra={"delivery_id": str(delivery_id)})
    return ReplayResponse(
        delivery_id=delivery_id,
        message="Delivery has been re-queued for processing.",
    )

