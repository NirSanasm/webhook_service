"""
Database models for the webhook delivery system.

Tables:
- webhook_events      — the original event submitted by the client
- webhook_deliveries  — one delivery record per target endpoint per event
- outbox_events       — the transactional outbox for reliable publishing
"""
import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from app.db.base import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class WebhookEvent(Base):
    """
    Represents an event submitted by a client.
    Immutable after creation.
    """

    __tablename__ = "webhook_events"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    event_type: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    deliveries: Mapped[list["WebhookDelivery"]] = relationship(
        "WebhookDelivery", back_populates="event", lazy="selectin"
    )

    def __repr__(self) -> str:
        return f"<WebhookEvent id={self.id} event_type={self.event_type}>"


class DeliveryStatus:
    """Enum-like constants for delivery status values."""

    PENDING = "pending"
    PROCESSING = "processing"
    DELIVERED = "delivered"
    FAILED = "failed"
    DEAD_LETTERED = "dead_lettered"

    ALL = [PENDING, PROCESSING, DELIVERED, FAILED, DEAD_LETTERED]


class WebhookDelivery(Base):
    """
    Tracks the delivery lifecycle for a single (event, endpoint) pair.

    At-least-once delivery:
      - A delivery may be attempted multiple times if a worker crashes
        after sending but before acknowledging the RabbitMQ message.
      - The receiver must implement idempotency using the delivery ID.

    Status transitions:
      pending → processing → delivered
                           → failed → (retry) → delivered
                                    → dead_lettered (max attempts reached)
    """

    __tablename__ = "webhook_deliveries"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    event_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("webhook_events.id"), nullable=False
    )
    endpoint_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    status: Mapped[str] = mapped_column(
        String(50), nullable=False, default=DeliveryStatus.PENDING
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=5)
    next_retry_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    response_status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    delivered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    event: Mapped["WebhookEvent"] = relationship("WebhookEvent", back_populates="deliveries")

    __table_args__ = (
        Index("ix_webhook_deliveries_status", "status"),
        Index("ix_webhook_deliveries_next_retry_at", "next_retry_at"),
        Index("ix_webhook_deliveries_event_id", "event_id"),
    )

    def __repr__(self) -> str:
        return f"<WebhookDelivery id={self.id} status={self.status} attempts={self.attempts}>"


class OutboxEvent(Base):
    """
    The transactional outbox table.

    Solves the dual-write problem:
      Without the outbox, a service would commit to the DB and then publish
      to RabbitMQ in two separate operations. If the process crashes between
      them, the DB change is durable but the message is lost.

    With the outbox:
      - Both the domain change AND the outbox record are written in ONE
        transaction. Either both commit or both roll back.
      - A separate outbox publisher reads unpublished records and publishes
        them to RabbitMQ. It marks records as published only AFTER receiving
        a publisher confirm from the broker.
      - If the publisher crashes before marking the record published, the
        same record will be published again on restart. Downstream consumers
        must handle duplicates (idempotency).

    SELECT FOR UPDATE SKIP LOCKED:
      When multiple publisher instances run, each claims a batch of records
      exclusively. SKIP LOCKED means they skip rows locked by other publishers
      instead of waiting, enabling safe horizontal scaling without contention.
    """

    __tablename__ = "outbox_events"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    aggregate_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False
    )
    event_type: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    published: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index("ix_outbox_events_published", "published"),
        Index("ix_outbox_events_created_at", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<OutboxEvent id={self.id} event_type={self.event_type} published={self.published}>"


class ReceiverIdempotencyRecord(Base):
    """
    Used by the mock receiver service to detect and deduplicate repeated deliveries.

    Why even with RabbitMQ and publisher confirms you still need this:
      - Publisher confirms guarantee the broker received the message.
      - Manual ack guarantees the consumer processed the message.
      - But between 'HTTP response received' and 'ack sent', the worker can crash.
      - RabbitMQ redelivers the message (redelivered=True flag).
      - Without receiver-side idempotency, the webhook fires twice.

    The delivery ID is the stable idempotency key across the entire pipeline.
    """

    __tablename__ = "receiver_idempotency_records"

    delivery_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True
    )
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    endpoint: Mapped[str] = mapped_column(String(255), nullable=False)
    status_returned: Mapped[int] = mapped_column(Integer, nullable=False)

    def __repr__(self) -> str:
        return f"<IdempotencyRecord delivery_id={self.delivery_id}>"

