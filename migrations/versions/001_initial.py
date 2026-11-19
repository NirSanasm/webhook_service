"""Initial schema: webhook_events, webhook_deliveries, outbox_events, receiver_idempotency_records

Revision ID: 001_initial
Revises: 
Create Date: 2024-01-01 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "001_initial"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # --- webhook_events ---
    op.create_table(
        "webhook_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(255), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_webhook_events_event_type", "webhook_events", ["event_type"])

    # --- webhook_deliveries ---
    op.create_table(
        "webhook_deliveries",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("endpoint_url", sa.String(2048), nullable=False),
        sa.Column("status", sa.String(50), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="5"),
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("response_status_code", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["event_id"], ["webhook_events.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_webhook_deliveries_status", "webhook_deliveries", ["status"])
    op.create_index(
        "ix_webhook_deliveries_next_retry_at", "webhook_deliveries", ["next_retry_at"]
    )
    op.create_index("ix_webhook_deliveries_event_id", "webhook_deliveries", ["event_id"])

    # --- outbox_events ---
    op.create_table(
        "outbox_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("aggregate_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(255), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("published", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_outbox_events_published", "outbox_events", ["published"])
    op.create_index("ix_outbox_events_created_at", "outbox_events", ["created_at"])

    # --- receiver_idempotency_records ---
    op.create_table(
        "receiver_idempotency_records",
        sa.Column("delivery_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("endpoint", sa.String(255), nullable=False),
        sa.Column("status_returned", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("delivery_id"),
    )


def downgrade() -> None:
    op.drop_table("receiver_idempotency_records")
    op.drop_index("ix_outbox_events_created_at", table_name="outbox_events")
    op.drop_index("ix_outbox_events_published", table_name="outbox_events")
    op.drop_table("outbox_events")
    op.drop_index("ix_webhook_deliveries_event_id", table_name="webhook_deliveries")
    op.drop_index("ix_webhook_deliveries_next_retry_at", table_name="webhook_deliveries")
    op.drop_index("ix_webhook_deliveries_status", table_name="webhook_deliveries")
    op.drop_table("webhook_deliveries")
    op.drop_index("ix_webhook_events_event_type", table_name="webhook_events")
    op.drop_table("webhook_events")

