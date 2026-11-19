"""
Pydantic v2 schemas for API request/response validation.
"""
import uuid
from datetime import datetime
from typing import Annotated, Any

from pydantic import AnyHttpUrl, BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Request schemas
# ---------------------------------------------------------------------------


class CreateEventRequest(BaseModel):
    event_type: Annotated[str, Field(min_length=1, max_length=255, examples=["order.created"])]
    payload: dict[str, Any] = Field(default_factory=dict)
    endpoint_url: Annotated[str, Field(min_length=1, max_length=2048)]
    max_attempts: Annotated[int, Field(ge=1, le=10, default=5)]

    @field_validator("event_type")
    @classmethod
    def event_type_no_spaces(cls, v: str) -> str:
        if " " in v:
            raise ValueError("event_type must not contain spaces")
        return v.lower()


class ReplayDeliveryRequest(BaseModel):
    force: bool = Field(
        default=False,
        description="If True, replay even if the delivery already succeeded. "
        "Use only for testing; not idempotent.",
    )


class ListDeliveriesParams(BaseModel):
    status: str | None = Field(default=None, description="Filter by delivery status")
    page: Annotated[int, Field(ge=1, default=1)]
    page_size: Annotated[int, Field(ge=1, le=100, default=20)]


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------


class DeliveryResponse(BaseModel):
    model_config = {"from_attributes": True}

    id: uuid.UUID
    event_id: uuid.UUID
    endpoint_url: str
    status: str
    attempts: int
    max_attempts: int
    next_retry_at: datetime | None
    last_error: str | None
    response_status_code: int | None
    created_at: datetime
    updated_at: datetime
    delivered_at: datetime | None


class EventResponse(BaseModel):
    model_config = {"from_attributes": True}

    id: uuid.UUID
    event_type: str
    created_at: datetime
    updated_at: datetime
    deliveries: list[DeliveryResponse] = []


class CreateEventResponse(BaseModel):
    event_id: uuid.UUID
    delivery_id: uuid.UUID
    message: str = "Event accepted. Delivery is being processed asynchronously."


class ReplayResponse(BaseModel):
    delivery_id: uuid.UUID
    message: str


class PaginatedDeliveriesResponse(BaseModel):
    items: list[DeliveryResponse]
    total: int
    page: int
    page_size: int
    has_next: bool


class HealthResponse(BaseModel):
    status: str
    service: str
    environment: str


class ReadinessResponse(BaseModel):
    status: str  # "ready" or "not_ready"
    checks: dict[str, str]  # component -> "ok" | "error: ..."

