"""
API routes for webhook events and deliveries.
"""
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_db
from app.core.exceptions import (
    DeliveryAlreadySucceededError,
    NotFoundError,
    SSRFProtectionError,
    ValidationError,
)
from app.schemas import (
    CreateEventRequest,
    CreateEventResponse,
    DeliveryResponse,
    EventResponse,
    PaginatedDeliveriesResponse,
    ReplayDeliveryRequest,
    ReplayResponse,
)
from app.services.event_service import (
    create_event_with_delivery,
    get_delivery,
    get_event,
    list_deliveries,
    replay_delivery,
)

router = APIRouter()


@router.post(
    "/events",
    response_model=CreateEventResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit a webhook event for delivery",
    description=(
        "Creates an event, a delivery record, and an outbox record in a single "
        "transaction. Returns immediately without waiting for delivery."
    ),
)
async def create_event(
    request: CreateEventRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> CreateEventResponse:
    try:
        return await create_event_with_delivery(db, request)
    except SSRFProtectionError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    except ValidationError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))


@router.get(
    "/events/{event_id}",
    response_model=EventResponse,
    summary="Get an event and its associated deliveries",
)
async def get_event_by_id(
    event_id: uuid.UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> EventResponse:
    try:
        return await get_event(db, event_id)
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))


@router.get(
    "/deliveries",
    response_model=PaginatedDeliveriesResponse,
    summary="List deliveries with optional status filter",
)
async def list_deliveries_endpoint(
    db: Annotated[AsyncSession, Depends(get_db)],
    status_filter: str | None = Query(default=None, alias="status"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> PaginatedDeliveriesResponse:
    return await list_deliveries(db, status=status_filter, page=page, page_size=page_size)


@router.get(
    "/deliveries/{delivery_id}",
    response_model=DeliveryResponse,
    summary="Get delivery details by ID",
)
async def get_delivery_by_id(
    delivery_id: uuid.UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> DeliveryResponse:
    try:
        return await get_delivery(db, delivery_id)
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))


@router.post(
    "/deliveries/{delivery_id}/replay",
    response_model=ReplayResponse,
    summary="Replay a failed or dead-lettered delivery",
    description=(
        "Re-queues the delivery by inserting a new outbox record. "
        "The outbox publisher will pick it up and publish it to RabbitMQ. "
        "Replaying an already-delivered webhook may cause the receiver to "
        "process the event twice unless receiver-side idempotency is implemented."
    ),
)
async def replay_delivery_endpoint(
    delivery_id: uuid.UUID,
    request: ReplayDeliveryRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> ReplayResponse:
    try:
        return await replay_delivery(db, delivery_id, force=request.force)
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except DeliveryAlreadySucceededError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))

