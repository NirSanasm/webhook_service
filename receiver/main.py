"""
Mock Receiver Service.

A separate FastAPI application that simulates various external webhook endpoints
for testing delivery, retry, idempotency, and backpressure scenarios.

Each endpoint has documented behaviour. Use them in experiments by setting
the endpoint_url in CreateEventRequest to the corresponding path.

Idempotency:
  Every endpoint checks a local in-memory store (and optionally PostgreSQL)
  for the delivery_id before processing. If already seen, it returns 200 without
  side effects. This simulates a well-behaved receiver.
"""
import asyncio
import logging
import os
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from app.observability.logging import configure_logging

configure_logging(service_name="receiver")
logger = logging.getLogger("receiver")

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------
FLAKY_FAIL_COUNT = int(os.getenv("RECEIVER_FLAKY_FAIL_COUNT", "3"))
SLOW_DELAY_SECONDS = float(os.getenv("RECEIVER_SLOW_DELAY_SECONDS", "10.0"))
RATE_LIMIT_WAIT_SECONDS = int(os.getenv("RECEIVER_RATE_LIMIT_WAIT_SECONDS", "5"))
TIMEOUT_DELAY_SECONDS = float(os.getenv("RECEIVER_TIMEOUT_DELAY_SECONDS", "60.0"))

# ---------------------------------------------------------------------------
# In-memory idempotency store
# (For production use PostgreSQL or Redis; here we use memory for simplicity)
# ---------------------------------------------------------------------------
_received_deliveries: dict[str, dict] = {}
_flaky_counters: dict[str, int] = defaultdict(int)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Mock receiver starting")
    yield
    logger.info("Mock receiver shutting down")


app = FastAPI(
    title="Webhook Delivery Lab — Mock Receiver",
    description="Simulates external webhook endpoints for testing retry, idempotency, and backpressure.",
    version="0.1.0",
    lifespan=lifespan,
)


def _check_idempotency(delivery_id: str, endpoint: str) -> bool:
    """
    Return True if this delivery_id has already been processed by this endpoint.
    If not seen before, register it and return False.

    This is receiver-side idempotency: the receiver deduplicates repeated
    deliveries using the stable delivery_id provided by the worker.
    """
    key = f"{endpoint}:{delivery_id}"
    if key in _received_deliveries:
        logger.info(
            "Duplicate delivery detected — ignoring",
            extra={"delivery_id": delivery_id, "endpoint": endpoint},
        )
        return True

    _received_deliveries[key] = {
        "delivery_id": delivery_id,
        "endpoint": endpoint,
        "received_at": datetime.now(timezone.utc).isoformat(),
    }
    return False


def _extract_delivery_id(request: Request, body: dict) -> str:
    """Extract delivery ID from header or body."""
    delivery_id = request.headers.get("X-Delivery-ID") or body.get("delivery_id", "")
    if not delivery_id:
        delivery_id = str(uuid.uuid4())
        logger.warning("No delivery_id provided, generated one", extra={"delivery_id": delivery_id})
    return delivery_id


@app.post("/webhooks/success")
async def webhook_success(request: Request):
    """
    Always returns HTTP 200.
    Simulates a healthy, well-behaved receiver.
    """
    body = await request.json()
    delivery_id = _extract_delivery_id(request, body)

    if _check_idempotency(delivery_id, "success"):
        return JSONResponse(
            content={"status": "already_processed", "delivery_id": delivery_id},
            status_code=200,
        )

    logger.info("Webhook received successfully", extra={"delivery_id": delivery_id, "endpoint": "success"})
    return {"status": "ok", "delivery_id": delivery_id}


@app.post("/webhooks/fail")
async def webhook_fail(request: Request):
    """
    Always returns HTTP 500.
    Forces the worker to schedule retries and eventually dead-letter.
    """
    body = await request.json()
    delivery_id = _extract_delivery_id(request, body)
    logger.warning("Webhook rejected with 500", extra={"delivery_id": delivery_id, "endpoint": "fail"})
    raise HTTPException(status_code=500, detail={"error": "Simulated server error", "delivery_id": delivery_id})


@app.post("/webhooks/flaky")
async def webhook_flaky(request: Request):
    """
    Fails FLAKY_FAIL_COUNT times then succeeds.
    Useful for observing the retry workflow with eventual success.
    """
    body = await request.json()
    delivery_id = _extract_delivery_id(request, body)

    if _check_idempotency(delivery_id, "flaky"):
        return JSONResponse(
            content={"status": "already_processed", "delivery_id": delivery_id},
            status_code=200,
        )

    _flaky_counters[delivery_id] += 1
    call_count = _flaky_counters[delivery_id]

    if call_count <= FLAKY_FAIL_COUNT:
        logger.warning(
            "Flaky endpoint failing",
            extra={"delivery_id": delivery_id, "call_count": call_count, "fail_until": FLAKY_FAIL_COUNT},
        )
        raise HTTPException(
            status_code=500,
            detail={
                "error": f"Flaky failure {call_count}/{FLAKY_FAIL_COUNT}",
                "delivery_id": delivery_id,
            },
        )

    logger.info(
        "Flaky endpoint succeeding after failures",
        extra={"delivery_id": delivery_id, "call_count": call_count},
    )
    return {"status": "ok", "delivery_id": delivery_id, "call_count": call_count}


@app.post("/webhooks/slow")
async def webhook_slow(request: Request):
    """
    Waits SLOW_DELAY_SECONDS before responding.
    Tests worker timeout handling and backpressure.
    """
    body = await request.json()
    delivery_id = _extract_delivery_id(request, body)

    logger.info(
        "Slow webhook: waiting before responding",
        extra={"delivery_id": delivery_id, "delay_seconds": SLOW_DELAY_SECONDS},
    )
    await asyncio.sleep(SLOW_DELAY_SECONDS)

    if _check_idempotency(delivery_id, "slow"):
        return JSONResponse(
            content={"status": "already_processed", "delivery_id": delivery_id},
            status_code=200,
        )

    return {"status": "ok", "delivery_id": delivery_id, "waited_seconds": SLOW_DELAY_SECONDS}


@app.post("/webhooks/rate-limit")
async def webhook_rate_limit(request: Request):
    """
    Returns HTTP 429 with a Retry-After header.
    Simulates a rate-limited external API.
    """
    body = await request.json()
    delivery_id = _extract_delivery_id(request, body)

    logger.warning(
        "Rate limit endpoint returning 429",
        extra={"delivery_id": delivery_id, "retry_after": RATE_LIMIT_WAIT_SECONDS},
    )
    return JSONResponse(
        content={
            "error": "Too Many Requests",
            "delivery_id": delivery_id,
            "retry_after": RATE_LIMIT_WAIT_SECONDS,
        },
        status_code=429,
        headers={"Retry-After": str(RATE_LIMIT_WAIT_SECONDS)},
    )


@app.post("/webhooks/timeout")
async def webhook_timeout(request: Request):
    """
    Waits longer than the worker timeout before responding.
    The worker should time out and schedule a retry.
    """
    body = await request.json()
    delivery_id = _extract_delivery_id(request, body)

    logger.info(
        "Timeout endpoint: hanging indefinitely",
        extra={"delivery_id": delivery_id, "delay_seconds": TIMEOUT_DELAY_SECONDS},
    )
    await asyncio.sleep(TIMEOUT_DELAY_SECONDS)
    return {"status": "ok", "delivery_id": delivery_id}


@app.post("/webhooks/invalid")
async def webhook_invalid(request: Request):
    """
    Returns HTTP 400 Bad Request — a permanent, non-retryable failure.
    The worker should dead-letter immediately without retrying.
    """
    body = await request.json()
    delivery_id = _extract_delivery_id(request, body)

    logger.warning(
        "Invalid endpoint returning 400",
        extra={"delivery_id": delivery_id},
    )
    raise HTTPException(
        status_code=400,
        detail={"error": "Invalid request — this will not be retried", "delivery_id": delivery_id},
    )


@app.get("/webhooks/received")
async def list_received():
    """
    Return a summary of all received deliveries.
    Useful for verifying idempotency during experiments.
    """
    return {
        "total": len(_received_deliveries),
        "deliveries": list(_received_deliveries.values()),
    }


@app.get("/health")
async def health():
    return {"status": "ok", "service": "receiver"}

