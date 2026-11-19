"""
Retry policy for webhook deliveries.

This module centralizes retry decisions so they can be tested independently
of RabbitMQ and the database.
"""
from dataclasses import dataclass
from enum import Enum

from app.core.config import get_settings

settings = get_settings()


class RetryDecision(str, Enum):
    """The outcome of evaluating whether a failed delivery should be retried."""

    RETRY = "retry"
    DEAD_LETTER = "dead_letter"
    NO_RETRY = "no_retry"  # permanent failure, no more attempts


@dataclass
class RetryResult:
    decision: RetryDecision
    delay_seconds: int = 0
    reason: str = ""
    routing_key: str = ""


def classify_http_status(status_code: int) -> tuple[bool, str]:
    """
    Classify an HTTP response status code as retryable or not.

    Returns (is_retryable, reason).

    Retryable:
      - 429 Too Many Requests (rate limited, usually transient)
      - 5xx Server errors (the receiver is having problems)

    Non-retryable:
      - 2xx (success — should not reach here)
      - 4xx (client errors — our request is malformed; retrying won't help)
        Exception: 429 is retryable.
      - The receiver explicitly rejected us; fix the payload instead.

    Note: The Retry-After header from a 429 response is respected by the
    worker when choosing the retry delay.
    """
    retryable_codes = set(settings.retryable_status_codes)
    permanent_failure_codes = set(settings.permanent_failure_codes)

    if status_code in retryable_codes:
        return True, f"HTTP {status_code} is transient"

    if 200 <= status_code < 300:
        return False, "success"

    if status_code in permanent_failure_codes:
        return False, f"HTTP {status_code} is a permanent client error"

    if 400 <= status_code < 500:
        return False, f"HTTP {status_code} is a client error (non-retryable)"

    if 500 <= status_code < 600:
        return True, f"HTTP {status_code} is a server error (retryable)"

    return False, f"HTTP {status_code} unclassified"


def evaluate_retry(
    attempts: int,
    max_attempts: int,
    http_status_code: int | None = None,
    is_retryable: bool = True,
) -> RetryResult:
    """
    Given the current delivery state, decide what to do next.

    Args:
        attempts: How many attempts have been made so far (including current).
        max_attempts: The configured maximum number of attempts.
        http_status_code: The HTTP response code (None for connection errors).
        is_retryable: Whether the failure type allows retrying.

    Returns:
        RetryResult with the decision and routing information.

    Retry schedule (attempt number → delay):
      1: immediate (no delay — first attempt, always try immediately)
      2: 10 seconds
      3: 60 seconds
      4: 300 seconds (5 minutes)
      5: final attempt (if this fails → dead letter)

    The 'attempts' counter is incremented BEFORE calling this function.
    So if attempts == 1, this was the first attempt.
    """
    from app.messaging.topology import (
        ROUTING_KEY_DEAD,
        ROUTING_KEY_RETRY_10S,
        ROUTING_KEY_RETRY_60S,
        ROUTING_KEY_RETRY_300S,
    )

    # Non-retryable failure: go directly to dead-letter
    if not is_retryable:
        return RetryResult(
            decision=RetryDecision.DEAD_LETTER,
            reason=f"Non-retryable failure (HTTP {http_status_code})",
            routing_key=ROUTING_KEY_DEAD,
        )

    # Exhausted all attempts
    if attempts >= max_attempts:
        return RetryResult(
            decision=RetryDecision.DEAD_LETTER,
            reason=f"Max attempts ({max_attempts}) exhausted",
            routing_key=ROUTING_KEY_DEAD,
        )

    # Determine retry delay based on the NEXT attempt number
    next_attempt = attempts + 1
    delay_map = {
        2: (10, ROUTING_KEY_RETRY_10S),
        3: (60, ROUTING_KEY_RETRY_60S),
        4: (300, ROUTING_KEY_RETRY_300S),
    }
    delay_seconds, routing_key = delay_map.get(
        next_attempt,
        (300, ROUTING_KEY_RETRY_300S),  # default to 300s for unexpected cases
    )

    return RetryResult(
        decision=RetryDecision.RETRY,
        delay_seconds=delay_seconds,
        reason=f"Attempt {attempts} failed; scheduling attempt {next_attempt} after {delay_seconds}s",
        routing_key=routing_key,
    )


def get_retry_delay_for_attempt(attempt: int) -> int:
    """Return the delay in seconds for the given attempt number (1-indexed)."""
    delays = {2: 10, 3: 60, 4: 300}
    return delays.get(attempt, 300)

