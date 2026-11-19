"""
Unit tests for retry policy logic.

These tests run without any external dependencies (no RabbitMQ, no PostgreSQL).
They verify the core retry decision-making logic in isolation.
"""
import pytest

from app.messaging.retry import (
    RetryDecision,
    classify_http_status,
    evaluate_retry,
    get_retry_delay_for_attempt,
)
from app.messaging.topology import (
    ROUTING_KEY_DEAD,
    ROUTING_KEY_RETRY_10S,
    ROUTING_KEY_RETRY_60S,
    ROUTING_KEY_RETRY_300S,
)


class TestClassifyHttpStatus:
    """Test HTTP status code classification."""

    def test_200_is_not_retryable_success(self):
        retryable, reason = classify_http_status(200)
        assert retryable is False
        assert "success" in reason

    def test_201_is_not_retryable_success(self):
        retryable, _ = classify_http_status(201)
        assert retryable is False

    def test_400_is_permanent_failure(self):
        retryable, _ = classify_http_status(400)
        assert retryable is False

    def test_401_is_permanent_failure(self):
        retryable, _ = classify_http_status(401)
        assert retryable is False

    def test_403_is_permanent_failure(self):
        retryable, _ = classify_http_status(403)
        assert retryable is False

    def test_404_is_permanent_failure(self):
        retryable, _ = classify_http_status(404)
        assert retryable is False

    def test_429_is_retryable(self):
        """429 Too Many Requests is transient — should retry."""
        retryable, _ = classify_http_status(429)
        assert retryable is True

    def test_500_is_retryable(self):
        retryable, _ = classify_http_status(500)
        assert retryable is True

    def test_502_is_retryable(self):
        retryable, _ = classify_http_status(502)
        assert retryable is True

    def test_503_is_retryable(self):
        retryable, _ = classify_http_status(503)
        assert retryable is True

    def test_504_is_retryable(self):
        retryable, _ = classify_http_status(504)
        assert retryable is True


class TestEvaluateRetry:
    """Test retry decision logic."""

    def test_first_attempt_failure_schedules_retry(self):
        result = evaluate_retry(attempts=1, max_attempts=5, http_status_code=500, is_retryable=True)
        assert result.decision == RetryDecision.RETRY
        assert result.routing_key == ROUTING_KEY_RETRY_10S
        assert result.delay_seconds == 10

    def test_second_attempt_failure_schedules_60s_retry(self):
        result = evaluate_retry(attempts=2, max_attempts=5, http_status_code=500, is_retryable=True)
        assert result.decision == RetryDecision.RETRY
        assert result.routing_key == ROUTING_KEY_RETRY_60S
        assert result.delay_seconds == 60

    def test_third_attempt_failure_schedules_300s_retry(self):
        result = evaluate_retry(attempts=3, max_attempts=5, http_status_code=500, is_retryable=True)
        assert result.decision == RetryDecision.RETRY
        assert result.routing_key == ROUTING_KEY_RETRY_300S
        assert result.delay_seconds == 300

    def test_max_attempts_reached_dead_letters(self):
        result = evaluate_retry(attempts=5, max_attempts=5, http_status_code=500, is_retryable=True)
        assert result.decision == RetryDecision.DEAD_LETTER
        assert result.routing_key == ROUTING_KEY_DEAD

    def test_non_retryable_immediately_dead_letters(self):
        """A 400 response should go directly to DLQ without retrying."""
        result = evaluate_retry(attempts=1, max_attempts=5, http_status_code=400, is_retryable=False)
        assert result.decision == RetryDecision.DEAD_LETTER
        assert result.routing_key == ROUTING_KEY_DEAD

    def test_non_retryable_on_first_attempt_no_retry(self):
        result = evaluate_retry(attempts=1, max_attempts=5, http_status_code=404, is_retryable=False)
        assert result.decision == RetryDecision.DEAD_LETTER

    def test_exactly_max_attempts_dead_letters(self):
        """When attempts == max_attempts, the next step is DLQ."""
        result = evaluate_retry(attempts=5, max_attempts=5, http_status_code=503)
        assert result.decision == RetryDecision.DEAD_LETTER

    def test_beyond_max_attempts_dead_letters(self):
        result = evaluate_retry(attempts=10, max_attempts=5, http_status_code=503)
        assert result.decision == RetryDecision.DEAD_LETTER


class TestRetryDelays:
    """Test exponential backoff delay values."""

    def test_attempt_2_delay_is_10s(self):
        assert get_retry_delay_for_attempt(2) == 10

    def test_attempt_3_delay_is_60s(self):
        assert get_retry_delay_for_attempt(3) == 60

    def test_attempt_4_delay_is_300s(self):
        assert get_retry_delay_for_attempt(4) == 300

    def test_attempt_5_defaults_to_300s(self):
        assert get_retry_delay_for_attempt(5) == 300

