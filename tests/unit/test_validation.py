"""
Unit tests for event validation and SSRF protection.
"""
import pytest

from app.core.exceptions import SSRFProtectionError
from app.schemas import CreateEventRequest
from app.services.event_service import categorize_endpoint, validate_endpoint_url


class TestSSRFProtection:
    """Verify that URL validation prevents SSRF attacks."""

    def test_allowed_receiver_url(self):
        """URLs starting with http://receiver should be allowed."""
        validate_endpoint_url("http://receiver:8001/webhooks/success")

    def test_allowed_localhost_url(self):
        validate_endpoint_url("http://localhost:8001/webhooks/success")

    def test_disallowed_external_url(self):
        with pytest.raises(SSRFProtectionError):
            validate_endpoint_url("http://evil.com/steal-data")

    def test_disallowed_internal_network(self):
        with pytest.raises(SSRFProtectionError):
            validate_endpoint_url("http://192.168.1.1/admin")

    def test_disallowed_metadata_service(self):
        """AWS IMDS endpoint must be blocked."""
        with pytest.raises(SSRFProtectionError):
            validate_endpoint_url("http://169.254.169.254/latest/meta-data/")

    def test_disallowed_file_url(self):
        with pytest.raises(SSRFProtectionError):
            validate_endpoint_url("file:///etc/passwd")


class TestCreateEventRequest:
    """Validate the Pydantic schema for event creation."""

    def test_valid_request(self):
        req = CreateEventRequest(
            event_type="order.created",
            payload={"order_id": "ORD-001"},
            endpoint_url="http://receiver:8001/webhooks/success",
        )
        assert req.event_type == "order.created"

    def test_event_type_uppercased_is_lowercased(self):
        req = CreateEventRequest(
            event_type="Order.Created",
            payload={},
            endpoint_url="http://receiver:8001/webhooks/success",
        )
        assert req.event_type == "order.created"

    def test_event_type_with_spaces_is_rejected(self):
        with pytest.raises(Exception):
            CreateEventRequest(
                event_type="order created",
                payload={},
                endpoint_url="http://receiver:8001/webhooks/success",
            )

    def test_empty_event_type_is_rejected(self):
        with pytest.raises(Exception):
            CreateEventRequest(
                event_type="",
                payload={},
                endpoint_url="http://receiver:8001/webhooks/success",
            )

    def test_max_attempts_validated(self):
        with pytest.raises(Exception):
            CreateEventRequest(
                event_type="order.created",
                payload={},
                endpoint_url="http://receiver:8001/webhooks/success",
                max_attempts=0,  # must be >= 1
            )

    def test_max_attempts_upper_bound(self):
        with pytest.raises(Exception):
            CreateEventRequest(
                event_type="order.created",
                payload={},
                endpoint_url="http://receiver:8001/webhooks/success",
                max_attempts=11,  # must be <= 10
            )


class TestEndpointCategorization:
    """Verify endpoint categorization for Prometheus labels."""

    def test_success_endpoint(self):
        assert categorize_endpoint("http://receiver/webhooks/success") == "success"

    def test_fail_endpoint(self):
        assert categorize_endpoint("http://receiver/webhooks/fail") == "fail"

    def test_flaky_endpoint(self):
        assert categorize_endpoint("http://receiver/webhooks/flaky") == "flaky"

    def test_rate_limit_endpoint(self):
        assert categorize_endpoint("http://receiver/webhooks/rate-limit") == "rate_limit"

    def test_unknown_endpoint(self):
        assert categorize_endpoint("http://receiver/webhooks/custom") == "unknown"

