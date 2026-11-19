"""
Unit tests for delivery status transitions.
"""
import pytest

from app.db.models import DeliveryStatus


class TestDeliveryStatusValues:
    """Verify delivery status constants are correct."""

    def test_status_values_complete(self):
        assert DeliveryStatus.PENDING == "pending"
        assert DeliveryStatus.PROCESSING == "processing"
        assert DeliveryStatus.DELIVERED == "delivered"
        assert DeliveryStatus.FAILED == "failed"
        assert DeliveryStatus.DEAD_LETTERED == "dead_lettered"

    def test_all_statuses_in_list(self):
        all_statuses = DeliveryStatus.ALL
        assert "pending" in all_statuses
        assert "processing" in all_statuses
        assert "delivered" in all_statuses
        assert "failed" in all_statuses
        assert "dead_lettered" in all_statuses

