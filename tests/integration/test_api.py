"""
Integration tests for the FastAPI API.

These tests use an in-process httpx TestClient. They mock the database
session to avoid requiring a live PostgreSQL instance. Tests that require
a real database are marked with @pytest.mark.integration.

For full integration tests against a live environment, use:
  make test-integration
"""
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from httpx import AsyncClient


@pytest.fixture
def app():
    """Return the FastAPI app with observability disabled."""
    import os
    os.environ["OTEL_ENABLED"] = "false"
    os.environ["LOG_FORMAT"] = "text"

    from app.main import app as fastapi_app
    return fastapi_app


@pytest.fixture
def client(app):
    """Synchronous test client — suitable for simple endpoint tests."""
    with TestClient(app) as c:
        yield c


class TestHealthEndpoints:
    """Smoke tests for health/readiness endpoints."""

    def test_health_returns_ok(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"

    def test_health_contains_service_name(self, client):
        response = client.get("/health")
        data = response.json()
        assert "service" in data

    def test_metrics_returns_text(self, client):
        response = client.get("/metrics")
        assert response.status_code == 200
        assert "text/plain" in response.headers["content-type"]


class TestCreateEvent:
    """Tests for POST /api/v1/events."""

    @pytest.mark.asyncio
    async def test_create_event_with_mocked_db(self, app):
        """
        Test that POST /api/v1/events calls the service and returns 202.
        Uses dependency override to avoid hitting a real database.
        """
        import uuid
        from app.api.dependencies import get_db
        from app.schemas import CreateEventResponse

        mock_event_id = uuid.uuid4()
        mock_delivery_id = uuid.uuid4()

        # Mock the service function
        with patch(
            "app.api.routes.events.create_event_with_delivery",
            new_callable=AsyncMock,
            return_value=CreateEventResponse(
                event_id=mock_event_id,
                delivery_id=mock_delivery_id,
            ),
        ):
            async with AsyncClient(app=app, base_url="http://test") as ac:
                response = await ac.post(
                    "/api/v1/events",
                    json={
                        "event_type": "order.created",
                        "payload": {"order_id": "ORD-001"},
                        "endpoint_url": "http://receiver:8001/webhooks/success",
                    },
                )

        assert response.status_code == 202
        data = response.json()
        assert "event_id" in data
        assert "delivery_id" in data

    @pytest.mark.asyncio
    async def test_ssrf_blocked_url_returns_422(self, app):
        """External URLs should be rejected by SSRF protection."""
        with patch("app.api.routes.events.create_event_with_delivery", new_callable=AsyncMock) as mock_svc:
            from app.core.exceptions import SSRFProtectionError
            mock_svc.side_effect = SSRFProtectionError("SSRF blocked")

            async with AsyncClient(app=app, base_url="http://test") as ac:
                response = await ac.post(
                    "/api/v1/events",
                    json={
                        "event_type": "order.created",
                        "payload": {},
                        "endpoint_url": "http://evil.com/steal",
                    },
                )

        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_invalid_event_type_rejected(self, app):
        """Event type with spaces should fail Pydantic validation."""
        async with AsyncClient(app=app, base_url="http://test") as ac:
            response = await ac.post(
                "/api/v1/events",
                json={
                    "event_type": "order created",  # spaces not allowed
                    "payload": {},
                    "endpoint_url": "http://receiver:8001/webhooks/success",
                },
            )
        assert response.status_code == 422

