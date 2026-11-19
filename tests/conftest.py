"""
conftest.py — shared pytest fixtures.
"""
import os

import pytest


@pytest.fixture(autouse=True)
def disable_otel():
    """Disable OpenTelemetry during tests to avoid connection errors."""
    os.environ["OTEL_ENABLED"] = "false"
    os.environ["LOG_FORMAT"] = "text"
    os.environ["DATABASE_URL"] = "postgresql+asyncpg://webhook:webhook@localhost:5432/webhookdb_test"
    yield

