"""
Core configuration using pydantic-settings.
All settings are read from environment variables with sensible defaults.

List-typed settings (allowed_endpoint_prefixes, retry_delays, etc.) accept
both JSON-array format ("[\"a\",\"b\"]") and comma-separated strings ("a,b,c")
when loaded from environment variables or .env files.
"""
from functools import lru_cache
import json
from typing import Any

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _parse_str_list(v: Any) -> list[str]:
    if isinstance(v, list):
        return [str(x) for x in v]
    if isinstance(v, str):
        v = v.strip()
        if not v:
            return []
        if v.startswith("[") and v.endswith("]"):
            try:
                return [str(x) for x in json.loads(v)]
            except Exception:
                pass
        return [item.strip() for item in v.split(",") if item.strip()]
    return [str(v)]


def _parse_int_list(v: Any) -> list[int]:
    if isinstance(v, list):
        return [int(x) for x in v]
    if isinstance(v, str):
        v = v.strip()
        if not v:
            return []
        if v.startswith("[") and v.endswith("]"):
            try:
                return [int(x) for x in json.loads(v)]
            except Exception:
                pass
        return [int(item.strip()) for item in v.split(",") if item.strip()]
    return [int(v)]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Application
    app_name: str = "webhook-delivery-lab"
    environment: str = "development"
    debug: bool = False
    log_level: str = "INFO"
    log_format: str = "json"  # "json" or "text"

    # API Service
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_workers: int = 1

    # Database
    database_url: str = "postgresql+asyncpg://webhook:webhook@localhost:5432/webhookdb"
    database_pool_size: int = 10
    database_max_overflow: int = 20
    database_pool_timeout: int = 30
    database_echo: bool = False

    # RabbitMQ
    rabbitmq_url: str = "amqp://webhook:webhook@localhost:5672/"
    rabbitmq_prefetch_count: int = 10

    # Worker
    worker_concurrency: int = 5
    worker_id: str = "worker-1"

    # Delivery
    delivery_timeout_seconds: float = 30.0
    delivery_max_attempts: int = 5
    # Retry delays in seconds for attempts 2, 3, 4 (attempt 1 is immediate)
    retry_delays: list[int] = Field(default=[10, 60, 300])
    # HTTP status codes that trigger a retry
    retryable_status_codes: list[int] = Field(default=[429, 500, 502, 503, 504])
    # HTTP status codes that are permanent failures (no retry)
    permanent_failure_codes: list[int] = Field(default=[400, 401, 403, 404, 410])

    # Outbox publisher
    outbox_poll_interval_seconds: float = 5.0
    outbox_batch_size: int = 50
    outbox_max_publish_attempts: int = 10

    # SSRF protection: allowlist of destination base URLs for webhook delivery
    # In production, tighten this list significantly.
    # Accepts comma-separated string from .env or JSON array.
    allowed_endpoint_prefixes: list[str] = Field(
        default=[
            "http://receiver",
            "http://localhost:8001",
            "http://127.0.0.1:8001",
            "http://host.docker.internal:8001",
        ]
    )

    # Observability
    otel_endpoint: str = "http://localhost:4317"
    otel_service_name: str = "webhook-api"
    otel_enabled: bool = True
    prometheus_port: int = 9090

    # Receiver service (for mock)
    receiver_host: str = "0.0.0.0"
    receiver_port: int = 8001
    receiver_flaky_fail_count: int = 3
    receiver_slow_delay_seconds: float = 10.0
    receiver_rate_limit_wait_seconds: int = 5

    @field_validator("allowed_endpoint_prefixes", mode="before")
    @classmethod
    def _parse_prefixes(cls, v: Any) -> list[str]:
        return _parse_str_list(v)

    @field_validator("retry_delays", "retryable_status_codes", "permanent_failure_codes", mode="before")
    @classmethod
    def _parse_int_lists(cls, v: Any) -> list[int]:
        return _parse_int_list(v)


@lru_cache
def get_settings() -> Settings:
    return Settings()

