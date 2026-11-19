"""
Custom exception hierarchy for the webhook delivery lab.
"""
from typing import Any


class WebhookLabError(Exception):
    """Base exception for all application errors."""

    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}


class NotFoundError(WebhookLabError):
    """Resource not found."""
    pass


class ValidationError(WebhookLabError):
    """Input validation failed."""
    pass


class SSRFProtectionError(WebhookLabError):
    """Endpoint URL not in the configured allowlist."""
    pass


class DeliveryError(WebhookLabError):
    """Webhook delivery attempt failed."""

    def __init__(
        self,
        message: str,
        status_code: int | None = None,
        retryable: bool = True,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, details)
        self.status_code = status_code
        self.retryable = retryable


class DeliveryAlreadySucceededError(WebhookLabError):
    """Attempted to replay a delivery that already succeeded."""
    pass


class MessagingError(WebhookLabError):
    """RabbitMQ or messaging operation failed."""
    pass


class DatabaseError(WebhookLabError):
    """Database operation failed."""
    pass


class OutboxPublishError(WebhookLabError):
    """Outbox event publishing failed."""
    pass

