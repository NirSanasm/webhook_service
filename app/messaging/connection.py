"""
RabbitMQ connection management.

Uses aio_pika's connect_robust() which handles automatic reconnection.
The connection is created once per application instance and reused.

Key concepts demonstrated:
- Robust connections: automatically reconnect after broker restarts
- Channel-level operations: a single connection can multiplex many channels
- Connection vs Channel: a Connection is the TCP socket; a Channel is a
  lightweight virtual connection inside it. Create one channel per task.
"""
import asyncio
import logging
from typing import Optional

import aio_pika
from aio_pika.abc import AbstractRobustConnection

from app.core.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

_connection: Optional[AbstractRobustConnection] = None
_lock = asyncio.Lock()


async def get_connection() -> AbstractRobustConnection:
    """
    Return the shared RabbitMQ connection, creating it if necessary.

    connect_robust() returns a connection that:
    - Automatically reconnects on network errors.
    - Re-declares exchanges, queues, and consumers after reconnection
      if you declared them on a robust channel.
    """
    global _connection
    async with _lock:
        if _connection is None or _connection.is_closed:
            logger.info("Creating RabbitMQ connection", extra={"url": settings.rabbitmq_url})
            _connection = await aio_pika.connect_robust(
                settings.rabbitmq_url,
                heartbeat=60,
                timeout=30,
            )
            logger.info("RabbitMQ connection established")
    return _connection


async def close_connection() -> None:
    """Gracefully close the shared connection. Call on application shutdown."""
    global _connection
    if _connection and not _connection.is_closed:
        await _connection.close()
        _connection = None
        logger.info("RabbitMQ connection closed")

