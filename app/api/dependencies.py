"""
FastAPI dependency injection helpers.
"""
from collections.abc import AsyncGenerator

import aio_pika
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings, get_settings
from app.db.session import get_db


async def get_settings_dep() -> Settings:
    return get_settings()


# Re-export get_db for convenience in route files
__all__ = ["get_db", "get_settings_dep"]

