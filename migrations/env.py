"""
Alembic environment script for async SQLAlchemy.
"""
import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy.ext.asyncio import create_async_engine

# Import all models so Alembic can detect them for autogenerate
from app.db.base import Base
from app.db.models import WebhookEvent, WebhookDelivery, OutboxEvent, ReceiverIdempotencyRecord  # noqa: F401
from app.core.config import get_settings

settings = get_settings()

# Alembic Config object
config = context.config

# Interpret the config file for Python logging.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def get_url() -> str:
    """Return the database URL from application settings."""
    return settings.database_url


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL without creating an Engine,
    allowing migration scripts to run without a live database connection.
    """
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Run migrations using an async engine."""
    engine = create_async_engine(get_url(), echo=False)

    async with engine.begin() as conn:
        await conn.run_sync(do_run_migrations)

    await engine.dispose()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

