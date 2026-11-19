"""
SQLAlchemy async session and engine setup.
"""
from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import get_settings

settings = get_settings()

# Create the async engine once per process.
# echo=True will log every SQL statement — useful for debugging.
engine = create_async_engine(
    settings.database_url,
    pool_size=settings.database_pool_size,
    max_overflow=settings.database_max_overflow,
    pool_timeout=settings.database_pool_timeout,
    echo=settings.database_echo,
    # Important: pool_pre_ping avoids stale connection errors after a DB restart
    pool_pre_ping=True,
)

# Session factory: expire_on_commit=False prevents lazy-loading errors after commit
AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency that provides a database session per request.
    The session is committed on success and rolled back on any exception.
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def get_db_session() -> AsyncSession:
    """
    Create a standalone database session for use outside of FastAPI.
    The caller is responsible for commit/rollback/close.
    """
    return AsyncSessionLocal()

