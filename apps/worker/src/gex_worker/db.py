"""Database engine and session management for the worker.

Pattern mirrors the receiver's `db.py` but uses a module-level singleton
instead of a class instance. Subscribers get a session via the
``db_session()`` async context manager.
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from gex_worker.config import APP_SETTINGS

_engine: AsyncEngine | None = None
_factory: async_sessionmaker[AsyncSession] | None = None


async def init_db() -> None:
    """Create engine + session factory. Idempotent — call once at startup."""
    global _engine, _factory
    if _engine is not None:
        return
    _engine = create_async_engine(
        APP_SETTINGS.database_url,
        pool_size=APP_SETTINGS.database_pool_size,
        max_overflow=APP_SETTINGS.database_max_overflow,
        pool_pre_ping=True,
    )
    _factory = async_sessionmaker(_engine, expire_on_commit=False, autoflush=False)


async def close_db() -> None:
    """Dispose engine. Idempotent — call once at shutdown."""
    global _engine, _factory
    if _engine is not None:
        await _engine.dispose()
    _engine = None
    _factory = None


@asynccontextmanager
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    """Async context manager: yields a session, commits on success, rollbacks on failure.

    Each ``async with db_session() as session:`` block is an independent
    transaction.  This is the preferred API for use with ``RetryMiddleware``
    because each retry gets a fresh session.
    """
    if _factory is None:
        raise RuntimeError("init_db() must be called before db_session()")
    async with _factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """FastStream dependency: yields a session for a single message handler.

    The session is committed on success and rolled back on failure.
    """
    if _factory is None:
        raise RuntimeError("init_db() must be called before get_db_session()")
    async with _factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
