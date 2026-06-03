"""Unit tests for ``gex_worker.db``.

Mocks engine + session factory — no real database needed.
Pattern analogue: ``tests/gex_receiver/test_db.py``.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from gex_worker.db import close_db, db_session, get_db_session, init_db

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_session_mock() -> AsyncMock:
    """Return an ``AsyncMock`` that acts like a yielded ``AsyncSession``."""
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    return session


def _patch_engine_and_factory():
    """Patch ``create_async_engine`` and ``async_sessionmaker`` on the db module.

    Returns the mock engine and a callable that creates mock sessions.
    """
    mock_engine = MagicMock()
    mock_engine.dispose = AsyncMock()

    def _patcher():
        mock_session = _make_session_mock()
        mock_factory = MagicMock(spec=async_sessionmaker)
        mock_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_factory.return_value.__aexit__ = AsyncMock(return_value=None)
        return mock_engine, mock_factory, mock_session

    return _patcher


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestInitDb:
    async def test_creates_engine_with_correct_args(self, reset_worker_db_state) -> None:
        with (
            patch("gex_worker.db.create_async_engine") as mock_create,
            patch("gex_worker.db.async_sessionmaker"),
        ):
            await init_db()

        mock_create.assert_called_once()
        args, kwargs = mock_create.call_args
        assert "mysql+asyncmy" in args[0]
        assert kwargs["pool_pre_ping"] is True
        assert "pool_size" in kwargs
        assert "max_overflow" in kwargs

    async def test_idempotent(self, reset_worker_db_state) -> None:
        with (
            patch("gex_worker.db.create_async_engine") as mock_create,
            patch("gex_worker.db.async_sessionmaker"),
        ):
            await init_db()
            await init_db()  # second call

        mock_create.assert_called_once()  # still only one engine creation

    async def test_raises_if_called_before_init(self, reset_worker_db_state) -> None:
        with pytest.raises(RuntimeError, match="init_db\\(\\) must be called"):
            async with db_session():
                pass  # pragma: no cover


class TestCloseDb:
    async def test_disposes_engine_and_clears_globals(self, reset_worker_db_state) -> None:
        mock_engine = MagicMock()
        mock_engine.dispose = AsyncMock()

        with patch("gex_worker.db.create_async_engine", return_value=mock_engine):
            await init_db()

        import gex_worker.db as db_mod

        assert db_mod._engine is not None
        assert db_mod._factory is not None

        await close_db()

        mock_engine.dispose.assert_awaited_once()
        assert db_mod._engine is None
        assert db_mod._factory is None

    async def test_close_when_never_initialized(self, reset_worker_db_state) -> None:
        await close_db()


class TestDbSession:
    async def test_commits_on_success(self, reset_worker_db_state) -> None:
        mock_engine = MagicMock()
        mock_session = _make_session_mock()
        mock_factory = MagicMock(spec=async_sessionmaker)
        mock_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_factory.return_value.__aexit__ = AsyncMock(return_value=None)

        with (
            patch("gex_worker.db.create_async_engine", return_value=mock_engine),
            patch("gex_worker.db.async_sessionmaker", return_value=mock_factory),
        ):
            await init_db()
            async with db_session() as session:
                await session.execute("SELECT 1")

        mock_session.commit.assert_awaited_once()
        mock_session.rollback.assert_not_called()

    async def test_rolls_back_on_exception(self, reset_worker_db_state) -> None:
        mock_engine = MagicMock()
        mock_session = _make_session_mock()
        mock_factory = MagicMock(spec=async_sessionmaker)
        mock_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_factory.return_value.__aexit__ = AsyncMock(return_value=None)

        with (
            patch("gex_worker.db.create_async_engine", return_value=mock_engine),
            patch("gex_worker.db.async_sessionmaker", return_value=mock_factory),
        ):
            await init_db()
            with pytest.raises(ValueError, match="boom"):
                async with db_session():
                    raise ValueError("boom")

        mock_session.rollback.assert_awaited_once()
        mock_session.commit.assert_not_called()


class TestGetDbSession:
    async def test_raises_when_not_initialized(self, reset_worker_db_state) -> None:
        with pytest.raises(RuntimeError, match="init_db\\(\\) must be called"):
            async for _ in get_db_session():
                pass  # pragma: no cover
