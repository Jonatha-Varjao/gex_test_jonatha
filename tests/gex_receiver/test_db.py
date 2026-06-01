"""Unit tests for the database module.

Mocked AsyncSession and AsyncEngine — no real database is needed.
"""

import json
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from gex_receiver.db import (
    Database,
    check_idempotency,
    insert_raw_payload,
)

pytestmark = pytest.mark.unit


class TestDatabaseInit:
    def test_init_stores_config(self):
        db = Database(database_url="mysql+asyncmy://u:p@host:3306/db", pool_size=5, max_overflow=15)
        assert db._database_url == "mysql+asyncmy://u:p@host:3306/db"
        assert db._pool_size == 5
        assert db._max_overflow == 15
        assert db._engine is None
        assert db._session_factory is None

    def test_init_uses_default_pool_sizes(self):
        db = Database(database_url="mysql+asyncmy://u:p@host:3306/db")
        assert db._pool_size == 10
        assert db._max_overflow == 20


class TestSessionFactory:
    def test_raises_when_not_connected(self):
        db = Database(database_url="mysql+asyncmy://u:p@host:3306/db")
        with pytest.raises(RuntimeError, match="Database.connect\\(\\) must be called"):
            _ = db.session_factory

    async def test_returns_factory_after_connect(self):
        db = Database(database_url="mysql+asyncmy://u:p@host:3306/db")
        mock_engine = MagicMock()
        mock_factory = MagicMock(spec=async_sessionmaker)
        with (
            patch("gex_receiver.db.create_async_engine", return_value=mock_engine),
            patch("gex_receiver.db.async_sessionmaker", return_value=mock_factory),
        ):
            await db.connect()
        assert db.session_factory is mock_factory


class TestDatabaseConnect:
    async def test_creates_engine_with_correct_args(self):
        db = Database(
            database_url="mysql+asyncmy://u:p@host:3306/db",
            pool_size=7,
            max_overflow=11,
        )
        with (
            patch("gex_receiver.db.create_async_engine") as mock_create,
            patch("gex_receiver.db.async_sessionmaker"),
        ):
            await db.connect()
        mock_create.assert_called_once_with(
            "mysql+asyncmy://u:p@host:3306/db",
            pool_size=7,
            max_overflow=11,
            pool_pre_ping=True,
        )

    async def test_sets_engine_and_session_factory(self):
        db = Database(database_url="mysql+asyncmy://u:p@host:3306/db")
        mock_engine = MagicMock()
        mock_factory = MagicMock(spec=async_sessionmaker)
        with (
            patch("gex_receiver.db.create_async_engine", return_value=mock_engine),
            patch("gex_receiver.db.async_sessionmaker", return_value=mock_factory),
        ):
            await db.connect()
        assert db._engine is mock_engine
        assert db._session_factory is mock_factory


class TestDatabaseClose:
    async def test_close_when_never_connected(self):
        db = Database(database_url="mysql+asyncmy://u:p@host:3306/db")
        await db.close()
        assert db._engine is None
        assert db._session_factory is None

    async def test_disposes_engine_and_clears_attrs(self):
        db = Database(database_url="mysql+asyncmy://u:p@host:3306/db")
        mock_engine = MagicMock()
        mock_engine.dispose = AsyncMock()
        db._engine = mock_engine
        db._session_factory = MagicMock()

        await db.close()

        mock_engine.dispose.assert_awaited_once()
        assert db._engine is None
        assert db._session_factory is None


def _make_session_with_result(rowcount: int = 1, lastrowid: int | None = 42) -> AsyncSession:
    session = MagicMock(spec=AsyncSession)
    session.execute = AsyncMock()
    result = MagicMock()
    result.rowcount = rowcount
    result.lastrowid = lastrowid
    session.execute.return_value = result
    return session


class TestInsertRawPayload:
    async def test_returns_inserted_row_id(self):
        session = _make_session_with_result(lastrowid=123)
        row_id = await insert_raw_payload(
            session=session,
            gateway="lous",
            received_at=datetime(2026, 1, 1, 12, 0, 0),
            headers={"X-Foo": "bar"},
            body_original={"k": "v"},
            body_decrypted=None,
            processing_status="accepted",
            error_detail=None,
            correlation_id="corr-1",
        )
        assert row_id == 123

    async def test_executes_insert_with_correct_params(self):
        session = _make_session_with_result(lastrowid=1)
        received_at = datetime(2026, 1, 1, 12, 0, 0)
        await insert_raw_payload(
            session=session,
            gateway="lous",
            received_at=received_at,
            headers={"X-Foo": "bar"},
            body_original={"k": "v"},
            body_decrypted={"decrypted": True},
            processing_status="accepted",
            error_detail=None,
            correlation_id="corr-1",
        )
        call = session.execute.await_args
        stmt, params = call.args
        assert isinstance(stmt, type(text("SELECT 1")))
        assert "INSERT INTO raw_payloads" in str(stmt)
        assert params["gateway"] == "lous"
        assert params["received_at"] == received_at
        assert json.loads(params["headers"]) == {"X-Foo": "bar"}
        assert json.loads(params["body_original"]) == {"k": "v"}
        assert json.loads(params["body_decrypted"]) == {"decrypted": True}
        assert params["processing_status"] == "accepted"
        assert params["error_detail"] is None
        assert params["correlation_id"] == "corr-1"

    async def test_body_decrypted_null_when_none(self):
        session = _make_session_with_result(lastrowid=1)
        await insert_raw_payload(
            session=session,
            gateway="lous",
            received_at=datetime(2026, 1, 1, 12, 0, 0),
            headers={},
            body_original={},
            body_decrypted=None,
            processing_status="schema_failed",
            error_detail="bad schema",
            correlation_id="corr-1",
        )
        params = session.execute.await_args.args[1]
        assert params["body_decrypted"] is None

    async def test_raises_when_no_lastrowid(self):
        session = _make_session_with_result(lastrowid=None)
        with pytest.raises(RuntimeError, match="Failed to insert raw_payload"):
            await insert_raw_payload(
                session=session,
                gateway="lous",
                received_at=datetime(2026, 1, 1, 12, 0, 0),
                headers={},
                body_original={},
                body_decrypted=None,
                processing_status="accepted",
                error_detail=None,
                correlation_id="corr-1",
            )

    async def test_error_detail_propagated(self):
        session = _make_session_with_result(lastrowid=1)
        await insert_raw_payload(
            session=session,
            gateway="grummer",
            received_at=datetime(2026, 1, 1, 12, 0, 0),
            headers={},
            body_original={},
            body_decrypted=None,
            processing_status="decrypt_failed",
            error_detail="AES decryption failed",
            correlation_id="corr-1",
        )
        params = session.execute.await_args.args[1]
        assert params["error_detail"] == "AES decryption failed"


class TestCheckIdempotency:
    async def test_returns_true_for_new_row(self):
        session = _make_session_with_result(rowcount=1)
        result = await check_idempotency(
            session=session,
            gateway="lous",
            transaction_id="tx-001",
            event="order.approved",
            correlation_id="corr-1",
        )
        assert result is True

    async def test_returns_false_for_duplicate_updated(self):
        session = _make_session_with_result(rowcount=2)
        result = await check_idempotency(
            session=session,
            gateway="lous",
            transaction_id="tx-001",
            event="order.approved",
            correlation_id="corr-1",
        )
        assert result is False

    async def test_returns_false_for_duplicate_unchanged(self):
        session = _make_session_with_result(rowcount=0)
        result = await check_idempotency(
            session=session,
            gateway="lous",
            transaction_id="tx-001",
            event="order.approved",
            correlation_id="corr-1",
        )
        assert result is False

    async def test_executes_on_duplicate_key_with_params(self):
        session = _make_session_with_result(rowcount=1)
        await check_idempotency(
            session=session,
            gateway="grummer",
            transaction_id="tx-99",
            event="order.rejected",
            correlation_id="corr-99",
        )
        stmt, params = session.execute.await_args.args
        assert "INSERT INTO processed_events" in str(stmt)
        assert "ON DUPLICATE KEY UPDATE" in str(stmt)
        assert "VALUES(correlation_id)" in str(stmt)
        assert params["gateway"] == "grummer"
        assert params["transaction_id"] == "tx-99"
        assert params["event"] == "order.rejected"
        assert params["correlation_id"] == "corr-99"
