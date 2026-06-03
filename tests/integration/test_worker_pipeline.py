"""Integration tests for the worker's core functions against a real MySQL 8.4 testcontainer.

These tests validate that:
  - ``process_lead`` drives ``sp_insert_lead`` correctly, creating the right rows.
  - ``process_sms`` flips ``distribution_status`` from ``pending`` to ``delivered``.

Dependencies external to ``gex_worker`` (``broker``, ``httpx.AsyncClient``) are
mocked — only the DB path is tested against real MySQL.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from gex_common.config import (
    CHANNEL_SMS,
    DIST_STATUS_DELIVERED,
)
from gex_worker.consumers import process_lead
from gex_worker.distributors import process_sms
from tests.gex_worker.conftest import make_dist_msg, make_lead_msg

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _count_rows(session: AsyncSession, table: str) -> int:
    result = await session.execute(text(f"SELECT COUNT(*) FROM {table}"))
    return int(result.scalar_one())


# ---------------------------------------------------------------------------
# 1. process_lead — SP-driven pipeline
# ---------------------------------------------------------------------------


class TestProcessLeadIntegration:
    async def test_new_lead_creates_rows_in_all_tables(self, db_session: AsyncSession) -> None:
        """A successful ``process_lead`` call should create 1 lead, 1 order,
        1 lead_event, and 4 distribution_status rows."""
        msg = make_lead_msg()
        broker = AsyncMock()

        # Freeze time so lag_seconds is deterministic.
        now = datetime(2026, 1, 1, 12, 0, 5, tzinfo=timezone.utc)

        class _FakeDt:
            @staticmethod
            def now(tz=None):  # noqa: ARG004
                return now

        with patch("gex_worker.consumers.datetime", _FakeDt):
            await process_lead(msg, "corr-int-1", db_session, broker)

        await db_session.commit()

        # SP created 1 lead row.
        assert await _count_rows(db_session, "leads") == 1
        assert await _count_rows(db_session, "orders") == 1
        assert await _count_rows(db_session, "lead_events") == 1
        assert await _count_rows(db_session, "distribution_status") == 4

        # Broker published 4 dist messages.
        assert broker.publish.await_count == 4

    async def test_duplicate_event_returns_is_new_false(self, db_session: AsyncSession) -> None:
        """A second call with the same ``(transaction_id, event)`` should
        return ``is_new=False`` and not create new distribution rows.

        The SP resolves ``p_order_id`` from the orders table after the upsert,
        so the idempotency check ``WHERE order_id = :oid AND event = :event``
        finds the existing event regardless of the candidate UUID.
        """
        msg = make_lead_msg()
        broker = AsyncMock()

        now = datetime(2026, 1, 1, 12, 0, 5, tzinfo=timezone.utc)

        class _FakeDt:
            @staticmethod
            def now(tz=None):  # noqa: ARG004
                return now

        with patch("gex_worker.consumers.datetime", _FakeDt):
            await process_lead(msg, "corr-int-2a", db_session, broker)
            await db_session.commit()

            broker.publish.reset_mock()

            await process_lead(msg, "corr-int-2b", db_session, broker)
            await db_session.commit()

        assert await _count_rows(db_session, "leads") == 1
        assert await _count_rows(db_session, "orders") == 1
        assert await _count_rows(db_session, "lead_events") == 1
        assert await _count_rows(db_session, "distribution_status") == 4
        broker.publish.assert_not_awaited()


# ---------------------------------------------------------------------------
# 2. process_sms — distribution_status UPDATE
# ---------------------------------------------------------------------------


class TestProcessSmsIntegration:
    async def test_flips_status_to_delivered(self, db_session: AsyncSession) -> None:
        """``process_sms`` should update ``distribution_status`` from
        ``pending`` to ``delivered`` and record the lag.

        The test runs the full pipeline: first ``process_lead`` creates the
        order and 4 distribution rows, then ``process_sms`` updates the SMS
        row.
        """
        lead_msg = make_lead_msg()
        broker = AsyncMock()

        STARTED_AT = datetime(2026, 1, 1, 12, 0, 1, tzinfo=timezone.utc)
        DELIVERED_AT = datetime(2026, 1, 1, 12, 0, 3, tzinfo=timezone.utc)

        class _FakeDt:
            call_count = 0

            def now(self, tz=None):  # noqa: ARG004
                self.call_count += 1
                if self.call_count == 1:
                    return STARTED_AT
                return DELIVERED_AT

        with patch("gex_worker.consumers.datetime", _FakeDt()):
            await process_lead(lead_msg, "corr-int-sms", db_session, broker)
        await db_session.commit()

        # Read the actual order_id from the DB.
        result = await db_session.execute(
            text("SELECT id FROM orders WHERE transaction_id = :tx LIMIT 1"),
            {"tx": lead_msg.transaction_id},
        )
        order_id = result.scalar_one()

        dist_msg = make_dist_msg(order_id=order_id)

        with (
            patch("gex_worker.distributors.random.random", return_value=0.5),
            patch("gex_worker.distributors.datetime", _FakeDt()),
            patch("gex_worker.distributors.httpx.AsyncClient") as mock_http_cls,
        ):
            mock_client_cm = MagicMock()
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.raise_for_status = MagicMock(return_value=None)
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client_cm.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cm.__aexit__ = AsyncMock(return_value=None)
            mock_http_cls.return_value = mock_client_cm

            await process_sms(dist_msg, "corr-int-sms", db_session)

        await db_session.commit()

        result = await db_session.execute(
            text(
                "SELECT status, attempts, delivered_at, lag_db_to_channel_seconds "
                "FROM distribution_status "
                "WHERE order_id = :oid AND channel = :ch"
            ),
            {"oid": order_id, "ch": CHANNEL_SMS},
        )
        row = result.one()
        assert row.status == DIST_STATUS_DELIVERED
        assert row.attempts == 1
        assert row.delivered_at is not None
        assert row.lag_db_to_channel_seconds == 2.0
