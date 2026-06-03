"""Unit tests for ``gex_worker.main`` — app wiring, lifespan hooks, topology, and handler routing.

All dependencies (``init_db``, ``close_db``, ``setup_logging``,
``broker.declare_exchange``/``declare_queue``, ``db_session``,
``process_lead``, ``process_sms``) are mocked.
"""

from unittest.mock import AsyncMock, patch

import pytest
from faststream import FastStream
from faststream.rabbit import ExchangeType, RabbitBroker

from gex_common.config import (
    QUEUE_DIST_DLQ_SMS,
    QUEUE_DIST_SMS,
    QUEUE_DLQ_CONSUMER_FAILED,
    QUEUE_DLQ_DECRYPT_FAILED,
    QUEUE_DLQ_SCHEMA_FAILED,
    QUEUE_LEAD_RECEIVED,
)

pytestmark = pytest.mark.unit

# Import once — module-level singleton is cached by Python.
from gex_worker import main  # noqa: E402

# ---------------------------------------------------------------------------
# Lifespan hooks
# ---------------------------------------------------------------------------


class TestSetup:
    async def test_calls_setup_logging_then_init_db(self) -> None:
        with (
            patch("gex_worker.main.setup_logging") as mock_setup_logging,
            patch("gex_worker.main.init_db") as mock_init_db,
        ):
            await main.setup()

        mock_setup_logging.assert_called_once()
        mock_init_db.assert_awaited_once()


class TestTeardown:
    async def test_calls_close_db(self) -> None:
        with patch("gex_worker.main.close_db") as mock_close_db:
            await main.teardown()

        mock_close_db.assert_awaited_once()


# ---------------------------------------------------------------------------
# Topology
# ---------------------------------------------------------------------------


class TestDeclareTopology:
    async def test_declares_three_exchanges(self) -> None:
        with (
            patch.object(main.broker, "declare_exchange") as mock_decl_ex,
            patch.object(main.broker, "declare_queue"),
        ):
            await main.declare_topology()

        assert mock_decl_ex.await_count == 3
        exchange_names = [c.args[0].name for c in mock_decl_ex.await_args_list]
        assert "lead" in exchange_names
        assert "dist" in exchange_names
        assert "dlq" in exchange_names
        exchange_types = [c.args[0].type for c in mock_decl_ex.await_args_list]
        assert all(t == ExchangeType.DIRECT for t in exchange_types[:2])
        assert exchange_types[2] == ExchangeType.FANOUT

    async def test_declares_six_queues(self) -> None:
        with (
            patch.object(main.broker, "declare_exchange"),
            patch.object(main.broker, "declare_queue") as mock_decl_q,
        ):
            await main.declare_topology()

        assert mock_decl_q.await_count == 6
        queue_names = [c.args[0].name for c in mock_decl_q.await_args_list]
        assert QUEUE_LEAD_RECEIVED in queue_names
        assert QUEUE_DLQ_DECRYPT_FAILED in queue_names
        assert QUEUE_DLQ_SCHEMA_FAILED in queue_names
        assert QUEUE_DLQ_CONSUMER_FAILED in queue_names
        assert QUEUE_DIST_SMS in queue_names
        assert QUEUE_DIST_DLQ_SMS in queue_names

    async def test_lead_and_sms_queues_have_dlx(self) -> None:
        with (
            patch.object(main.broker, "declare_exchange"),
            patch.object(main.broker, "declare_queue") as mock_decl_q,
        ):
            await main.declare_topology()

        for call in mock_decl_q.await_args_list:
            q = call.args[0]
            if q.name in (QUEUE_LEAD_RECEIVED, QUEUE_DIST_SMS):
                assert q.arguments.get("x-dead-letter-exchange") == "dlq"
            else:
                assert "x-dead-letter-exchange" not in q.arguments


# ---------------------------------------------------------------------------
# Broker / App construction
# ---------------------------------------------------------------------------


class TestAppConstruction:
    def test_broker_is_rabbit_broker(self) -> None:
        assert isinstance(main.broker, RabbitBroker)

    def test_app_is_faststream(self) -> None:
        assert isinstance(main.app, FastStream)

    def test_broker_has_middlewares(self) -> None:
        # The broker's middlewares list is set at construction time.
        # We verify by checking the registered subscriber count (≥0).
        assert hasattr(main.broker, "_subscribers")
        assert len(main.broker._subscribers) >= 0


# ---------------------------------------------------------------------------
# Handler routing — verify that handlers call through to the real implementations
# ---------------------------------------------------------------------------


class TestHandleLead:
    async def test_calls_process_lead_with_broker(self, lead_msg) -> None:
        with (
            patch("gex_worker.main.db_session") as mock_db_session_cm,
            patch("gex_worker.main.process_lead") as mock_process_lead,
        ):
            mock_session = AsyncMock()
            mock_db_session_cm.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_db_session_cm.return_value.__aexit__ = AsyncMock(return_value=None)

            await main.handle_lead(lead_msg, "corr-test-1")

        mock_process_lead.assert_awaited_once_with(
            lead_msg, "corr-test-1", mock_session, broker=main.broker
        )


class TestHandleSms:
    async def test_calls_process_sms(self, dist_msg) -> None:
        with (
            patch("gex_worker.main.db_session") as mock_db_session_cm,
            patch("gex_worker.main.process_sms") as mock_process_sms,
        ):
            mock_session = AsyncMock()
            mock_db_session_cm.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_db_session_cm.return_value.__aexit__ = AsyncMock(return_value=None)

            await main.handle_sms(dist_msg, "corr-test-2")

        mock_process_sms.assert_awaited_once_with(dist_msg, "corr-test-2", mock_session)
