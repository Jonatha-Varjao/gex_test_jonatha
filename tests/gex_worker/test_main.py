from unittest.mock import AsyncMock, patch

import pytest
from faststream import FastStream
from faststream.rabbit import ExchangeType, RabbitBroker

from gex_common.config import CONSTANTS

pytestmark = pytest.mark.unit

from gex_worker import main  # noqa: E402
from gex_worker.distributors import (  # noqa: E402
    PermanentDistributorFailure,
    SimulatedDistributorFailure,
    TransientDistributorFailure,
)


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
        assert CONSTANTS.queue_lead_received in queue_names
        assert CONSTANTS.queue_dlq_decrypt_failed in queue_names
        assert CONSTANTS.queue_dlq_schema_failed in queue_names
        assert CONSTANTS.queue_dlq_consumer_failed in queue_names
        assert CONSTANTS.queue_dist_sms in queue_names
        assert CONSTANTS.queue_dist_dlq_sms in queue_names

    async def test_lead_and_sms_queues_have_dlx(self) -> None:
        with (
            patch.object(main.broker, "declare_exchange"),
            patch.object(main.broker, "declare_queue") as mock_decl_q,
        ):
            await main.declare_topology()

        for call in mock_decl_q.await_args_list:
            q = call.args[0]
            if q.name in (CONSTANTS.queue_lead_received, CONSTANTS.queue_dist_sms):
                assert q.arguments.get("x-dead-letter-exchange") == "dlq"
            else:
                assert "x-dead-letter-exchange" not in q.arguments


class TestAppConstruction:
    def test_broker_is_rabbit_broker(self) -> None:
        assert isinstance(main.broker, RabbitBroker)

    def test_app_is_faststream(self) -> None:
        assert isinstance(main.app, FastStream)

    def test_broker_has_middlewares(self) -> None:
        assert hasattr(main.broker, "_subscribers")
        assert len(main.broker._subscribers) >= 0


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


class TestHandleSmsSuccess:
    async def test_calls_process_sms_when_success(self, dist_msg) -> None:
        with (
            patch("gex_worker.main.db_session") as mock_db_session_cm,
            patch("gex_worker.main.process_sms") as mock_process_sms,
        ):
            mock_session = AsyncMock()
            mock_db_session_cm.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_db_session_cm.return_value.__aexit__ = AsyncMock(return_value=None)

            await main.handle_sms(dist_msg, "corr-test-2")

        mock_process_sms.assert_awaited_once_with(dist_msg, "corr-test-2", mock_session)


class TestHandleSmsTransientFailure:
    """Transient failure → caller re-publishes with expiration."""

    @pytest.mark.parametrize("exception_cls", [TransientDistributorFailure, SimulatedDistributorFailure])
    async def test_republishes_with_delay_on_transient(
        self, exception_cls, dist_msg
    ) -> None:
        with (
            patch("gex_worker.main.db_session") as mock_db_session_cm,
            patch("gex_worker.main.process_sms", side_effect=exception_cls("oops")),
            patch.object(main.broker, "publish") as mock_publish,
            patch("gex_worker.main._increment_attempts", return_value=1),
        ):
            mock_session = AsyncMock()
            mock_db_session_cm.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_db_session_cm.return_value.__aexit__ = AsyncMock(return_value=None)

            await main.handle_sms(dist_msg, "corr-retry")

        mock_publish.assert_awaited_once()
        call = mock_publish.await_args
        assert call.args[0] is dist_msg
        assert call.args[1] == CONSTANTS.queue_dist_sms
        assert call.kwargs["exchange"] == "dist"
        assert call.kwargs["expiration"] == CONSTANTS.retry_backoffs_ms[0] / 1000.0
        assert call.kwargs["headers"]["x-attempts"] == "1"

    async def test_schedules_multiple_retries_with_backoff(self, dist_msg) -> None:
        with (
            patch("gex_worker.main.db_session") as mock_db_session_cm,
            patch("gex_worker.main.process_sms", side_effect=TransientDistributorFailure("oops")),
            patch.object(main.broker, "publish") as mock_publish,
            patch("gex_worker.main._increment_attempts", side_effect=[1, 2, 3]),
        ):
            mock_session = AsyncMock()
            mock_db_session_cm.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_db_session_cm.return_value.__aexit__ = AsyncMock(return_value=None)

            await main.handle_sms(dist_msg, "corr-retry")

        mock_publish.assert_awaited_once()
        delay = mock_publish.await_args.kwargs["expiration"]
        assert delay == CONSTANTS.retry_backoffs_ms[0] / 1000.0

    async def test_max_retries_publishes_dlq(self, dist_msg) -> None:
        with (
            patch("gex_worker.main.db_session") as mock_db_session_cm,
            patch("gex_worker.main.process_sms", side_effect=TransientDistributorFailure("oops")),
            patch.object(main.broker, "publish") as mock_publish,
            patch("gex_worker.main._increment_attempts", return_value=4),
        ):
            mock_session = AsyncMock()
            mock_db_session_cm.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_db_session_cm.return_value.__aexit__ = AsyncMock(return_value=None)

            await main.handle_sms(dist_msg, "corr-retry")  # should not raise

        # Last publish should be the DLQ message
        dlq_call = mock_publish.await_args_list[-1]
        from gex_common.models import DLQMessage
        assert isinstance(dlq_call.args[0], DLQMessage)
        assert dlq_call.args[1] == CONSTANTS.queue_dist_dlq_sms
        assert dlq_call.kwargs["exchange"] == "dist"


class TestHandleSmsPermanentFailure:
    """Permanent failure propagates uncaught to DlqMiddleware."""

    async def test_permanent_failure_propagates(self, dist_msg) -> None:
        with (
            patch("gex_worker.main.db_session") as mock_db_session_cm,
            patch(
                "gex_worker.main.process_sms",
                side_effect=PermanentDistributorFailure("permanent"),
            ),
            patch.object(main.broker, "publish") as mock_publish,
        ):
            mock_session = AsyncMock()
            mock_db_session_cm.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_db_session_cm.return_value.__aexit__ = AsyncMock(return_value=None)

            with pytest.raises(PermanentDistributorFailure):
                await main.handle_sms(dist_msg, "corr-perm")

        mock_publish.assert_not_called()
