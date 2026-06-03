"""Unit tests for ``gex_worker.consumers.process_lead``.

Mocks the session (stored procedure call) and broker (message publishing).
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gex_common.config import (
    QUEUE_DIST_CALLCENTER,
    QUEUE_DIST_EMAIL,
    QUEUE_DIST_SMS,
    QUEUE_DIST_WHATSAPP,
)
from gex_worker.consumers import process_lead

pytestmark = pytest.mark.unit

NOW_UTC = datetime(2026, 1, 1, 12, 0, 5, tzinfo=timezone.utc)  # 5s after msg.transaction_time


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sp_result(is_new: bool, order_id: str = "ORD-0001") -> MagicMock:
    """Build a mock ``CursorResult`` whose ``.first()`` row has ``.is_new`` / ``.order_id``."""
    row = MagicMock()
    row.is_new = is_new
    row.order_id = order_id
    result = MagicMock()
    result.first.return_value = row
    return result


def _make_session_mock(result: MagicMock) -> AsyncMock:
    session = AsyncMock()
    session.execute = AsyncMock(return_value=result)
    return session


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestProcessLead:
    @pytest.fixture(autouse=True)
    def _freeze_time(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("gex_worker.consumers.datetime", _FakeDatetime(NOW_UTC))

    @pytest.fixture
    def msg(self, lead_msg) -> MagicMock:
        return lead_msg

    @pytest.fixture
    def broker(self) -> AsyncMock:
        return AsyncMock()

    @pytest.fixture
    def session_new(self) -> AsyncMock:
        return _make_session_mock(_sp_result(is_new=True, order_id="ORD-NEW-001"))

    @pytest.fixture
    def session_dup(self) -> AsyncMock:
        return _make_session_mock(_sp_result(is_new=False, order_id="ORD-DUP-001"))

    async def test_publishes_four_dist_messages_when_new(self, msg, session_new, broker) -> None:
        await process_lead(msg, "corr-new", session_new, broker)

        assert broker.publish.await_count == 4
        expected_channels = [
            (QUEUE_DIST_SMS, "SMS"),
            (QUEUE_DIST_EMAIL, "EMAIL"),
            (QUEUE_DIST_CALLCENTER, "CALL_CENTER"),
            (QUEUE_DIST_WHATSAPP, "WHATSAPP"),
        ]
        for (queue, channel), call in zip(expected_channels, broker.publish.await_args_list):
            args, kwargs = call
            dist_msg, queue_name, exchange = args[0], args[1], kwargs.get("exchange")
            assert queue_name == queue
            assert exchange == "dist"
            assert dist_msg.channel == channel
            assert dist_msg.order_id == "ORD-NEW-001"

    async def test_does_not_publish_when_duplicate(self, msg, session_dup, broker) -> None:
        await process_lead(msg, "corr-dup", session_dup, broker)
        broker.publish.assert_not_called()

    async def test_sp_called_with_correct_params(self, msg, session_new, broker) -> None:
        with patch("gex_worker.consumers.uuid.uuid7") as mock_uuid:
            mock_uuid.return_value = "0190b6c0-7c3e-7abc-9def-000000000001"
            await process_lead(msg, "corr-params", session_new, broker)

        call = session_new.execute.await_args
        stmt = call.args[0]
        params = call.args[1]
        assert "CALL sp_insert_lead" in stmt.text
        assert params["p_correlation_id"] == "corr-params"
        assert params["p_gateway"] == msg.gateway
        assert params["p_transaction_id"] == msg.transaction_id
        assert params["p_event"] == msg.event
        assert params["p_lag_seconds"] == 5.0  # NOW_UTC - msg.transaction_time

    async def test_lag_seconds_computation(self, msg, session_new, broker) -> None:
        await process_lead(msg, "corr-lag", session_new, broker)

        params = session_new.execute.await_args.args[1]
        assert params["p_lag_seconds"] == 5.0

    async def test_raises_on_sp_error(self, msg, broker) -> None:
        session = AsyncMock()
        session.execute = AsyncMock(side_effect=RuntimeError("DB down"))

        with pytest.raises(RuntimeError, match="DB down"):
            await process_lead(msg, "corr-err", session, broker)

        broker.publish.assert_not_called()

    async def test_generates_six_uuid7_ids(self, msg, session_new, broker) -> None:
        with patch("gex_worker.consumers.uuid.uuid7") as mock_uuid:
            mock_uuid.side_effect = [
                "UUID-LEAD-001",
                "UUID-ORDER-001",
                "UUID-SMS-001",
                "UUID-EMAIL-001",
                "UUID-CC-001",
                "UUID-WAPP-001",
            ]
            await process_lead(msg, "corr-uuid", session_new, broker)

        params = session_new.execute.await_args.args[1]
        assert params["p_lead_id"] == "UUID-LEAD-001"
        assert params["p_order_id"] == "UUID-ORDER-001"
        assert params["p_dist_sms_id"] == "UUID-SMS-001"
        assert params["p_dist_email_id"] == "UUID-EMAIL-001"
        assert params["p_dist_callcenter_id"] == "UUID-CC-001"
        assert params["p_dist_whatsapp_id"] == "UUID-WAPP-001"


# ---------------------------------------------------------------------------
# Helper — replaces ``datetime`` module in test scope
# ---------------------------------------------------------------------------


class _FakeDatetime:
    """Duck-typed substitute for the ``datetime`` module.

    Exposes ``datetime.now(tz=...)`` returning a fixed value.
    """

    def __init__(self, fixed_now: datetime) -> None:
        self._fixed_now = fixed_now

    def now(self, tz: object = None) -> datetime:  # noqa: ARG002
        return self._fixed_now
