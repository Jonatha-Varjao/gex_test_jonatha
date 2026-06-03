"""Unit tests for ``gex_worker.exception_handlers``.

Pure helpers (``_routing_key``, ``_body_dict``) and the
``DlqMiddleware.after_processed`` callback.
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gex_common.config import CONSTANTS
from gex_common.models import DLQMessage
from gex_worker.exception_handlers import (
    DlqMiddleware,
    _body_dict,
    _routing_key,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# _routing_key
# ---------------------------------------------------------------------------


class TestRoutingKey:
    def test_extracts_from_raw_message(self) -> None:
        msg = MagicMock()
        msg.raw_message.routing_key = "lead.received"
        assert _routing_key(msg) == "lead.received"

    def test_returns_none_when_no_raw_message(self) -> None:
        msg = MagicMock(spec=[])  # no raw_message attribute at all
        assert _routing_key(msg) is None

    def test_raw_message_has_no_routing_key(self) -> None:
        msg = MagicMock()
        # raw_message exists but has no routing_key per spec
        msg.raw_message = object()
        assert _routing_key(msg) is None


# ---------------------------------------------------------------------------
# _body_dict
# ---------------------------------------------------------------------------


class TestBodyDict:
    def test_parses_valid_json(self) -> None:
        msg = MagicMock()
        msg.body = b'{"event": "order.approved", "gateway": "lous"}'
        result = _body_dict(msg)
        assert result == {"event": "order.approved", "gateway": "lous"}

    def test_returns_empty_dict_on_invalid_json(self) -> None:
        msg = MagicMock()
        msg.body = b"not-json"
        assert _body_dict(msg) == {}

    def test_returns_empty_dict_on_type_error(self) -> None:
        msg = MagicMock()
        msg.body = 12345  # non-bytes, non-str → TypeError in json.loads
        assert _body_dict(msg) == {}

    def test_missing_body_defaults_to_empty_dict(self) -> None:
        msg = MagicMock()
        del msg.body  # getattr(..., b"{}") fallback
        assert _body_dict(msg) == {}


# ---------------------------------------------------------------------------
# DlqMiddleware.after_processed
# ---------------------------------------------------------------------------


class TestDlqMiddleware:
    @pytest.fixture
    def mock_context(self) -> MagicMock:
        ctx = MagicMock()
        ctx.get = MagicMock()
        ctx.get_local = MagicMock()
        return ctx

    @pytest.fixture
    def fake_message(self) -> MagicMock:
        msg = MagicMock()
        msg.raw_message.routing_key = CONSTANTS.queue_lead_received
        msg.body = b'{"gateway": "lous"}'
        return msg

    @pytest.fixture
    def fake_broker(self) -> MagicMock:
        broker = MagicMock()
        broker.publish = AsyncMock()
        return broker

    @pytest.fixture
    def middleware(self, mock_context) -> DlqMiddleware:
        m = DlqMiddleware.__new__(DlqMiddleware)
        m.context = mock_context
        return m

    async def test_noop_when_no_exception(self, middleware, mock_context) -> None:
        result = await middleware.after_processed(exc_type=None, exc_val=None, _exc_tb=None)
        assert result is None
        mock_context.get_local.assert_not_called()

    async def test_returns_false_when_msg_missing(self, middleware, mock_context) -> None:
        mock_context.get_local.return_value = None
        result = await middleware.after_processed(
            exc_type=ValueError, exc_val=ValueError("boom"), _exc_tb=None
        )
        assert result is False

    async def test_returns_false_when_broker_missing(
        self, middleware, mock_context, fake_message
    ) -> None:
        mock_context.get_local.return_value = fake_message
        mock_context.get.return_value = None  # broker
        result = await middleware.after_processed(
            exc_type=ValueError, exc_val=ValueError("boom"), _exc_tb=None
        )
        assert result is False

    async def test_publishes_to_lead_dead_consumer_failed(
        self, middleware, mock_context, fake_message, fake_broker
    ) -> None:
        fake_message.raw_message.routing_key = CONSTANTS.queue_lead_received
        mock_context.get_local.return_value = fake_message
        mock_context.get.return_value = fake_broker

        result = await middleware.after_processed(
            exc_type=RuntimeError, exc_val=RuntimeError("oh no"), _exc_tb=None
        )

        assert result is False
        fake_broker.publish.assert_awaited_once()
        args, kwargs = fake_broker.publish.await_args
        dlq_msg, dlq_queue, exchange = args[0], args[1], kwargs.get("exchange")
        assert isinstance(dlq_msg, DLQMessage)
        assert dlq_queue == "lead.dead.consumer_failed"
        assert exchange == "lead"
        assert dlq_msg.gateway == "lous"

    async def test_publishes_to_dist_dead_sms(
        self, middleware, mock_context, fake_message, fake_broker
    ) -> None:
        fake_message.raw_message.routing_key = CONSTANTS.queue_dist_sms
        fake_message.body = b'{"gateway": "grummer"}'
        mock_context.get_local.return_value = fake_message
        mock_context.get.return_value = fake_broker

        result = await middleware.after_processed(
            exc_type=ConnectionError, exc_val=ConnectionError("timeout"), _exc_tb=None
        )

        assert result is False
        fake_broker.publish.assert_awaited_once()
        args = fake_broker.publish.await_args
        dlq_queue = args[0][1]
        exchange = args[1].get("exchange")
        assert dlq_queue == "dist.dead.sms"
        assert exchange == "dist"

    async def test_logs_and_returns_false_on_unknown_routing_key(
        self, middleware, mock_context, fake_message, fake_broker
    ) -> None:
        fake_message.raw_message.routing_key = "unknown.queue"
        mock_context.get_local.return_value = fake_message
        mock_context.get.return_value = fake_broker

        with patch("gex_worker.exception_handlers.logger") as mock_logger:
            result = await middleware.after_processed(
                exc_type=Exception, exc_val=Exception("weird"), _exc_tb=None
            )

        assert result is False
        fake_broker.publish.assert_not_called()
        mock_logger.exception.assert_called_once()

    async def test_correlation_id_reads_from_message_body(
        self, middleware, mock_context, fake_message, fake_broker
    ) -> None:
        fake_message.raw_message.routing_key = CONSTANTS.queue_dist_sms
        fake_message.body = json.dumps({
            "gateway": "lous",
            "correlation_id": "from-original-msg-123",
        }).encode("utf-8")
        mock_context.get_local.return_value = fake_message
        mock_context.get.return_value = fake_broker

        await middleware.after_processed(
            exc_type=RuntimeError, exc_val=RuntimeError("boom"), _exc_tb=None
        )

        fake_broker.publish.assert_awaited_once()
        args = fake_broker.publish.await_args
        dlq_msg = args[0][0]
        assert dlq_msg.correlation_id == "from-original-msg-123"
