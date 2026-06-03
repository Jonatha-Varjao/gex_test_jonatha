"""Unit tests for ``gex_worker.middleware``.

``CorrelationIdMiddleware``, ``RetryMiddleware``, and ``bind_structlog_context``
are tested in isolation with mocked FastStream internals.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
import structlog

from gex_common.config import CONSTANTS
from gex_worker.middleware import (
    CorrelationIdMiddleware,
    RetryMiddleware,
    bind_structlog_context,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_context() -> MagicMock:
    """Return a ``ContextRepo``-shaped MagicMock.

    The ``scope()`` method returns an async CM so that
    ``async with self.context.scope(...)`` works.
    """
    ctx = MagicMock()
    ctx_scope_cm = AsyncMock()
    ctx_scope_cm.__aenter__ = AsyncMock(return_value=None)
    ctx_scope_cm.__aexit__ = AsyncMock(return_value=None)
    ctx.scope.return_value = ctx_scope_cm
    return ctx


@pytest.fixture
def mock_message() -> MagicMock:
    """Return a ``StreamMessage``-shaped MagicMock with a fixed correlation_id."""
    msg = MagicMock()
    msg.correlation_id = "corr-from-msg"
    msg.message_id = "msg-42"
    return msg


@pytest.fixture
def call_next() -> AsyncMock:
    return AsyncMock(return_value=None)


# ---------------------------------------------------------------------------
# CorrelationIdMiddleware
# ---------------------------------------------------------------------------


class TestCorrelationIdMiddleware:
    async def test_uses_existing_correlation_id(
        self, mock_context, mock_message, call_next
    ) -> None:
        m = CorrelationIdMiddleware(mock_message, context=mock_context)
        await m.consume_scope(call_next, mock_message)
        mock_context.scope.assert_called_once_with("correlation_id", "corr-from-msg")

    async def test_generates_uuid4_when_missing(self, mock_context, call_next) -> None:
        msg = MagicMock()
        msg.correlation_id = ""
        msg.message_id = "msg-42"

        m = CorrelationIdMiddleware(msg, context=mock_context)
        await m.consume_scope(call_next, msg)

        assert mock_context.scope.call_count == 1
        cid = mock_context.scope.call_args[0][1]
        assert len(cid) == 36  # UUID4 string
        assert cid.count("-") == 4

    async def test_calls_super_consume_scope(self, mock_context, mock_message, call_next) -> None:
        m = CorrelationIdMiddleware(mock_message, context=mock_context)
        await m.consume_scope(call_next, mock_message)
        call_next.assert_awaited_once_with(mock_message)


# ---------------------------------------------------------------------------
# RetryMiddleware
# ---------------------------------------------------------------------------


class TestRetryMiddleware:
    async def test_success_no_retry(self, mock_context, call_next, patched_asyncio_sleep) -> None:
        msg = MagicMock()
        m = RetryMiddleware(msg, context=mock_context)
        await m.consume_scope(call_next, msg)

        call_next.assert_awaited_once()
        patched_asyncio_sleep.assert_not_called()

    async def test_retries_on_exception(self, mock_context, patched_asyncio_sleep) -> None:
        msg = MagicMock()
        call_next = AsyncMock()
        call_next.side_effect = [
            RuntimeError("attempt-1"),
            RuntimeError("attempt-2"),
            RuntimeError("attempt-3"),
            None,  # fourth attempt succeeds
        ]

        m = RetryMiddleware(msg, context=mock_context)
        await m.consume_scope(call_next, msg)

        assert call_next.await_count == 4  # initial + 3 retries
        assert patched_asyncio_sleep.await_count == 3
        for _, delay_ms in enumerate(CONSTANTS.retry_backoffs_ms):
            patched_asyncio_sleep.assert_any_await(delay_ms / 1000.0)

    async def test_re_raises_after_all_retries_exhausted(
        self, mock_context, patched_asyncio_sleep
    ) -> None:
        msg = MagicMock()
        call_next = AsyncMock(side_effect=RuntimeError("persistent"))

        m = RetryMiddleware(msg, context=mock_context)
        with pytest.raises(RuntimeError, match="persistent"):
            await m.consume_scope(call_next, msg)

        expected_attempts = len(CONSTANTS.retry_backoffs_ms) + 1  # initial + len(backoffs)
        assert call_next.await_count == expected_attempts
        assert patched_asyncio_sleep.await_count == len(CONSTANTS.retry_backoffs_ms)

    async def test_delay_values_match_backoffs(self, mock_context, patched_asyncio_sleep) -> None:
        msg = MagicMock()
        call_next = AsyncMock(side_effect=ValueError("nope"))

        m = RetryMiddleware(msg, context=mock_context)
        with pytest.raises(ValueError):
            await m.consume_scope(call_next, msg)

        expected_delays = [ms / 1000.0 for ms in CONSTANTS.retry_backoffs_ms]
        actual_calls = [c[0][0] for c in patched_asyncio_sleep.await_args_list]
        assert actual_calls == expected_delays


# ---------------------------------------------------------------------------
# bind_structlog_context
# ---------------------------------------------------------------------------


class TestBindStructlogContext:
    def test_binds_all_keys(self) -> None:
        structlog.contextvars.clear_contextvars()
        bind_structlog_context(
            correlation_id="corr-1",
            gateway="grummer",
            event="order.approved",
            customer_id="anon-xyz",
        )
        ctx = structlog.contextvars.get_contextvars()
        assert ctx["correlation_id"] == "corr-1"
        assert ctx["gateway"] == "grummer"
        assert ctx["event"] == "order.approved"
        assert ctx["customer_id"] == "anon-xyz"

    def test_clears_previous_context(self) -> None:
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(correlation_id="old", leftover="stale")
        bind_structlog_context(
            correlation_id="new",
            gateway="lous",
            event="order.refunded",
            customer_id="anon-abc",
        )
        ctx = structlog.contextvars.get_contextvars()
        assert ctx["correlation_id"] == "new"
        assert "leftover" not in ctx
