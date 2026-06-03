"""Unit tests for ``gex_worker.dlq``.

``get_dlq_correlation_id``, ``_queue_origin_for``, and ``build_dlq_message``
are pure / near-pure functions — trivial to test in isolation.
"""

from unittest.mock import MagicMock, patch

import pytest

from gex_common.models import DLQMessage
from gex_worker.dlq import (
    _queue_origin_for,
    build_dlq_message,
    get_dlq_correlation_id,
)

pytestmark = pytest.mark.unit


class TestGetDlqCorrelationId:
    def test_returns_fallback_when_outside_faststream(self) -> None:
        """Outside a FastStream context ``Context().get`` raises → fallback."""
        cid = get_dlq_correlation_id()
        assert cid == "no-correlation-id"

    def test_reads_from_context_when_available(self) -> None:
        mock_ctx = MagicMock()
        mock_ctx.get.return_value = "corr-foo"
        with patch("faststream.Context", return_value=mock_ctx):
            cid = get_dlq_correlation_id()
        assert cid == "corr-foo"


class TestQueueOriginFor:
    @pytest.mark.parametrize(
        "source,expected",
        [
            ("lead.received", "lead.dead.consumer_failed"),
            ("dist.sms", "dist.dead.sms"),
            ("dist.email", "dist.email"),  # fallback
            ("unknown.queue", "unknown.queue"),  # fallback
        ],
    )
    def test_mapping(self, source: str, expected: str) -> None:
        assert _queue_origin_for(source) == expected


class TestBuildDlqMessage:
    def test_pydantic_msg_uses_model_dump(self) -> None:
        msg = MagicMock()
        msg.model_dump.return_value = {"foo": "bar"}
        msg.gateway = "grummer"
        dlq = build_dlq_message(msg, ValueError("boom"), "lead.received")
        assert isinstance(dlq, DLQMessage)
        assert dlq.original_payload == {"foo": "bar"}
        assert dlq.error_reason == "ValueError: boom"
        assert dlq.gateway == "grummer"
        assert dlq.queue_origin == "lead.dead.consumer_failed"

    def test_non_pydantic_msg_wraps_raw(self) -> None:
        msg = object()
        dlq = build_dlq_message(msg, RuntimeError("fail"), "dist.sms")
        assert dlq.original_payload == {"raw": str(msg)}
        assert dlq.error_reason == "RuntimeError: fail"

    def test_gateway_defaults_to_unknown_when_missing(self) -> None:
        msg = MagicMock()
        msg.model_dump.return_value = {"nope": 1}
        del msg.gateway  # simulate object without .gateway
        dlq = build_dlq_message(msg, Exception("x"), "q")
        assert dlq.gateway == "unknown"

    def test_correlation_fallback(self) -> None:
        msg = MagicMock()
        msg.model_dump.return_value = {}
        msg.gateway = "grummer"
        with patch("gex_worker.dlq.get_dlq_correlation_id", return_value="no-correlation-id"):
            dlq = build_dlq_message(msg, Exception("x"), "q")
        assert dlq.correlation_id == "no-correlation-id"

    def test_error_reason_format(self) -> None:
        msg = MagicMock()
        msg.model_dump.return_value = {}
        msg.gateway = "grummer"
        try:
            raise ValueError("missing_key")
        except ValueError as e:
            dlq = build_dlq_message(msg, e, "q")
        assert dlq.error_reason == "ValueError: missing_key"
