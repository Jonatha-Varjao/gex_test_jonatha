"""Unit tests for ``gex_worker.dlq``."""

from unittest.mock import MagicMock, patch

import pytest

from gex_worker.dlq import get_dlq_correlation_id

pytestmark = pytest.mark.unit


class TestGetDlqCorrelationId:
    def test_returns_fallback_when_outside_faststream(self) -> None:
        cid = get_dlq_correlation_id()
        assert cid == "no-correlation-id"

    def test_reads_from_context_when_available(self) -> None:
        mock_ctx = MagicMock()
        mock_ctx.get.return_value = "corr-foo"
        with patch("faststream.Context", return_value=mock_ctx):
            cid = get_dlq_correlation_id()
        assert cid == "corr-foo"
