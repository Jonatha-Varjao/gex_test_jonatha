"""Unit tests for ``gex_worker.distributors.process_sms``.

Mocks ``httpx.AsyncClient``, ``random.random``, and ``datetime``
so no HTTP, no randomness, and no wall-clock dependency.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gex_common.config import CHANNEL_SMS, DIST_STATUS_DELIVERED
from gex_worker.distributors import SimulatedDistributorFailure, process_sms

pytestmark = pytest.mark.unit

STARTED_AT = datetime(2026, 1, 1, 12, 0, 1, tzinfo=timezone.utc)
DELIVERED_AT = datetime(2026, 1, 1, 12, 0, 3, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _settings_patches(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch module-level APP_SETTINGS references for deterministic tests."""
    monkeypatch.setattr("gex_worker.distributors.APP_SETTINGS.sms_failure_rate", 0.1)
    monkeypatch.setattr(
        "gex_worker.distributors.APP_SETTINGS.webhook_site_url", "https://hook.example.com"
    )


@pytest.fixture(autouse=True)
def _freeze_time(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace ``datetime.now`` in the distributors module with a fixed two-value sequence."""

    class _FakeDt:
        call_count = 0

        def now(self, tz: object = None) -> datetime:  # noqa: ARG002
            self.call_count += 1
            if self.call_count == 1:
                return STARTED_AT
            return DELIVERED_AT

    monkeypatch.setattr("gex_worker.distributors.datetime", _FakeDt())


@pytest.fixture
def session() -> AsyncMock:
    s = AsyncMock()
    s.execute = AsyncMock()
    return s


@pytest.fixture
def msg(dist_msg):
    return dist_msg


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestProcessSms:
    async def test_happy_path(self, msg, session, patched_random, fake_httpx_client) -> None:
        patched_random.return_value = 0.5  # above sms_failure_rate → no simulated failure
        await process_sms(msg, "corr-happy", session)

        # HTTP POST was called
        fake_httpx_client.post.assert_awaited_once()
        post_args = fake_httpx_client.post.await_args
        assert post_args[0][0] == "https://hook.example.com"  # url
        assert "json" in post_args[1] or "json" in post_args.kwargs

        # DB UPDATE was called
        session.execute.assert_awaited_once()
        stmt, params = session.execute.await_args.args
        assert "UPDATE distribution_status" in str(stmt)
        assert params["status"] == DIST_STATUS_DELIVERED
        assert params["channel"] == CHANNEL_SMS
        assert params["order_id"] == msg.order_id
        assert params["lag"] == 2.0  # DELIVERED_AT - STARTED_AT

    async def test_raises_simulated_failure(
        self, msg, session, patched_random, fake_httpx_client
    ) -> None:
        patched_random.return_value = 0.0  # always trigger simulated failure
        with pytest.raises(SimulatedDistributorFailure, match="simulated sms provider error"):
            await process_sms(msg, "corr-fail", session)

        fake_httpx_client.post.assert_not_called()
        session.execute.assert_not_called()

    async def test_raises_on_http_error(
        self, msg, session, patched_random, fake_httpx_client
    ) -> None:
        patched_random.return_value = 0.5  # above sms_failure_rate
        fake_httpx_client.post = AsyncMock(side_effect=ConnectionError("connection refused"))
        mock_client_cm = MagicMock()

        async def _make_client(*a, **kw):
            return AsyncMock(post=AsyncMock(side_effect=ConnectionError("connection refused")))

        mock_client_cm.__aenter__ = AsyncMock(side_effect=_make_client)
        mock_client_cm.__aexit__ = AsyncMock(return_value=None)

        with patch("gex_worker.distributors.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = mock_client_cm
            with pytest.raises(ConnectionError, match="connection refused"):
                await process_sms(msg, "corr-http", session)

    async def test_raises_on_http_status_error(
        self, msg, session, patched_random, fake_httpx_client
    ) -> None:
        patched_random.return_value = 0.5  # above sms_failure_rate
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = RuntimeError("429 Too Many Requests")
        mock_client_cm = MagicMock()

        async def _make_client(*a, **kw):
            return AsyncMock(post=AsyncMock(return_value=mock_response))

        mock_client_cm.__aenter__ = AsyncMock(side_effect=_make_client)
        mock_client_cm.__aexit__ = AsyncMock(return_value=None)

        with patch("gex_worker.distributors.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = mock_client_cm
            with pytest.raises(RuntimeError, match="429"):
                await process_sms(msg, "corr-429", session)

    async def test_simulated_failure_class(self) -> None:
        assert issubclass(SimulatedDistributorFailure, Exception)

    async def test_empty_webhook_url_still_called(
        self, msg, session, patched_random, fake_httpx_client, monkeypatch
    ) -> None:
        monkeypatch.setattr("gex_worker.distributors.APP_SETTINGS.webhook_site_url", "")
        patched_random.return_value = 0.5  # above sms_failure_rate
        await process_sms(msg, "corr-empty-url", session)
        fake_httpx_client.post.assert_awaited_once()
