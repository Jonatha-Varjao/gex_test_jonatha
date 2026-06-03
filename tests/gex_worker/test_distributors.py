from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from gex_common.config import CONSTANTS
from gex_worker.distributors import (
    PermanentDistributorFailure,
    SimulatedDistributorFailure,
    TransientDistributorFailure,
    process_sms,
)

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def never_fail(monkeypatch) -> None:
    monkeypatch.setattr(
        "gex_worker.distributors.random.random", MagicMock(return_value=1.0)
    )


def _mock_http_client(monkeypatch, status_code: int, **kwargs) -> None:
    mock_response = MagicMock()
    mock_response.status_code = status_code
    error = kwargs.get("error")
    if error:
        mock_response.raise_for_status = MagicMock(side_effect=error)
    else:
        mock_response.raise_for_status = MagicMock(return_value=None)
    mock_client = MagicMock()
    mock_client.post = AsyncMock(return_value=mock_response)
    mock_client_class = MagicMock()
    mock_client_class.return_value.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client_class.return_value.__aexit__ = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "gex_worker.distributors.httpx.AsyncClient", mock_client_class
    )


class TestSimulatedFailure:
    async def test_raises_when_random_below_threshold(
        self, dist_msg, monkeypatch
    ) -> None:
        monkeypatch.setattr(
            "gex_worker.distributors.random.random", MagicMock(return_value=0.05)
        )
        with pytest.raises(SimulatedDistributorFailure, match="simulated"):
            await process_sms(dist_msg, "corr-1", AsyncMock())

    async def test_skips_when_random_above_threshold(
        self, dist_msg, monkeypatch
    ) -> None:
        monkeypatch.setattr(
            "gex_worker.distributors.random.random", MagicMock(return_value=1.0)
        )
        _mock_http_client(monkeypatch, status_code=200)
        session = AsyncMock()
        session.execute = AsyncMock()
        await process_sms(dist_msg, "corr-1", session)
        session.execute.assert_called_once()


class TestPermanentFailure:
    """Non-retriable 4xx (403, 404, etc.) — straight to DLQ."""

    async def test_403_raises_permanent(self, dist_msg, monkeypatch) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 403
        _mock_http_client(
            monkeypatch,
            status_code=403,
            error=httpx.HTTPStatusError(
                "403 Forbidden",
                request=MagicMock(),
                response=mock_response,
            ),
        )
        session = AsyncMock()
        session.execute = AsyncMock()
        session.commit = AsyncMock()

        with pytest.raises(PermanentDistributorFailure, match="HTTPStatusError"):
            await process_sms(dist_msg, "corr-1", session)

        sql_text = session.execute.call_args[0][0].text
        assert "status = 'failed'" in sql_text
        session.commit.assert_awaited_once()


class TestTransientFailure:
    """5xx / 408 / 429 / timeout — raise TransientDistributorFailure for caller to retry."""

    @pytest.mark.parametrize("status_code", [408, 429])
    async def test_4xx_retriable_raises_transient(
        self, status_code, dist_msg, monkeypatch
    ) -> None:
        mock_response = MagicMock()
        mock_response.status_code = status_code
        _mock_http_client(
            monkeypatch,
            status_code=status_code,
            error=httpx.HTTPStatusError(
                f"{status_code}",
                request=MagicMock(),
                response=mock_response,
            ),
        )
        session = AsyncMock()
        session.execute = AsyncMock()

        with pytest.raises(TransientDistributorFailure, match="HTTPStatusError"):
            await process_sms(dist_msg, "corr-1", session)

        session.execute.assert_not_called()

    async def test_500_raises_transient(self, dist_msg, monkeypatch) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 500
        _mock_http_client(
            monkeypatch,
            status_code=500,
            error=httpx.HTTPStatusError(
                "500 Internal Server Error",
                request=MagicMock(),
                response=mock_response,
            ),
        )
        session = AsyncMock()

        with pytest.raises(TransientDistributorFailure):
            await process_sms(dist_msg, "corr-1", session)

    async def test_timeout_raises_transient(self, dist_msg, monkeypatch) -> None:
        mock_client = MagicMock()
        mock_client.post = AsyncMock(side_effect=httpx.TimeoutException(
            "Connection timed out"
        ))
        mock_client_class = MagicMock()
        mock_client_class.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_class.return_value.__aexit__ = AsyncMock(return_value=None)
        monkeypatch.setattr(
            "gex_worker.distributors.httpx.AsyncClient", mock_client_class
        )
        session = AsyncMock()

        with pytest.raises(TransientDistributorFailure, match="Timeout"):
            await process_sms(dist_msg, "corr-1", session)


class TestSuccess:
    async def test_updates_delivered(self, dist_msg, monkeypatch) -> None:
        _mock_http_client(monkeypatch, status_code=200)
        session = AsyncMock()
        session.execute = AsyncMock()

        await process_sms(dist_msg, "corr-1", session)

        session.execute.assert_called_once()
        sql_text = session.execute.call_args[0][0].text
        assert "status = :status" in sql_text
        assert "attempts = 1" in sql_text
        params = session.execute.call_args[0][1]
        assert params["status"] == CONSTANTS.dist_status_delivered
