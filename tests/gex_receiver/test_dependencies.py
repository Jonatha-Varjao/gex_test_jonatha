"""Unit tests for FastAPI dependency injection."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from gex_receiver.dependencies import get_db_session

pytestmark = pytest.mark.unit


class TestGetDBSession:
    async def test_yields_session_from_factory(self):
        mock_session = MagicMock()
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()
        mock_session_factory = MagicMock()
        mock_session_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_factory.return_value.__aexit__ = AsyncMock(return_value=False)

        mock_db = MagicMock()
        mock_db.session_factory = mock_session_factory

        request = MagicMock()
        request.app.state.db = mock_db

        gen = get_db_session(request)
        session = await gen.__anext__()
        assert session is mock_session

        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

        mock_session.commit.assert_awaited_once()
        mock_session.rollback.assert_not_called()

    async def test_rolls_back_on_exception(self):
        mock_session = MagicMock()
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()
        mock_session_factory = MagicMock()
        mock_session_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_factory.return_value.__aexit__ = AsyncMock(return_value=False)

        mock_db = MagicMock()
        mock_db.session_factory = mock_session_factory

        request = MagicMock()
        request.app.state.db = mock_db

        gen = get_db_session(request)
        await gen.__anext__()

        with pytest.raises(RuntimeError, match="boom"):
            await gen.athrow(RuntimeError("boom"))

        mock_session.rollback.assert_awaited_once()
        mock_session.commit.assert_not_called()
