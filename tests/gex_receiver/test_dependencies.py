"""Unit tests for FastAPI dependency injection."""

import re
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from gex_common.config import AppSettings
from gex_receiver.config import APP_SETTINGS
from gex_receiver.dependencies import (
    get_correlation_id,
    get_db_session,
    get_publisher,
    get_settings,
)
from gex_receiver.publishers import RabbitMQPublisher

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


class TestGetPublisher:
    def test_returns_publisher_from_app_state(self):
        mock_publisher = MagicMock(spec=RabbitMQPublisher)
        request = MagicMock()
        request.app.state.rmq_publisher = mock_publisher

        assert get_publisher(request) is mock_publisher


class TestGetSettings:
    def test_returns_settings_from_app_state(self):
        mock_settings = MagicMock(spec=AppSettings)
        request = MagicMock()
        request.app.state.settings = mock_settings

        assert get_settings(request) is mock_settings


class TestGetCorrelationId:
    def test_returns_header_value_when_present(self):
        request = MagicMock()
        request.headers = {"x-correlation-id": "my-trace-123"}

        assert get_correlation_id(request) == "my-trace-123"

    def test_generates_uuid_when_header_missing(self):
        request = MagicMock()
        request.headers = {}

        result = get_correlation_id(request)
        assert re.match(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            result,
        )

    def test_generates_uuid_when_header_empty(self):
        request = MagicMock()
        request.headers = {"x-correlation-id": ""}

        result = get_correlation_id(request)
        assert re.match(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            result,
        )


class TestDependenciesEndToEnd:
    """Verify the deps wire into a real FastAPI route via dependency_overrides."""

    async def test_settings_dep_returns_overridden_value(self):
        @asynccontextmanager
        async def _noop_lifespan(_app):
            yield

        app = FastAPI(lifespan=_noop_lifespan)
        app.state.settings = APP_SETTINGS

        from gex_receiver.dependencies import SettingsDep

        @app.get("/probe")
        async def probe(settings: SettingsDep) -> dict:
            return {"env": settings.environment}

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            r = await client.get("/probe")
        assert r.status_code == 200
        assert r.json()["env"] == "development"
