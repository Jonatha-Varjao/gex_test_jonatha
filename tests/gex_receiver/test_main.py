"""Unit tests for the app factory, lifespan, and StructLogMiddleware."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI

from gex_receiver.main import StructLogMiddleware, lifespan

pytestmark = pytest.mark.unit


class TestLifespan:
    async def test_startup_initializes_db_and_publisher(self):
        app = FastAPI()
        with (
            patch("gex_receiver.main.Database") as mock_db_class,
            patch("gex_receiver.main.RabbitMQPublisher") as mock_pub_class,
        ):
            mock_db = MagicMock()
            mock_db.connect = AsyncMock()
            mock_db.close = AsyncMock()
            mock_db_class.return_value = mock_db
            mock_pub = MagicMock()
            mock_pub.connect = AsyncMock()
            mock_pub.declare_topology = AsyncMock()
            mock_pub.close = AsyncMock()
            mock_pub_class.return_value = mock_pub

            async with lifespan(app):
                pass

        mock_db.connect.assert_awaited_once()
        mock_pub.connect.assert_awaited_once()
        mock_pub.declare_topology.assert_awaited_once()
        assert isinstance(app.state.db, MagicMock)
        assert isinstance(app.state.rmq_publisher, MagicMock)
        assert app.state.settings is not None

    async def test_shutdown_closes_publisher_then_db(self):
        app = FastAPI()
        call_order: list[str] = []

        with (
            patch("gex_receiver.main.Database") as mock_db_class,
            patch("gex_receiver.main.RabbitMQPublisher") as mock_pub_class,
        ):
            mock_db = MagicMock()
            mock_db.connect = AsyncMock()

            async def _close_db():
                call_order.append("db_close")

            mock_db.close = AsyncMock(side_effect=_close_db)
            mock_db_class.return_value = mock_db

            mock_pub = MagicMock()
            mock_pub.connect = AsyncMock()
            mock_pub.declare_topology = AsyncMock()

            async def _close_pub():
                call_order.append("pub_close")

            mock_pub.close = AsyncMock(side_effect=_close_pub)
            mock_pub_class.return_value = mock_pub

            async with lifespan(app):
                pass

        assert call_order == ["pub_close", "db_close"]


class TestStructLogMiddleware:
    async def test_passes_through_non_http_scope(self):
        captured = {}

        async def downstream_app(scope, receive, send):
            captured["scope_type"] = scope["type"]

        middleware = StructLogMiddleware(downstream_app)
        scope = {"type": "lifespan"}
        receive = MagicMock()
        send = MagicMock()

        await middleware(scope, receive, send)

        assert captured["scope_type"] == "lifespan"

    async def test_logs_request_completion_for_http(self):
        async def downstream_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

        middleware = StructLogMiddleware(downstream_app)
        with patch("gex_receiver.main.get_access_logger") as mock_logger_factory:
            mock_logger = MagicMock()
            mock_logger_factory.return_value = mock_logger

            scope = {
                "type": "http",
                "method": "GET",
                "path": "/test",
                "headers": [],
            }
            receive = MagicMock()
            send = AsyncMock()

            await middleware(scope, receive, send)

        mock_logger.info.assert_called_once()
        call_kwargs = mock_logger.info.call_args[1]
        assert mock_logger.info.call_args[0][0] == "request_completed"
        assert call_kwargs["status_code"] == 200
        assert "duration_ms" in call_kwargs

    async def test_logs_500_when_no_response_start_sent(self):
        async def downstream_app(scope, receive, send):
            pass

        middleware = StructLogMiddleware(downstream_app)
        with patch("gex_receiver.main.get_access_logger") as mock_logger_factory:
            mock_logger = MagicMock()
            mock_logger_factory.return_value = mock_logger

            scope = {
                "type": "http",
                "method": "GET",
                "path": "/silent",
                "headers": [],
            }
            receive = MagicMock()
            send = AsyncMock()

            await middleware(scope, receive, send)

        mock_logger.info.assert_called_once()
        assert mock_logger.info.call_args[1]["status_code"] == 500

    async def test_response_includes_x_correlation_id_header(self):
        sent_headers = []

        async def downstream_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

        async def tracking_send(message):
            if message["type"] == "http.response.start":
                sent_headers.extend((k.decode(), v.decode()) for k, v in message.get("headers", []))
            await send(message)

        middleware = StructLogMiddleware(downstream_app)
        with patch("gex_receiver.main.get_access_logger"):
            scope = {
                "type": "http",
                "method": "GET",
                "path": "/test",
                "headers": [(b"x-correlation-id", b"my-custom-id")],
            }
            receive = MagicMock()
            send = tracking_send

            await middleware(scope, receive, send)

        assert ("x-correlation-id", "my-custom-id") in sent_headers
