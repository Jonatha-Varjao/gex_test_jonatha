"""Unit tests for the app factory, lifespan, and LoggingMiddleware."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI

from gex_receiver.main import LoggingMiddleware, create_app, lifespan

pytestmark = pytest.mark.unit


class TestCreateApp:
    def test_create_app_returns_fastapi_instance(self):
        app = create_app()
        assert isinstance(app, FastAPI)
        assert app.title == "GEX Webhook Receiver"

    def test_app_has_webhook_route(self):
        app = create_app()
        routes = [r for r in app.routes if hasattr(r, "path")]
        webhook_routes = [r for r in routes if r.path.startswith("/webhooks")]
        assert len(webhook_routes) >= 1
        post_routes = [r for r in webhook_routes if "POST" in getattr(r, "methods", set())]
        assert len(post_routes) >= 1


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


class TestLoggingMiddleware:
    async def test_passes_through_non_http_scope(self):
        captured = {}

        async def downstream_app(scope, receive, send):
            captured["scope_type"] = scope["type"]

        middleware = LoggingMiddleware(downstream_app)
        scope = {"type": "lifespan"}
        receive = MagicMock()
        send = MagicMock()

        await middleware(scope, receive, send)

        assert captured["scope_type"] == "lifespan"

    async def test_logs_request_completion_for_http(self):
        captured_logs: list[dict] = []

        async def downstream_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

        middleware = LoggingMiddleware(downstream_app)
        with patch.object(middleware, "logger") as mock_logger:
            mock_logger.info = lambda msg, **kw: captured_logs.append({"msg": msg, **kw})

            scope = {"type": "http", "method": "GET", "path": "/test"}
            receive = MagicMock()
            send = AsyncMock()

            await middleware(scope, receive, send)

        assert len(captured_logs) == 1
        log = captured_logs[0]
        assert log["msg"] == "request_completed"
        assert log["method"] == "GET"
        assert log["path"] == "/test"
        assert log["status_code"] == 200
        assert "latency_ms" in log
        assert isinstance(log["latency_ms"], (int, float))

    async def test_logs_error_status_when_response_errors(self):
        captured_logs: list[dict] = []

        async def downstream_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 500, "headers": []})
            await send({"type": "http.response.body", "body": b"err"})

        middleware = LoggingMiddleware(downstream_app)
        with patch.object(middleware, "logger") as mock_logger:
            mock_logger.info = lambda msg, **kw: captured_logs.append({"msg": msg, **kw})

            scope = {"type": "http", "method": "POST", "path": "/webhooks/lous"}
            receive = MagicMock()
            send = AsyncMock()

            await middleware(scope, receive, send)

        assert captured_logs[0]["status_code"] == 500
        assert captured_logs[0]["method"] == "POST"
        assert captured_logs[0]["path"] == "/webhooks/lous"

    async def test_logs_500_when_no_response_start_sent(self):
        captured_logs: list[dict] = []

        async def downstream_app(scope, receive, send):
            pass

        middleware = LoggingMiddleware(downstream_app)
        with patch.object(middleware, "logger") as mock_logger:
            mock_logger.info = lambda msg, **kw: captured_logs.append({"msg": msg, **kw})

            scope = {"type": "http", "method": "GET", "path": "/silent"}
            receive = MagicMock()
            send = AsyncMock()

            await middleware(scope, receive, send)

        assert captured_logs[0]["status_code"] == 500

    async def test_init_stores_app_and_logger(self):
        app = FastAPI()
        middleware = LoggingMiddleware(app)
        assert middleware.app is app
        assert middleware.logger is not None
