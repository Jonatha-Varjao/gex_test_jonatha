import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from uuid import uuid4

import structlog
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from gex_common.logging import get_access_logger, get_app_logger, setup_logging
from gex_receiver.config import APP_SETTINGS
from gex_receiver.db import Database
from gex_receiver.health import router as health_router
from gex_receiver.publishers import RabbitMQPublisher
from gex_receiver.routes import router as webhook_router


class StructLogMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        structlog.contextvars.clear_contextvars()

        headers = {k.decode(): v.decode() for k, v in scope.get("headers", [])}
        correlation_id = headers.get("x-correlation-id") or str(uuid4())

        structlog.contextvars.bind_contextvars(
            request_id=correlation_id,
            http_method=scope["method"],
            http_path=scope["path"],
        )

        access = get_access_logger()
        start = time.perf_counter()
        status_code = 500
        response_started = False

        async def inner_send(message):
            nonlocal status_code, response_started
            if message["type"] == "http.response.start":
                status_code = message["status"]
                response_headers = list(message.get("headers", []))
                response_headers.append((b"x-correlation-id", correlation_id.encode()))
                message["headers"] = response_headers
                response_started = True
            await send(message)

        try:
            await self.app(scope, receive, inner_send)
        except Exception as e:
            get_app_logger().exception(
                "unhandled_exception",
                exception_class=e.__class__.__name__,
            )
            if not response_started:
                response = JSONResponse(
                    status_code=500,
                    content={"detail": "Internal server error"},
                )
                await response(scope, receive, send)
                status_code = 500
        finally:
            duration_ms = round((time.perf_counter() - start) * 1000, 2)
            access.info(
                "request_completed",
                status_code=status_code,
                duration_ms=duration_ms,
            )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan: startup initializes DB and RMQ, shutdown closes them."""
    setup_logging(APP_SETTINGS.log_level)

    db = Database(
        database_url=APP_SETTINGS.database_url,
        pool_size=APP_SETTINGS.database_pool_size,
        max_overflow=APP_SETTINGS.database_max_overflow,
    )
    await db.connect()
    app.state.db = db

    publisher = RabbitMQPublisher(APP_SETTINGS.rabbitmq_url)
    await publisher.connect()
    await publisher.declare_topology()
    app.state.rmq_publisher = publisher

    app.state.settings = APP_SETTINGS

    try:
        yield
    finally:
        await publisher.close()
        await db.close()


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="GEX Webhook Receiver",
        description="Receives webhooks from grummer (encrypted) and lous (plaintext) gateways",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.add_middleware(StructLogMiddleware)
    app.include_router(health_router)
    app.include_router(webhook_router)
    return app


app = create_app()
