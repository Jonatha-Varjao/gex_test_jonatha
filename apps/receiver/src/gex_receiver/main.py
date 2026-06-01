import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from gex_common.logging import setup_logging
from gex_receiver.config import APP_SETTINGS
from gex_receiver.db import Database
from gex_receiver.publishers import RabbitMQPublisher
from gex_receiver.routes import router


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


class LoggingMiddleware:
    """Middleware that logs each request with method, path, status, latency, and correlation_id."""

    def __init__(self, app):
        self.app = app
        self.logger = structlog.get_logger()

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        start_time = time.time()
        status_code = 500

        async def send_wrapper(message):
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)

        await self.app(scope, receive, send_wrapper)

        latency_ms = round((time.time() - start_time) * 1000, 2)
        self.logger.info(
            "request_completed",
            method=scope["method"],
            path=scope["path"],
            status_code=status_code,
            latency_ms=latency_ms,
        )


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="GEX Webhook Receiver",
        description="Receives webhooks from grummer (encrypted) and lous (plaintext) gateways",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.add_middleware(LoggingMiddleware)
    app.include_router(router)
    return app


app = create_app()
