from collections.abc import AsyncGenerator
from typing import Annotated
from uuid import uuid4

import structlog
from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from gex_common.config import AppSettings
from gex_receiver.db import Database
from gex_receiver.publishers import RabbitMQPublisher


async def get_db_session(request: Request) -> AsyncGenerator[AsyncSession, None]:
    """Yields an AsyncSession tied to the app-lifetime Database instance."""
    db: Database = request.app.state.db
    async with db.session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


def get_publisher(request: Request) -> RabbitMQPublisher:
    return request.app.state.rmq_publisher


def get_settings(request: Request) -> AppSettings:
    return request.app.state.settings


def get_correlation_id(request: Request) -> str:
    return structlog.contextvars.get_contextvars().get("correlation_id", str(uuid4()))


DbDep = Annotated[AsyncSession, Depends(get_db_session)]
PublisherDep = Annotated[RabbitMQPublisher, Depends(get_publisher)]
SettingsDep = Annotated[AppSettings, Depends(get_settings)]
CorrelationId = Annotated[str, Depends(get_correlation_id)]
