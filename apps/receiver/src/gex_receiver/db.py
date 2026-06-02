import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


class Database:
    """Async database engine and session factory management for the receiver."""

    def __init__(self, database_url: str, pool_size: int = 10, max_overflow: int = 20):
        self._database_url = database_url
        self._pool_size = pool_size
        self._max_overflow = max_overflow
        self._engine: AsyncEngine | None = None
        self._session_factory: async_sessionmaker[AsyncSession] | None = None

    @property
    def session_factory(self) -> async_sessionmaker[AsyncSession]:
        if self._session_factory is None:
            raise RuntimeError("Database.connect() must be called before using the session factory")
        return self._session_factory

    async def connect(self) -> None:
        self._engine = create_async_engine(
            self._database_url,
            pool_size=self._pool_size,
            max_overflow=self._max_overflow,
            pool_pre_ping=True,
        )
        self._session_factory = async_sessionmaker(
            self._engine, expire_on_commit=False, autoflush=False
        )

    async def close(self) -> None:
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None
            self._session_factory = None


async def insert_raw_payload(
    session: AsyncSession,
    gateway: str,
    received_at: datetime,
    headers: dict,
    body_original: dict,
    body_decrypted: dict | None,
    processing_status: str,
    error_detail: str | None,
    correlation_id: str,
) -> str:
    """Insert into raw_payloads table. Returns the UUIDv7 id of the inserted row."""
    payload_id = str(uuid.uuid7())
    stmt = text(
        """
        INSERT INTO raw_payloads (
            id, gateway, received_at, headers, body_original, body_decrypted,
            processing_status, error_detail, correlation_id
        ) VALUES (
            :id, :gateway, :received_at, :headers, :body_original, :body_decrypted,
            :processing_status, :error_detail, :correlation_id
        )
        """
    )
    params: dict[str, Any] = {
        "id": payload_id,
        "gateway": gateway,
        "received_at": received_at,
        "headers": _json_dump(headers),
        "body_original": _json_dump(body_original),
        "body_decrypted": _json_dump(body_decrypted) if body_decrypted is not None else None,
        "processing_status": processing_status,
        "error_detail": error_detail,
        "correlation_id": correlation_id,
    }
    await session.execute(stmt, params)
    return payload_id


async def check_idempotency(
    session: AsyncSession,
    gateway: str,
    transaction_id: str,
    event: str,
    correlation_id: str,
) -> bool:
    """Attempt INSERT into processed_events ON DUPLICATE KEY UPDATE.

    Returns True if inserted (new), False if duplicate.
    """
    event_id = str(uuid.uuid7())
    stmt = text(
        """
        INSERT INTO processed_events (
            id, gateway, transaction_id, event, correlation_id, created_at
        ) VALUES (
            :id, :gateway, :transaction_id, :event, :correlation_id, NOW(6)
        )
        ON DUPLICATE KEY UPDATE correlation_id = VALUES(correlation_id)
        """
    )
    params = {
        "id": event_id,
        "gateway": gateway,
        "transaction_id": transaction_id,
        "event": event,
        "correlation_id": correlation_id,
    }
    result = await session.execute(stmt, params)
    # MySQL ON DUPLICATE KEY UPDATE:
    #   rowcount == 1 -> new row inserted
    #   rowcount == 2 -> duplicate, updated existing
    #   rowcount == 0 -> duplicate, no values changed
    return result.rowcount == 1


def _json_dump(value: Any) -> str:
    import json

    return json.dumps(value, default=str, ensure_ascii=False)
