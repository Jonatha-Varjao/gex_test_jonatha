from sqlalchemy.ext.asyncio import AsyncSession

from gex_receiver.db import check_idempotency as _check_idempotency


async def check_idempotency(
    session: AsyncSession,
    gateway: str,
    transaction_id: str,
    event: str,
    correlation_id: str,
) -> bool:
    """Wrapper around db.check_idempotency for dependency injection.

    Returns True if the event is new (not seen before), False if it's a duplicate.
    """
    return await _check_idempotency(session, gateway, transaction_id, event, correlation_id)
