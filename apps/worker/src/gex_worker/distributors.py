import random
from datetime import datetime, timezone

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from gex_common.config import CONSTANTS
from gex_common.logging import anonymize_customer_id, get_app_logger
from gex_common.models import DistributionMessage
from gex_worker.config import APP_SETTINGS
from gex_worker.middleware import bind_structlog_context

logger = get_app_logger()


class SimulatedDistributorFailure(Exception):
    """Simulated random failure for testing the retry path."""


class TransientDistributorFailure(Exception):
    """HTTP 5xx / 408 / 429 / timeout — caller should re-publish with delay."""


class PermanentDistributorFailure(Exception):
    """Non-retriable HTTP 4xx (except 408/429) — bypass retry, straight to DLQ."""

    _no_retry = True


async def process_sms(
    msg: DistributionMessage,
    correlation_id: str,
    session: AsyncSession,
) -> None:
    """POST to webhook.site once. Raises on failure; no internal retry."""
    bind_structlog_context(
        correlation_id=correlation_id,
        gateway=msg.gateway,
        event="dist.sms",
        customer_id=anonymize_customer_id(msg.customer.email),
    )

    if random.random() < APP_SETTINGS.sms_failure_rate:
        logger.warning("sms_simulated_failure", order_id=msg.order_id)
        raise SimulatedDistributorFailure("simulated sms provider error")

    url = APP_SETTINGS.webhook_site_url
    payload = msg.model_dump(mode="json")
    started_at = datetime.now(timezone.utc)

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(url, json=payload)
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        code = e.response.status_code
        if 400 <= code < 500 and code not in (408, 429):
            await _mark_failed(session, msg, e)
            raise PermanentDistributorFailure(f"{type(e).__name__}: {e}") from e
        raise TransientDistributorFailure(f"{type(e).__name__}: {e}") from e
    except (httpx.TimeoutException, httpx.ConnectError) as e:
        raise TransientDistributorFailure(f"{type(e).__name__}: {e}") from e

    delivered_at = datetime.now(timezone.utc)
    lag = (delivered_at - started_at).total_seconds()
    await session.execute(
        text(
            "UPDATE distribution_status "
            "SET status = :status, delivered_at = :delivered_at, "
            "    lag_db_to_channel_seconds = :lag, attempts = 1, "
            "    updated_at = CURRENT_TIMESTAMP(6) "
            "WHERE order_id = :order_id AND channel = :channel"
        ),
        {
            "status": CONSTANTS.dist_status_delivered,
            "delivered_at": delivered_at,
            "lag": lag,
            "order_id": msg.order_id,
            "channel": CONSTANTS.channel_sms,
        },
    )
    logger.info("sms_delivered", order_id=msg.order_id, lag_db_to_channel_seconds=lag)


async def _mark_failed(session: AsyncSession, msg: DistributionMessage, error: Exception) -> None:
    await session.execute(
        text(
            "UPDATE distribution_status "
            "SET status = 'failed', attempts = 1, error_detail = :error, "
            "    updated_at = CURRENT_TIMESTAMP(6) "
            "WHERE order_id = :order_id AND channel = :channel"
        ),
        {
            "error": f"{type(error).__name__}: {error}"[:1000],
            "order_id": msg.order_id,
            "channel": CONSTANTS.channel_sms,
        },
    )
    await session.commit()
    logger.error("sms_failed", order_id=msg.order_id, error=str(error))
