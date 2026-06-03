"""Distributor that processes messages from the ``dist.sms`` queue.

Simulates a 10 % random failure rate.  On success the corresponding row
in ``distribution_status`` is updated to ``delivered`` and the
DB→channel lag is recorded.  On final failure (after retries) the
ExceptionMiddleware routes the message to ``dist.dead.sms``.
"""

import random
from datetime import datetime, timezone

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from gex_common.config import CHANNEL_SMS, DIST_STATUS_DELIVERED
from gex_common.logging import anonymize_customer_id, get_app_logger
from gex_common.models import DistributionMessage
from gex_worker.config import APP_SETTINGS
from gex_worker.middleware import bind_structlog_context

logger = get_app_logger()


class SimulatedDistributorFailure(Exception):
    """Raised to trigger retry when the simulated failure rate is hit."""


async def process_sms(
    msg: DistributionMessage,
    correlation_id: str,
    session: AsyncSession,
) -> None:
    """POST to webhook.site, simulate 10 % failure, update distribution_status.

    Raises on any error; ``RetryMiddleware`` retries transient failures
    and ``ExceptionMiddleware`` routes the final failure to the DLQ.
    """
    bind_structlog_context(
        correlation_id=correlation_id,
        gateway=msg.gateway,
        event="dist.sms",
        customer_id=anonymize_customer_id(msg.customer.email),
    )

    # 10 % simulated failure.
    if random.random() < APP_SETTINGS.sms_failure_rate:
        logger.warning("sms_simulated_failure", order_id=msg.order_id)
        raise SimulatedDistributorFailure("simulated sms provider error")

    # POST to the configured webhook.site URL.
    url = APP_SETTINGS.webhook_site_url
    payload = msg.model_dump(mode="json")

    started_at = datetime.now(timezone.utc)
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(url, json=payload)
    response.raise_for_status()
    delivered_at = datetime.now(timezone.utc)

    # Compute DB → channel lag.
    lag_db_to_channel = (delivered_at - started_at).total_seconds()

    # Update distribution_status.
    await session.execute(
        text(
            "UPDATE distribution_status "
            "SET status = :status, delivered_at = :delivered_at, "
            "    lag_db_to_channel_seconds = :lag, "
            "    attempts = attempts + 1, "
            "    updated_at = CURRENT_TIMESTAMP(6) "
            "WHERE order_id = :order_id AND channel = :channel"
        ),
        {
            "status": DIST_STATUS_DELIVERED,
            "delivered_at": delivered_at,
            "lag": lag_db_to_channel,
            "order_id": msg.order_id,
            "channel": CHANNEL_SMS,
        },
    )

    logger.info(
        "sms_delivered",
        order_id=msg.order_id,
        webhook_url=url,
        lag_db_to_channel_seconds=lag_db_to_channel,
    )
