from datetime import datetime, timedelta, timezone
from typing import Annotated

from faststream import Context, FastStream
from faststream.rabbit import (
    ExchangeType,
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from gex_common.config import CONSTANTS
from gex_common.logging import get_app_logger, setup_logging
from gex_common.models import DLQMessage, DistributionMessage, LeadReceivedMessage
from gex_worker.config import APP_SETTINGS
from gex_worker.consumers import process_lead
from gex_worker.db import close_db, db_session, init_db
from gex_worker.distributors import (
    SimulatedDistributorFailure,
    TransientDistributorFailure,
    process_sms,
)
from gex_worker.exception_handlers import DlqMiddleware
from gex_worker.middleware import CorrelationIdMiddleware, RetryMiddleware

logger = get_app_logger()

broker = RabbitBroker(
    APP_SETTINGS.rabbitmq_url,
    middlewares=[CorrelationIdMiddleware, RetryMiddleware, DlqMiddleware],
)
app = FastStream(broker)


@app.on_startup
async def setup() -> None:
    setup_logging(APP_SETTINGS.log_level)
    await init_db()


@app.after_startup
async def declare_topology() -> None:
    await broker.declare_exchange(RabbitExchange("lead", ExchangeType.DIRECT, durable=True))
    await broker.declare_exchange(RabbitExchange("dist", ExchangeType.DIRECT, durable=True))
    await broker.declare_exchange(RabbitExchange("dlq", ExchangeType.FANOUT, durable=True))

    await broker.declare_queue(
        RabbitQueue(
            CONSTANTS.queue_lead_received,
            durable=True,
            arguments={"x-dead-letter-exchange": "dlq"},
        )
    )
    await broker.declare_queue(RabbitQueue(CONSTANTS.queue_dlq_decrypt_failed, durable=True))
    await broker.declare_queue(RabbitQueue(CONSTANTS.queue_dlq_schema_failed, durable=True))
    await broker.declare_queue(RabbitQueue(CONSTANTS.queue_dlq_consumer_failed, durable=True))

    await broker.declare_queue(
        RabbitQueue(
            CONSTANTS.queue_dist_sms,
            durable=True,
            arguments={"x-dead-letter-exchange": "dlq"},
        )
    )
    await broker.declare_queue(RabbitQueue(CONSTANTS.queue_dist_dlq_sms, durable=True))


@app.on_shutdown
async def teardown() -> None:
    await close_db()


@broker.subscriber(
    RabbitQueue(
        CONSTANTS.queue_lead_received,
        durable=True,
        arguments={"x-dead-letter-exchange": "dlq"},
    ),
    exchange=RabbitExchange("lead", ExchangeType.DIRECT, durable=True),
)
async def handle_lead(
    msg: LeadReceivedMessage,
    correlation_id: Annotated[str, Context()],
) -> None:
    async with db_session() as session:
        await process_lead(msg, correlation_id, session, broker=broker)


@broker.subscriber(
    RabbitQueue(
        CONSTANTS.queue_dist_sms,
        durable=True,
        arguments={"x-dead-letter-exchange": "dlq"},
    ),
    exchange=RabbitExchange("dist", ExchangeType.DIRECT, durable=True),
)
async def handle_sms(
    msg: DistributionMessage,
    correlation_id: Annotated[str, Context()],
) -> None:
    async with db_session() as session:
        try:
            await process_sms(msg, correlation_id, session)
        except (TransientDistributorFailure, SimulatedDistributorFailure) as e:
            await _schedule_retry_or_dlq(msg, correlation_id, e, session)


async def _schedule_retry_or_dlq(
    msg: DistributionMessage,
    correlation_id: str,
    error: Exception,
    session: AsyncSession,
) -> None:
    attempts = await _increment_attempts(session, msg, error)

    if attempts > APP_SETTINGS.distributor_max_retries:
        await session.execute(
            text(
                "UPDATE distribution_status "
                "SET status = 'failed', updated_at = CURRENT_TIMESTAMP(6) "
                "WHERE order_id = :order_id AND channel = :channel"
            ),
            {"order_id": msg.order_id, "channel": CONSTANTS.channel_sms},
        )
        await session.commit()
        dlq_msg = DLQMessage(
            original_payload=msg.model_dump(),
            error_reason=(
                f"max_retries_exceeded ({APP_SETTINGS.distributor_max_retries}): "
                f"{type(error).__name__}: {error}"
            ),
            gateway=msg.gateway,
            correlation_id=correlation_id,
            queue_origin=CONSTANTS.queue_dist_dlq_sms,
        )
        await broker.publish(dlq_msg, CONSTANTS.queue_dist_dlq_sms, exchange="dist")
        logger.warning(
            "sms_dlq_published",
            queue=CONSTANTS.queue_dist_dlq_sms,
            order_id=msg.order_id,
            correlation_id=correlation_id,
        )
        return

    delay_ms = CONSTANTS.retry_backoffs_ms[attempts - 1]
    delay_seconds = delay_ms / 1000.0
    await session.execute(
        text(
            "UPDATE distribution_status "
            "SET next_retry_at = :next_retry_at, "
            "    updated_at = CURRENT_TIMESTAMP(6) "
            "WHERE order_id = :order_id AND channel = :channel"
        ),
        {
            "next_retry_at": datetime.now(timezone.utc) + timedelta(seconds=delay_seconds),
            "order_id": msg.order_id,
            "channel": CONSTANTS.channel_sms,
        },
    )
    await session.commit()

    await broker.publish(
        msg,
        CONSTANTS.queue_dist_sms,
        exchange="dist",
        expiration=delay_seconds,
        headers={
            "x-attempts": str(attempts),
            "x-last-error": f"{type(error).__name__}"[:200],
        },
    )
    logger.warning(
        "sms_retry_scheduled",
        order_id=msg.order_id,
        correlation_id=correlation_id,
        attempts=attempts,
        delay_seconds=delay_seconds,
        error=str(error),
    )


async def _increment_attempts(
    session: AsyncSession,
    msg: DistributionMessage,
    error: Exception,
) -> int:
    await session.execute(
        text(
            "UPDATE distribution_status "
            "SET attempts = attempts + 1, error_detail = :error, "
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

    result = await session.execute(
        text(
            "SELECT attempts FROM distribution_status "
            "WHERE order_id = :order_id AND channel = :channel"
        ),
        {"order_id": msg.order_id, "channel": CONSTANTS.channel_sms},
    )
    row = result.first()
    return int(row.attempts) if row else 1
