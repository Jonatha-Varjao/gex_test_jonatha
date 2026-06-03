"""FastStream application entry point for the GEX worker.

Run with::

    uv run --package gex-worker faststream run gex_worker.main:app

Middleware stack (order matters):
  1. ``CorrelationIdMiddleware`` – extract / generate ``correlation_id``
  2. ``RetryMiddleware`` – exponential backoff (1s, 4s, 16s)
  3. ``ExceptionMiddleware`` – final-failure → DLQ with error reason
"""

from typing import Annotated

from faststream import Context, FastStream
from faststream.rabbit import (
    ExchangeType,
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
)

from gex_common.config import (
    QUEUE_DIST_DLQ_SMS,
    QUEUE_DIST_SMS,
    QUEUE_DLQ_CONSUMER_FAILED,
    QUEUE_DLQ_DECRYPT_FAILED,
    QUEUE_DLQ_SCHEMA_FAILED,
    QUEUE_LEAD_RECEIVED,
)
from gex_common.logging import setup_logging
from gex_common.models import DistributionMessage, LeadReceivedMessage
from gex_worker.config import APP_SETTINGS
from gex_worker.consumers import process_lead
from gex_worker.db import close_db, db_session, init_db
from gex_worker.distributors import process_sms
from gex_worker.exception_handlers import DlqMiddleware
from gex_worker.middleware import CorrelationIdMiddleware, RetryMiddleware

broker = RabbitBroker(
    APP_SETTINGS.rabbitmq_url,
    middlewares=[CorrelationIdMiddleware, RetryMiddleware, DlqMiddleware],
)
app = FastStream(broker)


@app.on_startup
async def setup() -> None:
    """Initialize DB and logging (runs BEFORE broker.connect())."""
    setup_logging(APP_SETTINGS.log_level)
    await init_db()


@app.after_startup
async def declare_topology() -> None:
    """Declare exchanges and queues (runs AFTER broker.connect())."""
    # Exchanges (idempotent — receiver also declares them).
    await broker.declare_exchange(RabbitExchange("lead", ExchangeType.DIRECT, durable=True))
    await broker.declare_exchange(RabbitExchange("dist", ExchangeType.DIRECT, durable=True))
    await broker.declare_exchange(RabbitExchange("dlq", ExchangeType.FANOUT, durable=True))

    # Lead consumer queues.
    await broker.declare_queue(
        RabbitQueue(
            QUEUE_LEAD_RECEIVED,
            durable=True,
            arguments={"x-dead-letter-exchange": "dlq"},
        )
    )
    await broker.declare_queue(RabbitQueue(QUEUE_DLQ_DECRYPT_FAILED, durable=True))
    await broker.declare_queue(RabbitQueue(QUEUE_DLQ_SCHEMA_FAILED, durable=True))
    await broker.declare_queue(RabbitQueue(QUEUE_DLQ_CONSUMER_FAILED, durable=True))

    # SMS distributor queues.
    await broker.declare_queue(
        RabbitQueue(
            QUEUE_DIST_SMS,
            durable=True,
            arguments={"x-dead-letter-exchange": "dlq"},
        )
    )
    await broker.declare_queue(RabbitQueue(QUEUE_DIST_DLQ_SMS, durable=True))


@app.on_shutdown
async def teardown() -> None:
    """Tear down DB connections."""
    await close_db()


@broker.subscriber(
    RabbitQueue(QUEUE_LEAD_RECEIVED, durable=True, arguments={"x-dead-letter-exchange": "dlq"}),
    exchange=RabbitExchange("lead", ExchangeType.DIRECT, durable=True),
)
async def handle_lead(
    msg: LeadReceivedMessage,
    correlation_id: Annotated[str, Context()],
) -> None:
    """Consume from ``lead.received`` and persist the lead."""
    async with db_session() as session:
        await process_lead(msg, correlation_id, session, broker=broker)


@broker.subscriber(
    RabbitQueue(QUEUE_DIST_SMS, durable=True, arguments={"x-dead-letter-exchange": "dlq"}),
    exchange=RabbitExchange("dist", ExchangeType.DIRECT, durable=True),
)
async def handle_sms(
    msg: DistributionMessage,
    correlation_id: Annotated[str, Context()],
) -> None:
    """Consume from ``dist.sms`` and POST to the SMS provider."""
    async with db_session() as session:
        await process_sms(msg, correlation_id, session)
