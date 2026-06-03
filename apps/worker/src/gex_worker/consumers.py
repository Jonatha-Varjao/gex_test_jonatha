"""Consumer that processes messages from the ``lead.received`` queue.

The handler generates the relational IDs (lead, order, dist channels) and
calls ``sp_insert_lead``.  On a new event it publishes four distribution
messages to the ``dist.*`` queues.
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from gex_common.config import CONSTANTS
from gex_common.logging import anonymize_customer_id, get_app_logger
from gex_common.models import DistributionMessage, LeadReceivedMessage
from gex_worker.middleware import bind_structlog_context

logger = get_app_logger()


async def process_lead(
    msg: LeadReceivedMessage,
    correlation_id: str,
    session: AsyncSession,
    broker,
) -> None:
    """Persist lead via ``sp_insert_lead`` and publish 4 distribution messages.

    Raises on any error; ``RetryMiddleware`` retries transient failures
    and ``ExceptionMiddleware`` routes the final failure to the DLQ.
    """
    bind_structlog_context(
        correlation_id=correlation_id,
        gateway=msg.gateway,
        event=msg.event,
        customer_id=anonymize_customer_id(msg.customer.email),
    )

    # Generate UUIDv7 ids for all relational rows that the SP creates.
    # event_id comes from the receiver (Option A4 — end-to-end traceability).
    lead_id = str(uuid.uuid7())
    order_id = str(uuid.uuid7())
    dist_sms_id = str(uuid.uuid7())
    dist_email_id = str(uuid.uuid7())
    dist_callcenter_id = str(uuid.uuid7())
    dist_whatsapp_id = str(uuid.uuid7())

    # Lag between gateway timestamp and NOW (worker's DB write timestamp).
    now_utc = datetime.now(timezone.utc)
    lag_seconds = (now_utc - msg.transaction_time).total_seconds()

    # Call the stored procedure.
    result = await session.execute(
        text(
            "CALL sp_insert_lead("
            ":p_lead_id, :p_order_id, :p_event_id, "
            ":p_dist_sms_id, :p_dist_email_id, :p_dist_callcenter_id, "
            ":p_dist_whatsapp_id, :p_email, :p_first_name, :p_last_name, "
            ":p_phone, :p_country, :p_gateway, :p_transaction_id, "
            ":p_transaction_time, :p_event, :p_product_id, :p_product_name, "
            ":p_product_niche, :p_quantity, :p_amount_usd, :p_payment_method, "
            ":p_payment_status, :p_correlation_id, :p_lag_seconds)"
        ),
        {
            "p_lead_id": lead_id,
            "p_order_id": order_id,
            "p_event_id": msg.event_id,
            "p_dist_sms_id": dist_sms_id,
            "p_dist_email_id": dist_email_id,
            "p_dist_callcenter_id": dist_callcenter_id,
            "p_dist_whatsapp_id": dist_whatsapp_id,
            "p_email": msg.customer.email,
            "p_first_name": msg.customer.first_name,
            "p_last_name": msg.customer.last_name,
            "p_phone": msg.customer.phone,
            "p_country": msg.customer.country,
            "p_gateway": msg.gateway,
            "p_transaction_id": msg.transaction_id,
            "p_transaction_time": msg.transaction_time,
            "p_event": msg.event,
            "p_product_id": msg.product.id,
            "p_product_name": msg.product.name,
            "p_product_niche": msg.product.niche,
            "p_quantity": msg.product.quantity,
            "p_amount_usd": msg.payment.amount_usd,
            "p_payment_method": msg.payment.method,
            "p_payment_status": msg.payment.status,
            "p_correlation_id": correlation_id,
            "p_lag_seconds": lag_seconds,
        },
    )
    row = result.first()
    is_new = bool(row.is_new)

    logger.info(
        "lead_persisted",
        is_new=is_new,
        lag_seconds=lag_seconds,
        order_id=row.order_id,
    )

    # Publish to the 4 distribution queues if this is a new event.
    if is_new:
        dist_publishes = [
            (CONSTANTS.queue_dist_sms, "SMS"),
            (CONSTANTS.queue_dist_email, "EMAIL"),
            (CONSTANTS.queue_dist_callcenter, "CALL_CENTER"),
            (CONSTANTS.queue_dist_whatsapp, "WHATSAPP"),
        ]
        for queue_name, channel_name in dist_publishes:
            dist_msg = DistributionMessage(
                event_id=msg.event_id,
                order_id=row.order_id,
                transaction_id=msg.transaction_id,
                channel=channel_name,
                customer=msg.customer,
                product=msg.product,
                payment=msg.payment,
                gateway=msg.gateway,
                correlation_id=correlation_id,
            )
            await broker.publish(dist_msg, queue_name, exchange="dist")
        logger.info("dist_messages_published", count=4)
