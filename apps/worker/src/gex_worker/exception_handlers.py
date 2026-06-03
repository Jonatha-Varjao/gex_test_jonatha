"""Custom ``BaseMiddleware`` that routes final-failure messages to the DLQ.

Runs in ``after_processed`` (called from ``__aexit__`` after all middleware
``consume_scope`` methods have completed).  Returns ``False`` so the exception
continues propagating — outer middlewares (e.g. ``AcknowledgementMiddleware``)
still see it and can ack / nack the message appropriately.

At this point ``self.context.get_local("message")`` returns the raw
``RabbitMessage`` (not the decoded Pydantic model), so we identify the
source queue via the wire-format ``routing_key`` and build the DLQ payload
from the raw body bytes.
"""

import json

from faststream._internal.middlewares import BaseMiddleware

from gex_common.config import QUEUE_DIST_SMS, QUEUE_LEAD_RECEIVED
from gex_common.logging import get_app_logger
from gex_common.models import DLQMessage
from gex_worker.dlq import get_dlq_correlation_id

logger = get_app_logger()

_DLQ_ROUTES: dict[str, tuple[str, str]] = {
    QUEUE_LEAD_RECEIVED: ("lead.dead.consumer_failed", "lead"),
    QUEUE_DIST_SMS: ("dist.dead.sms", "dist"),
}


def _routing_key(msg: object) -> str | None:
    """Extract routing key from a ``RabbitMessage``."""
    raw = getattr(msg, "raw_message", None)
    if raw is not None:
        return getattr(raw, "routing_key", None)
    return None


def _body_dict(msg: object) -> dict:
    """Extract the JSON body from a ``RabbitMessage``."""
    body = getattr(msg, "body", b"{}")
    try:
        return json.loads(body)
    except json.JSONDecodeError, TypeError:
        return {}


class DlqMiddleware(BaseMiddleware):
    """Middleware that publishes failed-message details to the DLQ.

    The exception has already been retried by the inner ``RetryMiddleware``
    before it reaches this handler; the final exception is what we publish.
    """

    async def after_processed(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: object | None = None,
    ) -> bool | None:
        if exc_type is None or exc_val is None:
            return None

        msg = self.context.get_local("message")
        if msg is None:
            logger.exception("dlq_no_message", error=str(exc_val))
            return False

        broker = self.context.get("broker")
        if broker is None:
            logger.exception("dlq_no_broker", error=str(exc_val))
            return False

        rk = _routing_key(msg)
        route = _DLQ_ROUTES.get(rk)
        if route is None:
            logger.exception(
                "unhandled_exception_no_dlq_route",
                error=str(exc_val),
                routing_key=rk,
            )
            return False

        dlq_queue, exchange = route

        payload = _body_dict(msg)
        gateway = payload.get("gateway", "unknown")
        dlq_msg = DLQMessage(
            original_payload=payload,
            error_reason=f"{exc_val.__class__.__name__}: {exc_val}",
            gateway=gateway,
            correlation_id=get_dlq_correlation_id(),
            queue_origin=dlq_queue,
        )

        await broker.publish(dlq_msg, dlq_queue, exchange=exchange)
        logger.error("dlq_published", queue=dlq_queue, error=str(exc_val), gateway=gateway)

        return False  # Do not suppress — outer middlewares still need the exception
