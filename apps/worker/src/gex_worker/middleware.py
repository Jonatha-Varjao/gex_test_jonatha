"""FastStream middlewares for the worker: correlation-id propagation,
retry-with-backoff, and structlog context binding.
"""

import asyncio
from uuid import uuid4

import structlog
from faststream import BaseMiddleware
from faststream.message import StreamMessage
from faststream.types import AsyncFuncAny

from gex_common.config import CONSTANTS
from gex_common.logging import get_app_logger

logger = get_app_logger()


class CorrelationIdMiddleware(BaseMiddleware):
    """Extracts x-correlation-id from incoming AMQP message headers and
    places it in FastStream's local Context so subscribers can read it
    via ``correlation_id: Annotated[str, Context()]``.

    Generates a new UUID4 if the incoming message has no correlation_id.
    """

    async def consume_scope(self, call_next: AsyncFuncAny, msg: StreamMessage) -> None:
        correlation_id = msg.correlation_id
        if not correlation_id:
            correlation_id = str(uuid4())
            logger.warning("correlation_id_missing_generated", msg_id=msg.message_id)
        with self.context.scope("correlation_id", correlation_id):
            return await super().consume_scope(call_next, msg)


class RetryMiddleware(BaseMiddleware):
    """Retries failed handler invocations with exponential backoff.

    Backoff schedule:  CONSTANTS.retry_backoffs_ms = [1000, 4000, 16000]  (1s, 4s, 16s).
    After all retries are exhausted the original exception is re-raised
    so the ExceptionMiddleware can route the message to the appropriate DLQ.

    Exceptions with ``_no_retry = True`` bypass retry and propagate
    directly to ``DlqMiddleware``.
    """

    async def consume_scope(self, call_next: AsyncFuncAny, msg: StreamMessage) -> None:
        for attempt in range(len(CONSTANTS.retry_backoffs_ms) + 1):
            try:
                return await call_next(msg)
            except Exception as e:
                if getattr(e, "_no_retry", False):
                    raise
                if attempt == len(CONSTANTS.retry_backoffs_ms):
                    logger.exception("failed_after_retries", attempts=attempt + 1)
                    raise
                delay_ms = CONSTANTS.retry_backoffs_ms[attempt]
                logger.warning(
                    "retrying_after_failure",
                    attempt=attempt + 1,
                    delay_ms=delay_ms,
                )
                await asyncio.sleep(delay_ms / 1000.0)
        return None


def bind_structlog_context(
    *, correlation_id: str, gateway: str, event: str, customer_id: str
) -> None:
    """Bind per-message context to structlog contextvars for log tracing."""
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(
        correlation_id=correlation_id,
        gateway=gateway,
        event=event,
        customer_id=customer_id,
    )
