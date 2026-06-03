"""DLQ message construction for the worker's ExceptionMiddleware.

When RetryMiddleware exhausts its retries, the ExceptionMiddleware
handler calls ``publish_to_dlq`` which builds a ``DLQMessage`` with
the error reason and publishes it to the appropriate dead-letter queue.
"""

from gex_common.models import DLQMessage


def get_dlq_correlation_id() -> str:
    try:
        from faststream import Context

        return Context().get("correlation_id", "no-correlation-id")
    except Exception:
        return "no-correlation-id"


def _queue_origin_for(source_queue: str) -> str:
    mapping = {
        "lead.received": "lead.dead.consumer_failed",
        "dist.sms": "dist.dead.sms",
    }
    return mapping.get(source_queue, source_queue)


def build_dlq_message(msg, error: Exception, source_queue: str) -> DLQMessage:
    """Construct a ``DLQMessage`` from the original message and exception."""
    if hasattr(msg, "model_dump"):
        original_payload = msg.model_dump(mode="json")
    else:
        original_payload = {"raw": str(msg)}
    return DLQMessage(
        original_payload=original_payload,
        error_reason=f"{error.__class__.__name__}: {error}",
        gateway=getattr(msg, "gateway", "unknown"),
        correlation_id=get_dlq_correlation_id(),
        queue_origin=_queue_origin_for(source_queue),
    )
