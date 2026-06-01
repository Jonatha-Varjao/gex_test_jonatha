import json
from datetime import datetime
from typing import Any

import aio_pika

from gex_common.config import (
    QUEUE_DIST_CALLCENTER,
    QUEUE_DIST_DLQ_SMS,
    QUEUE_DIST_EMAIL,
    QUEUE_DIST_SMS,
    QUEUE_DIST_WHATSAPP,
    QUEUE_DLQ_CONSUMER_FAILED,
    QUEUE_DLQ_DECRYPT_FAILED,
    QUEUE_DLQ_SCHEMA_FAILED,
    QUEUE_LEAD_RECEIVED,
)
from gex_common.models import (
    DistributionMessage,
    DLQMessage,
    LeadReceivedMessage,
)


class RabbitMQPublisher:
    """Manages aio-pika connection and publishes messages to RabbitMQ."""

    def __init__(self, url: str):
        self._url = url
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._channel: aio_pika.abc.AbstractChannel | None = None
        self._exchange_lead: aio_pika.abc.AbstractExchange | None = None
        self._exchange_dist: aio_pika.abc.AbstractExchange | None = None
        self._exchange_dlq: aio_pika.abc.AbstractExchange | None = None

    async def connect(self) -> None:
        self._connection = await aio_pika.connect_robust(self._url)
        self._channel = await self._connection.channel()

    async def declare_topology(self) -> None:
        if self._channel is None:
            raise RuntimeError("connect() must be called before declare_topology()")

        self._exchange_lead = await self._channel.declare_exchange(
            "lead", aio_pika.ExchangeType.DIRECT, durable=True
        )
        self._exchange_dist = await self._channel.declare_exchange(
            "dist", aio_pika.ExchangeType.DIRECT, durable=True
        )
        self._exchange_dlq = await self._channel.declare_exchange(
            "dlq", aio_pika.ExchangeType.FANOUT, durable=True
        )

        lead_received = await self._channel.declare_queue(
            QUEUE_LEAD_RECEIVED,
            durable=True,
            arguments={"x-dead-letter-exchange": "dlq"},
        )
        await lead_received.bind(self._exchange_lead, routing_key=QUEUE_LEAD_RECEIVED)

        for queue_name in (
            QUEUE_DLQ_DECRYPT_FAILED,
            QUEUE_DLQ_SCHEMA_FAILED,
            QUEUE_DLQ_CONSUMER_FAILED,
        ):
            q = await self._channel.declare_queue(queue_name, durable=True)
            await q.bind(self._exchange_lead, routing_key=queue_name)

        dist_sms = await self._channel.declare_queue(
            QUEUE_DIST_SMS,
            durable=True,
            arguments={"x-dead-letter-exchange": "dlq"},
        )
        await dist_sms.bind(self._exchange_dist, routing_key=QUEUE_DIST_SMS)

        for queue_name in (QUEUE_DIST_EMAIL, QUEUE_DIST_CALLCENTER, QUEUE_DIST_WHATSAPP):
            q = await self._channel.declare_queue(queue_name, durable=True)
            await q.bind(self._exchange_dist, routing_key=queue_name)

        dist_dead_sms = await self._channel.declare_queue(QUEUE_DIST_DLQ_SMS, durable=True)
        await dist_dead_sms.bind(self._exchange_dist, routing_key=QUEUE_DIST_DLQ_SMS)

    async def publish_lead_received(self, msg: LeadReceivedMessage) -> None:
        if self._exchange_lead is None:
            raise RuntimeError("declare_topology() must be called before publishing")
        body = _serialize(msg.model_dump())
        await self._exchange_lead.publish(
            aio_pika.Message(
                body=body,
                content_type="application/json",
                headers={"x-correlation-id": msg.correlation_id},
            ),
            routing_key=QUEUE_LEAD_RECEIVED,
        )

    async def publish_dlq(self, msg: DLQMessage, queue_name: str) -> None:
        if self._exchange_lead is None:
            raise RuntimeError("declare_topology() must be called before publishing")
        body = _serialize(msg.model_dump())
        await self._exchange_lead.publish(
            aio_pika.Message(
                body=body,
                content_type="application/json",
                headers={
                    "x-correlation-id": msg.correlation_id,
                    "x-error-reason": msg.error_reason[:200],
                },
            ),
            routing_key=queue_name,
        )

    async def publish_distribution(self, msg: DistributionMessage, queue_name: str) -> None:
        if self._exchange_dist is None:
            raise RuntimeError("declare_topology() must be called before publishing")
        body = _serialize(msg.model_dump())
        await self._exchange_dist.publish(
            aio_pika.Message(
                body=body,
                content_type="application/json",
                headers={"x-correlation-id": msg.correlation_id},
            ),
            routing_key=queue_name,
        )

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
            self._channel = None
            self._exchange_lead = None
            self._exchange_dist = None
            self._exchange_dlq = None


def _serialize(data: dict[str, Any]) -> bytes:
    def default(obj: Any) -> Any:
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    return json.dumps(data, default=default, ensure_ascii=False).encode("utf-8")
