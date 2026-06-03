import json

import aio_pika

from gex_common.config import CONSTANTS
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
        lead_received = await self._channel.declare_queue(
            CONSTANTS.queue_lead_received,
            durable=True,
            arguments={"x-dead-letter-exchange": "dlq"},
        )
        await lead_received.bind(self._exchange_lead, routing_key=CONSTANTS.queue_lead_received)

        for queue_name in (
            CONSTANTS.queue_dlq_decrypt_failed,
            CONSTANTS.queue_dlq_schema_failed,
            CONSTANTS.queue_dlq_consumer_failed,
        ):
            q = await self._channel.declare_queue(queue_name, durable=True)
            await q.bind(self._exchange_lead, routing_key=queue_name)

        dist_sms = await self._channel.declare_queue(
            CONSTANTS.queue_dist_sms,
            durable=True,
            arguments={"x-dead-letter-exchange": "dlq"},
        )
        await dist_sms.bind(self._exchange_dist, routing_key=CONSTANTS.queue_dist_sms)

        for queue_name in (
            CONSTANTS.queue_dist_email,
            CONSTANTS.queue_dist_callcenter,
            CONSTANTS.queue_dist_whatsapp,
        ):
            q = await self._channel.declare_queue(queue_name, durable=True)
            await q.bind(self._exchange_dist, routing_key=queue_name)

        dist_dead_sms = await self._channel.declare_queue(
            CONSTANTS.queue_dist_dlq_sms, durable=True
        )
        await dist_dead_sms.bind(self._exchange_dist, routing_key=CONSTANTS.queue_dist_dlq_sms)

    async def _publish(
        self,
        exchange: aio_pika.abc.AbstractExchange,
        msg: LeadReceivedMessage | DLQMessage | DistributionMessage,
        routing_key: str,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        if exchange is None:
            raise RuntimeError("declare_topology() must be called before publishing")
        headers = {"x-correlation-id": msg.correlation_id, **(extra_headers or {})}
        await exchange.publish(
            aio_pika.Message(
                body=json.dumps(msg.model_dump(mode="json"), ensure_ascii=False).encode("utf-8"),
                content_type="application/json",
                headers=headers,
                correlation_id=msg.correlation_id,
            ),
            routing_key=routing_key,
        )

    async def publish_lead_received(self, msg: LeadReceivedMessage) -> None:
        await self._publish(self._exchange_lead, msg, CONSTANTS.queue_lead_received)

    async def publish_dlq(self, msg: DLQMessage, queue_name: str) -> None:
        await self._publish(
            self._exchange_lead,
            msg,
            queue_name,
            extra_headers={"x-error-reason": msg.error_reason[:200]},
        )

    async def publish_distribution(self, msg: DistributionMessage, queue_name: str) -> None:
        await self._publish(self._exchange_dist, msg, queue_name)

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
            self._channel = None
            self._exchange_lead = None
            self._exchange_dist = None
