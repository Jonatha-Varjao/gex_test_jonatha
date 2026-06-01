"""Integration tests for RabbitMQPublisher.

These tests use real RabbitMQ via testcontainers. Marked as @pytest.mark.integration
and excluded from default unit test runs. Set DOCKER_HOST to point at your docker
socket (e.g. unix://$HOME/.docker/run/docker.sock for rootless).
"""

import json
from datetime import datetime

import pytest

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
    CustomerData,
    DistributionMessage,
    DLQMessage,
    LeadReceivedMessage,
    PaymentData,
    ProductData,
)
from gex_receiver.publishers import RabbitMQPublisher

pytestmark = pytest.mark.integration


def _make_lead_msg(correlation_id: str = "test-corr-1") -> LeadReceivedMessage:
    return LeadReceivedMessage(
        transaction_id="tx-001",
        transaction_time=datetime(2026, 1, 1, 12, 0, 0, tzinfo=None),
        event="order.approved",
        customer=CustomerData(
            email="test@example.com",
            first_name="Test",
            country="US",
        ),
        product=ProductData(
            id="prod-1",
            name="Fit Burn",
            niche="weight_loss",
            quantity=1,
        ),
        payment=PaymentData(
            amount_usd=99.99,
            method="credit_card",
            status="approved",
        ),
        gateway="lous",
        correlation_id=correlation_id,
    )


def _make_dlq_msg(correlation_id: str = "test-corr-1") -> DLQMessage:
    return DLQMessage(
        original_payload={"foo": "bar"},
        error_reason="Decryption failed",
        gateway="grummer",
        correlation_id=correlation_id,
        queue_origin=QUEUE_DLQ_DECRYPT_FAILED,
    )


def _make_dist_msg(correlation_id: str = "test-corr-1") -> DistributionMessage:
    return DistributionMessage(
        order_id=42,
        transaction_id="tx-001",
        channel="SMS",
        customer=CustomerData(
            email="test@example.com",
            first_name="Test",
            country="US",
        ),
        product=ProductData(
            id="prod-1",
            name="Fit Burn",
            niche="weight_loss",
            quantity=1,
        ),
        payment=PaymentData(
            amount_usd=99.99,
            method="credit_card",
            status="approved",
        ),
        gateway="lous",
        correlation_id=correlation_id,
    )


class TestRabbitMQPublisher:
    async def test_connect_and_declare_topology(self, rmq_publisher: RabbitMQPublisher):
        """All exchanges and queues are declared without error."""
        # If the fixture connected and declared, we get here.
        # Verify the publisher has the expected references.
        assert rmq_publisher._exchange_lead is not None
        assert rmq_publisher._exchange_dist is not None
        assert rmq_publisher._exchange_dlq is not None
        assert rmq_publisher._channel is not None
        assert rmq_publisher._connection is not None

    async def test_publish_lead_received(self, rmq_publisher: RabbitMQPublisher, rmq_test_channel):
        """Publishing a LeadReceivedMessage sends it to lead.received."""
        msg = _make_lead_msg("corr-publish-lead")
        await rmq_publisher.publish_lead_received(msg)

        # Read the message from the queue
        queue = await rmq_test_channel.declare_queue(
            QUEUE_LEAD_RECEIVED, durable=True, passive=True
        )
        received = await queue.get(timeout=5)
        assert received is not None

        body = json.loads(received.body.decode())
        assert body["transaction_id"] == "tx-001"
        assert body["gateway"] == "lous"
        assert body["correlation_id"] == "corr-publish-lead"
        assert received.headers["x-correlation-id"] == "corr-publish-lead"
        await received.ack()

    async def test_publish_dlq(self, rmq_publisher: RabbitMQPublisher, rmq_test_channel):
        """Publishing a DLQMessage to lead.dead.decrypt_failed works."""
        msg = _make_dlq_msg("corr-dlq")
        await rmq_publisher.publish_dlq(msg, QUEUE_DLQ_DECRYPT_FAILED)

        queue = await rmq_test_channel.declare_queue(
            QUEUE_DLQ_DECRYPT_FAILED, durable=True, passive=True
        )
        received = await queue.get(timeout=5)
        assert received is not None

        body = json.loads(received.body.decode())
        assert body["gateway"] == "grummer"
        assert body["error_reason"] == "Decryption failed"
        assert body["queue_origin"] == QUEUE_DLQ_DECRYPT_FAILED
        assert body["correlation_id"] == "corr-dlq"
        assert received.headers["x-correlation-id"] == "corr-dlq"
        await received.ack()

    async def test_publish_distribution(self, rmq_publisher: RabbitMQPublisher, rmq_test_channel):
        """Publishing a DistributionMessage to dist.sms works."""
        msg = _make_dist_msg("corr-dist")
        await rmq_publisher.publish_distribution(msg, QUEUE_DIST_SMS)

        queue = await rmq_test_channel.declare_queue(QUEUE_DIST_SMS, durable=True, passive=True)
        received = await queue.get(timeout=5)
        assert received is not None

        body = json.loads(received.body.decode())
        assert body["channel"] == "SMS"
        assert body["order_id"] == 42
        assert body["correlation_id"] == "corr-dist"
        await received.ack()

    async def test_all_required_queues_exist(
        self, rmq_publisher: RabbitMQPublisher, rmq_test_channel
    ):
        """All expected queues are declared and accessible."""
        for queue_name in (
            QUEUE_LEAD_RECEIVED,
            QUEUE_DLQ_DECRYPT_FAILED,
            QUEUE_DLQ_SCHEMA_FAILED,
            QUEUE_DLQ_CONSUMER_FAILED,
            QUEUE_DIST_SMS,
            QUEUE_DIST_EMAIL,
            QUEUE_DIST_CALLCENTER,
            QUEUE_DIST_WHATSAPP,
            QUEUE_DIST_DLQ_SMS,
        ):
            # passive=True fails if queue doesn't exist
            q = await rmq_test_channel.declare_queue(queue_name, durable=True, passive=True)
            assert q is not None
            assert q.name == queue_name

    async def test_close_closes_connection(self, rmq_publisher: RabbitMQPublisher):
        """close() clears all internal references."""
        await rmq_publisher.close()
        assert rmq_publisher._connection is None
        assert rmq_publisher._channel is None
        assert rmq_publisher._exchange_lead is None
        assert rmq_publisher._exchange_dist is None
        assert rmq_publisher._exchange_dlq is None
