"""Integration tests for RabbitMQPublisher against a real RabbitMQ 4.2 testcontainer.

These tests use real RabbitMQ via testcontainers. Marked as @pytest.mark.integration.
Requires DOCKER_HOST to point at the Docker Desktop socket on Linux:
  export DOCKER_HOST="unix://$HOME/.docker/desktop/docker.sock"
"""

import json
from datetime import datetime

import pytest

from gex_common.config import CONSTANTS
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
        event_id="0190b6c0-7c3e-7abc-9def-123456789012",
        transaction_id="tx-001",
        transaction_time=datetime(2026, 1, 1, 12, 0, 0, tzinfo=None),
        event=CONSTANTS.event_order_approved,
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
            status=CONSTANTS.payment_approved,
        ),
        gateway=CONSTANTS.gateway_lous,
        correlation_id=correlation_id,
    )


def _make_dlq_msg(correlation_id: str = "test-corr-1") -> DLQMessage:
    return DLQMessage(
        original_payload={"foo": "bar"},
        error_reason="Decryption failed",
        gateway=CONSTANTS.gateway_grummer,
        correlation_id=correlation_id,
        queue_origin=CONSTANTS.queue_dlq_decrypt_failed,
    )


def _make_dist_msg(correlation_id: str = "test-corr-1") -> DistributionMessage:
    return DistributionMessage(
        event_id="0190b6c0-7c3e-7abc-9def-123456789012",
        order_id="0190b6c0-7c3e-7abc-9def-123456789012",
        transaction_id="tx-001",
        channel=CONSTANTS.channel_sms,
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
            status=CONSTANTS.payment_approved,
        ),
        gateway=CONSTANTS.gateway_lous,
        correlation_id=correlation_id,
    )


class TestRabbitMQPublisher:
    async def test_connect_and_declare_topology(self, rmq_publisher: RabbitMQPublisher):
        """All exchanges and queues are declared without error."""
        # If the fixture connected and declared, we get here.
        # Verify the publisher has the expected references.
        assert rmq_publisher._exchange_lead is not None
        assert rmq_publisher._exchange_dist is not None
        assert rmq_publisher._channel is not None
        assert rmq_publisher._connection is not None

    async def test_publish_lead_received(self, rmq_publisher: RabbitMQPublisher, rmq_test_channel):
        """Publishing a LeadReceivedMessage sends it to lead.received."""
        msg = _make_lead_msg("corr-publish-lead")
        await rmq_publisher.publish_lead_received(msg)

        # Read the message from the queue
        queue = await rmq_test_channel.declare_queue(
            CONSTANTS.queue_lead_received, durable=True, passive=True
        )
        received = await queue.get(timeout=5)
        assert received is not None

        body = json.loads(received.body.decode())
        assert body["transaction_id"] == "tx-001"
        assert body["gateway"] == CONSTANTS.gateway_lous
        assert body["correlation_id"] == "corr-publish-lead"
        assert received.headers["x-correlation-id"] == "corr-publish-lead"
        await received.ack()

    async def test_publish_dlq(self, rmq_publisher: RabbitMQPublisher, rmq_test_channel):
        """Publishing a DLQMessage to lead.dead.decrypt_failed works."""
        msg = _make_dlq_msg("corr-dlq")
        await rmq_publisher.publish_dlq(msg, CONSTANTS.queue_dlq_decrypt_failed)

        queue = await rmq_test_channel.declare_queue(
            CONSTANTS.queue_dlq_decrypt_failed, durable=True, passive=True
        )
        received = await queue.get(timeout=5)
        assert received is not None

        body = json.loads(received.body.decode())
        assert body["gateway"] == CONSTANTS.gateway_grummer
        assert body["error_reason"] == "Decryption failed"
        assert body["queue_origin"] == CONSTANTS.queue_dlq_decrypt_failed
        assert body["correlation_id"] == "corr-dlq"
        assert received.headers["x-correlation-id"] == "corr-dlq"
        await received.ack()

    async def test_publish_distribution(self, rmq_publisher: RabbitMQPublisher, rmq_test_channel):
        """Publishing a DistributionMessage to dist.sms works."""
        msg = _make_dist_msg("corr-dist")
        await rmq_publisher.publish_distribution(msg, CONSTANTS.queue_dist_sms)

        queue = await rmq_test_channel.declare_queue(
            CONSTANTS.queue_dist_sms, durable=True, passive=True
        )
        received = await queue.get(timeout=5)
        assert received is not None

        body = json.loads(received.body.decode())
        assert body["channel"] == CONSTANTS.channel_sms
        assert body["order_id"] == "0190b6c0-7c3e-7abc-9def-123456789012"
        assert body["correlation_id"] == "corr-dist"
        await received.ack()

    async def test_all_required_queues_exist(
        self, rmq_publisher: RabbitMQPublisher, rmq_test_channel
    ):
        """All expected queues are declared and accessible."""
        for queue_name in (
            CONSTANTS.queue_lead_received,
            CONSTANTS.queue_dlq_decrypt_failed,
            CONSTANTS.queue_dlq_schema_failed,
            CONSTANTS.queue_dlq_consumer_failed,
            CONSTANTS.queue_dist_sms,
            CONSTANTS.queue_dist_email,
            CONSTANTS.queue_dist_callcenter,
            CONSTANTS.queue_dist_whatsapp,
            CONSTANTS.queue_dist_dlq_sms,
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
