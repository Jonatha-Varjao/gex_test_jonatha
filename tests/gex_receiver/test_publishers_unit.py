"""Unit tests for RabbitMQPublisher with mocked aio-pika.

These tests verify the publisher's topology declaration, message serialization,
routing keys, and error paths without requiring a real RabbitMQ broker.
"""

import json
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

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

pytestmark = pytest.mark.unit


def _make_lead_msg(correlation_id: str = "corr-1") -> LeadReceivedMessage:
    return LeadReceivedMessage(
        event_id="0190b6c0-7c3e-7abc-9def-123456789012",
        transaction_id="tx-001",
        transaction_time=datetime(2026, 1, 1, 12, 0, 0),
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


def _make_dlq_msg(correlation_id: str = "corr-1") -> DLQMessage:
    return DLQMessage(
        original_payload={"foo": "bar"},
        error_reason="Decryption failed",
        gateway=CONSTANTS.gateway_grummer,
        correlation_id=correlation_id,
        queue_origin=CONSTANTS.queue_dlq_decrypt_failed,
    )


def _make_dist_msg(correlation_id: str = "corr-1") -> DistributionMessage:
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


def _make_publisher_with_mocks() -> tuple[RabbitMQPublisher, MagicMock, MagicMock]:
    """Create a publisher with mocked aio_pika connection and channel."""
    publisher = RabbitMQPublisher("amqp://guest:guest@localhost/")
    mock_channel = MagicMock()
    mock_channel.declare_exchange = AsyncMock()
    mock_channel.declare_queue = AsyncMock()
    mock_connection = MagicMock()
    mock_connection.channel = AsyncMock(return_value=mock_channel)
    mock_connection.close = AsyncMock()
    publisher._connection = mock_connection
    publisher._channel = mock_channel
    return publisher, mock_connection, mock_channel


class TestConnect:
    async def test_connect_opens_connection_and_channel(self):
        publisher = RabbitMQPublisher("amqp://test:test@host:5672/")
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        with patch(
            "gex_receiver.publishers.aio_pika.connect_robust",
            AsyncMock(return_value=mock_connection),
        ):
            await publisher.connect()

        assert publisher._connection is mock_connection
        assert publisher._channel is mock_channel
        mock_connection.channel.assert_awaited_once()


class TestDeclareTopology:
    async def test_raises_when_channel_not_initialized(self):
        publisher = RabbitMQPublisher("amqp://test/")
        with pytest.raises(RuntimeError, match="connect\\(\\) must be called"):
            await publisher.declare_topology()

    async def test_declares_two_exchanges(self):
        publisher, _conn, channel = _make_publisher_with_mocks()
        channel.declare_exchange = AsyncMock()
        channel.declare_queue = AsyncMock()

        await publisher.declare_topology()

        assert channel.declare_exchange.await_count == 2
        exchange_calls = channel.declare_exchange.await_args_list
        names = [c.args[0] for c in exchange_calls]
        assert "lead" in names
        assert "dist" in names

    async def test_declares_all_required_queues(self):
        publisher, _conn, channel = _make_publisher_with_mocks()
        channel.declare_exchange = AsyncMock()
        declared_queues: list[str] = []

        def _queue_factory(name, **kwargs):
            declared_queues.append(name)
            q = MagicMock()
            q.bind = AsyncMock()
            return q

        channel.declare_queue = AsyncMock(side_effect=_queue_factory)

        await publisher.declare_topology()

        assert CONSTANTS.queue_lead_received in declared_queues
        assert CONSTANTS.queue_dlq_decrypt_failed in declared_queues
        assert CONSTANTS.queue_dlq_schema_failed in declared_queues
        assert CONSTANTS.queue_dlq_consumer_failed in declared_queues
        assert CONSTANTS.queue_dist_sms in declared_queues
        assert CONSTANTS.queue_dist_email in declared_queues
        assert CONSTANTS.queue_dist_callcenter in declared_queues
        assert CONSTANTS.queue_dist_whatsapp in declared_queues
        assert CONSTANTS.queue_dist_dlq_sms in declared_queues


class TestPublishLeadReceived:
    async def test_raises_when_topology_not_declared(self):
        publisher = RabbitMQPublisher("amqp://test/")
        with pytest.raises(RuntimeError, match="declare_topology\\(\\) must be called"):
            await publisher.publish_lead_received(_make_lead_msg())

    async def test_publishes_to_lead_exchange_with_routing_key(self):
        publisher, _conn, channel = _make_publisher_with_mocks()
        await publisher.declare_topology()
        channel.declare_exchange = AsyncMock()
        publisher._exchange_lead.publish = AsyncMock()

        msg = _make_lead_msg("corr-lead-1")
        await publisher.publish_lead_received(msg)

        publisher._exchange_lead.publish.assert_awaited_once()
        call = publisher._exchange_lead.publish.await_args
        assert call.kwargs["routing_key"] == CONSTANTS.queue_lead_received
        assert call.args[0].content_type == "application/json"
        assert call.args[0].headers == {"x-correlation-id": "corr-lead-1"}
        assert call.args[0].correlation_id == "corr-lead-1"

    async def test_serializes_message_body(self):
        publisher, _conn, channel = _make_publisher_with_mocks()
        await publisher.declare_topology()
        publisher._exchange_lead.publish = AsyncMock()

        await publisher.publish_lead_received(_make_lead_msg())

        sent_msg = publisher._exchange_lead.publish.await_args.args[0]
        body = json.loads(sent_msg.body.decode("utf-8"))
        assert body["transaction_id"] == "tx-001"
        assert body["event"] == CONSTANTS.event_order_approved
        assert body["gateway"] == CONSTANTS.gateway_lous
        assert body["correlation_id"] == "corr-1"
        assert "2026-01-01T12:00:00" in body["transaction_time"]


class TestPublishDLQ:
    async def test_publishes_to_lead_exchange_with_supplied_routing_key(self):
        publisher, _conn, channel = _make_publisher_with_mocks()
        await publisher.declare_topology()
        publisher._exchange_lead.publish = AsyncMock()

        await publisher.publish_dlq(_make_dlq_msg("corr-dlq"), CONSTANTS.queue_dlq_schema_failed)

        call = publisher._exchange_lead.publish.await_args
        assert call.kwargs["routing_key"] == CONSTANTS.queue_dlq_schema_failed
        assert call.args[0].headers["x-correlation-id"] == "corr-dlq"
        assert call.args[0].headers["x-error-reason"] == "Decryption failed"
        assert call.args[0].correlation_id == "corr-dlq"

    async def test_truncates_error_reason_to_200_chars(self):
        publisher, _conn, channel = _make_publisher_with_mocks()
        await publisher.declare_topology()
        publisher._exchange_lead.publish = AsyncMock()

        long_reason = "x" * 500
        msg = DLQMessage(
            original_payload={},
            error_reason=long_reason,
            gateway=CONSTANTS.gateway_grummer,
            correlation_id="c",
            queue_origin=CONSTANTS.queue_dlq_decrypt_failed,
        )
        await publisher.publish_dlq(msg, CONSTANTS.queue_dlq_decrypt_failed)

        sent_msg = publisher._exchange_lead.publish.await_args.args[0]
        assert len(sent_msg.headers["x-error-reason"]) == 200


class TestPublishDistribution:
    async def test_publishes_to_dist_exchange_with_routing_key(self):
        publisher, _conn, channel = _make_publisher_with_mocks()
        await publisher.declare_topology()
        publisher._exchange_dist.publish = AsyncMock()

        await publisher.publish_distribution(
            _make_dist_msg("corr-dist"), CONSTANTS.queue_dist_email
        )

        call = publisher._exchange_dist.publish.await_args
        assert call.kwargs["routing_key"] == CONSTANTS.queue_dist_email
        assert call.args[0].headers == {"x-correlation-id": "corr-dist"}
        assert call.args[0].content_type == "application/json"
        assert call.args[0].correlation_id == "corr-dist"


class TestClose:
    async def test_close_when_never_connected(self):
        publisher = RabbitMQPublisher("amqp://test/")
        await publisher.close()
        assert publisher._connection is None
        assert publisher._channel is None

    async def test_close_closes_connection_and_clears_attrs(self):
        publisher, connection, _channel = _make_publisher_with_mocks()
        publisher._exchange_lead = MagicMock()
        publisher._exchange_dist = MagicMock()

        await publisher.close()

        connection.close.assert_awaited_once()
        assert publisher._connection is None
        assert publisher._channel is None
        assert publisher._exchange_lead is None
        assert publisher._exchange_dist is None
