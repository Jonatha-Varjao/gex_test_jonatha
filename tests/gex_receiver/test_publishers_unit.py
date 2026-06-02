"""Unit tests for RabbitMQPublisher with mocked aio-pika.

These tests verify the publisher's topology declaration, message serialization,
routing keys, and error paths without requiring a real RabbitMQ broker.
"""

import json
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gex_common.config import (
    CHANNEL_SMS,
    EVENT_ORDER_APPROVED,
    GATEWAY_GRUMMER,
    GATEWAY_LOUS,
    PAYMENT_APPROVED,
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
from gex_receiver.publishers import RabbitMQPublisher, _serialize

pytestmark = pytest.mark.unit


def _make_lead_msg(correlation_id: str = "corr-1") -> LeadReceivedMessage:
    return LeadReceivedMessage(
        transaction_id="tx-001",
        transaction_time=datetime(2026, 1, 1, 12, 0, 0),
        event=EVENT_ORDER_APPROVED,
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
            status=PAYMENT_APPROVED,
        ),
        gateway=GATEWAY_LOUS,
        correlation_id=correlation_id,
    )


def _make_dlq_msg(correlation_id: str = "corr-1") -> DLQMessage:
    return DLQMessage(
        original_payload={"foo": "bar"},
        error_reason="Decryption failed",
        gateway=GATEWAY_GRUMMER,
        correlation_id=correlation_id,
        queue_origin=QUEUE_DLQ_DECRYPT_FAILED,
    )


def _make_dist_msg(correlation_id: str = "corr-1") -> DistributionMessage:
    return DistributionMessage(
        order_id="0190b6c0-7c3e-7abc-9def-123456789012",
        transaction_id="tx-001",
        channel=CHANNEL_SMS,
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
            status=PAYMENT_APPROVED,
        ),
        gateway=GATEWAY_LOUS,
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


class TestSerialize:
    def test_serializes_datetime(self):
        data = {"created_at": datetime(2026, 1, 1, 12, 0, 0)}
        result = _serialize(data)
        decoded = json.loads(result.decode("utf-8"))
        assert decoded["created_at"] == "2026-01-01T12:00:00"

    def test_raises_on_unsupported_type(self):
        with pytest.raises(TypeError, match="not JSON serializable"):
            _serialize({"value": object()})


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

    async def test_declares_three_exchanges(self):
        publisher, _conn, channel = _make_publisher_with_mocks()
        channel.declare_exchange = AsyncMock()
        channel.declare_queue = AsyncMock()

        await publisher.declare_topology()

        assert channel.declare_exchange.await_count == 3
        exchange_calls = channel.declare_exchange.await_args_list
        names = [c.args[0] for c in exchange_calls]
        assert "lead" in names
        assert "dist" in names
        assert "dlq" in names

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

        assert QUEUE_LEAD_RECEIVED in declared_queues
        assert QUEUE_DLQ_DECRYPT_FAILED in declared_queues
        assert QUEUE_DLQ_SCHEMA_FAILED in declared_queues
        assert QUEUE_DLQ_CONSUMER_FAILED in declared_queues
        assert QUEUE_DIST_SMS in declared_queues
        assert QUEUE_DIST_EMAIL in declared_queues
        assert QUEUE_DIST_CALLCENTER in declared_queues
        assert QUEUE_DIST_WHATSAPP in declared_queues
        assert QUEUE_DIST_DLQ_SMS in declared_queues


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
        assert call.kwargs["routing_key"] == QUEUE_LEAD_RECEIVED
        assert call.args[0].content_type == "application/json"
        assert call.args[0].headers == {"x-correlation-id": "corr-lead-1"}

    async def test_serializes_message_body(self):
        publisher, _conn, channel = _make_publisher_with_mocks()
        await publisher.declare_topology()
        publisher._exchange_lead.publish = AsyncMock()

        await publisher.publish_lead_received(_make_lead_msg())

        sent_msg = publisher._exchange_lead.publish.await_args.args[0]
        body = json.loads(sent_msg.body.decode("utf-8"))
        assert body["transaction_id"] == "tx-001"
        assert body["event"] == EVENT_ORDER_APPROVED
        assert body["gateway"] == GATEWAY_LOUS
        assert body["correlation_id"] == "corr-1"
        assert "2026-01-01T12:00:00" in body["transaction_time"]


class TestPublishDLQ:
    async def test_publishes_to_lead_exchange_with_supplied_routing_key(self):
        publisher, _conn, channel = _make_publisher_with_mocks()
        await publisher.declare_topology()
        publisher._exchange_lead.publish = AsyncMock()

        await publisher.publish_dlq(_make_dlq_msg("corr-dlq"), QUEUE_DLQ_SCHEMA_FAILED)

        call = publisher._exchange_lead.publish.await_args
        assert call.kwargs["routing_key"] == QUEUE_DLQ_SCHEMA_FAILED
        assert call.args[0].headers["x-correlation-id"] == "corr-dlq"
        assert call.args[0].headers["x-error-reason"] == "Decryption failed"

    async def test_truncates_error_reason_to_200_chars(self):
        publisher, _conn, channel = _make_publisher_with_mocks()
        await publisher.declare_topology()
        publisher._exchange_lead.publish = AsyncMock()

        long_reason = "x" * 500
        msg = DLQMessage(
            original_payload={},
            error_reason=long_reason,
            gateway=GATEWAY_GRUMMER,
            correlation_id="c",
            queue_origin=QUEUE_DLQ_DECRYPT_FAILED,
        )
        await publisher.publish_dlq(msg, QUEUE_DLQ_DECRYPT_FAILED)

        sent_msg = publisher._exchange_lead.publish.await_args.args[0]
        assert len(sent_msg.headers["x-error-reason"]) == 200


class TestPublishDistribution:
    async def test_publishes_to_dist_exchange_with_routing_key(self):
        publisher, _conn, channel = _make_publisher_with_mocks()
        await publisher.declare_topology()
        publisher._exchange_dist.publish = AsyncMock()

        await publisher.publish_distribution(_make_dist_msg("corr-dist"), QUEUE_DIST_EMAIL)

        call = publisher._exchange_dist.publish.await_args
        assert call.kwargs["routing_key"] == QUEUE_DIST_EMAIL
        assert call.args[0].headers == {"x-correlation-id": "corr-dist"}
        assert call.args[0].content_type == "application/json"


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
        publisher._exchange_dlq = MagicMock()

        await publisher.close()

        connection.close.assert_awaited_once()
        assert publisher._connection is None
        assert publisher._channel is None
        assert publisher._exchange_lead is None
        assert publisher._exchange_dist is None
        assert publisher._exchange_dlq is None
