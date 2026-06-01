"""Unit tests for the webhook endpoint.

These tests mock the DB and publisher dependencies. The route logic itself
is exercised end-to-end (validation, decrypt, schema, idempotency, routing).
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from gex_common.config import (
    GATEWAY_GRUMMER,
    GATEWAY_LOUS,
    QUEUE_DLQ_SCHEMA_FAILED,
)
from gex_common.models import (
    CustomerData,
    PaymentData,
    ProductData,
    WebhookPayload,
)
from gex_receiver.publishers import RabbitMQPublisher
from gex_receiver.routes import router

pytestmark = pytest.mark.unit


def _valid_lous_payload() -> dict:
    return {
        "transaction_id": "tx-valid-001",
        "transaction_time": "2026-01-15T10:30:00+00:00",
        "event": "order.approved",
        "customer": {
            "email": "test@example.com",
            "first_name": "Test",
            "last_name": "User",
            "phone": "+18005551234",
            "country": "US",
        },
        "product": {
            "id": "prod-1",
            "name": "Fit Burn",
            "niche": "weight_loss",
            "quantity": 1,
        },
        "payment": {
            "amount_usd": 99.99,
            "method": "credit_card",
            "status": "approved",
        },
    }


def _build_mock_payload() -> WebhookPayload:
    return WebhookPayload(
        transaction_id="tx-valid-001",
        transaction_time=datetime(2026, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
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
        gateway=GATEWAY_LOUS,
        correlation_id="test-corr",
    )


@pytest.fixture
def mock_session() -> AsyncMock:
    """Returns an AsyncMock for AsyncSession."""
    return AsyncMock(spec=AsyncSession)


@pytest.fixture
def mock_publisher() -> AsyncMock:
    """Returns an AsyncMock for RabbitMQPublisher."""
    publisher = AsyncMock(spec=RabbitMQPublisher)
    return publisher


@pytest.fixture
def mock_settings() -> MagicMock:
    """Returns a mock AppSettings with grummer_secret_hex."""
    settings = MagicMock()
    settings.grummer_secret_hex = "bd8fe643a6e1b725db136104efebe450243ed7ef6c6a755ab9b2315fb648f153"
    return settings


@pytest.fixture
def api_client(mock_session, mock_publisher, mock_settings):
    """Create a FastAPI test client with mocked dependencies."""
    from gex_receiver.dependencies import (
        get_db_session,
        get_publisher,
        get_settings,
    )

    app = FastAPI()
    app.include_router(router)

    async def override_session():
        yield mock_session

    app.dependency_overrides[get_db_session] = override_session
    app.dependency_overrides[get_publisher] = lambda: mock_publisher
    app.dependency_overrides[get_settings] = lambda: mock_settings

    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


class TestInvalidGateway:
    async def test_unknown_gateway_returns_422(self, api_client, mock_session, mock_publisher):
        async with api_client as client:
            response = await client.post("/webhooks/unknown", json={})
        assert response.status_code == 422
        assert "Invalid gateway" in response.text
        mock_session.execute.assert_not_called()
        mock_publisher.publish_lead_received.assert_not_called()
        mock_publisher.publish_dlq.assert_not_called()


class TestLousValidPayloads:
    async def test_valid_approved_returns_200_accepted(
        self, api_client, mock_session, mock_publisher
    ):
        # Patch check_idempotency at the module level for the route
        import gex_receiver.routes as routes_module

        original = routes_module.check_idempotency
        routes_module.check_idempotency = AsyncMock(return_value=True)
        try:
            async with api_client as client:
                response = await client.post(
                    f"/webhooks/{GATEWAY_LOUS}", json=_valid_lous_payload()
                )
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "accepted"
            assert "correlation_id" in data
            mock_publisher.publish_lead_received.assert_awaited_once()
            mock_publisher.publish_dlq.assert_not_called()
        finally:
            routes_module.check_idempotency = original

    async def test_non_approved_returns_200_discarded(
        self, api_client, mock_session, mock_publisher
    ):
        import gex_receiver.routes as routes_module

        original = routes_module.check_idempotency
        routes_module.check_idempotency = AsyncMock(return_value=True)
        try:
            payload = _valid_lous_payload()
            payload["event"] = "order.refunded"
            payload["payment"]["status"] = "refunded"
            async with api_client as client:
                response = await client.post(f"/webhooks/{GATEWAY_LOUS}", json=payload)
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "discarded"
            mock_publisher.publish_lead_received.assert_not_called()
            mock_publisher.publish_dlq.assert_not_called()
        finally:
            routes_module.check_idempotency = original

    async def test_duplicate_returns_200_duplicate(self, api_client, mock_session, mock_publisher):
        import gex_receiver.routes as routes_module

        original = routes_module.check_idempotency
        routes_module.check_idempotency = AsyncMock(return_value=False)
        try:
            async with api_client as client:
                response = await client.post(
                    f"/webhooks/{GATEWAY_LOUS}", json=_valid_lous_payload()
                )
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "duplicate"
            mock_publisher.publish_lead_received.assert_not_called()
        finally:
            routes_module.check_idempotency = original

    async def test_invalid_schema_returns_202(self, api_client, mock_session, mock_publisher):
        bad_payload = {
            "transaction_id": "tx-001",
            "event": "order.approved",
            # Missing transaction_time, customer, product, payment
        }
        async with api_client as client:
            response = await client.post(f"/webhooks/{GATEWAY_LOUS}", json=bad_payload)
        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "schema_failed"
        assert "error_detail" in data
        mock_publisher.publish_dlq.assert_awaited_once()
        call_args = mock_publisher.publish_dlq.call_args
        assert call_args.args[1] == QUEUE_DLQ_SCHEMA_FAILED


class TestGrummerPayloads:
    async def test_grummer_encrypted_valid_accepted(
        self, api_client, mock_session, mock_publisher, grummer_encrypted_payloads
    ):
        import gex_receiver.routes as routes_module

        original = routes_module.check_idempotency
        routes_module.check_idempotency = AsyncMock(return_value=True)
        try:
            grummer_item = grummer_encrypted_payloads[0]
            async with api_client as client:
                response = await client.post(
                    f"/webhooks/{GATEWAY_GRUMMER}",
                    json=grummer_item["body"],
                    headers={"X-GR-Encrypted": grummer_item["headers"]["X-GR-Encrypted"]},
                )
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "accepted"
            mock_publisher.publish_lead_received.assert_awaited_once()
        finally:
            routes_module.check_idempotency = original

    async def test_grummer_decrypt_fails_returns_202(
        self, api_client, mock_session, mock_publisher, mock_settings
    ):
        # Use a known-bad ciphertext that will fail decrypt
        bad_body = {"iv": "AAAAAAAAAAAAAAAAAAAAAA==", "ciphertext": "Z2FyYmFnZQ=="}
        async with api_client as client:
            response = await client.post(
                f"/webhooks/{GATEWAY_GRUMMER}",
                json=bad_body,
                headers={"X-GR-Encrypted": "true"},
            )
        # Bad ciphertext may either fail decrypt (202) or fail schema (202)
        assert response.status_code == 202
        data = response.json()
        assert data["status"] in ("decrypt_failed", "schema_failed")

    async def test_grummer_not_encrypted_treated_as_plaintext(
        self, api_client, mock_session, mock_publisher
    ):
        import gex_receiver.routes as routes_module

        original = routes_module.check_idempotency
        routes_module.check_idempotency = AsyncMock(return_value=True)
        try:
            async with api_client as client:
                response = await client.post(
                    f"/webhooks/{GATEWAY_GRUMMER}",
                    json=_valid_lous_payload(),
                    # No X-GR-Encrypted header → treated as plaintext
                )
            # Treated as plaintext, validated as a lous-style payload
            # Should pass schema and be accepted
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "accepted"
        finally:
            routes_module.check_idempotency = original


class TestCorrelationId:
    async def test_correlation_id_in_response(self, api_client, mock_session, mock_publisher):
        import gex_receiver.routes as routes_module

        original = routes_module.check_idempotency
        routes_module.check_idempotency = AsyncMock(return_value=True)
        try:
            async with api_client as client:
                response = await client.post(
                    f"/webhooks/{GATEWAY_LOUS}",
                    json=_valid_lous_payload(),
                    headers={"X-Correlation-ID": "my-custom-id-123"},
                )
            assert response.status_code == 200
            data = response.json()
            assert data["correlation_id"] == "my-custom-id-123"
        finally:
            routes_module.check_idempotency = original

    async def test_correlation_id_generated_if_missing(
        self, api_client, mock_session, mock_publisher
    ):
        import gex_receiver.routes as routes_module

        original = routes_module.check_idempotency
        routes_module.check_idempotency = AsyncMock(return_value=True)
        try:
            async with api_client as client:
                response = await client.post(
                    f"/webhooks/{GATEWAY_LOUS}",
                    json=_valid_lous_payload(),
                )
            assert response.status_code == 200
            data = response.json()
            assert "correlation_id" in data
            assert len(data["correlation_id"]) > 0
        finally:
            routes_module.check_idempotency = original


class TestServiceUnavailable:
    async def test_publisher_failure_returns_503(self, api_client, mock_session, mock_publisher):
        import gex_receiver.routes as routes_module

        original = routes_module.check_idempotency
        routes_module.check_idempotency = AsyncMock(return_value=True)
        # Make the publisher raise an exception
        mock_publisher.publish_lead_received.side_effect = Exception("RMQ down")
        try:
            async with api_client as client:
                response = await client.post(
                    f"/webhooks/{GATEWAY_LOUS}",
                    json=_valid_lous_payload(),
                )
            assert response.status_code == 503
        finally:
            routes_module.check_idempotency = original


class TestInvalidJsonBody:
    async def test_malformed_json_returns_400(self, api_client, mock_session, mock_publisher):
        async with api_client as client:
            response = await client.post(
                f"/webhooks/{GATEWAY_LOUS}",
                content=b"not-valid-json{",
                headers={"Content-Type": "application/json"},
            )
        assert response.status_code == 400
        assert "Invalid JSON" in response.text
        mock_session.execute.assert_not_called()
        mock_publisher.publish_lead_received.assert_not_called()


class TestValidationDefensivePath:
    async def test_payload_none_after_valid_returns_503(
        self, api_client, mock_session, mock_publisher
    ):
        import gex_receiver.routes as routes_module
        from gex_common.validation import ValidationResult

        original_idempotency = routes_module.check_idempotency
        original_validate = routes_module.validate_schema
        routes_module.check_idempotency = AsyncMock(return_value=True)
        routes_module.validate_schema = MagicMock(
            return_value=ValidationResult(is_valid=True, payload=None, errors=[])
        )
        try:
            async with api_client as client:
                response = await client.post(
                    f"/webhooks/{GATEWAY_LOUS}",
                    json=_valid_lous_payload(),
                )
            assert response.status_code == 503
        finally:
            routes_module.check_idempotency = original_idempotency
            routes_module.validate_schema = original_validate
