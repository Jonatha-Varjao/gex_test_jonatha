from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from gex_common.models import (
    CustomerData,
    DLQMessage,
    LeadReceivedMessage,
    PaymentData,
    ProcessingResult,
    ProductData,
    WebhookPayload,
)


class TestCustomerData:
    def test_valid_customer(self):
        c = CustomerData(email="test@example.com", first_name="John", country="US")
        assert c.email == "test@example.com"
        assert c.first_name == "John"
        assert c.last_name is None
        assert c.phone is None

    def test_country_lowercases_are_uppercased(self):
        c = CustomerData(email="test@example.com", country="us")
        assert c.country == "US"

    def test_email_str_rejects_invalid(self):
        with pytest.raises(ValidationError):
            CustomerData(email="not-an-email", country="US")


class TestProductData:
    @pytest.mark.parametrize("quantity", [0, -1])
    def test_quantity_must_be_positive(self, quantity):
        with pytest.raises(ValidationError):
            ProductData(id="P1", name="Widget", niche="tech", quantity=quantity)


class TestPaymentData:
    @pytest.mark.parametrize("status", ["approved", "declined", "pending", "refunded"])
    def test_valid_statuses(self, status):
        p = PaymentData(amount_usd=10.0, method="cc", status=status)
        assert p.status == status

    def test_invalid_status_rejected(self):
        with pytest.raises(ValidationError):
            PaymentData(amount_usd=10.0, method="cc", status="unknown_status")

    def test_amount_zero_allowed(self):
        p = PaymentData(amount_usd=0.0, method="cc", status="approved")
        assert p.amount_usd == 0.0

    def test_negative_amount_rejected(self):
        with pytest.raises(ValidationError):
            PaymentData(amount_usd=-1.0, method="cc", status="approved")


class TestWebhookPayload:
    def test_full_valid_payload(self):
        cd = CustomerData(email="test@example.com", first_name="John", country="US")
        pd = ProductData(id="P1", name="Widget", niche="tech", quantity=2)
        pay = PaymentData(amount_usd=50.0, method="cc", status="approved")
        p = WebhookPayload(
            transaction_id="txn-001",
            transaction_time=datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc),
            event="order.approved",
            customer=cd,
            product=pd,
            payment=pay,
            gateway="lous",
            correlation_id="corr-001",
        )
        assert p.transaction_id == "txn-001"
        assert p.gateway == "lous"

    def test_aware_datetime_required(self):
        cd = CustomerData(email="test@example.com", country="US")
        pd = ProductData(id="P1", name="Widget", niche="tech", quantity=2)
        pay = PaymentData(amount_usd=50.0, method="cc", status="approved")
        with pytest.raises(ValidationError):
            WebhookPayload(
                transaction_id="txn-001",
                transaction_time=datetime(2024, 1, 15, 10, 30),
                event="order.approved",
                customer=cd,
                product=pd,
                payment=pay,
                gateway="lous",
                correlation_id="corr-001",
            )


class TestLeadReceivedMessage:
    def test_serialization_roundtrip(self):
        cd = CustomerData(email="test@example.com", country="US")
        pd = ProductData(id="P1", name="Widget", niche="tech", quantity=1)
        pay = PaymentData(amount_usd=10.0, method="cc", status="approved")
        msg = LeadReceivedMessage(
            transaction_id="txn-001",
            transaction_time=datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc),
            event="order.approved",
            customer=cd,
            product=pd,
            payment=pay,
            gateway="lous",
            correlation_id="corr-001",
        )
        data = msg.model_dump()
        restored = LeadReceivedMessage.model_validate(data)
        assert restored.transaction_id == "txn-001"
        assert restored.correlation_id == "corr-001"


class TestDLQMessage:
    def test_creation_with_error(self):
        msg = DLQMessage(
            original_payload={"key": "value"},
            error_reason="decrypt failed",
            gateway="grummer",
            correlation_id="corr-001",
            queue_origin="lead.dead.decrypt_failed",
        )
        assert msg.error_reason == "decrypt failed"
        assert msg.gateway == "grummer"


class TestProcessingResult:
    @pytest.mark.parametrize(
        "status,error_detail",
        [
            ("accepted", None),
            ("duplicate", None),
            ("schema_failed", "missing field"),
        ],
    )
    def test_processing_result(self, status, error_detail):
        r = ProcessingResult(status=status, correlation_id="corr-001", error_detail=error_detail)
        assert r.status == status
        assert r.error_detail == error_detail
