from datetime import datetime

import pytest
from pydantic import ValidationError

from gex_common.config import CONSTANTS
from gex_common.models import (
    CustomerData,
    PaymentData,
    ProductData,
    WebhookPayload,
)


class TestCustomerData:
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
    def test_invalid_status_rejected(self):
        with pytest.raises(ValidationError):
            PaymentData(amount_usd=10.0, method="cc", status="unknown_status")

    def test_negative_amount_rejected(self):
        with pytest.raises(ValidationError):
            PaymentData(amount_usd=-1.0, method="cc", status=CONSTANTS.payment_approved)


class TestWebhookPayload:
    def test_aware_datetime_required(self):
        cd = CustomerData(email="test@example.com", country="US")
        pd = ProductData(id="P1", name="Widget", niche="tech", quantity=2)
        pay = PaymentData(amount_usd=50.0, method="cc", status=CONSTANTS.payment_approved)
        with pytest.raises(ValidationError):
            WebhookPayload(
                transaction_id="txn-001",
                transaction_time=datetime(2024, 1, 15, 10, 30),
                event=CONSTANTS.event_order_approved,
                customer=cd,
                product=pd,
                payment=pay,
                gateway=CONSTANTS.gateway_lous,
                correlation_id="corr-001",
            )
