from datetime import datetime
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, EmailStr, Field, StringConstraints


class CustomerData(BaseModel):
    email: EmailStr
    first_name: str = "Customer"
    last_name: str | None = None
    phone: str | None = None
    country: Annotated[str, StringConstraints(min_length=2, max_length=2, to_upper=True)]


class ProductData(BaseModel):
    id: str
    name: str
    niche: str
    quantity: Annotated[int, Field(gt=0)]


class PaymentData(BaseModel):
    amount_usd: Annotated[float, Field(ge=0)]
    method: str
    status: Literal["approved", "declined", "pending", "refunded"]


class WebhookPayload(BaseModel):
    transaction_id: str
    transaction_time: AwareDatetime
    event: str
    customer: CustomerData
    product: ProductData
    payment: PaymentData
    gateway: str
    correlation_id: str


class GrummerEncryptedBody(BaseModel):
    iv: str
    ciphertext: str


class LeadReceivedMessage(BaseModel):
    event_id: str
    transaction_id: str
    transaction_time: datetime
    event: str
    customer: CustomerData
    product: ProductData
    payment: PaymentData
    gateway: str
    correlation_id: str


class DistributionMessage(BaseModel):
    event_id: str
    order_id: str | None = None
    transaction_id: str
    channel: str
    customer: CustomerData
    product: ProductData
    payment: PaymentData
    gateway: str
    correlation_id: str


class DLQMessage(BaseModel):
    original_payload: dict
    error_reason: str
    gateway: str
    correlation_id: str
    queue_origin: str


class ProcessingResult(BaseModel):
    status: str
    correlation_id: str
    error_detail: str | None = None
