from dataclasses import dataclass, field
from datetime import datetime

import email_validator
import phonenumbers

from gex_common.models import CustomerData, PaymentData, ProductData, WebhookPayload


@dataclass
class ValidationError:
    field: str
    message: str


@dataclass
class ValidationResult:
    is_valid: bool = False
    payload: WebhookPayload | None = None
    errors: list[ValidationError] = field(default_factory=list)
    email_valid: bool = True
    phone_valid: bool = True
    name_defaulted: bool = False


def normalize_email(email: str) -> str:
    return email.strip().lower()


def validate_email(email: str) -> bool:
    try:
        email_validator.validate_email(email, check_deliverability=False)
        return True
    except email_validator.EmailNotValidError:
        return False


def normalize_phone(phone: str) -> str:
    if phone is None:
        return ""
    result = ""
    for i, ch in enumerate(phone.strip()):
        if ch == "+" and i == 0:
            result += ch
        elif ch.isdigit():
            result += ch
    return result


def validate_phone(phone: str) -> bool:
    if not phone:
        return False
    try:
        parsed = phonenumbers.parse(phone, None)
        return phonenumbers.is_valid_number(parsed)
    except phonenumbers.NumberParseException:
        return False


def format_phone_e164(phone: str) -> str:
    if not phone:
        return phone
    try:
        parsed = phonenumbers.parse(phone, None)
        if phonenumbers.is_valid_number(parsed):
            return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    except phonenumbers.NumberParseException:
        pass
    normalized = normalize_phone(phone)
    if normalized.startswith("+"):
        digits_only = normalized[1:]
    else:
        digits_only = normalized
        if len(digits_only) == 10:
            return "+1" + digits_only
    if 10 <= len(digits_only) <= 15:
        return normalized
    return phone


def normalize_name(name: str | None) -> str:
    if not name or not name.strip():
        return "Customer"
    return name.strip()


def validate_schema(data: dict) -> ValidationResult:
    errors: list[ValidationError] = []
    email_valid = True
    phone_valid = True
    name_defaulted = False
    required_top = ["transaction_id", "transaction_time", "event"]
    for field_name in required_top:
        val = data.get(field_name)
        if val is None or (isinstance(val, str) and val.strip() == ""):
            errors.append(
                ValidationError(field=field_name, message=f"Missing required field: {field_name}")
            )
    if not any(e.field == "transaction_time" for e in errors):
        raw_time = data.get("transaction_time")
        parsed_time = None
        if raw_time is not None:
            if isinstance(raw_time, str):
                try:
                    parsed_time = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
                    if parsed_time.tzinfo is None:
                        errors.append(
                            ValidationError(
                                field="transaction_time",
                                message=(
                                    "transaction_time must include timezone info "
                                    "(ISO 8601 with timezone)"
                                ),
                            )
                        )
                except ValueError, TypeError:
                    errors.append(
                        ValidationError(
                            field="transaction_time",
                            message=f"Invalid ISO 8601 datetime: {raw_time}",
                        )
                    )
            elif isinstance(raw_time, datetime):
                parsed_time = raw_time
                if raw_time.tzinfo is None:
                    errors.append(
                        ValidationError(
                            field="transaction_time",
                            message="transaction_time must include timezone info",
                        )
                    )
    customer = data.get("customer")
    if not customer or not isinstance(customer, dict):
        errors.append(ValidationError(field="customer", message="Missing required field: customer"))
    else:
        email_raw = customer.get("email")
        if email_raw is None or (isinstance(email_raw, str) and email_raw.strip() == ""):
            errors.append(
                ValidationError(
                    field="customer.email",
                    message="Missing required field: customer.email",
                )
            )
        elif isinstance(email_raw, str):
            normalized_email = normalize_email(email_raw)
            if not validate_email(normalized_email):
                errors.append(
                    ValidationError(
                        field="customer.email",
                        message=f"Invalid email format: {email_raw}",
                    )
                )
                email_valid = False
            else:
                email_raw = normalized_email
        first_name_raw = customer.get("first_name")
        if not first_name_raw or (isinstance(first_name_raw, str) and not first_name_raw.strip()):
            name_defaulted = True
        country_raw = customer.get("country")
        if not country_raw or (
            isinstance(country_raw, str)
            and (country_raw.strip() == "" or len(country_raw.strip()) != 2)
        ):
            errors.append(
                ValidationError(
                    field="customer.country",
                    message="customer.country must be a 2-letter ISO 3166-1 alpha-2 code",
                )
            )
        phone_raw = customer.get("phone")
        if phone_raw is not None and isinstance(phone_raw, str):
            formatted = format_phone_e164(phone_raw)
            if formatted and validate_phone(formatted):
                phone_raw = formatted
            else:
                phone_valid = False
    product = data.get("product")
    if not product or not isinstance(product, dict):
        errors.append(ValidationError(field="product", message="Missing required field: product"))
    else:
        for field_name in ["id", "name", "niche"]:
            val = product.get(field_name)
            if val is None or (isinstance(val, str) and val.strip() == ""):
                errors.append(
                    ValidationError(
                        field=f"product.{field_name}",
                        message=f"Missing required field: product.{field_name}",
                    )
                )
        quantity = product.get("quantity")
        if quantity is None or not isinstance(quantity, (int, float)) or quantity <= 0:
            errors.append(
                ValidationError(
                    field="product.quantity",
                    message="product.quantity must be a positive integer",
                )
            )
    payment = data.get("payment")
    if not payment or not isinstance(payment, dict):
        errors.append(ValidationError(field="payment", message="Missing required field: payment"))
    else:
        amount_raw = payment.get("amount_usd")
        if amount_raw is None:
            errors.append(
                ValidationError(
                    field="payment.amount_usd",
                    message="Missing required field: payment.amount_usd",
                )
            )
        elif isinstance(amount_raw, (int, float)) and amount_raw < 0:
            errors.append(
                ValidationError(
                    field="payment.amount_usd",
                    message="payment.amount_usd must be >= 0",
                )
            )
        elif isinstance(amount_raw, str):
            try:
                float(amount_raw)
            except ValueError, TypeError:
                errors.append(
                    ValidationError(
                        field="payment.amount_usd",
                        message="payment.amount_usd must be a number",
                    )
                )
        method_raw = payment.get("method")
        if not method_raw or (isinstance(method_raw, str) and method_raw.strip() == ""):
            errors.append(
                ValidationError(
                    field="payment.method",
                    message="Missing required field: payment.method",
                )
            )
        status_raw = payment.get("status")
        valid_statuses = {"approved", "declined", "pending", "refunded"}
        if status_raw not in valid_statuses:
            errors.append(
                ValidationError(
                    field="payment.status",
                    message=f"payment.status must be one of {valid_statuses}, got: {status_raw}",
                )
            )
    if errors:
        return ValidationResult(
            is_valid=False,
            errors=errors,
            email_valid=email_valid,
            phone_valid=phone_valid,
            name_defaulted=name_defaulted,
        )
    try:
        customer_data = CustomerData(
            email=normalize_email(data["customer"]["email"]),
            first_name=normalize_name(data["customer"].get("first_name") or "Customer"),
            last_name=data["customer"].get("last_name"),
            phone=format_phone_e164(data["customer"].get("phone", ""))
            if data["customer"].get("phone")
            else None,
            country=data["customer"]["country"].strip().upper(),
        )
        product_data = ProductData(
            id=str(data["product"]["id"]),
            name=data["product"]["name"],
            niche=data["product"]["niche"],
            quantity=int(data["product"]["quantity"]),
        )
        payment_data = PaymentData(
            amount_usd=float(data["payment"]["amount_usd"]),
            method=data["payment"]["method"],
            status=data["payment"]["status"],
        )
        payload = WebhookPayload(
            transaction_id=data["transaction_id"],
            transaction_time=parsed_time,
            event=data["event"],
            customer=customer_data,
            product=product_data,
            payment=payment_data,
            gateway=data.get("gateway", ""),
            correlation_id=data.get("correlation_id", ""),
        )
    except Exception as e:
        errors.append(
            ValidationError(field="__construct__", message=f"Failed to construct payload: {e}")
        )
        return ValidationResult(
            is_valid=False,
            errors=errors,
            email_valid=email_valid,
            phone_valid=phone_valid,
            name_defaulted=name_defaulted,
        )
    return ValidationResult(
        is_valid=True,
        payload=payload,
        email_valid=email_valid,
        phone_valid=phone_valid,
        name_defaulted=name_defaulted,
    )
