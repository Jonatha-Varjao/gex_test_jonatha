from dataclasses import dataclass, field

import email_validator
import phonenumbers
from pydantic import ValidationError as PydanticValidationError

from gex_common.models import WebhookPayload


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
    email_valid = True
    phone_valid = True
    name_defaulted = False

    try:
        payload = WebhookPayload(
            **{**data, "gateway": data.get("gateway", ""), "correlation_id": data.get("correlation_id", "")}
        )
    except PydanticValidationError as e:
        errors = []
        for err in e.errors():
            field = ".".join(str(p) for p in err["loc"])
            errors.append(ValidationError(field=field, message=err["msg"]))
            if field == "customer.email":
                email_valid = False
        return ValidationResult(is_valid=False, errors=errors, email_valid=email_valid)
    except Exception as e:
        return ValidationResult(
            is_valid=False,
            errors=[ValidationError(field="__construct__", message=f"Failed to construct payload: {e}")],
        )

    errors: list[ValidationError] = []

    for field_name in ["transaction_id", "event"]:
        val = getattr(payload, field_name, None)
        if not val or (isinstance(val, str) and not val.strip()):
            errors.append(ValidationError(field=field_name, message=f"Missing required field: {field_name}"))

    if not payload.customer.email.strip():
        errors.append(ValidationError(field="customer.email", message="Missing required field: customer.email"))
        email_valid = False
    else:
        payload.customer.email = normalize_email(payload.customer.email)

    if not payload.customer.country or len(payload.customer.country.strip()) != 2:
        errors.append(ValidationError(field="customer.country", message="customer.country must be a 2-letter ISO 3166-1 alpha-2 code"))

    if not payload.customer.first_name or not payload.customer.first_name.strip():
        payload.customer.first_name = "Customer"
        name_defaulted = True

    for field_name in ["id", "name", "niche"]:
        val = getattr(payload.product, field_name, None)
        if not val or (isinstance(val, str) and not val.strip()):
            errors.append(ValidationError(field=f"product.{field_name}", message=f"Missing required field: product.{field_name}"))

    if not payload.payment.method.strip():
        errors.append(ValidationError(field="payment.method", message="Missing required field: payment.method"))

    if errors:
        return ValidationResult(is_valid=False, errors=errors, email_valid=email_valid, phone_valid=phone_valid)

    if payload.customer.phone:
        payload.customer.phone = format_phone_e164(payload.customer.phone)
        try:
            parsed = phonenumbers.parse(payload.customer.phone, None)
            if not phonenumbers.is_valid_number(parsed):
                phone_valid = False
        except phonenumbers.NumberParseException:
            phone_valid = False

    return ValidationResult(
        is_valid=True,
        payload=payload,
        email_valid=email_valid,
        phone_valid=phone_valid,
        name_defaulted=name_defaulted,
    )
