from datetime import datetime

import pytest
from hypothesis import given
from hypothesis import strategies as st

from gex_common.validation import (
    format_phone_e164,
    normalize_email,
    normalize_name,
    normalize_phone,
    validate_email,
    validate_phone,
    validate_schema,
)


class TestNormalizeEmail:
    @pytest.mark.parametrize(
        "input_,expected",
        [
            ("  test@example.com  ", "test@example.com"),
            ("  Test@Example.COM  ", "test@example.com"),
        ],
    )
    def test_trims_and_lowercases(self, input_, expected):
        assert normalize_email(input_) == expected

    def test_already_normalized(self):
        assert normalize_email("test@example.com") == "test@example.com"


class TestValidateEmail:
    def test_valid_email(self):
        assert validate_email("user@example.com") is True

    @pytest.mark.parametrize(
        "email",
        [
            "userexample.com",
            "user@",
            "user @example.com",
            "",
        ],
    )
    def test_invalid_emails(self, email):
        assert validate_email(email) is False


class TestNormalizePhone:
    def test_strips_non_digits_except_plus(self):
        assert normalize_phone("+1 (650) 555-1234") == "+16505551234"

    @pytest.mark.parametrize(
        "input_,expected",
        [
            ("", ""),
            (None, ""),
        ],
    )
    def test_empty_or_none(self, input_, expected):
        assert normalize_phone(input_) == expected


class TestValidatePhone:
    def test_valid_e164(self):
        assert validate_phone("+16505551234") is True

    @pytest.mark.parametrize(
        "phone",
        [
            "+1234",
            "",
        ],
    )
    def test_invalid_phones(self, phone):
        assert validate_phone(phone) is False


class TestFormatPhoneE164:
    def test_us_number_with_country_code(self):
        assert format_phone_e164("+1 650 555 1234") == "+16505551234"

    @pytest.mark.parametrize(
        "input_,expected",
        [
            ("+16505551234", "+16505551234"),
            ("123", "123"),
            ("", ""),
            ("6505551234", "+16505551234"),
        ],
    )
    def test_edge_cases(self, input_, expected):
        assert format_phone_e164(input_) == expected


class TestNormalizeName:
    def test_valid_name_returns_trimmed(self):
        assert normalize_name("  John  ") == "John"

    @pytest.mark.parametrize(
        "name",
        [
            "",
            None,
            "   ",
        ],
    )
    def test_empty_values_return_customer(self, name):
        assert normalize_name(name) == "Customer"


class TestPhoneProperties:
    valid_e164 = st.sampled_from(
        [
            "+16505551234",
            "+442071234567",
            "+5511987654321",
            "+33612345678",
            "+4915112345678",
            "+919876543210",
            "+61412345678",
            "+81312345678",
            "+8613812345678",
        ]
    )

    phone_like = st.one_of(
        valid_e164,
        st.from_regex(r"\+?\d{1,15}"),
        st.text(alphabet="+0123456789 -().", min_size=0, max_size=25),
        st.just(""),
        st.just(None),
    )

    @given(phone_like)
    def test_normalize_phone_never_crashes(self, phone):
        if phone is None:
            phone = ""
        result = normalize_phone(phone)
        assert isinstance(result, str)

    @given(phone_like)
    def test_format_phone_never_crashes(self, phone):
        if phone is None:
            phone = ""
        result = format_phone_e164(phone)
        assert isinstance(result, str)

    @given(valid_e164)
    def test_valid_e164_phones_validate_true(self, phone):
        assert validate_phone(phone) is True

    @given(st.text(min_size=1, max_size=20).filter(lambda s: not any(c.isdigit() for c in s)))
    def test_no_digit_strings_validate_false(self, phone):
        assert validate_phone(phone) is False

    @given(valid_e164)
    def test_valid_e164_normalize_idempotent(self, phone):
        assert normalize_phone(phone) == phone

    @given(valid_e164)
    def test_format_valid_phone_starts_with_plus(self, phone):
        assert format_phone_e164(phone).startswith("+")


class TestValidateSchema:
    def test_valid_lous_payload(self, valid_lous_body):
        result = validate_schema(valid_lous_body)
        assert result.is_valid is True
        assert result.payload is not None
        assert result.payload.event == valid_lous_body["event"]
        assert result.payload.transaction_id == valid_lous_body["transaction_id"]

    def test_missing_transaction_id(self, valid_lous_body):
        data = {**valid_lous_body, "transaction_id": ""}
        result = validate_schema(data)
        assert result.is_valid is False
        assert any(e.field == "transaction_id" for e in result.errors)

    def test_missing_customer_email(self, valid_lous_body):
        data = valid_lous_body.copy()
        data["customer"] = {**data["customer"], "email": ""}
        result = validate_schema(data)
        assert result.is_valid is False
        assert any(e.field == "customer.email" for e in result.errors)

    def test_invalid_email_format(self, valid_lous_body):
        data = valid_lous_body.copy()
        data["customer"] = {**data["customer"], "email": "not-an-email"}
        result = validate_schema(data)
        assert result.is_valid is False
        assert result.email_valid is False

    def test_empty_first_name_defaults(self, valid_lous_body):
        data = valid_lous_body.copy()
        data["customer"] = {**data["customer"], "first_name": ""}
        result = validate_schema(data)
        assert result.is_valid is True
        assert result.name_defaulted is True
        assert result.payload.customer.first_name == "Customer"

    def test_invalid_phone_still_valid(self, valid_lous_body):
        data = valid_lous_body.copy()
        data["customer"] = {**data["customer"], "phone": "123"}
        result = validate_schema(data)
        assert result.is_valid is True
        assert result.phone_valid is False

    def test_invalid_payment_status(self, valid_lous_body):
        data = valid_lous_body.copy()
        data["payment"] = {**data["payment"], "status": "unknown_status"}
        result = validate_schema(data)
        assert result.is_valid is False
        assert any(e.field == "payment.status" for e in result.errors)

    def test_negative_quantity(self, valid_lous_body):
        data = valid_lous_body.copy()
        data["product"] = {**data["product"], "quantity": -1}
        result = validate_schema(data)
        assert result.is_valid is False
        assert any(e.field == "product.quantity" for e in result.errors)

    def test_negative_amount(self, valid_lous_body):
        data = valid_lous_body.copy()
        data["payment"] = {**data["payment"], "amount_usd": -5.0}
        result = validate_schema(data)
        assert result.is_valid is False
        assert any(e.field == "payment.amount_usd" for e in result.errors)

    def test_missing_customer_object(self, valid_lous_body):
        data = {**valid_lous_body, "customer": None}
        result = validate_schema(data)
        assert result.is_valid is False
        assert any(e.field == "customer" for e in result.errors)

    def test_invalid_amount_string(self, valid_lous_body):
        data = valid_lous_body.copy()
        data["payment"] = {**data["payment"], "amount_usd": "not-a-number"}
        result = validate_schema(data)
        assert result.is_valid is False
        assert any(e.field == "payment.amount_usd" for e in result.errors)

    def test_email_normalized_to_lowercase(self, valid_lous_body):
        data = valid_lous_body.copy()
        data["customer"] = {**data["customer"], "email": "  Test@EXAMPLE.com  "}
        result = validate_schema(data)
        assert result.is_valid is True
        assert result.payload.customer.email == "test@example.com"

    def test_country_uppercased(self, valid_lous_body):
        data = valid_lous_body.copy()
        data["customer"] = {**data["customer"], "country": "us"}
        result = validate_schema(data)
        assert result.is_valid is True
        assert result.payload.customer.country == "US"

    def test_all_lous_payloads(self, lous_payloads):
        valid_count = 0
        invalid_count = 0
        for payload in lous_payloads:
            result = validate_schema(payload["body"])
            if result.is_valid:
                valid_count += 1
            else:
                invalid_count += 1
        assert valid_count > 0
        print(f"Lous: {valid_count} valid, {invalid_count} invalid out of {len(lous_payloads)}")

    @pytest.mark.parametrize("field", ["product", "payment"])
    def test_composite_field_null(self, field, valid_lous_body):
        data = {**valid_lous_body, field: None}
        result = validate_schema(data)
        assert result.is_valid is False
        assert any(e.field == field for e in result.errors)

    @pytest.mark.parametrize(
        "field,mutation",
        [
            (
                "payment.method",
                lambda d: {**d, "payment": {**d["payment"], "method": ""}},
            ),
            (
                "payment.amount_usd",
                lambda d: {**d, "payment": {**d["payment"], "amount_usd": None}},
            ),
            (
                "payment.status",
                lambda d: {**d, "payment": {**d["payment"], "status": None}},
            ),
            ("product.name", lambda d: {**d, "product": {**d["product"], "name": ""}}),
            ("event", lambda d: {**d, "event": None}),
            (
                "customer.country",
                lambda d: {**d, "customer": {**d["customer"], "country": ""}},
            ),
        ],
    )
    def test_missing_required_field(self, field, mutation, valid_lous_body):
        data = mutation(valid_lous_body)
        result = validate_schema(data)
        assert result.is_valid is False
        assert any(e.field == field for e in result.errors)

    @pytest.mark.parametrize(
        "time_value",
        [
            "2024-01-15T10:30:00",
            datetime(2024, 1, 15, 10, 30),
        ],
    )
    def test_transaction_time_without_timezone(self, time_value, valid_lous_body):
        data = {**valid_lous_body, "transaction_time": time_value}
        result = validate_schema(data)
        assert result.is_valid is False
        assert any(e.field == "transaction_time" for e in result.errors)

    def test_transaction_time_aware_datetime_object(self, valid_lous_body):
        from datetime import timezone

        data = {
            **valid_lous_body,
            "transaction_time": datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc),
        }
        result = validate_schema(data)
        assert result.is_valid is True

    def test_transaction_time_int_triggers_except_handler(self, valid_lous_body):
        data = {**valid_lous_body, "transaction_time": {}}
        result = validate_schema(data)
        assert result.is_valid is False
        assert any(e.field == "__construct__" for e in result.errors)
