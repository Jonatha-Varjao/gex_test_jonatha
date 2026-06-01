import structlog

from gex_common.logging import anonymize_customer_id, get_logger, setup_logging


class TestSetupLogging:
    def test_setup_logging_info(self):
        setup_logging("INFO")
        logger = structlog.get_logger()
        assert logger is not None


class TestGetLogger:
    def test_logger_with_correlation_id(self):
        get_logger(correlation_id="test-123")

    def test_logger_with_gateway_and_event(self):
        get_logger(gateway="lous", event="order.approved")

    def test_logger_without_context(self):
        logger = get_logger()
        assert logger is not None


class TestAnonymizeCustomerId:
    def test_returns_8_char_hash(self):
        result = anonymize_customer_id("test@example.com")
        assert len(result) == 8
        assert result.isalnum()

    def test_deterministic(self):
        result1 = anonymize_customer_id("test@example.com")
        result2 = anonymize_customer_id("test@example.com")
        assert result1 == result2

    def test_different_emails_different_hashes(self):
        result1 = anonymize_customer_id("alice@example.com")
        result2 = anonymize_customer_id("bob@example.com")
        assert result1 != result2
