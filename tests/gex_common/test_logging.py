from gex_common.logging import anonymize_customer_id


class TestAnonymizeCustomerId:
    def test_returns_8_char_hash(self):
        result = anonymize_customer_id("test@example.com")
        assert len(result) == 8
        assert result.isalnum()
