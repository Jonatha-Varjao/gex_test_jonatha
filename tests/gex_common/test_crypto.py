import base64
import json

import pytest

from gex_common.config import AppSettings
from gex_common.crypto import DecryptionError, decrypt_grummer

GRUMMER_SECRET = AppSettings().grummer_secret_hex


class TestDecryptGrummer:
    def test_decrypt_valid_payload(self, first_grummer_encrypted):
        body = first_grummer_encrypted["body"]
        result = decrypt_grummer(body["iv"], body["ciphertext"], GRUMMER_SECRET)
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert isinstance(parsed, dict)
        assert "transaction_id" in parsed

    def test_decrypt_wrong_key(self, first_grummer_encrypted):
        body = first_grummer_encrypted["body"]
        wrong_key = "aa" * 32
        with pytest.raises(DecryptionError):
            decrypt_grummer(body["iv"], body["ciphertext"], wrong_key)

    @pytest.mark.parametrize(
        "iv,ciphertext",
        [
            ("AAAAAAAAbbbbbbbb==", "not-valid-base64!!!"),
            ("not-valid-base64!!!", "AAAAAAAAAAAAAAAAAAAAAA=="),
            ("AAAAAAAAAAAAAAAAAAAAAA==", ""),
        ],
    )
    def test_decrypt_invalid_inputs(self, iv, ciphertext):
        with pytest.raises(DecryptionError):
            decrypt_grummer(iv, ciphertext, GRUMMER_SECRET)

    def test_decrypt_invalid_padding(self):
        iv = base64.b64encode(b"\x00" * 16).decode()
        ciphertext = base64.b64encode(b"\x00" * 16).decode()
        with pytest.raises(DecryptionError):
            decrypt_grummer(iv, ciphertext, GRUMMER_SECRET)

    def test_decrypt_all_grummer_payloads(self, grummer_encrypted_payloads):
        successes = 0
        failures = 0
        for payload in grummer_encrypted_payloads:
            body = payload["body"]
            try:
                result = decrypt_grummer(body["iv"], body["ciphertext"], GRUMMER_SECRET)
                json.loads(result)
                successes += 1
            except DecryptionError:
                failures += 1
        assert successes > 0

    def test_decryption_error_from_exception_has_original_error(self):
        iv = base64.b64encode(b"\x00" * 16).decode()
        ciphertext = base64.b64encode(b"\x00" * 16).decode()
        try:
            decrypt_grummer(iv, ciphertext, GRUMMER_SECRET)
        except DecryptionError as e:
            assert e.original_error is not None
            assert str(e)

    def test_decryption_error_from_validation_has_no_original(self):
        with pytest.raises(DecryptionError) as exc_info:
            decrypt_grummer("AAAA", "AAAA", "00" * 32)
        assert exc_info.value.original_error is None

    def test_decryption_error_non_hex_key(self):
        with pytest.raises(DecryptionError, match="Invalid hex key"):
            decrypt_grummer("AAAA", "AAAA", "not-hex-string")

    def test_decryption_error_invalid_key_length(self):
        with pytest.raises(DecryptionError, match="Invalid AES key length"):
            decrypt_grummer("AAAA", "AAAA", "ff")

    def test_decryption_error_invalid_iv_length(self):
        iv = base64.b64encode(b"short").decode()
        with pytest.raises(DecryptionError, match="Invalid IV length"):
            decrypt_grummer(iv, "AAAA", GRUMMER_SECRET)
