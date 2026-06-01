import base64
import binascii

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.padding import PKCS7


class DecryptionError(Exception):
    def __init__(self, message: str, original_error: Exception | None = None):
        super().__init__(message)
        self.original_error = original_error


VALID_AES_KEY_LENGTHS = {16, 24, 32}
AES_BLOCK_SIZE_BITS = 128
AES_BLOCK_SIZE_BYTES = AES_BLOCK_SIZE_BITS // 8


def decrypt_grummer(iv_b64: str, ciphertext_b64: str, secret_hex: str) -> str:
    try:
        try:
            key = bytes.fromhex(secret_hex)
        except ValueError as e:
            raise DecryptionError(f"Invalid hex key: {e}", e) from e
        try:
            iv = base64.b64decode(iv_b64, validate=True)
            ciphertext = base64.b64decode(ciphertext_b64, validate=True)
        except binascii.Error as e:
            raise DecryptionError(f"Invalid base64 input: {e}", e) from e
        if len(key) not in VALID_AES_KEY_LENGTHS:
            valid_lengths = sorted(VALID_AES_KEY_LENGTHS)
            raise DecryptionError(
                f"Invalid AES key length: {len(key)} bytes (expected one of {valid_lengths})"
            )
        if len(iv) != AES_BLOCK_SIZE_BYTES:
            raise DecryptionError(
                f"Invalid IV length: {len(iv)} bytes (expected {AES_BLOCK_SIZE_BYTES})"
            )
        if len(ciphertext) == 0 or len(ciphertext) % AES_BLOCK_SIZE_BYTES != 0:
            raise DecryptionError(
                f"Invalid ciphertext length: {len(ciphertext)} bytes "
                f"(must be a non-zero multiple of {AES_BLOCK_SIZE_BYTES})"
            )
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
        decryptor = cipher.decryptor()
        padded = decryptor.update(ciphertext) + decryptor.finalize()
        unpadder = PKCS7(AES_BLOCK_SIZE_BITS).unpadder()
        plaintext_bytes = unpadder.update(padded) + unpadder.finalize()
        return plaintext_bytes.decode("utf-8")
    except DecryptionError:
        raise
    except Exception as e:
        raise DecryptionError(f"Decryption failed: {e}", original_error=e) from e
