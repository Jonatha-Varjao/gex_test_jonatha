# gex_common — Shared Library

Shared modules used by both `gex_receiver` and `gex_worker`. Installed as an editable workspace dependency via `uv sync --package`.

## Modules

| Module | Purpose |
|--------|---------|
| `config.py` | `AppSettings` (flat, no nesting) — reads `DATABASE_URL`, `GRUMMER_SECRET_HEX`, `RABBITMQ_URL`, etc. from env |
| `crypto.py` | AES-256-CBC decrypt with PKCS7 padding (`decrypt_grummer`), exceptions `DecryptionError` |
| `validation.py` | Schema models (`WebhookPayload`), email/phone normalization, `validate_schema()` → `ValidationResult` |
| `models.py` | Pydantic v2 data models: `CustomerData`, `ProductData`, `PaymentData`, `EventMessage` | 
| `logging.py` | structlog configuration, `get_app_logger()`, JSON-formatted with `correlation_id` context |

## Dependencies

- `pydantic>=2.0`, `cryptography`, `structlog`, `pyyaml`

This library has no standalone entry point — import from `gex_common.*` in your app code.
