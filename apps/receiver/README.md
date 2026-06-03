# gex_receiver — HTTP Webhook Receiver

FastAPI application that receives encrypted (grummer) and plaintext (lous) webhooks, validates, deduplicates, and publishes approved leads to RabbitMQ `lead.received`.

## Quick Start (local dev with docker infrastructure)

```bash
docker compose up -d mysql rabbitmq
uv sync --all-packages
uv run --package gex-receiver uvicorn gex_receiver.main:app --reload --port 8000
```

Or with FastAPI CLI:

```bash
uv run fastapi dev apps/receiver/src/gex_receiver/main.py
```

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Liveness check |
| GET | `/health/ready` | Readiness check (DB + RMQ) |
| POST | `/webhooks/{gateway}` | Receive webhook (grummer or lous) |

## Idempotency

The receiver deduplicates on `(transaction_id, event)` via `processed_events` table with `INSERT ... ON DUPLICATE KEY UPDATE`. See `docs/explicativo.md` for the natural-key decision.

## Environment Variables

Receiver reads from `gex_common.config.AppSettings`. Key vars:

| Variable | Default | Purpose |
|----------|---------|---------|
| `DATABASE_URL` | `mysql+asyncmy://gex:gex@localhost:3306/gex` | MySQL connection |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ connection |
| `GRUMMER_SECRET_HEX` | *(required)* | 32-byte AES key |
| `MAX_REQUEST_SIZE_KB` | `1024` | Max body size |

All env vars are documented in the root `README.md` and template in `.env.example`.

## Source Layout

```
src/gex_receiver/
├── main.py             # FastAPI app creation, lifespan, middleware
├── routes.py           # POST /webhooks/{gateway}, GET /health, GET /health/ready
├── dependencies.py     # FastAPI Depends() with Annotated type aliases
├── db.py               # insert_raw_payload(), check_idempotency()
├── idempotency.py      # Wrapper around check_idempotency
├── publishers.py       # RabbitMQ publisher (lead.received, DLQ, dist queues)
├── health.py           # /health and /health/ready handlers
└── __init__.py         # Re-exports app + create_app
```

The receiver image is built from `apps/receiver/Dockerfile` (multi-stage, `--no-editable` install).
