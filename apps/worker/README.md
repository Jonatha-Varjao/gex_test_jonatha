# gex_worker — FastStream Event Consumer

FastStream application that consumes from `lead.received`, executes `sp_insert_lead`, publishes to 4 distribution queues, and routes failures to DLQ.

## Quick Start (local dev with docker infrastructure)

```bash
docker compose up -d mysql rabbitmq receiver
uv sync --all-packages
uv run --package gex-worker faststream run gex_worker.main:app
```

## Consumer Topology

| Queue | Exchange | Routing Key | Purpose |
|-------|----------|-------------|---------|
| `lead.received` | `lead` | `lead.received` | Approved leads from receiver |
| `lead.dead.decrypt_failed` | `lead` | `lead.dead.decrypt_failed` | Decrypt failures |
| `lead.dead.schema_failed` | `lead` | `lead.dead.schema_failed` | Schema validation failures |
| `lead.dead.consumer_failed` | `lead` | `lead.dead.consumer_failed` | Worker processing failures after retry |
| `dist.sms` | `dist` | `dist.sms` | SMS distribution (via webhook.site) |
| `dist.dead.sms` | `dist` | `dist.dead.sms` | SMS failures after retry |

## Retry + DLQ Strategy

- Backoffs: `[1000ms, 4000ms, 16000ms]` (×4 geometric progression)
- Implemented via custom `RetryMiddleware(BaseMiddleware)`
- After 3 attempts, `DlqMiddleware.after_processed()` publishes to `lead.dead.consumer_failed` or `dist.dead.sms`

## SMS Distributor

The `dist.sms` consumer POSTs the order to a webhook.site URL. Configurable failure rate (`SMS_FAILURE_RATE`, default 0.1). On webhook.site rate limiting (429), all messages fail and route to `dist.dead.sms` via retry.

## Source Layout

```
src/gex_worker/
├── main.py              # FastStream app creation, setup, middleware
├── config.py            # Worker-specific AppSettings
├── db.py                # DB session factory, helpers
├── consumers.py         # lead.received handler
├── distributors.py      # dist.sms handler (webhook POST)
├── dlq.py               # DLQ message building
├── middleware.py         # CorrelationId + Retry middlewares
├── exception_handlers.py # DlqMiddleware(BaseMiddleware)
└── __init__.py
```

The worker image is built from `apps/worker/Dockerfile`.
