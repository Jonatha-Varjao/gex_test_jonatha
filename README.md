# GEX Webhook Pipeline

A webhook processing pipeline that receives encrypted (grummer) and plaintext (lous) payloads, validates, queues, consumes, and distributes leads to multiple channels (SMS, email, call center, WhatsApp).

## Architecture

```
Gateway (grummer / lous)
  │
  ▼
┌─────────────────────────────────────────┐
│  gex_receiver (FastAPI)                 │
│  POST /webhooks/{gateway}               │
│                                         │
│  1. Validate gateway                    │
│  2. Grummer + encrypted → decrypt       │
│  3. Validate schema                     │
│  4. Idempotency check                   │
│  5. Route: order.approved → publish     │
│  6. Persist raw_payloads                │
│  7. Publish to RabbitMQ                 │
└───────────┬─────────────────────────────┘
            │ lead.received
            ▼
┌─────────────────────────────────────────┐
│  gex_worker (FastStream) [WIP]          │
│  Consume → persist → distribute         │
└─────────────────────────────────────────┘
```

**Tech stack:** Python 3.14, FastAPI, FastStream, aio-pika, SQLAlchemy Core, asyncmy, structlog, pytest, testcontainers, ruff, vulture.

## Project Structure

```
gex_test_jonatha/
├── libs/
│   └── common/src/gex_common/   # Shared library: config, crypto, models, validation, logging
├── apps/
│   ├── receiver/src/gex_receiver/  # HTTP layer (FastAPI) — DONE
│   └── worker/src/gex_worker/     # Background jobs (FastStream) — WIP
├── data/                          # Challenge-provided payloads, secret, expected summary
├── sql/                           # SQL scripts (DB layer — not yet implemented)
├── tests/                         # Test suite
│   ├── gex_common/
│   ├── gex_receiver/              # Unit + integration tests
│   └── gex_worker/                # (empty)
├── scripts/                       # (empty)
├── docs/                          # (empty — conceptual docs in gex_docs repo)
├── docker-compose.yml             # MySQL 8.4 + RabbitMQ 4.2
├── pyproject.toml                 # Workspace root + tool config
└── uv.lock
```

## Running the Project

### Prerequisites

- Python 3.14+
- [uv](https://github.com/astral-sh/uv) (dependency manager)
- Docker + Docker Compose (for infrastructure or integration tests)

### Initial Setup

```bash
# Install all workspace members into the shared .venv
uv sync --all-packages
```

This creates a single `.venv` at the project root with `gex_common`, `gex_receiver`, and `gex_worker` installed as editable packages.

### Bare Metal — Local Development

**1. Start infrastructure (MySQL + RabbitMQ):**

```bash
docker-compose up -d
```

**2. Run the receiver (FastAPI):**

```bash
uv run --package gex-receiver uvicorn gex_receiver.main:app --reload --port 8000
```

Or using the FastAPI CLI:

```bash
uv run fastapi dev apps/receiver/src/gex_receiver/main.py
```

**3. (Once implemented) Run the worker (FastStream):**

```bash
uv run --package gex-worker faststream run gex_worker.main:app
```

### Docker Compose — Full Stack

Receiver and worker Dockerfiles are not yet included. The current `docker-compose.yml` only provisions MySQL and RabbitMQ. Run the apps bare-metal until Dockerfiles are added.

## Environment Variables

Configured via `.env` (see `.env.example` for template):

| Variable | Default | Purpose |
|----------|---------|---------|
| `DATABASE_URL` | `mysql+asyncmy://gex:gex@localhost:3306/gex` | MySQL connection (async) |
| `DATABASE_POOL_SIZE` | `10` | SQLAlchemy pool size |
| `DATABASE_MAX_OVERFLOW` | `20` | Max overflow connections |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ connection |
| `GRUMMER_SECRET_HEX` | *(required for grummer)* | 32-byte AES key in hex |
| `WEBHOOK_SITE_URL` | *(empty)* | Target URL for SMS distributor |
| `SMS_FAILURE_RATE` | `0.1` | Simulated SMS failure rate (0-1) |
| `LOG_LEVEL` | `INFO` | structlog log level |
| `ENVIRONMENT` | `development` | Environment name |
| `MAX_REQUEST_SIZE_KB` | `1024` | Max webhook body size |
| `CONSUMER_CONCURRENCY` | `1` | Worker consumer concurrency |

## Quality Gate Suite

### Lint — Ruff

```bash
uv run ruff check .
```

Auto-fix:

```bash
uv run ruff check --fix .
```

### Format — Ruff

```bash
uv run ruff format --check .   # check only
uv run ruff format .            # apply formatting
```

### Tests — Pytest

```bash
# All tests
uv run pytest

# Unit tests only (fast, no external dependencies)
uv run pytest -m unit

# Integration tests (requires Docker; testcontainers spins up a RabbitMQ)
uv run pytest -m integration

# Specific file
uv run pytest tests/gex_receiver/test_routes.py

# With coverage
uv run pytest --cov
```

Test markers are defined in `pyproject.toml`:
- `unit` — no external dependencies
- `integration` — uses testcontainers (requires Docker; set `DOCKER_HOST` to your socket)

#### Docker setup for integration tests

Integration tests use [testcontainers](https://github.com/testcontainers/testcontainers-python) to spin up a RabbitMQ container per test session. The conftest sets `TESTCONTAINERS_RYUK_DISABLED=true` automatically — the Ryuk reaper sidecar is incompatible with Docker Desktop (see below).

Each developer must point Docker at their socket via the `DOCKER_HOST` env var (add to your `~/.bashrc` / `~/.zshrc` to persist):

```bash
# Docker Desktop (Linux / macOS)
export DOCKER_HOST="unix://$HOME/.docker/desktop/docker.sock"

# Rootless Docker
export DOCKER_HOST="unix://$HOME/.docker/run/docker.sock"

# System Docker (you're in the `docker` group) — also the default if unset
export DOCKER_HOST="unix:///var/run/docker.sock"
```

**Docker Desktop users** also need to allow testcontainers to mount the host socket into the spawned container. In Docker Desktop → **Settings → Resources → File Sharing**, add:

- `/home/varjao` (or `$HOME` on macOS: `/Users/<you>`)
- `/tmp`

Then **Apply & Restart** Docker Desktop. Without this, the RabbitMQ container fails to start with `mounts denied: The path /socket_mnt/...` in the error trace.

Verify your setup before running tests:

```bash
# This should print the RabbitMQ version banner, not a permission error
DOCKER_HOST="unix://$HOME/.docker/desktop/docker.sock" docker run --rm rabbitmq:4.2-management rabbitmq-diagnostics ping
```

### Dead Code — Vulture

```bash
uv run vulture libs apps --exclude tests/
```

### All at Once

```bash
uv run ruff check . && \
uv run ruff format --check . && \
uv run pytest -m unit --ignore=tests/gex_receiver/test_publishers.py && \
uv run vulture libs apps --exclude tests/
```

## API Reference

### `POST /webhooks/{gateway}`

Receives a webhook from `grummer` (AES-256-CBC encrypted) or `lous` (plaintext).

**Path params:**
- `gateway` — `grummer` or `lous` (422 if invalid)

**Headers:**
- `X-GR-Encrypted: true` — required for grummer encrypted payloads
- `X-Correlation-ID` — optional; auto-generated UUID4 if absent

**Request body:**

For `grummer` (encrypted):
```json
{
  "iv": "base64-encoded-16-bytes",
  "ciphertext": "base64-encoded-ciphertext"
}
```

For `lous` or `grummer` plaintext:
```json
{
  "transaction_id": "tx-001",
  "transaction_time": "2026-01-15T10:30:00+00:00",
  "event": "order.approved",
  "customer": {
    "email": "user@example.com",
    "first_name": "Jane",
    "phone": "+18005551234",
    "country": "US"
  },
  "product": {
    "id": "prod-1",
    "name": "Fit Burn",
    "niche": "weight_loss",
    "quantity": 1
  },
  "payment": {
    "amount_usd": 99.99,
    "method": "credit_card",
    "status": "approved"
  }
}
```

**Response codes:**

| Code | Status | Meaning |
|------|--------|---------|
| 200 | `accepted` | Valid + approved, published to `lead.received` |
| 200 | `duplicate` | Idempotency check: already processed |
| 200 | `discarded` | Valid but not approved (logged only) |
| 202 | `decrypt_failed` | Grummer payload failed AES decryption |
| 202 | `schema_failed` | Schema validation failed |
| 422 | — | Invalid gateway |
| 503 | — | DB or RabbitMQ unavailable |

**Response body:**

```json
{
  "status": "accepted",
  "correlation_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

## Status

| Component | Status |
|-----------|--------|
| `gex_common` (config, crypto, models, validation, logging) | Done |
| `gex_common` tests (93 tests) | Passing |
| `gex_receiver` (HTTP layer) | Done |
| `gex_receiver` tests (15 unit + 6 integration) | Passing |
| `gex_worker` (background jobs) | Not started |
| SQL scripts (`001_create_tables.sql`, etc.) | Not started |
| Integration with real MySQL | Deferred to DB layer phase |

## Development Order

1. ✅ **HTTP Layer** (gex_receiver) — this PR
2. ⏳ **Background Jobs Layer** (gex_worker) — next
3. ⏳ **DB Layer** (SQL scripts, audit queries, stored procedure)
4. ⏳ **Documentation** (architecture diagram, `docs/explicativo.md`, Loom)
