# Webhook Pipeline

A webhook processing pipeline that receives encrypted (grummer) and plaintext (lous) payloads, validates, queues, consumes, and distributes leads to multiple channels (SMS, email, call center, WhatsApp).

## Architecture

```mermaid
flowchart LR
    G[Gateway<br/>grummer / lous] -->|HTTP POST| R[ _receiver<br/>FastAPI]

    R -->|decrypt_failed| DLQ1[lead.dead.decrypt_failed]
    R -->|schema_failed| DLQ2[lead.dead.schema_failed]
    R -->|accepted + approved| LEAD[(lead.received)]
    R -->|discarded_non_approved| RP[(raw_payloads)]

    LEAD --> W[ _worker<br/>FastStream consumer]
    W -->|sp_insert_lead| DB[(MySQL 8.4<br/>leads, orders,<br/>lead_events,<br/>distribution_status)]
    W -->|4x publish| DIST{4 distribution queues}
    W -->|3 retries exhausted| DLQ3[lead.dead.consumer_failed]

    DIST -->|dist.sms| SMS[dist.sms consumer<br/>POST webhook.site]
    DIST -.->|dist.email / callcenter / whatsapp| PLACE[placeholders]

    SMS -->|simulated 10% fail<br/>+ 3 retries| DLQ4[dist.dead.sms]
    SMS -->|2xx| DB
```

**Tech stack:** Python 3.14, FastAPI, FastStream, aio-pika, SQLAlchemy Core, asyncmy, structlog, pytest, testcontainers, ruff, vulture, MySQL 8.4, RabbitMQ 4.2.

## Project Structure

```
 _test_jonatha/
├── libs/
│   └── common/src/ _common/    # Shared library: config, crypto, models, validation, logging
├── apps/
│   ├── receiver/
│   │   ├── Dockerfile              # Multi-stage build (receiver only)
│   │   └── src/ _receiver/       # HTTP layer (FastAPI)
│   │       ├── main.py, routes.py, dependencies.py
│   │       ├── db.py, idempotency.py
│   │       ├── publishers.py, health.py
│   │       └── __init__.py
│   └── worker/
│       ├── Dockerfile              # Multi-stage build (worker only)
│       └── src/ _worker/         # Background jobs (FastStream)
│           ├── main.py, config.py, db.py
│           ├── consumers.py, distributors.py
│           ├── dlq.py, middleware.py, exception_handlers.py
│           └── __init__.py
├── data/                           # Challenge-provided assets
│   ├── webhook_payloads.json       # 200 webhooks (grummer + lous)
│   └── expected_summary_meta.json  # Spec for the E2E distribution
├── sql/                            # MySQL DDL + queries
│   ├── 001_create_tables.sql
│   ├── 002_indexes.sql
│   ├── 003_stored_procs.sql
│   └── audit_queries.sql
├── tests/
│   ├──  _common/                 
│   ├──  _receiver/               
│   ├──  _worker/                 
│   └── integration/                
├── scripts/
│   └── load_payloads.py            # 200-payload E2E driver
├── docs/                           # explicativo.md + _explain/
├── Dockerfile.e2e                  # E2E driver image (python:3.14-slim + httpx)
├── docker-compose.yml              # mysql + rabbitmq + receiver + worker (+ e2e profile)
├── pyproject.toml                  # Workspace root + tool config
└── uv.lock
```

## Running the Project

### Prerequisites

- **Docker + Docker Compose** the only host-side requirement

That's it. The receiver and worker ship as pre-built multi-stage images; no Python interpreter, `uv`, or compiler is needed on the host.

### Docker Compose - Full Stack (recommended)

**1. Configure secrets:**

```bash
cp .env.example .env
# edit .env and set GRUMMER_SECRET_HEX + WEBHOOK_SITE_URL
```
- to Set up a `WEBHOOK_SITE_URL` go to [webhook](https://webhook.site/) and copy "Your Unique URL" that was generated and put in the .env

**2. Bring up the stack:**

```bash
docker compose up -d --build
```

This builds two lean production images from per-app Dockerfiles and starts four services:

| Service | Image | Port | Profile |
|---------|-------|------|---------|
| `mysql` | `mysql:8.4` | 3306 | (always) |
| `rabbitmq` | `rabbitmq:4.2-management` | 5672 / 15672 | (always) |
| `receiver` | ` _test_jonatha-receiver:latest` (built from `apps/receiver/Dockerfile`) | 8000 | (always) |
| `worker` | ` _test_jonatha-worker:latest` (built from `apps/worker/Dockerfile`) |  | (always) |
| `e2e-driver` | ` _test_jonatha-e2e:latest` (built from `Dockerfile.e2e`) |  | `e2e` |

The receiver image contains ` _common` + ` _receiver` only; the worker image contains ` _common` + ` _worker` only. The modularization is preserved at the image layer - a worker rebuild does not need to touch the receiver.

**3. Verify the stack:**

```bash
curl http://localhost:8000/health       # → {"status":"ok"}
curl http://localhost:8000/health/ready # → {"status":"ok","db":"ok","rmq":"ok"}
```

**4. Run the 200-payload E2E (one-shot, exits when done):**

```bash
docker compose --profile e2e up e2e-driver
```

Expected distribution (matches `data/expected_summary_meta.json`):

```
HTTP 200: 165   HTTP 202: 35   HTTP 503: 0
  accepted                125
  duplicate               20
  decrypt_failed          15
  schema_failed           20
  discarded_non_approved  20
```

**5. Stop:**

```bash
docker compose down           # keeps volumes
docker compose down -v        # nukes the DB + RMQ volumes (clean slate)
```

### Bare-Metal - Local Development (optional)

For hacking on the apps with a live-reload Python process, run only the **infrastructure** under Docker and the apps on the host with `uv`:

**1. Start infrastructure only:**

```bash
docker compose up -d mysql rabbitmq
```

**2. Run the receiver (FastAPI, with reload):**

```bash
uv sync --all-packages
uv run --package  -receiver uvicorn  _receiver.main:app --reload --port 8000
```

Or using the FastAPI CLI:

```bash
uv run fastapi dev apps/receiver/src/ _receiver/main.py
```

**3. Run the worker (FastStream):**

```bash
uv run --package  -worker faststream run  _worker.main:app
```

For the E2E from the host, point the driver at the in-network host port:

```bash
uv run --package  -receiver python scripts/load_payloads.py --receiver http://localhost:8000
```

## Environment Variables

Configured via `.env` (see `.env.example` for template):

| Variable | Default | Purpose |
|----------|---------|---------|
| `DATABASE_URL` | `mysql+asyncmy:// : @localhost:3306/ ` | MySQL connection (async) |
| `DATABASE_POOL_SIZE` | `10` | SQLAlchemy pool size |
| `DATABASE_MAX_OVERFLOW` | `20` | Max overflow connections |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ connection |
| `GRUMMER_SECRET_HEX` | *(required for grummer)* | 32-byte AES key in hex  |
| `WEBHOOK_SITE_URL` | *(empty)* | Target URL for SMS distributor |
| `SMS_FAILURE_RATE` | `0.1` | Simulated SMS failure rate (0-1) |
| `LOG_LEVEL` | `INFO` | structlog log level |
| `ENVIRONMENT` | `development` | Environment name |

## Quality Gate Suite

### Lint - Ruff

```bash
uv run ruff check .
```

Auto-fix:

```bash
uv run ruff check --fix .
```

### Format - Ruff

```bash
uv run ruff format --check .   # check only
uv run ruff format .            # apply formatting
```

### Tests - Pytest

```bash
# All tests
uv run pytest

# Unit tests only (fast, no external dependencies)
uv run pytest -m unit

# Integration tests (requires Docker; testcontainers spins up a RabbitMQ)
uv run pytest -m integration

# Specific file
uv run pytest tests/ _receiver/test_routes.py

# With coverage
uv run pytest --cov
```

Test markers are defined in `pyproject.toml`:
- `unit` - no external dependencies
- `integration` - uses testcontainers (requires Docker; set `DOCKER_HOST` to your socket)

#### Docker setup for integration tests

Integration tests use [testcontainers](https://github.com/testcontainers/testcontainers-python) to spin up a RabbitMQ container per test session.

Each developer must point Docker at their socket via the `DOCKER_HOST` env var (add to your `~/.bashrc` / `~/.zshrc` to persist):

```bash
# Docker Desktop (Linux / macOS)
export DOCKER_HOST="unix://$HOME/.docker/desktop/docker.sock"

# Rootless Docker
export DOCKER_HOST="unix://$HOME/.docker/run/docker.sock"

# System Docker (you're in the `docker` group)
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

### Dead Code - Vulture

```bash
uv run vulture libs apps --exclude tests/
```

### All at Once

```bash
uv run ruff check . && \
uv run ruff format --check . && \
uv run pytest -m unit --ignore=tests/ _receiver/test_publishers.py && \
uv run vulture libs apps --exclude tests/
```

## API Reference

### `POST /webhooks/{gateway}`

Receives a webhook from `grummer` (AES-256-CBC encrypted) or `lous` (plaintext).

**Path params:**
- `gateway` - `grummer` or `lous` (422 if invalid)

**Headers:**
- `X-GR-Encrypted: true` - required for grummer encrypted payloads
- `X-Correlation-ID` - optional; auto-generated UUID4 if absent

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
| 200 | `duplicate` | Idempotency check: already processed (natural key `(transaction_id, event)`) |
| 200 | `discarded` | Valid but not approved (logged only) |
| 202 | `decrypt_failed` | Grummer payload failed AES decryption |
| 202 | `schema_failed` | Schema validation failed |
| 422 |  | Invalid gateway |
| 503 |  | DB or RabbitMQ unavailable |

**Idempotency natural key**: the receiver dedups on `(transaction_id, event)`, *not* `(gateway, transaction_id, event)`. The `gateway` column is stored in `processed_events` for tracking but is excluded from the `UNIQUE` constraint, so the same order arriving from two different gateways is treated as one event. See `sql/001_create_tables.sql:51`.

**Response body:**

```json
{
  "status": "accepted",
  "correlation_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

## Status

| Component | Status | Tests |
|-----------|--------|-------|
| ` _common` (config, crypto, models, validation, logging) | Done | 75 unit |
| ` _receiver` (HTTP layer + idempotency + DLQ publisher) | Done | 48 unit |
| ` _worker` (FastStream consumer + 3-attempt retry + DLQ middleware + SMS distributor) | Done |  |
| DB layer (CRUD + stored procedures, `tests/integration/test_mysql_db.py`) | Done | 16 integration |
| RabbitMQ publisher (`tests/integration/test_rabbitmq_publisher.py`) | Done | 6 integration |
| Audit queries (`sql/audit_queries.sql`) | Done |  |
| E2E with real MySQL 8.4 + RabbitMQ 4.2 (`scripts/load_payloads.py`) | Verified, see [End-to-End Validation](#end-to-end-validation) |  |
| ` _worker` unit tests | Pending | 0 |
| Receiver + worker Dockerfiles (`apps/<app>/Dockerfile`) | Done |  |
| `docs/explicativo.md` | Done |  |
| Loom walkthrough | Pending |  |

Total: **145 tests collected** (123 unit, 22 integration).

## End-to-End Validation

The pipeline is validated end-to-end against the real dataset in `data/webhook_payloads.json` (200 webhooks). **Reproduction requires only Docker** - no Python, `uv`, or compiler is needed on the host. The driver, receiver, and worker are all built as separate per-app Docker images and brought up with `docker compose`.

### 1. Build the images and start the stack

```bash
cp .env.example .env
# edit .env: set GRUMMER_SECRET_HEX + WEBHOOK_SITE_URL

docker compose up -d --build
```

This builds and starts:

- `mysql` (port 3306) - DDL/stored procs from `sql/*.sql` auto-load on first boot
- `rabbitmq` (5672, UI on 15672) - management plugin
- `receiver` (port 8000) - built from `apps/receiver/Dockerfile`
- `worker` - built from `apps/worker/Dockerfile`

Wait for all four to be `(healthy)` (or `Up` for the worker, which has no HTTP surface):

```bash
docker compose ps
```

### 2. Run the 200-payload E2E (one-shot driver)

```bash
docker compose --profile e2e up e2e-driver
```

The `e2e-driver` service (profile-gated) is a tiny image built from `Dockerfile.e2e` - it has only Python 3.14 and `httpx`, plus `data/webhook_payloads.json` baked in. It talks to the receiver over the in-network hostname `receiver:8000`, runs once, and exits with code 0.

Sample output:

```
=== Run 1/1 ===  Sent 200 in 0.5s (392.5 req/s)
HTTP 200: 165   HTTP 202: 35   HTTP 503: 0
By response status:
  accepted                125
  decrypt_failed          15
  discarded_non_approved  20
  duplicate               20
  schema_failed           20
e2e-driver-1 exited with code 0
```

### 3. Verify the database state

The breakdown matches `data/expected_summary_meta.json` exactly. `processed_events` (idempotency log) and `raw_payloads` (audit) reflect the same counts:

| Layer | Value | Source |
|-------|-------|--------|
| `raw_payloads.processing_status = 'accepted'` | **125** | HTTP 200, `order.approved`, payment approved |
| `raw_payloads.processing_status = 'duplicate'` | **20** | Natural key `(transaction_id, event)` already seen |
| `raw_payloads.processing_status = 'decrypt_failed'` | **15** | AES-256-CBC PKCS7 failure → DLQ `lead.dead.decrypt_failed` |
| `raw_payloads.processing_status = 'schema_failed'` | **20** | Pydantic validation error → DLQ `lead.dead.schema_failed` |
| `raw_payloads.processing_status = 'discarded_non_approved'` | **20** | `payment.status ≠ approved` (declined/refunded/pending) |
| `leads` | **125** | Unique by normalized email |
| `orders` | **125** | Unique by `transaction_id` (per spec) |
| `lead_events` | **125** | One `order.approved` per accepted order |
| `distribution_status` | **500** | 4 channels × 125 orders; updated by `dist.sms` consumer |
| `lead_dead_letter` | populated as failures are DLQ-persisted | (DLQ consumer that writes here is optional and pending) |

Inspect with:

```bash
docker exec  _test_jonatha-mysql-1 \
  mysql -uroot -p root   \
  -e "SELECT processing_status, COUNT(*) FROM raw_payloads GROUP BY processing_status;"
```

### 4. Tear down

```bash
docker compose down           # keeps the DB and RMQ volumes
docker compose down -v        # nukes the volumes too (clean slate)
```
