# syntax=docker/dockerfile:1.7
# ─────────────────────────────────────────────────────────────────────────────
# gex-worker production image
#
# Build context: REPO ROOT (uv needs the workspace).
# Output:        lean runtime image that only contains the gex-worker +
#                gex-common .venv. No HTTP surface — this is an AMQP consumer.
#
# Mirrors apps/receiver/Dockerfile. The receiver image does NOT contain this
# code and vice-versa — modularization is preserved at the image layer.
# ─────────────────────────────────────────────────────────────────────────────

# ── Stage 1: builder ───────────────────────────────────────────────────────
FROM python:3.14-slim-trixie AS builder

# Build-time only: C toolchain + MySQL/MariaDB client headers for asyncmy.
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libc6-dev \
        libmariadb-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.11.17 /uv /uvx /bin/

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=0 \
    UV_NO_CACHE=1

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY libs/common/pyproject.toml libs/common/README.md libs/common/
COPY apps/worker/pyproject.toml apps/worker/

# (apps/receiver is intentionally NOT copied — different image.)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync \
        --package gex-worker \
        --frozen \
        --no-install-workspace \
        --no-dev \
        --no-default-groups

COPY libs/common/src libs/common/src
COPY apps/worker/src apps/worker/src

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync \
        --package gex-worker \
        --frozen \
        --no-dev \
        --no-default-groups \
        --no-editable

# ── Stage 2: runtime ────────────────────────────────────────────────────────
FROM python:3.14-slim-trixie

RUN groupadd --system --gid 10001 gex && \
    useradd --system --uid 10001 --gid gex --no-create-home --shell /usr/sbin/nologin gex

WORKDIR /app

# (Venv lives at /app/.venv in the builder, so the shebangs inside the
# entry-point scripts resolve correctly in the runtime stage too.)
COPY --from=builder --chown=gex:gex /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONFAULTHANDLER=1

USER gex

# No EXPOSE (FastStream AMQP consumer — no HTTP surface).
# No HEALTHCHECK (no HTTP to ping; container liveness suffices).
CMD ["faststream", "run", "gex_worker.main:app", "--workers", "1"]
