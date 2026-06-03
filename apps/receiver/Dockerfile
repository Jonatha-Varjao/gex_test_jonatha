# syntax=docker/dockerfile:1.7
# ─────────────────────────────────────────────────────────────────────────────
# gex-receiver production image
#
# Build context: REPO ROOT (uv needs the workspace).
# Output:        lean runtime image that only contains the gex-receiver +
#                gex-common .venv (no source code, no uv binary, no .git).
#
# Why --no-editable: the project is baked into site-packages as compiled
# bytecode. The final image carries .venv only — no apps/* or libs/* COPY.
# ─────────────────────────────────────────────────────────────────────────────

# ── Stage 1: builder ───────────────────────────────────────────────────────
FROM python:3.14-slim-trixie AS builder

# Build-time only: C toolchain + MySQL/MariaDB client headers for asyncmy's
# Cython extension. These are NOT carried into the runtime stage, keeping the
# final image lean.
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libc6-dev \
        libmariadb-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

# uv binary, pinned to the same version that produced uv.lock.
COPY --from=ghcr.io/astral-sh/uv:0.11.17 /uv /uvx /bin/

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=0 \
    UV_NO_CACHE=1

WORKDIR /app

# Copy ONLY what uv needs to resolve the workspace + dependencies.
# (apps/worker is intentionally NOT copied — it is a different image.)
COPY pyproject.toml uv.lock ./
COPY libs/common/pyproject.toml libs/common/README.md libs/common/
COPY apps/receiver/pyproject.toml apps/receiver/README.md apps/receiver/

# Layer 1: third-party deps + transitive workspace resolution, no project yet.
# Workspace pattern from https://docs.astral.sh/uv/guides/integration/docker/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync \
        --package gex-receiver \
        --frozen \
        --no-install-workspace \
        --no-dev \
        --no-default-groups

# Now copy the actual source for gex-receiver and gex-common and install them
# non-editable into the venv (bakes the code into site-packages).
COPY libs/common/src libs/common/src
COPY apps/receiver/src apps/receiver/src

# Layer 2: install the workspace members (gex-receiver + gex-common) non-editable.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync \
        --package gex-receiver \
        --frozen \
        --no-dev \
        --no-default-groups \
        --no-editable

# ── Stage 2: runtime ────────────────────────────────────────────────────────
FROM python:3.14-slim-trixie

# Non-root runtime user (uid 10001 to avoid collisions).
RUN groupadd --system --gid 10001 gex && \
    useradd --system --uid 10001 --gid gex --no-create-home --shell /usr/sbin/nologin gex

WORKDIR /app

# Copy just the populated venv from the builder.
# (The venv lives at /app/.venv in the builder, so the shebangs inside the
# entry-point scripts resolve correctly in the runtime stage too.)
COPY --from=builder --chown=gex:gex /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONFAULTHANDLER=1

USER gex

EXPOSE 8000

# Stdlib healthcheck (no extra deps in the prod image).
HEALTHCHECK --interval=5s --timeout=3s --retries=10 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8000/health',timeout=2).status==200 else 1)"

# 2 workers so the FastAPI process can handle a few concurrent webhooks;
# tune via compose env if needed.
CMD ["uvicorn", "gex_receiver.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
