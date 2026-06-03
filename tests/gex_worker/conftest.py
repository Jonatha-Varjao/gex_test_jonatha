"""Fixtures for gex_worker unit tests.

Patterns mirror ``tests/gex_receiver/conftest.py`` (mocked DB, publisher) and
``tests/gex_receiver/test_publishers_unit.py`` (model helpers).
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from gex_common.config import CONSTANTS
from gex_common.models import (
    CustomerData,
    DistributionMessage,
    LeadReceivedMessage,
    PaymentData,
    ProductData,
)

# ---------------------------------------------------------------------------
# Helpers — factory functions for test models
# ---------------------------------------------------------------------------


def make_lead_msg(**overrides: object) -> LeadReceivedMessage:
    """Build a ``LeadReceivedMessage`` with sensible defaults."""
    defaults: dict = dict(
        event_id="0190b6c0-7c3e-7abc-9def-123456789012",
        correlation_id="corr-test",
        transaction_id="tx-001",
        transaction_time=datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        event=CONSTANTS.event_order_approved,
        gateway=CONSTANTS.gateway_grummer,
        customer=CustomerData(
            email="test@example.com",
            first_name="Test",
            country="US",
        ),
        product=ProductData(
            id="prod-1",
            name="Fit Burn",
            niche="weight_loss",
            quantity=1,
        ),
        payment=PaymentData(
            amount_usd=99.99,
            method="credit_card",
            status=CONSTANTS.payment_approved,
        ),
    )
    defaults.update(overrides)
    return LeadReceivedMessage(**defaults)


def make_dist_msg(**overrides: object) -> DistributionMessage:
    """Build a ``DistributionMessage`` with sensible defaults."""
    defaults: dict = dict(
        event_id="0190b6c0-7c3e-7abc-9def-123456789012",
        order_id="0190b6c0-7c3e-7abc-9def-987654321098",
        transaction_id="tx-001",
        channel=CONSTANTS.channel_sms,
        customer=CustomerData(
            email="test@example.com",
            first_name="Test",
            country="US",
        ),
        product=ProductData(
            id="prod-1",
            name="Fit Burn",
            niche="weight_loss",
            quantity=1,
        ),
        payment=PaymentData(
            amount_usd=99.99,
            method="credit_card",
            status=CONSTANTS.payment_approved,
        ),
        gateway=CONSTANTS.gateway_grummer,
        correlation_id="corr-test",
    )
    defaults.update(overrides)
    return DistributionMessage(**defaults)


# ---------------------------------------------------------------------------
# Module-level state reset
# ---------------------------------------------------------------------------


@pytest.fixture
def reset_worker_db_state() -> None:
    """Clear ``_engine`` / ``_factory`` module-level globals before & after each test."""
    import gex_worker.db as db_mod

    db_mod._engine = None
    db_mod._factory = None
    yield
    db_mod._engine = None
    db_mod._factory = None


# ---------------------------------------------------------------------------
# Monkey-patches for side-effect-heavy deps
# ---------------------------------------------------------------------------


@pytest.fixture
def patched_asyncio_sleep(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    """Replace ``asyncio.sleep`` with a no-op so ``RetryMiddleware`` doesn't wait."""
    mock = AsyncMock(return_value=None)
    monkeypatch.setattr("gex_worker.middleware.asyncio.sleep", mock)
    return mock


@pytest.fixture
def patched_random(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Deterministic ``random.random()`` — returns 1.0 (never fail) by default."""
    mock = MagicMock(return_value=1.0)
    monkeypatch.setattr("gex_worker.distributors.random.random", mock)
    return mock


# ---------------------------------------------------------------------------
# Fixture-wrapped factory functions (so test files don't need relative imports)
# ---------------------------------------------------------------------------


@pytest.fixture
def lead_msg() -> LeadReceivedMessage:
    return make_lead_msg()


@pytest.fixture
def dist_msg() -> DistributionMessage:
    return make_dist_msg()


@pytest.fixture
def fake_httpx_client(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Replace ``httpx.AsyncClient`` with a context manager that returns a mock client.

    The mock client's ``.post()`` returns a mock response with ``status_code=200``.
    """
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status = MagicMock(return_value=None)

    mock_client = MagicMock()
    mock_client.post = AsyncMock(return_value=mock_response)

    mock_client_class = MagicMock()
    mock_client_class.return_value.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client_class.return_value.__aexit__ = AsyncMock(return_value=None)

    monkeypatch.setattr("gex_worker.distributors.httpx.AsyncClient", mock_client_class)
    return mock_client
