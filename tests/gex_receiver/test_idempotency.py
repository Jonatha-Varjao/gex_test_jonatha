"""Unit tests for the idempotency wrapper."""

from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from gex_receiver import idempotency as idempotency_module
from gex_receiver.idempotency import check_idempotency

pytestmark = pytest.mark.unit


class TestIdempotency:
    async def test_new_event_returns_true(self):
        session = AsyncMock(spec=AsyncSession)
        original = idempotency_module._check_idempotency
        idempotency_module._check_idempotency = AsyncMock(return_value=True)
        try:
            result = await check_idempotency(session, "lous", "tx-001", "order.approved", "corr-1")
            assert result is True
        finally:
            idempotency_module._check_idempotency = original

    async def test_duplicate_event_returns_false(self):
        session = AsyncMock(spec=AsyncSession)
        original = idempotency_module._check_idempotency
        idempotency_module._check_idempotency = AsyncMock(return_value=False)
        try:
            result = await check_idempotency(session, "lous", "tx-001", "order.approved", "corr-1")
            assert result is False
        finally:
            idempotency_module._check_idempotency = original
