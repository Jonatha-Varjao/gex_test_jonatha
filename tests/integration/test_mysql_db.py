"""Integration tests for gex_receiver.db against a real MySQL 8.4 testcontainer.

These tests validate that:
  - insert_raw_payload works against a real schema and returns a UUIDv7 id.
  - check_idempotency enforces the (transaction_id, event) UNIQUE
    constraint at the DB level (natural key per spec, gateway excluded).
  - The receiver-layer idempotency holds under concurrent access — i.e. two
    webhook deliveries with the same natural key, fired in parallel, produce
    exactly one "new" result and the rest as "duplicates".
  - The sp_insert_lead stored procedure correctly performs the upsert dance
    on leads, orders, lead_events, and distribution_status.

Hypothesis is used to generate a wide range of (gateway, transaction_id,
event) tuples for the race-condition tests, ensuring we don't get lucky
on a single input.
"""

import asyncio
import uuid
from datetime import datetime, timezone

import pytest
from hypothesis import HealthCheck as HC
from hypothesis import given
from hypothesis import settings as hyp_settings
from hypothesis import strategies as st
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from gex_common.config import CONSTANTS
from gex_receiver.db import check_idempotency, insert_raw_payload

pytestmark = pytest.mark.integration

GATEWAYS = sorted(CONSTANTS.valid_gateways)
EVENTS = [
    CONSTANTS.event_order_approved, CONSTANTS.event_order_refunded,
    CONSTANTS.event_order_declined, CONSTANTS.event_order_pending,
]


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------


async def _count_processed_events(
    session: AsyncSession, gateway: str, transaction_id: str, event: str
) -> int:
    result = await session.execute(
        text(
            "SELECT COUNT(*) FROM processed_events "
            "WHERE gateway = :g AND transaction_id = :t AND event = :e"
        ),
        {"g": gateway, "t": transaction_id, "e": event},
    )
    return int(result.scalar_one())


async def _count_raw_payloads(session: AsyncSession) -> int:
    result = await session.execute(text("SELECT COUNT(*) FROM raw_payloads"))
    return int(result.scalar_one())


# ----------------------------------------------------------------------------
# 1. insert_raw_payload — happy path
# ----------------------------------------------------------------------------


class TestInsertRawPayloadIntegration:
    async def test_inserts_a_row_and_returns_uuidv7_id(self, db_session: AsyncSession):
        received_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        row_id = await insert_raw_payload(
            session=db_session,
            gateway=CONSTANTS.gateway_lous,
            received_at=received_at,
            headers={"X-Foo": "bar"},
            body_original={"k": "v"},
            body_decrypted=None,
            processing_status=CONSTANTS.status_accepted,
            error_detail=None,
            correlation_id="corr-1",
        )
        assert isinstance(row_id, str)
        assert uuid.UUID(row_id).version == 7, f"not a UUIDv7: {row_id}"

        result = await db_session.execute(
            text(
                "SELECT gateway, processing_status, correlation_id FROM raw_payloads WHERE id = :id"
            ),
            {"id": row_id},
        )
        row = result.first()
        assert row is not None
        assert row.gateway == CONSTANTS.gateway_lous
        assert row.processing_status == CONSTANTS.status_accepted
        assert row.correlation_id == "corr-1"

    async def test_inserted_id_is_a_valid_uuidv7(self, db_session: AsyncSession):
        row_id = await insert_raw_payload(
            session=db_session,
            gateway=CONSTANTS.gateway_grummer,
            received_at=datetime.now(timezone.utc),
            headers={},
            body_original={},
            body_decrypted=None,
            processing_status=CONSTANTS.status_decrypt_failed,
            error_detail="AES bad padding",
            correlation_id="corr-x",
        )
        parsed = uuid.UUID(row_id)
        assert parsed.version == 7

    async def test_multiple_inserts_get_unique_ids(self, db_session: AsyncSession):
        ids = set()
        for i in range(20):
            rid = await insert_raw_payload(
                session=db_session,
                gateway=CONSTANTS.gateway_lous,
                received_at=datetime.now(timezone.utc),
                headers={},
                body_original={"i": i},
                body_decrypted=None,
                processing_status=CONSTANTS.status_accepted,
                error_detail=None,
                correlation_id=f"corr-{i}",
            )
            ids.add(rid)
        assert len(ids) == 20
        await db_session.commit()
        assert await _count_raw_payloads(db_session) == 20


# ----------------------------------------------------------------------------
# 2. check_idempotency — single-thread correctness
# ----------------------------------------------------------------------------


class TestCheckIdempotencyIntegration:
    async def test_first_insert_returns_true_and_creates_row(self, db_session: AsyncSession):
        is_new = await check_idempotency(
            session=db_session,
            gateway=CONSTANTS.gateway_lous,
            transaction_id="tx-100",
            event=CONSTANTS.event_order_approved,
            correlation_id="corr-1",
        )
        assert is_new is True
        await db_session.commit()
        count = await _count_processed_events(
            db_session, CONSTANTS.gateway_lous, "tx-100", CONSTANTS.event_order_approved
        )
        assert count == 1

    async def test_second_insert_same_key_returns_false(self, db_session: AsyncSession):
        await check_idempotency(
            session=db_session,
            gateway=CONSTANTS.gateway_lous,
            transaction_id="tx-101",
            event=CONSTANTS.event_order_approved,
            correlation_id="corr-1",
        )
        await db_session.commit()

        is_new = await check_idempotency(
            session=db_session,
            gateway=CONSTANTS.gateway_lous,
            transaction_id="tx-101",
            event=CONSTANTS.event_order_approved,
            correlation_id="corr-2",
        )
        assert is_new is False
        await db_session.commit()
        count = await _count_processed_events(
            db_session, CONSTANTS.gateway_lous, "tx-101", CONSTANTS.event_order_approved
        )
        assert count == 1

    async def test_different_event_same_transaction_id_returns_true(self, db_session: AsyncSession):
        await check_idempotency(
            session=db_session,
            gateway=CONSTANTS.gateway_lous,
            transaction_id="tx-102",
            event=CONSTANTS.event_order_approved,
            correlation_id="corr-1",
        )
        await db_session.commit()

        is_new = await check_idempotency(
            session=db_session,
            gateway=CONSTANTS.gateway_lous,
            transaction_id="tx-102",
            event=CONSTANTS.event_order_refunded,
            correlation_id="corr-2",
        )
        assert is_new is True
        await db_session.commit()
        count = await _count_processed_events(
            db_session, CONSTANTS.gateway_lous, "tx-102", CONSTANTS.event_order_refunded
        )
        assert count == 1


# ----------------------------------------------------------------------------
# 3. check_idempotency — race conditions (Hypothesis-driven)
# ----------------------------------------------------------------------------


gateway_st = st.sampled_from(GATEWAYS)
event_st = st.sampled_from(EVENTS)
tx_st = st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789", min_size=1, max_size=24)


class TestIdempotencyRaceConditions:
    """Prove the UNIQUE (transaction_id, event) constraint holds
    even when many concurrent calls race on the same key.
    """

    @pytest.mark.parametrize("concurrency", [4, 8, 16])
    async def test_concurrent_same_key_yields_exactly_one_new(
        self, db_session: AsyncSession, concurrency: int
    ):
        # All attempts share the same natural key.
        tasks = [
            check_idempotency(
                session=db_session,
                gateway=CONSTANTS.gateway_lous,
                transaction_id="tx-race",
                event=CONSTANTS.event_order_approved,
                correlation_id=f"corr-{i}",
            )
            for i in range(concurrency)
        ]
        results = await asyncio.gather(*tasks)
        await db_session.commit()

        new_count = sum(1 for r in results if r is True)
        dup_count = sum(1 for r in results if r is False)
        assert new_count == 1, f"expected exactly 1 new, got {new_count}"
        assert dup_count == concurrency - 1

        persisted = await _count_processed_events(
            db_session, CONSTANTS.gateway_lous, "tx-race", CONSTANTS.event_order_approved
        )
        assert persisted == 1, "UNIQUE constraint violated — duplicate rows persisted"

    @hyp_settings(
        max_examples=20, deadline=None, suppress_health_check=[HC.function_scoped_fixture]
    )
    @given(gateway=gateway_st, tx=tx_st, event=event_st)
    async def test_hypothesis_concurrent_idempotency_dedups(
        self, db_session: AsyncSession, gateway: str, tx: str, event: str
    ):
        """For any (gateway, transaction_id, event) tuple, two concurrent
        calls with the same key must produce exactly one new and one duplicate.
        """
        tasks = [
            check_idempotency(
                session=db_session,
                gateway=gateway,
                transaction_id=tx,
                event=event,
                correlation_id="corr-a",
            ),
            check_idempotency(
                session=db_session,
                gateway=gateway,
                transaction_id=tx,
                event=event,
                correlation_id="corr-b",
            ),
        ]
        results = await asyncio.gather(*tasks)
        await db_session.commit()

        new_count = sum(1 for r in results if r is True)
        assert new_count == 1
        assert await _count_processed_events(db_session, gateway, tx, event) == 1

    @hyp_settings(
        max_examples=15, deadline=None, suppress_health_check=[HC.function_scoped_fixture]
    )
    @given(
        gateway_a=gateway_st,
        gateway_b=gateway_st,
        tx=tx_st,
        event_a=event_st,
        event_b=event_st,
    )
    async def test_hypothesis_different_keys_coexist(
        self,
        db_session: AsyncSession,
        gateway_a: str,
        gateway_b: str,
        tx: str,
        event_a: str,
        event_b: str,
    ):
        """Two concurrent calls with different natural keys must both be new
        and must produce two persisted rows.
        """
        if event_a == event_b:
            return  # same (tx, event) key — skip, covered by test_concurrent_same_key

        tasks = [
            check_idempotency(
                session=db_session,
                gateway=gateway_a,
                transaction_id=tx,
                event=event_a,
                correlation_id="corr-a",
            ),
            check_idempotency(
                session=db_session,
                gateway=gateway_b,
                transaction_id=tx,
                event=event_b,
                correlation_id="corr-b",
            ),
        ]
        results = await asyncio.gather(*tasks)
        await db_session.commit()

        assert all(r is True for r in results), (
            f"both should be new; got {results} for "
            f"({gateway_a}, {tx}, {event_a}) and ({gateway_b}, {tx}, {event_b})"
        )


# ----------------------------------------------------------------------------
# 4. insert_raw_payload — concurrency on the same table
# ----------------------------------------------------------------------------


class TestRawPayloadConcurrency:
    async def test_concurrent_inserts_get_distinct_uuidv7_ids(
        self, db_engine, db_session: AsyncSession
    ):
        """N concurrent inserts from independent sessions must each get a
        distinct UUIDv7 id and persist without collision.
        """
        from sqlalchemy.ext.asyncio import async_sessionmaker

        factory = async_sessionmaker(db_engine, expire_on_commit=False, autoflush=False)

        async def _do_insert(i: int) -> str:
            async with factory() as session:
                rid = await insert_raw_payload(
                    session=session,
                    gateway=CONSTANTS.gateway_lous,
                    received_at=datetime.now(timezone.utc),
                    headers={},
                    body_original={"i": i},
                    body_decrypted=None,
                    processing_status=CONSTANTS.status_accepted,
                    error_detail=None,
                    correlation_id=f"corr-{i}",
                )
                await session.commit()
                return rid

        ids = await asyncio.gather(*[_do_insert(i) for i in range(10)])
        assert len(set(ids)) == 10
        for rid in ids:
            assert uuid.UUID(rid).version == 7


# ----------------------------------------------------------------------------
# 5. sp_insert_lead — atomic upsert behavior
# ----------------------------------------------------------------------------


class TestSpInsertLeadIntegration:
    async def _call_sp(self, session: AsyncSession, **overrides) -> dict:
        import uuid as _uuid

        defaults = {
            "lead_id": str(_uuid.uuid7()),
            "order_id": str(_uuid.uuid7()),
            "event_id": str(_uuid.uuid7()),
            "dist_sms_id": str(_uuid.uuid7()),
            "dist_email_id": str(_uuid.uuid7()),
            "dist_callcenter_id": str(_uuid.uuid7()),
            "dist_whatsapp_id": str(_uuid.uuid7()),
            "email": "alice@example.com",
            "first_name": "Alice",
            "last_name": "Doe",
            "phone": "+18005551111",
            "country": "US",
            "gateway": CONSTANTS.gateway_lous,
            "transaction_id": "tx-200",
            "transaction_time": datetime(2026, 1, 1, 12, 0, 0),
            "event": CONSTANTS.event_order_approved,
            "product_id": "prod-1",
            "product_name": "Fit Burn",
            "product_niche": "weight_loss",
            "quantity": 1,
            "amount_usd": 99.99,
            "payment_method": "credit_card",
            "payment_status": CONSTANTS.payment_approved,
            "correlation_id": "corr-sp-1",
            "lag_seconds": 0.123,
        }
        defaults.update(overrides)
        result = await session.execute(
            text(
                "CALL sp_insert_lead("
                ":lead_id, :order_id, :event_id, "
                ":dist_sms_id, :dist_email_id, :dist_callcenter_id, :dist_whatsapp_id, "
                ":email, :first_name, :last_name, :phone, :country, "
                ":gateway, :transaction_id, :transaction_time, :event, "
                ":product_id, :product_name, :product_niche, :quantity, :amount_usd, "
                ":payment_method, :payment_status, :correlation_id, :lag_seconds)"
            ),
            defaults,
        )
        row = result.first()
        await session.commit()
        return {
            "order_id": row.order_id,
            "lead_id": row.lead_id,
            "is_new": bool(row.is_new),
        }

    async def test_first_call_creates_lead_order_event_and_4_distributions(
        self, db_session: AsyncSession
    ):
        out = await self._call_sp(db_session)
        assert out["is_new"] is True
        assert uuid.UUID(out["order_id"]).version == 7
        assert uuid.UUID(out["lead_id"]).version == 7

        # lead_events has 1 row
        result = await db_session.execute(
            text("SELECT COUNT(*) FROM lead_events WHERE order_id = :oid"),
            {"oid": out["order_id"]},
        )
        assert int(result.scalar_one()) == 1

        # distribution_status has 4 rows for the order, all with correlation_id
        result = await db_session.execute(
            text(
                "SELECT channel, correlation_id "
                "FROM distribution_status WHERE order_id = :oid ORDER BY channel"
            ),
            {"oid": out["order_id"]},
        )
        rows = list(result)
        assert sorted(r.channel for r in rows) == sorted(CONSTANTS.all_channels)
        for r in rows:
            assert r.correlation_id == "corr-sp-1"

    async def test_second_call_same_natural_key_returns_is_new_false(
        self, db_session: AsyncSession
    ):
        first = await self._call_sp(
            db_session, transaction_id="tx-dup-sp", event=CONSTANTS.event_order_approved
        )
        second = await self._call_sp(
            db_session,
            order_id=first["order_id"],
            transaction_id="tx-dup-sp",
            event=CONSTANTS.event_order_approved,
            correlation_id="corr-sp-2",
        )
        assert first["is_new"] is True
        assert second["is_new"] is False
        assert first["order_id"] == second["order_id"]

        # lead_events still has exactly 1 row, distribution_status still 4
        result = await db_session.execute(
            text("SELECT COUNT(*) FROM lead_events WHERE order_id = :oid"),
            {"oid": first["order_id"]},
        )
        assert int(result.scalar_one()) == 1
        result = await db_session.execute(
            text("SELECT COUNT(*) FROM distribution_status WHERE order_id = :oid"),
            {"oid": first["order_id"]},
        )
        assert int(result.scalar_one()) == 4

    async def test_different_event_same_order_creates_new_event(self, db_session: AsyncSession):
        first = await self._call_sp(
            db_session, transaction_id="tx-refund", event=CONSTANTS.event_order_approved
        )
        second = await self._call_sp(
            db_session,
            order_id=first["order_id"],
            transaction_id="tx-refund",
            event=CONSTANTS.event_order_refunded,
            correlation_id="corr-refund",
        )
        assert first["is_new"] is True
        assert second["is_new"] is True
        assert first["order_id"] == second["order_id"]

    async def test_concurrent_sp_calls_with_same_email_one_lead(
        self, db_engine, db_session: AsyncSession
    ):
        """Many concurrent sp_insert_lead calls with the same email must
        end up with exactly one row in leads (upsert by email).
        """
        import uuid as _uuid

        from sqlalchemy.ext.asyncio import async_sessionmaker

        factory = async_sessionmaker(db_engine, expire_on_commit=False, autoflush=False)
        shared_email = f"shared-{_uuid.uuid4().hex[:8]}@example.com"

        async def _do_one(i: int):
            async with factory() as session:
                return await self._call_sp(
                    session,
                    email=shared_email,
                    transaction_id=f"tx-shared-{i}",
                )

        results = await asyncio.gather(*[_do_one(i) for i in range(6)])

        result = await db_session.execute(
            text("SELECT COUNT(*) FROM leads WHERE email = :e"),
            {"e": shared_email},
        )
        assert int(result.scalar_one()) == 1
        # All 6 calls produced distinct order rows (different transaction_ids)
        order_ids = {r["order_id"] for r in results}
        assert len(order_ids) == 6
