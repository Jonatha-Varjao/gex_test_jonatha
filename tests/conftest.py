import json
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

import pytest  # noqa: E402
import pytest_asyncio  # noqa: E402
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine  # noqa: E402
from testcontainers.mysql import MySqlContainer  # noqa: E402
from testcontainers.rabbitmq import RabbitMqContainer  # noqa: E402

from gex_common.config import (  # noqa: E402
    GATEWAY_GRUMMER,
    GATEWAY_LOUS,
    QUEUE_DIST_CALLCENTER,
    QUEUE_DIST_DLQ_SMS,
    QUEUE_DIST_EMAIL,
    QUEUE_DIST_SMS,
    QUEUE_DIST_WHATSAPP,
    QUEUE_DLQ_CONSUMER_FAILED,
    QUEUE_DLQ_DECRYPT_FAILED,
    QUEUE_DLQ_SCHEMA_FAILED,
    QUEUE_LEAD_RECEIVED,
)
from gex_receiver.publishers import RabbitMQPublisher  # noqa: E402

DATA_DIR = Path(__file__).parent.parent / "data"
SQL_DIR = Path(__file__).parent.parent / "sql"

# Testcontainers' Ryuk reaper needs the docker socket mounted into a sidecar
# container; with Docker Desktop this mount path can't be added to the
# file-sharing list, so Ryuk is disabled. The session-scoped containers
# die with the test process, so no reaper is needed.
import os  # noqa: E402

# Auto-detect the Docker socket — Docker Desktop on Linux exposes a per-user
# socket at ~/.docker/desktop/docker.sock. System docker uses /var/run/docker.sock.
# We try Desktop first so users not in the "docker" group can still run tests.
_DOCKER_CANDIDATES = [
    f"unix://{Path.home()}/.docker/desktop/docker.sock",
    "unix:///var/run/docker.sock",
]
for _candidate in _DOCKER_CANDIDATES:
    _socket_path = _candidate.replace("unix://", "")
    if Path(_socket_path).exists():
        os.environ.setdefault("DOCKER_HOST", _candidate)
        break

os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")


@pytest.fixture(scope="session")
def webhook_payloads():
    with open(DATA_DIR / "webhook_payloads.json") as f:
        return json.load(f)


@pytest.fixture(scope="session")
def lous_payloads(webhook_payloads):
    return [p for p in webhook_payloads if p.get("gateway") == GATEWAY_LOUS]


@pytest.fixture(scope="session")
def grummer_payloads(webhook_payloads):
    return [p for p in webhook_payloads if p.get("gateway") == GATEWAY_GRUMMER]


@pytest.fixture(scope="session")
def grummer_encrypted_payloads(grummer_payloads):
    return [p for p in grummer_payloads if p.get("headers", {}).get("X-GR-Encrypted") == "true"]


@pytest.fixture
def valid_lous_body(lous_payloads):
    return lous_payloads[0]["body"]


@pytest.fixture
def first_grummer_encrypted(grummer_encrypted_payloads):
    return grummer_encrypted_payloads[0]


_QUEUES_TO_PURGE = [
    QUEUE_LEAD_RECEIVED,
    QUEUE_DLQ_DECRYPT_FAILED,
    QUEUE_DLQ_SCHEMA_FAILED,
    QUEUE_DLQ_CONSUMER_FAILED,
    QUEUE_DIST_SMS,
    QUEUE_DIST_EMAIL,
    QUEUE_DIST_CALLCENTER,
    QUEUE_DIST_WHATSAPP,
    QUEUE_DIST_DLQ_SMS,
]


@pytest.fixture(scope="session")
def rabbitmq_container():
    """Spin up a RabbitMQ 4.2-management testcontainer (session-scoped)."""
    with RabbitMqContainer("rabbitmq:4.2-management") as rmq:
        yield rmq


@pytest_asyncio.fixture
async def rmq_publisher(rabbitmq_container):
    """RabbitMQPublisher connected to the test container. Purges all known
    queues after each test so the next test starts with a clean slate.
    """
    publisher = RabbitMQPublisher(_amqp_url(rabbitmq_container))
    await publisher.connect()
    await publisher.declare_topology()
    yield publisher

    if publisher._channel is not None:
        for queue_name in _QUEUES_TO_PURGE:
            try:
                q = await publisher._channel.declare_queue(queue_name, durable=True, passive=True)
                await q.purge()
            except Exception:
                pass
    await publisher.close()


@pytest_asyncio.fixture
async def rmq_test_channel(rabbitmq_container):
    """A separate channel for assertions in tests (not the publisher's)."""
    import aio_pika

    connection = await aio_pika.connect_robust(_amqp_url(rabbitmq_container))
    channel = await connection.channel()
    yield channel
    await connection.close()


def _amqp_url(container) -> str:
    """Build an amqp:// URL from a testcontainers RabbitMqContainer."""
    p = container.get_connection_params()
    creds = p.credentials
    return f"amqp://{creds.username}:{creds.password}@{p.host}:{p.port}{p.virtual_host}"


# ----------------------------------------------------------------------------
# MySQL testcontainer fixtures (used by tests/gex_receiver/test_db_integration.py)
# ----------------------------------------------------------------------------


_MYSQL_SCHEMA_SCRIPTS = [
    "001_create_tables.sql",
    "002_indexes.sql",
    "003_stored_procs.sql",
]


def _split_sql_statements(sql_text: str) -> list[str]:
    """Split a multi-statement SQL file into individual statements.

    Handles two statement formats:
      - Regular SQL terminated with ``;``
      - Stored procedures wrapped in ``DELIMITER // ... //`` blocks
    ``DELIMITER`` is a mysql CLI directive, not valid SQL — we strip it
    entirely and merge procedure bodies into single statements.
    """
    statements: list[str] = []
    current: list[str] = []
    in_delimiter_block = False
    for raw_line in sql_text.splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        if stripped.upper().startswith("DELIMITER"):
            in_delimiter_block = "//" in stripped
            continue
        current.append(raw_line)
        if in_delimiter_block:
            if stripped.endswith("//"):
                merged = "\n".join(current).strip()
                if merged.endswith("//"):
                    merged = merged[:-2].strip()
                statements.append(merged)
                current = []
        else:
            if stripped.endswith(";"):
                merged = "\n".join(current).strip().rstrip(";")
                statements.append(merged)
                current = []
    tail = "\n".join(current).strip()
    if tail:
        statements.append(tail)
    return [s for s in statements if s]


def _mysql_async_url(container) -> str:
    """Build a SQLAlchemy async URL from a testcontainers MySqlContainer."""
    user = container.username
    pwd = container.password
    host = container.get_container_host_ip()
    port = container.get_exposed_port(3306)
    db = container.dbname
    return f"mysql+asyncmy://{user}:{pwd}@{host}:{port}/{db}"


@pytest.fixture(scope="session")
def mysql_container():
    """Spin up a MySQL 8.4 testcontainer (session-scoped) and initialize
    the schema by running 001_create_tables.sql, 002_indexes.sql, and
    003_stored_procs.sql from the host via asyncmy.
    """
    import asyncio

    with MySqlContainer("mysql:8.4") as container:

        async def _init_schema():
            dsn_sync = (
                f"mysql+asyncmy://{container.username}:{container.password}"
                f"@{container.get_container_host_ip()}:"
                f"{container.get_exposed_port(3306)}/{container.dbname}"
            )
            engine = create_async_engine(dsn_sync, echo=False)
            try:
                from sqlalchemy import text

                # Database inherits MySQL 8.4's character set defaults;
                # all tables use the default utf8mb4 collation.
                for script_name in _MYSQL_SCHEMA_SCRIPTS:
                    sql_path = SQL_DIR / script_name
                    sql_text = sql_path.read_text()
                    statements = [
                        s
                        for s in _split_sql_statements(sql_text)
                        if not s.upper().startswith(("CREATE DATABASE", "USE "))
                    ]
                    if not statements:
                        continue
                    async with engine.begin() as conn:
                        for stmt in statements:
                            await conn.execute(text(stmt))
            finally:
                await engine.dispose()

        asyncio.run(_init_schema())
        yield container


@pytest_asyncio.fixture
async def db_session(mysql_container):
    """Yield an AsyncSession against the MySQL testcontainer.

    Each test runs against a clean schema: we TRUNCATE every table before
    yielding so order-dependent tests don't bleed state.
    """
    engine = create_async_engine(_mysql_async_url(mysql_container), echo=False)
    factory = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)

    from sqlalchemy import text

    async with engine.begin() as conn:
        await conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
        for table in (
            "lead_dead_letter",
            "distribution_status",
            "lead_events",
            "orders",
            "leads",
            "processed_events",
            "raw_payloads",
        ):
            await conn.execute(text(f"TRUNCATE TABLE {table}"))
        await conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))

    async with factory() as session:
        yield session

    await engine.dispose()


@pytest_asyncio.fixture
async def db_engine(mysql_container):
    """Yield the raw async engine for tests that need it (e.g. concurrency)."""
    engine = create_async_engine(_mysql_async_url(mysql_container), echo=False)
    yield engine
    await engine.dispose()
