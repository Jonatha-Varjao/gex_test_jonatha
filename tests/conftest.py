import json
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

import pytest  # noqa: E402
import pytest_asyncio  # noqa: E402
from testcontainers.rabbitmq import RabbitMqContainer  # noqa: E402

from gex_common.config import (  # noqa: E402
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

# Testcontainers' Ryuk reaper needs the docker socket mounted into a sidecar
# container; with Docker Desktop this mount path can't be added to the
# file-sharing list, so Ryuk is disabled. The session-scoped RabbitMQ
# testcontainer dies with the test process, so no reaper is needed.
# Developers point testcontainers at their docker socket via the standard
# DOCKER_HOST env var, e.g.:
#   export DOCKER_HOST="unix://$HOME/.docker/run/docker.sock"  # rootless
#   export DOCKER_HOST="unix://$HOME/.docker/desktop/docker.sock"  # Docker Desktop
#   export DOCKER_HOST="unix:///var/run/docker.sock"  # system docker (also the default)
import os  # noqa: E402

os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")


@pytest.fixture(scope="session")
def webhook_payloads():
    with open(DATA_DIR / "webhook_payloads.json") as f:
        return json.load(f)


@pytest.fixture(scope="session")
def grummer_secret():
    return (DATA_DIR / "grummer_secret.txt").read_text().strip()


@pytest.fixture(scope="session")
def lous_payloads(webhook_payloads):
    return [p for p in webhook_payloads if p.get("gateway") == "lous"]


@pytest.fixture(scope="session")
def grummer_payloads(webhook_payloads):
    return [p for p in webhook_payloads if p.get("gateway") == "grummer"]


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
