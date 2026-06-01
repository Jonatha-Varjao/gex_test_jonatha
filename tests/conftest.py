import json
from pathlib import Path

import pytest

DATA_DIR = Path(__file__).parent.parent / "data"


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
