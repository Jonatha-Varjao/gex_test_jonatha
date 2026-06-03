"""Send webhook payloads from data/webhook_payloads.json to a running gex-receiver.

Usage:
    uv run python scripts/load_payloads.py
    uv run python scripts/load_payloads.py --repeat 10 --concurrency 50 --receiver http://localhost:8000
"""

import argparse
import asyncio
import json
import sys
import time
from collections import Counter
from pathlib import Path

import httpx

PROJECT_ROOT = Path(__file__).parent.parent
DEFAULT_RECEIVER = "http://localhost:8000"
DEFAULT_REPEAT = 1
DEFAULT_CONCURRENCY = 20
ALLOWED_HEADERS = {"content-type", "x-gr-encrypted", "x-correlation-id"}
MAX_RETRIES = 3


def _filter_headers(raw: dict) -> dict:
    return {k: v for k, v in raw.items() if k.lower() in ALLOWED_HEADERS}


async def _send_one(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    receiver: str,
    payload: dict,
) -> tuple[int, str | None]:
    url = f"{receiver}/webhooks/{payload['gateway']}"
    headers = _filter_headers(payload.get("headers", {}))
    body = payload["body"]

    for attempt in range(MAX_RETRIES + 1):
        try:
            async with sem:
                resp = await client.post(url, json=body, headers=headers, timeout=10.0)
            if resp.status_code != 503:
                status = None
                if resp.headers.get("content-type", "").startswith("application/json"):
                    try:
                        status = resp.json().get("status")
                    except Exception:
                        pass
                return resp.status_code, status
        except httpx.RequestError, httpx.TimeoutException:
            if attempt == MAX_RETRIES:
                return 0, None
        await asyncio.sleep(2**attempt)
    return 503, None


async def _run_one(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    receiver: str,
    payloads: list[dict],
) -> tuple[Counter, Counter, int]:
    tasks = [_send_one(client, sem, receiver, p) for p in payloads]
    results = await asyncio.gather(*tasks)
    http_codes = Counter(r[0] for r in results)
    statuses = Counter(r[1] for r in results if r[1] is not None)
    return http_codes, statuses, len(results)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Load-test gex-receiver by POSTing webhook_payloads.json"
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=DEFAULT_REPEAT,
        help="Number of times to send all 200 payloads (default: %(default)s)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help="Max in-flight POST requests (default: %(default)s)",
    )
    parser.add_argument(
        "--receiver",
        default=DEFAULT_RECEIVER,
        help="GEX receiver base URL (default: %(default)s)",
    )
    args = parser.parse_args()

    payloads_path = PROJECT_ROOT / "data" / "webhook_payloads.json"
    payloads = json.loads(payloads_path.read_text())
    total_payloads = len(payloads)
    print(f"Loaded {total_payloads} payloads from {payloads_path}")

    limits = httpx.Limits(
        max_connections=args.concurrency + 10,
        max_keepalive_connections=args.concurrency,
    )

    total_http: Counter = Counter()
    total_status: Counter = Counter()
    overall_start = time.perf_counter()

    async def run_all() -> None:
        nonlocal total_http, total_status
        async with httpx.AsyncClient(limits=limits) as client:
            for i in range(1, args.repeat + 1):
                sem = asyncio.Semaphore(args.concurrency)
                start = time.perf_counter()
                http_codes, statuses, sent = await _run_one(client, sem, args.receiver, payloads)
                elapsed = time.perf_counter() - start
                rate = sent / elapsed if elapsed > 0 else 0.0

                print(
                    f"\n=== Run {i}/{args.repeat} ===  Sent {sent} in {elapsed:.1f}s"
                    f" ({rate:.1f} req/s)"
                )
                other = sum(v for k, v in http_codes.items() if k not in (200, 202, 503))
                print(
                    f"HTTP 200: {http_codes.get(200, 0)}   "
                    f"HTTP 202: {http_codes.get(202, 0)}   "
                    f"HTTP 503: {http_codes.get(503, 0)}   "
                    f"Other: {other}"
                )
                if statuses:
                    print("By response status:")
                    for st, count in sorted(statuses.items()):
                        print(f"  {st:30s} {count}")

                total_http.update(http_codes)
                total_status.update(statuses)

    asyncio.run(run_all())

    total_elapsed = time.perf_counter() - overall_start
    total_sent = sum(total_http.values())
    errors = total_http.get(0, 0) + total_http.get(503, 0)

    print(f"{'=' * 50}")
    print(
        f"=== TOTAL ===  {total_sent} sent in {total_elapsed:.1f}s"
        f" ({total_sent / total_elapsed:.1f} req/s)"
    )
    other_total = sum(v for k, v in total_http.items() if k not in (200, 202, 503))
    print(
        f"HTTP 200: {total_http.get(200, 0)}   "
        f"HTTP 202: {total_http.get(202, 0)}   "
        f"HTTP 503: {total_http.get(503, 0)}   "
        f"Other: {other_total}"
    )
    if total_status:
        print("By response status:")
        for st, count in sorted(total_status.items()):
            print(f"  {st:30s} {count}")

    if errors:
        print(f"\n⚠ {errors} request(s) had errors (503 or connection failure)")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
