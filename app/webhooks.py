"""
Webhook delivery: tell the submitter when a job (or workflow) finishes.

How it works
------------
1. When a job with a `callback_url` reaches a final state, `db.update_job_status`
   inserts a `webhook_deliveries` row **in the same transaction** — a
   transactional outbox, so a finished job can't lose its webhook.
2. The scheduler service (every replica) runs `deliver_due_webhooks` in a loop.
   Each delivery is claimed with an optimistic version check, so one replica
   sends it at a time.
3. A 2xx answer marks it DELIVERED. Anything else is retried with exponential
   backoff (10 s, 20 s, 40 s … capped at 1 h) up to WEBHOOK_MAX_ATTEMPTS, then
   marked FAILED. Deliveries are therefore *at least once*: receivers should
   de-duplicate on the `X-JobScheduler-Delivery` header.

Verifying a webhook (receiver side)
-----------------------------------
    expected = "sha256=" + hmac_sha256(WEBHOOK_SIGNING_SECRET, f"{timestamp}.{raw_body}")
    accept if hmac.compare_digest(expected, X-JobScheduler-Signature)
         and abs(now - int(X-JobScheduler-Timestamp)) < 300   # rejects replays

`verify_signature` below implements exactly that.
"""

import asyncio
import fnmatch
import hashlib
import hmac
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import urlsplit

import httpx

from app import db
from app.config import settings

logger = logging.getLogger(__name__)

SIGNATURE_HEADER = "X-JobScheduler-Signature"
TIMESTAMP_HEADER = "X-JobScheduler-Timestamp"
DELIVERY_HEADER = "X-JobScheduler-Delivery"
EVENT_HEADER = "X-JobScheduler-Event"

#: Max deliveries sent at once by one scheduler replica.
_CONCURRENCY = 20


def check_callback_url(url: str) -> Optional[str]:
    """Return why `url` can't be used as a callback, or None if it can."""
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        return "callback_url must be an absolute http(s) URL"
    if parts.username or parts.password:
        return "callback_url must not contain credentials"
    allowed = settings.WEBHOOK_ALLOWED_HOSTS
    if allowed and not any(fnmatch.fnmatch(parts.hostname.lower(), pattern.lower()) for pattern in allowed):
        return f"callback_url host {parts.hostname!r} is not in WEBHOOK_ALLOWED_HOSTS"
    return None


def sign(body: bytes, timestamp: str, secret: Optional[str] = None) -> str:
    """The signature header value for `body` sent at `timestamp` (unix seconds, as a string)."""
    key = (secret if secret is not None else settings.WEBHOOK_SIGNING_SECRET).encode()
    digest = hmac.new(key, timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def verify_signature(body: bytes, timestamp: str, signature: str, secret: str, tolerance_seconds: int = 300) -> bool:
    """Receiver-side check: right signature, and sent within `tolerance_seconds` (no replays)."""
    try:
        fresh = abs(time.time() - int(timestamp)) <= tolerance_seconds
    except ValueError:
        return False
    return fresh and hmac.compare_digest(sign(body, timestamp, secret), signature)


def backoff(attempt: int) -> timedelta:
    """Delay before retry number `attempt` (1-based): 10 s doubling, capped at 1 hour."""
    return timedelta(seconds=min(10 * 2 ** (attempt - 1), 3600))


async def _send(client: httpx.AsyncClient, delivery: dict) -> None:
    body = json.dumps(delivery["payload"], separators=(",", ":"), default=str).encode()
    timestamp = str(int(time.time()))
    headers = {
        "Content-Type": "application/json",
        "User-Agent": f"{settings.APP_NAME}-webhooks/{settings.APP_VERSION}",
        EVENT_HEADER: delivery["event"],
        DELIVERY_HEADER: str(delivery["id"]),
        TIMESTAMP_HEADER: timestamp,
    }
    if settings.WEBHOOK_SIGNING_SECRET:
        headers[SIGNATURE_HEADER] = sign(body, timestamp)

    status_code, error = None, None
    problem = check_callback_url(delivery["url"])  # the allow-list may have changed since it was queued
    if problem:
        error = problem
    else:
        try:
            response = await client.post(delivery["url"], content=body, headers=headers)
            status_code = response.status_code
            if not response.is_success:
                error = f"HTTP {status_code}: {response.text[:200]}"
        except httpx.HTTPError as exc:
            error = f"{type(exc).__name__}: {exc}"[:500]

    attempt = delivery["attempts"] + 1
    delivered = error is None
    give_up = not delivered and (problem is not None or attempt >= settings.WEBHOOK_MAX_ATTEMPTS)
    retry_at = None if delivered or give_up else datetime.now(timezone.utc) + backoff(attempt)
    await db.record_webhook_attempt(delivery, delivered, status_code, error, retry_at)

    if delivered:
        await db.increment_counter("webhook_delivered")
        logger.info("[WEBHOOK] delivered id=%s event=%s to %s", delivery["id"], delivery["event"], delivery["url"])
    elif give_up:
        await db.increment_counter("webhook_failed")
        logger.error(
            "[WEBHOOK FAILED] id=%s to %s after %d attempts: %s", delivery["id"], delivery["url"], attempt, error
        )
    else:
        logger.warning("[WEBHOOK RETRY] id=%s attempt %d to %s: %s", delivery["id"], attempt, delivery["url"], error)


async def deliver_due_webhooks(client: Optional[httpx.AsyncClient] = None) -> int:
    """Send every due delivery this replica can claim. Returns how many were attempted."""
    lease = settings.WEBHOOK_TIMEOUT_SECONDS * 3 + 30  # longer than one attempt can take
    deliveries = await db.claim_due_webhooks(lease_seconds=lease)
    if not deliveries:
        return 0
    own_client = client is None
    client = client or httpx.AsyncClient(timeout=settings.WEBHOOK_TIMEOUT_SECONDS, follow_redirects=False)
    semaphore = asyncio.Semaphore(_CONCURRENCY)

    async def send_one(delivery: dict) -> None:
        async with semaphore:
            try:
                await _send(client, delivery)
            except Exception:
                logger.exception("Webhook delivery id=%s crashed; it will be retried", delivery["id"])

    try:
        await asyncio.gather(*(send_one(d) for d in deliveries))
    finally:
        if own_client:
            await client.aclose()
    return len(deliveries)
