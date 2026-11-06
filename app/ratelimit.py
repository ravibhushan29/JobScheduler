"""
Per-API-key rate limiting: at most N requests per key per minute.

The count is kept in Redis (the broker), so the limit holds across every API
replica: three replicas don't give a key three times its limit. It is a fixed
one-minute window (`ratelimit:<key name>:<minute>`, INCR + EXPIRE), which is
simple and cheap; a key can burst to 2N across a minute boundary at worst.

If Redis can't be reached the request is allowed (and logged): an outage of
the limiter must not take the whole API down. With the in-memory broker
(tests, local dev) counts are kept in this process instead.
"""

import logging
import threading
import time
from dataclasses import dataclass
from typing import Optional

from app.config import settings

logger = logging.getLogger(__name__)

WINDOW_SECONDS = 60


@dataclass(frozen=True)
class Decision:
    allowed: bool
    limit: int
    remaining: int
    reset_in: int  # seconds until the window resets


class _MemoryCounter:
    """Process-local fallback for the in-memory broker."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counts: dict[str, int] = {}

    def incr(self, key: str) -> int:
        with self._lock:
            if len(self._counts) > 10_000:  # drop old windows
                self._counts.clear()
            self._counts[key] = self._counts.get(key, 0) + 1
            return self._counts[key]


_memory = _MemoryCounter()
_redis_client = None
_redis_lock = threading.Lock()


def _redis():
    """A redis-py client for the broker, or None when the broker isn't Redis."""
    global _redis_client
    url = settings.CELERY_BROKER_URL
    if not url.startswith(("redis://", "rediss://", "sentinel://")):
        return None
    with _redis_lock:
        if _redis_client is None:
            import redis

            if url.startswith("sentinel://"):
                from redis.sentinel import Sentinel

                hosts = []
                for part in url.split(";"):
                    hostport = part.split("@")[-1].removeprefix("sentinel://").split("/")[0]
                    host, _, port = hostport.partition(":")
                    hosts.append((host, int(port or 26379)))
                sentinel = Sentinel(
                    hosts,
                    socket_timeout=0.5,
                    sentinel_kwargs={"password": settings.REDIS_PASSWORD} if settings.REDIS_PASSWORD else None,
                )
                _redis_client = sentinel.master_for(
                    settings.REDIS_SENTINEL_MASTER, password=settings.REDIS_PASSWORD or None, socket_timeout=0.5
                )
            else:
                _redis_client = redis.Redis.from_url(url, socket_timeout=0.5, socket_connect_timeout=0.5)
        return _redis_client


def check(key_name: str, limit: Optional[int]) -> Decision:
    """Count one request for `key_name` and decide whether it is within `limit` per minute."""
    if not limit or limit <= 0:
        return Decision(True, 0, 0, 0)
    now = time.time()
    window = int(now // WINDOW_SECONDS)
    reset_in = int(WINDOW_SECONDS - now % WINDOW_SECONDS) or WINDOW_SECONDS
    counter_key = f"ratelimit:{key_name}:{window}"

    client = _redis()
    try:
        if client is None:
            count = _memory.incr(counter_key)
        else:
            pipe = client.pipeline()
            pipe.incr(counter_key)
            pipe.expire(counter_key, WINDOW_SECONDS * 2)
            count = pipe.execute()[0]
    except Exception as exc:
        logger.warning("Rate limiter unavailable (%s); allowing the request", exc)
        return Decision(True, limit, limit, reset_in)
    return Decision(count <= limit, limit, max(limit - count, 0), reset_in)
