"""
Authentication, authorization (scopes), tenancy, rate limits and the audit log.

Two kinds of API key, both sent in the `X-API-Key` header:

* **Bootstrap keys** — the comma-separated `API_KEYS` env var. Full access
  (every scope, every tenant). Use them to operate the service and to create
  managed keys. Several may be active at once, so they rotate without downtime.
* **Managed keys** — created with `POST /v1/admin/api-keys`. Each has:
    - `scopes`: what it may do (see `SCOPES`);
    - `tenant` (optional): it sees and creates only that tenant's jobs,
      schedules and workflows; NULL means every tenant;
    - `allowed_tasks` (optional): the only tasks it may run or schedule;
    - `rate_limit_per_minute` (optional): see `app.ratelimit`.
  Only a SHA-256 hash is stored; the key is shown once, at creation.

If `API_KEYS` is empty, auth is disabled (a warning is logged at startup) and
every request acts as an anonymous admin. Local development only.

Every route declares the scope it needs: `Depends(require("jobs:read"))`.
"""

import hashlib
import logging
import secrets
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

from fastapi import HTTPException, Request, Security
from fastapi.security import APIKeyHeader
from starlette.concurrency import run_in_threadpool

from app import db, ratelimit
from app.config import settings

logger = logging.getLogger(__name__)

API_KEY_HEADER = "X-API-Key"

#: Every scope a key can hold. "admin" implies all of them.
SCOPES = {
    "jobs:read": "List and inspect jobs and workflows, stream job events",
    "jobs:write": "Submit, retry and cancel jobs and workflows",
    "schedules:read": "List and inspect schedules",
    "schedules:write": "Create, change and delete schedules",
    "admin": "Manage API keys, read the audit log; implies every other scope",
}

#: Prefix of managed keys, so leaked keys are easy to recognise (e.g. by secret scanners).
KEY_PREFIX = "jsk_"

_api_key_header = APIKeyHeader(name=API_KEY_HEADER, auto_error=False)


@dataclass(frozen=True)
class Principal:
    """Who is calling, and what they may do."""

    name: str
    scopes: frozenset[str]
    tenant: Optional[str] = None  # None = every tenant
    allowed_tasks: Optional[frozenset[str]] = None  # None = every public task
    rate_limit_per_minute: Optional[int] = None
    key_id: Optional[int] = None  # managed keys only
    extra: dict = field(default_factory=dict, compare=False)

    def has(self, scope: str) -> bool:
        return "admin" in self.scopes or scope in self.scopes

    def may_run(self, task_name: str) -> bool:
        return self.allowed_tasks is None or task_name in self.allowed_tasks

    def owns(self, row_tenant: Optional[str]) -> bool:
        """True if this principal may see a row belonging to `row_tenant`."""
        return self.tenant is None or row_tenant == self.tenant


ANONYMOUS = Principal(name="anonymous", scopes=frozenset({"admin"}))


def hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


def generate_key() -> str:
    return KEY_PREFIX + secrets.token_urlsafe(32)


# ---------------------------------------------------------------------------
# Managed-key lookup, cached briefly per process
# ---------------------------------------------------------------------------
_cache_lock = threading.Lock()
_cache: dict[str, tuple[float, Optional[Principal]]] = {}
_last_touched: dict[int, float] = {}


def clear_key_cache() -> None:
    """Forget cached keys (after a key is revoked, so this replica stops accepting it at once)."""
    with _cache_lock:
        _cache.clear()


async def _managed_principal(key: str) -> Optional[Principal]:
    digest = hash_key(key)
    now = time.monotonic()
    with _cache_lock:
        hit = _cache.get(digest)
    if hit and now - hit[0] < settings.API_KEY_CACHE_SECONDS:
        return hit[1]

    row = await db.get_active_api_key(digest)
    principal = None
    if row is not None:
        principal = Principal(
            name=row["name"],
            scopes=frozenset(row["scopes"]),
            tenant=row["tenant"],
            allowed_tasks=frozenset(row["allowed_tasks"]) if row["allowed_tasks"] is not None else None,
            rate_limit_per_minute=row["rate_limit_per_minute"],
            key_id=row["id"],
        )
        if now - _last_touched.get(row["id"], 0) > 60:  # record use at most once a minute per key
            _last_touched[row["id"]] = now
            await db.touch_api_key(row["id"])
    with _cache_lock:
        if len(_cache) > 10_000:
            _cache.clear()
        _cache[digest] = (now, principal)
    return principal


async def authenticate(request: Request, api_key: Optional[str] = Security(_api_key_header)) -> Principal:
    """FastAPI dependency: identify the caller (401 if unknown) and apply its rate limit (429)."""
    cached = getattr(request.state, "principal", None)
    if cached is not None:
        return cached

    if not settings.API_KEYS:
        principal = ANONYMOUS
    elif not api_key:
        raise _unauthorized()
    else:
        index = next((i for i, key in enumerate(settings.API_KEYS) if secrets.compare_digest(api_key, key)), None)
        if index is not None:
            # compare_digest takes the same time whether or not the key matches,
            # so response timing doesn't leak how much of a guess was right.
            principal = Principal(name=f"bootstrap-key-{index + 1}", scopes=frozenset({"admin"}))
        else:
            principal = await _managed_principal(api_key)
            if principal is None:
                raise _unauthorized()

    limit = principal.rate_limit_per_minute or settings.DEFAULT_RATE_LIMIT_PER_MINUTE
    decision = await run_in_threadpool(ratelimit.check, principal.name, limit)
    if decision.limit:
        request.state.rate_limit_headers = {
            "X-RateLimit-Limit": str(decision.limit),
            "X-RateLimit-Remaining": str(decision.remaining),
            "X-RateLimit-Reset": str(decision.reset_in),
        }
    if not decision.allowed:
        try:
            await db.increment_counter("rate_limited")
        except Exception:
            logger.debug("Could not count rate_limited", exc_info=True)
        raise HTTPException(
            status_code=429,
            detail=f"Rate limit of {decision.limit} requests per minute exceeded; retry in {decision.reset_in} s.",
            headers={"Retry-After": str(decision.reset_in), **request.state.rate_limit_headers},
        )

    request.state.principal = principal
    return principal


def _unauthorized() -> HTTPException:
    return HTTPException(
        status_code=401,
        detail=f"Missing or invalid API key. Send it in the {API_KEY_HEADER} header.",
        headers={"WWW-Authenticate": "ApiKey"},
    )


def require(scope: str):
    """Dependency factory: the caller must hold `scope` (403 otherwise). Returns the Principal."""
    if scope not in SCOPES:
        raise ValueError(f"Unknown scope {scope!r}")

    async def dependency(request: Request, api_key: Optional[str] = Security(_api_key_header)) -> Principal:
        principal = await authenticate(request, api_key)
        if not principal.has(scope):
            raise HTTPException(status_code=403, detail=f"This API key lacks the {scope!r} scope.")
        return principal

    return dependency


#: Backwards-compatible name used by older imports: any valid key.
require_api_key = authenticate


def check_task_allowed(principal: Principal, task_name: str) -> None:
    if not principal.may_run(task_name):
        raise HTTPException(status_code=403, detail=f"This API key may not run {task_name!r}.")


def scoped_key(principal: Principal, idempotency_key: Optional[str]) -> Optional[str]:
    """
    The stored form of a client's Idempotency-Key: prefixed with the tenant, so
    two tenants using the same key never see each other's jobs.
    """
    if idempotency_key is None or principal.tenant is None:
        return idempotency_key
    return f"tenant:{principal.tenant}:{idempotency_key}"


async def audit(
    request: Request,
    principal: Principal,
    action: str,
    resource_type: Optional[str] = None,
    resource_id: Optional[str] = None,
    details: Optional[dict] = None,
) -> None:
    """Record who did what. Never fails the request: an audit write error is logged."""
    try:
        await db.add_audit_entry(
            actor=principal.name,
            action=action,
            tenant=principal.tenant,
            resource_type=resource_type,
            resource_id=str(resource_id) if resource_id is not None else None,
            details=details,
            client_ip=request.client.host if request.client else None,
        )
    except Exception:
        logger.exception("Could not write audit entry %s for %s", action, principal.name)
