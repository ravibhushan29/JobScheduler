"""
`/admin` — managed API keys and the audit log. Needs the `admin` scope.

Keys: the secret is generated here, returned ONCE in the create response,
and stored only as a SHA-256 hash. Revoking takes effect at once on the API
replica that handled it, and within API_KEY_CACHE_SECONDS on the others.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from app import db
from app.api.errors import error_responses
from app.api.schemas import ApiKeyCreatedResponse, ApiKeyCreateRequest, ApiKeyResponse, AuditEntry
from app.api.security import Principal, audit, clear_key_cache, generate_key, hash_key, require

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["Admin"], responses=error_responses(401, 403, 429))
_admin = Depends(require("admin"))


@router.post(
    "/api-keys",
    response_model=ApiKeyCreatedResponse,
    status_code=201,
    responses=error_responses(409, 422),
    summary="Create an API key",
)
async def create_api_key(payload: ApiKeyCreateRequest, request: Request, principal: Principal = _admin):
    """
    Create a managed key with the given scopes, optional tenant, optional
    allowed tasks and optional rate limit. **The `key` in the answer is shown
    only once** — store it in your secret manager straight away.

    **Errors:** `409` the name is taken · `422` unknown scope or task.
    """
    key = generate_key()
    key_id = await db.create_api_key(
        name=payload.name,
        key_hash=hash_key(key),
        key_prefix=key[:10],
        scopes=payload.scopes,
        tenant=payload.tenant,
        allowed_tasks=payload.allowed_tasks,
        rate_limit_per_minute=payload.rate_limit_per_minute,
    )
    if key_id is None:
        raise HTTPException(status_code=409, detail=f"An API key named {payload.name!r} already exists.")
    await audit(
        request,
        principal,
        "apikey.create",
        "api_key",
        key_id,
        {"name": payload.name, "scopes": payload.scopes, "tenant": payload.tenant},
    )
    logger.info("API key %r (id=%s) created by %s", payload.name, key_id, principal.name)
    return ApiKeyCreatedResponse(**(await db.get_api_key(key_id)), key=key)


@router.get("/api-keys", response_model=list[ApiKeyResponse], summary="List API keys")
async def list_api_keys(principal: Principal = _admin) -> list[ApiKeyResponse]:
    """Every managed key, including revoked ones. Secrets are never returned."""
    return [ApiKeyResponse.model_validate(row) for row in await db.list_api_keys()]


@router.delete(
    "/api-keys/{key_id}", response_model=ApiKeyResponse, responses=error_responses(404), summary="Revoke an API key"
)
async def revoke_api_key(key_id: int, request: Request, principal: Principal = _admin) -> ApiKeyResponse:
    """Revoke a key. It stops working at once here and within a few seconds on every replica."""
    if not await db.revoke_api_key(key_id):
        raise HTTPException(status_code=404, detail=f"No active API key with id {key_id}.")
    clear_key_cache()
    await audit(request, principal, "apikey.revoke", "api_key", key_id)
    return ApiKeyResponse.model_validate(await db.get_api_key(key_id))


@router.get("/audit-log", response_model=list[AuditEntry], summary="Read the audit log")
async def read_audit_log(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    actor: Optional[str] = Query(default=None, description="API key name"),
    action: Optional[str] = Query(default=None, examples=["job.cancel"]),
    tenant: Optional[str] = Query(default=None),
    principal: Principal = _admin,
) -> list[AuditEntry]:
    """Who did what, newest first: submits, retries, cancels, schedule and key changes."""
    rows = await db.list_audit_entries(limit=limit, offset=offset, actor=actor, action=action, tenant=tenant)
    return [AuditEntry.model_validate(row) for row in rows]
