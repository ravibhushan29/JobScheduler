"""
API-key authentication.

Clients send a key in the `X-API-Key` header; it must match one of the
comma-separated keys in the `API_KEYS` env var. Several keys may be active at
once, so a key can be rotated without downtime: add the new one, move
clients over, then remove the old one.

If `API_KEYS` is empty, auth is disabled (and a warning is logged at startup).
"""

import secrets
from typing import Optional

from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader

from app.config import settings

API_KEY_HEADER = "X-API-Key"

_api_key_header = APIKeyHeader(name=API_KEY_HEADER, auto_error=False)


async def require_api_key(api_key: Optional[str] = Security(_api_key_header)) -> None:
    """FastAPI dependency: reject the request with 401 unless it carries a valid key."""
    if not settings.API_KEYS:
        return
    # compare_digest takes the same time whether or not the key matches, so
    # response timing doesn't leak how much of a guess was right.
    if api_key and any(secrets.compare_digest(api_key, key) for key in settings.API_KEYS):
        return
    raise HTTPException(
        status_code=401,
        detail=f"Missing or invalid API key. Send it in the {API_KEY_HEADER} header.",
        headers={"WWW-Authenticate": "ApiKey"},
    )
