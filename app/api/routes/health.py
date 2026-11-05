"""Liveness and readiness probes. Not behind API-key auth, so orchestrators can call them."""

import logging

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

from app import db
from app.api.errors import error_body, error_responses
from app.celery_app import celery_app
from app.config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/health", tags=["Health"])


@router.get("", summary="Liveness probe")
async def liveness() -> dict:
    """200 whenever the API process is running. Checks no dependencies — use `/health/ready` for that."""
    return {"status": "ok", "service": settings.APP_NAME, "version": settings.APP_VERSION}


def _check_broker() -> None:
    with celery_app.connection_for_write() as conn:
        conn.ensure_connection(max_retries=1, timeout=2)


@router.get("/ready", summary="Readiness probe", responses=error_responses(503))
async def readiness():
    """
    200 only if the job database and the Redis broker both respond; otherwise
    503 with each check's outcome in `error.details`. Point load-balancer
    health checks here so traffic only reaches an API that can queue work.
    """
    checks: dict[str, str] = {}
    try:
        await db.ping()
        checks["database"] = "ok"
    except Exception as exc:
        checks["database"] = f"error: {exc}"
    try:
        await run_in_threadpool(_check_broker)
        checks["broker"] = "ok"
    except Exception as exc:
        checks["broker"] = f"error: {exc}"

    if all(result == "ok" for result in checks.values()):
        return {"status": "ready", "checks": checks}
    logger.warning("Readiness check failed: %s", checks)
    return JSONResponse(status_code=503, content=error_body(503, "Not ready.", details=checks))
