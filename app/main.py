"""
FastAPI entry point: builds the app and mounts the routers in `app.api.routes`.

Run:
    uvicorn app.main:app --port 8000
    Docs: http://localhost:8000/docs (Swagger UI), /redoc (ReDoc)

The API only queues work and reads state; Celery workers run the tasks.
Every non-2xx response uses one error shape (see `app.api.errors`), and every
route except `/health*` requires an `X-API-Key` header when `API_KEYS` is set.
"""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI

from app.logging_config import setup_logging

setup_logging(log_filename="api.log")  # before other app imports, so their loggers use it

from app import db  # noqa: E402
from app.api.errors import register_exception_handlers  # noqa: E402
from app.api.routes import health, jobs, schedules, submit  # noqa: E402
from app.api.security import require_api_key  # noqa: E402
from app.celery_app import celery_app  # noqa: E402
from app.config import settings  # noqa: E402

# Workers ride out database outages by retrying result writes for minutes;
# the API must not — a status request during an outage should fail fast (503).
celery_app.conf.result_backend_max_retries = 1

logger = logging.getLogger(__name__)

TAGS_METADATA = [
    {"name": "Health", "description": "Liveness and readiness probes (no API key needed)."},
    {"name": "Tasks", "description": "Queue a task. Returns its `task_id` at once; the task runs on a worker."},
    {
        "name": "Jobs",
        "description": "List, inspect, retry or cancel submitted jobs. `/jobs` reads the job history; "
        "`/jobs/{task_id}/status` reads Celery's live result backend.",
    },
    {
        "name": "Schedules",
        "description": "Run a task once at a given time, every N seconds, or on a cron expression (UTC). "
        "Changes take effect without a redeploy. Each due run fires exactly once, however many "
        "scheduler replicas are running.",
    },
]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Create/upgrade the schema (unless migrations run separately), and warn if auth is off."""
    if settings.AUTO_MIGRATE:
        await db.init_db()
    if not settings.API_KEYS:
        logger.warning("API_KEYS is not set: the API accepts unauthenticated requests. Never expose it like this.")
    yield


app = FastAPI(
    title="Job Scheduler & Queue API",
    description=(
        "Queue background jobs, follow their progress, and schedule them at runtime.\n\n"
        "**Auth:** send `X-API-Key: <key>` on every request except `/health*` "
        "(click *Authorize* above).\n\n"
        "**Safe retries:** send an `Idempotency-Key` header on POST requests; repeating a request "
        "with the same key returns the original job or schedule instead of creating another.\n\n"
        "**Errors:** every non-2xx response has the shape "
        '`{"error": {"status_code", "type", "message", "details"}}` — branch on `type`.'
    ),
    version=settings.APP_VERSION,
    openapi_tags=TAGS_METADATA,
    lifespan=lifespan,
)

register_exception_handlers(app)

app.include_router(health.router)
for router in (submit.router, jobs.router, schedules.router):
    app.include_router(router, dependencies=[Depends(require_api_key)])
