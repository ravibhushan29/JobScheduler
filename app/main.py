"""
FastAPI entry point: builds the app and mounts the routers in `app.api.routes`.

Run:
    uvicorn app.main:app --port 8000
    Docs: http://localhost:8000/docs (Swagger UI), /redoc (ReDoc)
    Admin console: http://localhost:8000/ui

The API only queues work and reads state; Celery workers run the tasks.
Every non-2xx response uses one error shape (see `app.api.errors`), and every
route except `/health*`, `/metrics` and `/ui` requires an `X-API-Key` header
when `API_KEYS` is set.

Versioning: every endpoint lives under `/v1`. The original unversioned paths
(`/tasks/add`, `/jobs`, `/schedules`, …) are kept as aliases of v1 for existing
clients; they are hidden from the OpenAPI document and answer with a
`Deprecation` header pointing at their `/v1` path. See docs/18-api-versioning.md.
"""

import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, Request, Response
from fastapi.responses import FileResponse

from app.logging_config import setup_logging

setup_logging(log_filename="api.log")  # before other app imports, so their loggers use it

from app import db, metrics  # noqa: E402
from app.api.errors import register_exception_handlers  # noqa: E402
from app.api.routes import admin, health, jobs, schedules, submit, workflows  # noqa: E402
from app.api.security import authenticate  # noqa: E402
from app.celery_app import celery_app  # noqa: E402
from app.config import settings  # noqa: E402
from app.tracing import setup_tracing  # noqa: E402

# Workers ride out database outages by retrying result writes for minutes;
# the API must not — a status request during an outage should fail fast (503).
celery_app.conf.result_backend_max_retries = 1

logger = logging.getLogger(__name__)

TAGS_METADATA = [
    {"name": "Health", "description": "Liveness and readiness probes, and Prometheus metrics (no API key needed)."},
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
    {
        "name": "Workflows",
        "description": "Run jobs in dependency order: fan-out, fan-in, and skip-downstream-on-failure.",
    },
    {"name": "Admin", "description": "Managed API keys (scopes, tenants, rate limits) and the audit log."},
]

#: Routers served under /v1. The first three are also served unversioned (legacy aliases).
_LEGACY_ROUTERS = (submit.router, jobs.router, schedules.router)
_V1_ONLY_ROUTERS = (workflows.router, admin.router)
_STATIC = Path(__file__).parent / "static"


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
        "**Auth:** send `X-API-Key: <key>` on every request except `/health*` and `/metrics` "
        "(click *Authorize* above). Keys have scopes (`jobs:read`, `jobs:write`, `schedules:read`, "
        "`schedules:write`, `admin`) and may be limited to one tenant, some tasks, and a rate.\n\n"
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
setup_tracing("job-scheduler-api", fastapi_app=app)

app.include_router(health.router)
for router in _LEGACY_ROUTERS + _V1_ONLY_ROUTERS:
    app.include_router(router, prefix="/v1", dependencies=[Depends(authenticate)])
for router in _LEGACY_ROUTERS:
    app.include_router(router, dependencies=[Depends(authenticate)], include_in_schema=False)


@app.middleware("http")
async def _observe(request: Request, call_next):
    """HTTP metrics, rate-limit headers, and deprecation headers on unversioned paths."""
    started = time.perf_counter()
    response = await call_next(request)
    route = request.scope.get("route")
    template = getattr(route, "path", "unmatched")
    # Routers included with prefix="/v1" report their own path; add the version back.
    if route is not None and request.url.path.startswith("/v1/") and not template.startswith("/v1"):
        template = "/v1" + template
    metrics.HTTP_REQUESTS.labels(request.method, template, str(response.status_code)).inc()
    metrics.HTTP_LATENCY.labels(request.method, template).observe(time.perf_counter() - started)
    for name, value in getattr(request.state, "rate_limit_headers", {}).items():
        response.headers.setdefault(name, value)
    if template.startswith(("/tasks", "/jobs", "/schedules")):
        response.headers["Deprecation"] = "true"
        response.headers["Link"] = f'</v1{request.url.path}>; rel="successor-version"'
    return response


@app.get("/metrics", tags=["Health"], summary="Prometheus metrics", include_in_schema=settings.METRICS_ENABLED)
async def prometheus_metrics() -> Response:
    """
    Cluster-wide job, queue, schedule, webhook and workflow metrics, plus this
    process's HTTP metrics, in the Prometheus text format. See docs/10-operations.md.
    """
    if not settings.METRICS_ENABLED:
        return Response(status_code=404)
    from starlette.concurrency import run_in_threadpool

    body, content_type = await run_in_threadpool(metrics.render)
    return Response(content=body, media_type=content_type)


@app.get("/ui", include_in_schema=False)
async def admin_console() -> FileResponse:
    """The operator console: a single static page that calls this API with your key."""
    return FileResponse(
        _STATIC / "admin.html",
        headers={
            "Content-Security-Policy": "default-src 'self'; script-src 'self' 'unsafe-inline'; "
            "style-src 'self' 'unsafe-inline'; img-src 'self' data:; frame-ancestors 'none'",
            "X-Frame-Options": "DENY",
            "Referrer-Policy": "no-referrer",
            "Cache-Control": "no-store",
        },
    )
