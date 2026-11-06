"""
`/jobs` — submit, list, inspect, stream, retry and cancel jobs.

Two sources of truth:
  * the `jobs` table (job history) — `GET /jobs`, `GET /jobs/{id}`;
  * Celery's result backend (live state, incl. PROGRESS) — `GET /jobs/{id}/status`
    and `/events`. Its rows expire after CELERY_RESULT_EXPIRES; the endpoints then
    fall back to the job history.

Cancelling is cluster-wide: the job is marked REVOKED in the database, and
the execution guard in every worker refuses to run a REVOKED job, even if
the revoke broadcast never reached that worker.

Tenancy: a key bound to a tenant sees only that tenant's jobs; any other
task_id answers 404, exactly as if it didn't exist.
"""

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any, Optional

from celery import states
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse
from starlette.concurrency import run_in_threadpool

from app import db
from app.api.enqueue import submit_task
from app.api.errors import error_responses
from app.api.routes.submit import IdempotencyKey, to_response
from app.api.schemas import (
    BulkRetryRequest,
    BulkRetryResponse,
    CancelResponse,
    FailureGroup,
    FailureGroupsResponse,
    JobListResponse,
    JobRecord,
    JobSubmitRequest,
    TaskStatusResponse,
    TaskSubmitResponse,
    WebhookDelivery,
)
from app.api.security import Principal, audit, check_task_allowed, require, scoped_key
from app.celery_app import celery_app
from app.tasks import TASK_REGISTRY

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/jobs", tags=["Jobs"], responses=error_responses(401, 403, 429))
_reader = Depends(require("jobs:read"))
_writer = Depends(require("jobs:write"))

#: How often the event stream re-reads a job's state, and sends a keep-alive comment.
EVENT_POLL_SECONDS = 1.0
EVENT_KEEPALIVE_SECONDS = 15.0


async def _get_job_or_404(task_id: str, principal: Principal) -> dict:
    job = await db.get_job(task_id)
    if job is None or not principal.owns(job["tenant"]):
        raise HTTPException(status_code=404, detail=f"Unknown task_id: {task_id}")
    return job


def _json_safe(value: Any) -> Any:
    """Make an arbitrary value (e.g. an exception object) JSON-serializable."""
    if isinstance(value, BaseException):
        return {"exc_type": type(value).__name__, "exc_message": str(value)}
    return json.loads(json.dumps(value, default=str))


def _as_utc(value: Any) -> Optional[datetime]:
    """Celery's backend returns naive UTC datetimes; make them timezone-aware."""
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if isinstance(value, datetime) and value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value


# ---------------------------------------------------------------------------
# Submit and list
# ---------------------------------------------------------------------------
@router.post(
    "",
    response_model=TaskSubmitResponse,
    status_code=201,
    responses=error_responses(409, 422, 503),
    summary="Submit any public task",
)
async def submit_job(
    payload: JobSubmitRequest,
    request: Request,
    response: Response,
    idempotency_key: Optional[str] = IdempotencyKey,
    principal: Principal = _writer,
) -> TaskSubmitResponse:
    """
    Queue `task_name(*args, **kwargs)`, with optional per-job settings:
    `priority`, a delayed start (`delay_seconds` or `run_at`), `timeout_seconds`,
    `max_retries`, and a `callback_url` webhook called when it finishes.

    Send an `Idempotency-Key` header to make retries safe.

    **Errors:** `422` unknown task or arguments that don't fit it · `403` the
    key may not run this task · `409` Idempotency-Key reused for a different request.
    """
    check_task_allowed(principal, payload.task_name)
    submission = await submit_task(
        TASK_REGISTRY[payload.task_name],
        payload.args,
        payload.kwargs,
        idempotency_key=scoped_key(principal, idempotency_key),
        tenant=principal.tenant,
        priority=payload.priority,
        run_after=payload.start_time(),
        timeout_seconds=payload.timeout_seconds,
        max_retries=payload.max_retries,
        callback_url=payload.callback_url,
    )
    if submission.created:
        await audit(request, principal, "job.submit", "job", submission.task_id, {"task_name": payload.task_name})
    return to_response(submission, response)


@router.get("", response_model=JobListResponse, summary="List jobs")
async def list_jobs(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    status: Optional[str] = Query(default=None, description="Exact status, e.g. SUCCESS, FAILURE, RETRY, WAITING."),
    task_name: Optional[str] = Query(default=None, examples=["app.tasks.add"]),
    workflow_id: Optional[str] = Query(default=None),
    principal: Principal = _reader,
) -> JobListResponse:
    """Jobs from the job history, newest first, optionally filtered."""
    filters = {"status": status, "tenant": principal.tenant, "task_name": task_name, "workflow_id": workflow_id}
    rows = await db.list_jobs(limit=limit, offset=offset, **filters)
    return JobListResponse(
        total=await db.count_jobs(**filters),
        limit=limit,
        offset=offset,
        jobs=[JobRecord.from_row(row) for row in rows],
    )


# ---------------------------------------------------------------------------
# Dead-letter view (before /{task_id}, so "failures" isn't read as a task_id)
# ---------------------------------------------------------------------------
@router.get("/failures", response_model=FailureGroupsResponse, summary="Failed jobs, grouped by task and error")
async def list_failures(
    limit: int = Query(default=50, ge=1, le=500), principal: Principal = _reader
) -> FailureGroupsResponse:
    """
    The dead-letter view: FAILURE jobs that haven't been re-submitted yet,
    grouped by task and error message, biggest group first. Use
    `POST /jobs/failures/retry` to re-submit a whole group at once.
    """
    groups = await db.failure_groups(tenant=principal.tenant, limit=limit)
    return FailureGroupsResponse(groups=[FailureGroup.model_validate(group) for group in groups])


@router.post(
    "/failures/retry",
    response_model=BulkRetryResponse,
    responses=error_responses(422, 503),
    summary="Re-submit failed jobs in bulk",
)
async def retry_failures(
    payload: BulkRetryRequest, request: Request, principal: Principal = _writer
) -> BulkRetryResponse:
    """
    Re-submit up to `limit` FAILURE jobs that haven't been re-submitted yet,
    optionally only those of one `task_name` and/or with one exact `error`.
    Each becomes a new job with `retry_of` pointing at the failed one.

    Safe to repeat or to run twice at once: each failed job is re-submitted
    at most once (an internal idempotency key per failed job).
    """
    failed = await db.failed_jobs_to_retry(principal.tenant, payload.task_name, payload.error, payload.limit)
    retried = []
    for job in failed:
        task = TASK_REGISTRY.get(job["task_name"])
        if task is None or not principal.may_run(job["task_name"]):
            continue
        submission = await submit_task(
            task,
            job["args"],
            job["kwargs"],
            retry_of=job["task_id"],
            idempotency_key=f"bulk-retry:{job['task_id']}",
            tenant=job["tenant"],
            priority=db.PRIORITY_NAMES.get(job["priority"], "normal"),
            timeout_seconds=job["timeout_seconds"],
            max_retries=job["max_retries"],
            callback_url=job["callback_url"],
        )
        if submission.created:
            retried.append({"retry_of": job["task_id"], "task_id": submission.task_id})
    await audit(
        request,
        principal,
        "jobs.bulk_retry",
        "job",
        None,
        {"task_name": payload.task_name, "error": payload.error, "retried": len(retried)},
    )
    logger.info("Bulk retry by %s: %d jobs", principal.name, len(retried))
    return BulkRetryResponse(retried=len(retried), jobs=retried)


# ---------------------------------------------------------------------------
# One job
# ---------------------------------------------------------------------------
async def _live_status(task_id: str, job: Optional[dict]) -> TaskStatusResponse:
    meta = await run_in_threadpool(celery_app.backend.get_task_meta, task_id)
    state = meta.get("status", states.PENDING)

    if state == states.PENDING:
        record = JobRecord.from_row(job)
        return TaskStatusResponse(
            task_id=task_id,
            state=states.PENDING if record.status in ("QUEUED", db.WAITING) else record.status,
            source="job_record",
            result=record.result,
            error=record.error,
            traceback=record.traceback,
            name=record.task_name,
            args=record.args,
            kwargs=record.kwargs,
            retries=record.retries,
            worker=record.worker,
            queue=record.queue,
            date_done=record.finished_at,
        )
    if job is not None and job["status"] in db.TERMINAL_STATES and state not in db.TERMINAL_STATES:
        state = job["status"]  # e.g. cancelled while Celery still shows STARTED

    result = meta.get("result")
    return TaskStatusResponse(
        task_id=task_id,
        state=state,
        source="result_backend",
        result=_json_safe(result),
        error=str(result) if state == states.FAILURE else None,
        traceback=meta.get("traceback"),
        name=meta.get("name"),
        args=meta.get("args"),
        kwargs=meta.get("kwargs"),
        retries=meta.get("retries"),
        worker=meta.get("worker"),
        queue=meta.get("queue") or (job["queue"] if job else None),
        date_done=_as_utc(meta.get("date_done")),
        children=_json_safe(meta.get("children")),
    )


@router.get(
    "/{task_id}/status",
    response_model=TaskStatusResponse,
    responses=error_responses(404),
    summary="Live state from the result backend",
)
async def get_task_status(task_id: str, principal: Principal = _reader) -> TaskStatusResponse:
    """
    The task's current state as Celery records it, including PROGRESS updates
    from running tasks. If Celery has no record (never started, or its result
    expired) the state comes from the job history and `source` is `job_record`.

    **Errors:** `404` if the task_id is unknown (or belongs to another tenant).
    """
    job = await _get_job_or_404(task_id, principal)
    return await _live_status(task_id, job)


@router.get(
    "/{task_id}/events",
    responses={**error_responses(404), 200: {"content": {"text/event-stream": {}}}},
    summary="Stream live state changes (server-sent events)",
)
async def stream_job_events(task_id: str, request: Request, principal: Principal = _reader) -> StreamingResponse:
    """
    A [server-sent events](https://developer.mozilla.org/docs/Web/API/Server-sent_events)
    stream: one `status` event (the same JSON as `/status`) each time the
    state or progress changes, then the stream closes after a final state
    (SUCCESS, FAILURE, REVOKED). A `: keep-alive` comment is sent every 15 s.

    Use it instead of polling, from a server (or a browser behind your own backend):

        curl -N -H "X-API-Key: $KEY" $API/v1/jobs/$ID/events
    """
    await _get_job_or_404(task_id, principal)

    async def events() -> AsyncIterator[str]:
        last, quiet = None, 0.0
        while not await request.is_disconnected():
            status = await _live_status(task_id, await db.get_job(task_id))
            body = status.model_dump_json()
            if body != last:
                yield f"event: status\ndata: {body}\n\n"
                last, quiet = body, 0.0
                if status.state in db.TERMINAL_STATES:
                    return
            elif quiet >= EVENT_KEEPALIVE_SECONDS:
                yield ": keep-alive\n\n"
                quiet = 0.0
            await asyncio.sleep(EVENT_POLL_SECONDS)
            quiet += EVENT_POLL_SECONDS

    headers = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}  # no proxy buffering
    return StreamingResponse(events(), media_type="text/event-stream", headers=headers)


@router.get(
    "/{task_id}/webhooks",
    response_model=list[WebhookDelivery],
    responses=error_responses(404),
    summary="Webhook deliveries for a job",
)
async def list_job_webhooks(task_id: str, principal: Principal = _reader) -> list[WebhookDelivery]:
    """Each callback this job triggered, with its status, attempts and last error."""
    await _get_job_or_404(task_id, principal)
    return [WebhookDelivery.model_validate(row) for row in await db.list_webhook_deliveries(task_id)]


@router.get("/{task_id}", response_model=JobRecord, responses=error_responses(404), summary="One job's history record")
async def get_job(task_id: str, principal: Principal = _reader) -> JobRecord:
    """
    The full job record: status, result, failure reason and traceback, retry
    count, worker, options, and links to the schedule, workflow or job it came from.

    **Errors:** `404` if the task_id is not in the job history.
    """
    return JobRecord.from_row(await _get_job_or_404(task_id, principal))


@router.post(
    "/{task_id}/retry",
    response_model=TaskSubmitResponse,
    status_code=201,
    responses=error_responses(400, 404, 409, 503),
    summary="Re-submit a job",
)
async def retry_job(
    task_id: str,
    request: Request,
    response: Response,
    force: bool = Query(default=False, description="Re-submit even if the job did not fail."),
    idempotency_key: Optional[str] = IdempotencyKey,
    principal: Principal = _writer,
) -> TaskSubmitResponse:
    """
    Queue the job's task again with the same args and options, as a new job
    whose `retry_of` points back to this one.

    **Errors:** `404` unknown task_id · `409` the job isn't FAILURE and `force`
    is false · `400` the task is no longer registered.
    """
    job = await _get_job_or_404(task_id, principal)
    if job["status"] != states.FAILURE and not force:
        raise HTTPException(
            status_code=409,
            detail=f"Job status is {job['status']}, not FAILURE. Pass ?force=true to re-submit anyway.",
        )
    task = TASK_REGISTRY.get(job["task_name"])
    if task is None:
        raise HTTPException(status_code=400, detail=f"Task {job['task_name']!r} is not registered; cannot retry.")
    check_task_allowed(principal, task.name)

    submission = await submit_task(
        task,
        job["args"],
        job["kwargs"],
        retry_of=task_id,
        idempotency_key=scoped_key(principal, idempotency_key),
        tenant=job["tenant"],
        priority=db.PRIORITY_NAMES.get(job["priority"], "normal"),
        timeout_seconds=job["timeout_seconds"],
        max_retries=job["max_retries"],
        callback_url=job["callback_url"],
    )
    if submission.created:
        await audit(request, principal, "job.retry", "job", submission.task_id, {"retry_of": task_id})
    logger.info("Retried job %s as %s", task_id, submission.task_id)
    return to_response(submission, response)


@router.delete(
    "/{task_id}",
    response_model=CancelResponse,
    responses=error_responses(404, 409),
    summary="Cancel a queued, waiting or running job",
)
async def cancel_job(task_id: str, request: Request, principal: Principal = _writer) -> CancelResponse:
    """
    Revoke the job. A queued (or delayed, or waiting workflow step) job will
    never start, on any worker; a running one is terminated. In a workflow,
    the steps that depend on it are skipped.

    **Errors:** `404` unknown task_id · `409` the job has already finished
    (SUCCESS, FAILURE or REVOKED).
    """
    job = await _get_job_or_404(task_id, principal)
    if job["status"] in db.TERMINAL_STATES:
        raise HTTPException(status_code=409, detail=f"Job already finished with status {job['status']}.")

    await run_in_threadpool(celery_app.control.revoke, task_id, terminate=True)
    if job["status"] == db.WAITING:
        await db.cancel_waiting_step(task_id)
    else:
        await db.update_job_status(task_id, status=states.REVOKED, finished=True)
    await audit(request, principal, "job.cancel", "job", task_id)
    logger.info("Revoked task %s", task_id)
    return CancelResponse(task_id=task_id)
