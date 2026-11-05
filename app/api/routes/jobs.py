"""
`/jobs` — list, inspect, retry and cancel submitted jobs.

Two sources of truth:
  * the `jobs` table (job history) — `GET /jobs`, `GET /jobs/{id}`;
  * Celery's result backend (live state, incl. PROGRESS) — `GET /jobs/{id}/status`.
    Its rows expire after CELERY_RESULT_EXPIRES; the endpoint then falls
    back to the job history.

Cancelling is cluster-wide: the job is marked REVOKED in the database, and
the execution guard in every worker refuses to run a REVOKED job, even if
the revoke broadcast never reached that worker.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from celery import states
from fastapi import APIRouter, HTTPException, Query, Response
from starlette.concurrency import run_in_threadpool

from app import db
from app.api.enqueue import submit_task
from app.api.errors import error_responses
from app.api.routes.submit import IdempotencyKey, to_response
from app.api.schemas import CancelResponse, JobListResponse, JobRecord, TaskStatusResponse, TaskSubmitResponse
from app.celery_app import celery_app
from app.tasks import TASK_REGISTRY

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/jobs", tags=["Jobs"], responses=error_responses(401))


async def _get_job_or_404(task_id: str) -> dict:
    job = await db.get_job(task_id)
    if job is None:
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


@router.get("", response_model=JobListResponse, summary="List jobs")
async def list_jobs(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    status: Optional[str] = Query(default=None, description="Exact status, e.g. SUCCESS, FAILURE, RETRY."),
) -> JobListResponse:
    """Jobs from the job history, newest first, optionally filtered by status."""
    rows = await db.list_jobs(limit=limit, offset=offset, status=status)
    return JobListResponse(
        total=await db.count_jobs(status=status),
        limit=limit,
        offset=offset,
        jobs=[JobRecord.from_row(row) for row in rows],
    )


@router.get(
    "/{task_id}/status",
    response_model=TaskStatusResponse,
    responses=error_responses(404),
    summary="Live state from the result backend",
)
async def get_task_status(task_id: str) -> TaskStatusResponse:
    """
    The task's current state as Celery records it, including PROGRESS updates
    from running tasks. If Celery has no record (never started, or its result
    expired) the state comes from the job history and `source` is `job_record`.

    **Errors:** `404` if neither source knows the task_id.
    """
    job = await db.get_job(task_id)
    meta = await run_in_threadpool(celery_app.backend.get_task_meta, task_id)
    state = meta.get("status", states.PENDING)

    if state == states.PENDING:
        if job is None:
            raise HTTPException(status_code=404, detail=f"Unknown task_id: {task_id}")
        record = JobRecord.from_row(job)
        return TaskStatusResponse(
            task_id=task_id,
            state=states.PENDING if record.status == "QUEUED" else record.status,
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

    result = meta.get("result")
    date_done = meta.get("date_done")
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
        date_done=_as_utc(date_done),
        children=_json_safe(meta.get("children")),
    )


@router.get("/{task_id}", response_model=JobRecord, responses=error_responses(404), summary="One job's history record")
async def get_job(task_id: str) -> JobRecord:
    """
    The full job record: status, result, failure reason and traceback, retry
    count, worker, and links to the schedule or job it came from.

    **Errors:** `404` if the task_id is not in the job history.
    """
    return JobRecord.from_row(await _get_job_or_404(task_id))


@router.post(
    "/{task_id}/retry",
    response_model=TaskSubmitResponse,
    status_code=201,
    responses=error_responses(400, 404, 409, 503),
    summary="Re-submit a job",
)
async def retry_job(
    task_id: str,
    response: Response,
    force: bool = Query(default=False, description="Re-submit even if the job did not fail."),
    idempotency_key: Optional[str] = IdempotencyKey,
) -> TaskSubmitResponse:
    """
    Queue the job's task again with the same args, as a new job whose
    `retry_of` points back to this one.

    **Errors:** `404` unknown task_id · `409` the job isn't FAILURE and `force`
    is false · `400` the task is no longer registered.
    """
    job = await _get_job_or_404(task_id)
    if job["status"] != states.FAILURE and not force:
        raise HTTPException(
            status_code=409,
            detail=f"Job status is {job['status']}, not FAILURE. Pass ?force=true to re-submit anyway.",
        )
    task = TASK_REGISTRY.get(job["task_name"])
    if task is None:
        raise HTTPException(status_code=400, detail=f"Task {job['task_name']!r} is not registered; cannot retry.")

    submission = await submit_task(task, job["args"], job["kwargs"], retry_of=task_id, idempotency_key=idempotency_key)
    logger.info("Retried job %s as %s", task_id, submission.task_id)
    return to_response(submission, response)


@router.delete(
    "/{task_id}",
    response_model=CancelResponse,
    responses=error_responses(404, 409),
    summary="Cancel a queued or running job",
)
async def cancel_job(task_id: str) -> CancelResponse:
    """
    Revoke the job. A queued job will never start (on any worker); a
    running one is terminated.

    **Errors:** `404` unknown task_id · `409` the job has already finished
    (SUCCESS, FAILURE or REVOKED).
    """
    job = await _get_job_or_404(task_id)
    if job["status"] in db.TERMINAL_STATES:
        raise HTTPException(status_code=409, detail=f"Job already finished with status {job['status']}.")

    await run_in_threadpool(celery_app.control.revoke, task_id, terminate=True)
    await db.update_job_status(task_id, status=states.REVOKED, finished=True)
    logger.info("Revoked task %s", task_id)
    return CancelResponse(task_id=task_id)
