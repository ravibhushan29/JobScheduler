"""
`/tasks/*` — queue one sample task per call. Each returns its task_id at once.

Every endpoint accepts an optional `Idempotency-Key` header. Re-sending a
request with the same key returns the original job (200 and
`Idempotent-Replayed: true`) instead of creating a second one, so clients can
retry timeouts safely.
"""

from typing import Optional

from fastapi import APIRouter, Depends, Header, Request, Response

from app import tasks
from app.api.enqueue import Submission, submit_task
from app.api.errors import error_responses
from app.api.schemas import (
    IDEMPOTENCY_HEADER,
    AddRequest,
    BatchTestResponse,
    FlakyTaskRequest,
    LongTaskRequest,
    NotificationRequest,
    TaskSubmitResponse,
)
from app.api.security import Principal, audit, check_task_allowed, require, scoped_key

router = APIRouter(prefix="/tasks", tags=["Tasks"], responses=error_responses(401, 403, 409, 422, 429, 503))
_writer = Depends(require("jobs:write"))

IdempotencyKey = Header(
    default=None,
    alias=IDEMPOTENCY_HEADER,
    max_length=255,
    description="Optional. Re-sending the same key returns the original job instead of queuing another.",
)


def to_response(submission: Submission, response: Response) -> TaskSubmitResponse:
    """201 for a new job; 200 + `Idempotent-Replayed: true` when an earlier request already created it."""
    if not submission.created:
        response.status_code = 200
        response.headers["Idempotent-Replayed"] = "true"
    return TaskSubmitResponse(task_id=submission.task_id, run_after=submission.run_after)


async def _submit(
    request: Request, principal: Principal, task, args: list, idempotency_key: Optional[str]
) -> Submission:
    """Submit a sample task on behalf of `principal`: permission check, tenant, audit."""
    check_task_allowed(principal, task.name)
    submission = await submit_task(
        task, args, idempotency_key=scoped_key(principal, idempotency_key), tenant=principal.tenant
    )
    if submission.created:
        await audit(request, principal, "job.submit", "job", submission.task_id, {"task_name": task.name})
    return submission


@router.post("/add", response_model=TaskSubmitResponse, status_code=201, summary="Queue add(x, y)")
async def submit_add(
    payload: AddRequest,
    request: Request,
    response: Response,
    idempotency_key: Optional[str] = IdempotencyKey,
    principal: Principal = _writer,
) -> TaskSubmitResponse:
    """The simplest task — a quick check that API, broker and worker all work together."""
    submission = await _submit(request, principal, tasks.add, [payload.x, payload.y], idempotency_key)
    return to_response(submission, response)


@router.post("/long", response_model=TaskSubmitResponse, status_code=201, summary="Queue a slow job")
async def submit_long(
    payload: LongTaskRequest,
    request: Request,
    response: Response,
    idempotency_key: Optional[str] = IdempotencyKey,
    principal: Principal = _writer,
) -> TaskSubmitResponse:
    """Runs on the separate `long` queue and reports PROGRESS (see `/jobs/{task_id}/status`)."""
    submission = await _submit(request, principal, tasks.long_running_task, [payload.duration_seconds], idempotency_key)
    return to_response(submission, response)


@router.post("/flaky", response_model=TaskSubmitResponse, status_code=201, summary="Queue a randomly failing job")
async def submit_flaky(
    payload: FlakyTaskRequest,
    request: Request,
    response: Response,
    idempotency_key: Optional[str] = IdempotencyKey,
    principal: Principal = _writer,
) -> TaskSubmitResponse:
    """Fails with `fail_probability` and is retried automatically with exponential backoff."""
    submission = await _submit(request, principal, tasks.flaky_task, [payload.fail_probability], idempotency_key)
    return to_response(submission, response)


@router.post("/notify", response_model=TaskSubmitResponse, status_code=201, summary="Queue a simulated notification")
async def submit_notification(
    payload: NotificationRequest,
    request: Request,
    response: Response,
    idempotency_key: Optional[str] = IdempotencyKey,
    principal: Principal = _writer,
) -> TaskSubmitResponse:
    """Simulated provider call that fails 30% of the time and retries itself via `self.retry()`."""
    submission = await _submit(
        request, principal, tasks.send_notification, [payload.email, payload.message], idempotency_key
    )
    return to_response(submission, response)


@router.post("/test-batch", response_model=BatchTestResponse, status_code=201, summary="Queue one of every sample task")
async def submit_test_batch(request: Request, principal: Principal = _writer) -> BatchTestResponse:
    """
    Smoke test: queues every sample task, including one `flaky_task` that
    always fails, so `GET /jobs?status=FAILURE` and `POST /jobs/{id}/retry`
    have something to work on. That job reaches FAILURE only after its
    automatic retries (up to about a minute with default settings).
    """

    async def queue(task, args) -> str:
        return (await _submit(request, principal, task, args, None)).task_id

    return BatchTestResponse(
        add=await queue(tasks.add, [2, 2]),
        long_running_task=await queue(tasks.long_running_task, [3]),
        flaky_task_will_fail=await queue(tasks.flaky_task, [1.0]),
        flaky_task_will_succeed=await queue(tasks.flaky_task, [0.0]),
        send_notification=await queue(tasks.send_notification, ["test@example.com", "batch test"]),
    )
