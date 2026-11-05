"""
`/tasks/*` — queue one sample task per call. Each returns its task_id at once.

Every endpoint accepts an optional `Idempotency-Key` header. Re-sending a
request with the same key returns the original job (200 and
`Idempotent-Replayed: true`) instead of creating a second one, so clients can
retry timeouts safely.
"""

from typing import Optional

from fastapi import APIRouter, Header, Response

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

router = APIRouter(prefix="/tasks", tags=["Tasks"], responses=error_responses(401, 409, 422, 503))

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
    return TaskSubmitResponse(task_id=submission.task_id)


@router.post("/add", response_model=TaskSubmitResponse, status_code=201, summary="Queue add(x, y)")
async def submit_add(
    payload: AddRequest, response: Response, idempotency_key: Optional[str] = IdempotencyKey
) -> TaskSubmitResponse:
    """The simplest task — a quick check that API, broker and worker all work together."""
    submission = await submit_task(tasks.add, [payload.x, payload.y], idempotency_key=idempotency_key)
    return to_response(submission, response)


@router.post("/long", response_model=TaskSubmitResponse, status_code=201, summary="Queue a slow job")
async def submit_long(
    payload: LongTaskRequest, response: Response, idempotency_key: Optional[str] = IdempotencyKey
) -> TaskSubmitResponse:
    """Runs on the separate `long` queue and reports PROGRESS (see `/jobs/{task_id}/status`)."""
    submission = await submit_task(tasks.long_running_task, [payload.duration_seconds], idempotency_key=idempotency_key)
    return to_response(submission, response)


@router.post("/flaky", response_model=TaskSubmitResponse, status_code=201, summary="Queue a randomly failing job")
async def submit_flaky(
    payload: FlakyTaskRequest, response: Response, idempotency_key: Optional[str] = IdempotencyKey
) -> TaskSubmitResponse:
    """Fails with `fail_probability` and is retried automatically with exponential backoff."""
    submission = await submit_task(tasks.flaky_task, [payload.fail_probability], idempotency_key=idempotency_key)
    return to_response(submission, response)


@router.post("/notify", response_model=TaskSubmitResponse, status_code=201, summary="Queue a simulated notification")
async def submit_notification(
    payload: NotificationRequest, response: Response, idempotency_key: Optional[str] = IdempotencyKey
) -> TaskSubmitResponse:
    """Simulated provider call that fails 30% of the time and retries itself via `self.retry()`."""
    submission = await submit_task(
        tasks.send_notification, [payload.email, payload.message], idempotency_key=idempotency_key
    )
    return to_response(submission, response)


@router.post("/test-batch", response_model=BatchTestResponse, status_code=201, summary="Queue one of every sample task")
async def submit_test_batch() -> BatchTestResponse:
    """
    Smoke test: queues every sample task, including one `flaky_task` that
    always fails, so `GET /jobs?status=FAILURE` and `POST /jobs/{id}/retry`
    have something to work on. That job reaches FAILURE only after its
    automatic retries (up to about a minute with default settings).
    """

    async def queue(task, args) -> str:
        return (await submit_task(task, args)).task_id

    return BatchTestResponse(
        add=await queue(tasks.add, [2, 2]),
        long_running_task=await queue(tasks.long_running_task, [3]),
        flaky_task_will_fail=await queue(tasks.flaky_task, [1.0]),
        flaky_task_will_succeed=await queue(tasks.flaky_task, [0.0]),
        send_notification=await queue(tasks.send_notification, ["test@example.com", "batch test"]),
    )
