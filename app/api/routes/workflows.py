"""
`/workflows` — run several jobs in dependency order (fan-out / fan-in).

A workflow is a set of steps, each a public task plus the steps it
`depends_on`. Steps with no dependencies are queued at once; every other
step waits (status WAITING) until all of its dependencies succeed, then it
is queued too. If a step fails or is cancelled, every step downstream of it
is skipped (REVOKED), and the workflow ends FAILURE (or REVOKED).

All of this happens in the database, inside the same transaction that
records each step's final state (see `db._advance_workflow`), so it keeps
working across crashes and any number of replicas: each step runs once, and
a step that depends on several others is released exactly once.
"""

import logging
import uuid
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from starlette.concurrency import run_in_threadpool

from app import db, publisher
from app.api.errors import error_responses
from app.api.routes.submit import IdempotencyKey
from app.api.schemas import WorkflowCreateRequest, WorkflowResponse, WorkflowSummary
from app.api.security import Principal, audit, check_task_allowed, require, scoped_key
from app.celery_app import celery_app
from app.tasks import TASK_REGISTRY

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/workflows", tags=["Workflows"], responses=error_responses(401, 403, 429))
_reader = Depends(require("jobs:read"))
_writer = Depends(require("jobs:write"))

_EXAMPLE = {
    "fan_out_fan_in": {
        "summary": "Two steps in parallel, then one that needs both",
        "value": {
            "name": "example-report",
            "steps": [
                {"id": "a", "task_name": "app.tasks.add", "args": [1, 2]},
                {"id": "b", "task_name": "app.tasks.add", "args": [3, 4]},
                {"id": "report", "task_name": "app.tasks.long_running_task", "args": [2], "depends_on": ["a", "b"]},
            ],
        },
    }
}


async def _get_or_404(workflow_id: str, principal: Principal) -> dict:
    workflow = await db.get_workflow(workflow_id)
    if workflow is None or not principal.owns(workflow["tenant"]):
        raise HTTPException(status_code=404, detail=f"Unknown workflow id: {workflow_id}")
    return workflow


@router.post(
    "",
    response_model=WorkflowResponse,
    status_code=201,
    responses=error_responses(409, 422, 503),
    summary="Start a workflow",
    openapi_extra={"requestBody": {"content": {"application/json": {"examples": _EXAMPLE}}}},
)
async def create_workflow(
    payload: WorkflowCreateRequest,
    request: Request,
    response: Response,
    idempotency_key: Optional[str] = IdempotencyKey,
    principal: Principal = _writer,
) -> WorkflowResponse:
    """
    Start the workflow and return it with every step's job (`task_id`).
    Follow it with `GET /workflows/{id}`, or pass `callback_url` to be told
    when it finishes. Steps may also have their own options (priority,
    timeout, retries, callback).

    **Errors:** `422` unknown task, arguments that don't fit, duplicate or
    unknown step ids, or a dependency cycle · `403` the key may not run a task ·
    `409` Idempotency-Key reused for a different request.
    """
    for step in payload.steps:
        check_task_allowed(principal, step.task_name)
    key = scoped_key(principal, idempotency_key)
    workflow_id = str(uuid.uuid4())
    steps = []
    for step in payload.steps:
        task = TASK_REGISTRY[step.task_name]
        steps.append(
            {
                "task_id": str(uuid.uuid4()),
                "step": step.id,
                "task_name": task.name,
                "args": step.args,
                "kwargs": step.kwargs,
                "queue": task.queue,
                "depends_on": step.depends_on,
                "priority": db.PRIORITIES[step.priority],
                "timeout_seconds": step.timeout_seconds,
                "max_retries": step.max_retries,
                "callback_url": step.callback_url,
            }
        )

    if not await db.create_workflow(workflow_id, payload.name, steps, principal.tenant, payload.callback_url, key):
        existing = await db.get_workflow_by_idempotency_key(key)
        same = existing is not None and [(s["workflow_step"], s["task_name"]) for s in existing["steps"]] == [
            (step.id, step.task_name) for step in payload.steps
        ]
        if not same:
            raise HTTPException(
                status_code=409, detail="This Idempotency-Key was already used for a different request."
            )
        response.status_code = 200
        response.headers["Idempotent-Replayed"] = "true"
        return WorkflowResponse.from_row(existing)

    for step in steps:  # the roots start now; the rest are released as their dependencies succeed
        if not step["depends_on"]:
            await publisher.publish_job(step)
    logger.info("Started workflow %s (%s) with %d steps", workflow_id, payload.name, len(steps))
    await audit(request, principal, "workflow.create", "workflow", workflow_id, {"steps": len(steps)})
    return WorkflowResponse.from_row(await db.get_workflow(workflow_id))


@router.get("", response_model=list[WorkflowSummary], summary="List workflows")
async def list_workflows(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    principal: Principal = _reader,
) -> list[WorkflowSummary]:
    """Workflows, newest first. Use `GET /workflows/{id}` for the steps."""
    rows = await db.list_workflows(limit=limit, offset=offset, tenant=principal.tenant)
    return [WorkflowSummary.model_validate(row) for row in rows]


@router.get("/{workflow_id}", response_model=WorkflowResponse, responses=error_responses(404), summary="Get a workflow")
async def get_workflow(workflow_id: str, principal: Principal = _reader) -> WorkflowResponse:
    """The workflow's status and every step's status, result and error."""
    return WorkflowResponse.from_row(await _get_or_404(workflow_id, principal))


@router.delete(
    "/{workflow_id}",
    response_model=WorkflowResponse,
    responses=error_responses(404, 409),
    summary="Cancel a workflow",
)
async def cancel_workflow(workflow_id: str, request: Request, principal: Principal = _writer) -> WorkflowResponse:
    """
    Cancel every step that hasn't finished: waiting steps never start, queued
    ones never start, running ones are terminated.

    **Errors:** `404` unknown id · `409` the workflow has already finished.
    """
    workflow = await _get_or_404(workflow_id, principal)
    if workflow["finished_at"] is not None:
        raise HTTPException(status_code=409, detail=f"Workflow already finished with status {workflow['status']}.")
    await db.skip_waiting_steps(workflow_id, "Cancelled with its workflow")
    workflow = await db.get_workflow(workflow_id)  # re-read: a step may have been released meanwhile
    for step in workflow["steps"]:
        if step["status"] in ("QUEUED", "STARTED", "RETRY"):
            await run_in_threadpool(celery_app.control.revoke, step["task_id"], terminate=True)
            await db.update_job_status(step["task_id"], status="REVOKED", finished=True)
    await audit(request, principal, "workflow.cancel", "workflow", workflow_id)
    logger.info("Cancelled workflow %s", workflow_id)
    return WorkflowResponse.from_row(await db.get_workflow(workflow_id))
