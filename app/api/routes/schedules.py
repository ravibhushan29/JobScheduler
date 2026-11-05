"""
`/schedules` — create and manage dynamic schedules at runtime.

The scheduler service (`app.scheduler`, any number of replicas) fires each
due run exactly once, within about SCHEDULE_POLL_INTERVAL_SECONDS of it
becoming due. Timing rules are in `app.scheduling`. System schedules (those
with a `name`) are defined in code and are read-only here.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Body, HTTPException, Query, Response

from app import db, scheduling
from app.api.errors import error_responses
from app.api.routes.submit import IdempotencyKey
from app.api.schemas import (
    ScheduleCreateRequest,
    ScheduleDeleteResponse,
    ScheduleResponse,
    ScheduleUpdateRequest,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/schedules", tags=["Schedules"], responses=error_responses(401))

#: One valid example per schedule type, offered as a picker in Swagger UI.
_CREATE_EXAMPLES = {
    "interval": {
        "summary": "Interval — every 2 hours",
        "value": {"task_name": "app.tasks.heartbeat", "schedule_type": "interval", "interval_seconds": 7200},
    },
    "cron": {
        "summary": "Cron — weekdays at 08:00 UTC",
        "value": {"task_name": "app.tasks.scheduled_cleanup", "schedule_type": "cron", "cron_expr": "0 8 * * 1-5"},
    },
    "once": {
        "summary": "Once — at a specific time (must be in the future)",
        "value": {
            "task_name": "app.tasks.send_notification",
            "args": ["someone@example.com", "Reminder: standup in 10 minutes"],
            "schedule_type": "once",
            "run_at": "2030-01-01T09:50:00+00:00",
        },
    },
}

_UPDATE_EXAMPLES = {
    "pause": {"summary": "Pause", "value": {"enabled": False}},
    "resume": {"summary": "Resume", "value": {"enabled": True}},
    "retime_interval": {"summary": "Change the interval", "value": {"interval_seconds": 120}},
    "retime_cron": {"summary": "Change the cron expression", "value": {"cron_expr": "*/5 * * * *"}},
}


async def _get_schedule_or_404(schedule_id: int) -> dict:
    schedule = await db.get_schedule(schedule_id)
    if schedule is None:
        raise HTTPException(status_code=404, detail=f"Unknown schedule id: {schedule_id}")
    return schedule


def _reject_system(schedule: dict) -> None:
    if schedule["name"] is not None:
        raise HTTPException(
            status_code=409,
            detail=f"{schedule['name']} is a system schedule defined in code (app.tasks.SYSTEM_SCHEDULES).",
        )


@router.post(
    "",
    response_model=ScheduleResponse,
    status_code=201,
    responses=error_responses(409, 422),
    summary="Create a schedule (once / interval / cron)",
)
async def create_schedule(
    response: Response,
    payload: ScheduleCreateRequest = Body(..., openapi_examples=_CREATE_EXAMPLES),
    idempotency_key: Optional[str] = IdempotencyKey,
) -> ScheduleResponse:
    """
    Create a schedule for any public task. Send an `Idempotency-Key` header
    to make retries safe: a repeated request returns the original schedule
    (200, `Idempotent-Replayed: true`) instead of creating a second one that
    would fire the same job twice.

    **Errors:** `422` if the task isn't public, the timing field doesn't match
    `schedule_type`, `run_at` is in the past or has no timezone, the cron
    expression is invalid, or `args`/`kwargs` don't fit the task's signature ·
    `409` the Idempotency-Key was used for a different schedule.
    """
    first_run = scheduling.first_run_at(
        payload.schedule_type, datetime.now(timezone.utc), payload.run_at, payload.cron_expr
    )
    schedule_id = await db.create_schedule(
        task_name=payload.task_name,
        args=payload.args,
        kwargs=payload.kwargs,
        schedule_type=payload.schedule_type,
        next_run_at=first_run,
        run_at=payload.run_at,
        interval_seconds=payload.interval_seconds,
        cron_expr=payload.cron_expr,
        idempotency_key=idempotency_key,
    )
    if schedule_id is None:
        return _replay_schedule(await db.get_schedule_by_idempotency_key(idempotency_key), payload, response)
    logger.info("Created schedule id=%s task=%s type=%s", schedule_id, payload.task_name, payload.schedule_type)
    return ScheduleResponse.from_row(await db.get_schedule(schedule_id))


def _replay_schedule(existing: Optional[dict], payload: ScheduleCreateRequest, response: Response) -> ScheduleResponse:
    same_request = existing is not None and (
        existing["task_name"],
        existing["schedule_type"],
        existing["args"],
        existing["kwargs"],
    ) == (payload.task_name, payload.schedule_type, payload.args, payload.kwargs)
    if not same_request:
        raise HTTPException(status_code=409, detail="This Idempotency-Key was already used for a different request.")
    response.status_code = 200
    response.headers["Idempotent-Replayed"] = "true"
    return ScheduleResponse.from_row(existing)


@router.get("", response_model=list[ScheduleResponse], summary="List schedules")
async def list_schedules(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    enabled_only: bool = Query(default=False, description="Only schedules that are currently active."),
) -> list[ScheduleResponse]:
    """Schedules, newest first."""
    rows = await db.list_schedules(limit=limit, offset=offset, enabled_only=enabled_only)
    return [ScheduleResponse.from_row(row) for row in rows]


@router.get("/{schedule_id}", response_model=ScheduleResponse, responses=error_responses(404), summary="Get a schedule")
async def get_schedule(schedule_id: int) -> ScheduleResponse:
    """**Errors:** `404` if the id doesn't exist."""
    return ScheduleResponse.from_row(await _get_schedule_or_404(schedule_id))


@router.patch(
    "/{schedule_id}",
    response_model=ScheduleResponse,
    responses=error_responses(400, 404, 409, 422),
    summary="Pause, resume or retime a schedule",
)
async def update_schedule(
    schedule_id: int,
    payload: ScheduleUpdateRequest = Body(..., openapi_examples=_UPDATE_EXAMPLES),
) -> ScheduleResponse:
    """
    Change `enabled`, `interval_seconds` or `cron_expr`. A new timing takes
    effect immediately: `next_run_at` is recalculated (last run + new
    interval, or the next cron match from now). Resuming a schedule whose
    `next_run_at` has passed fires it on the next poll.

    A `once` schedule can't be retimed — delete it and create a new one.

    **Errors:** `404` unknown id · `400` the field doesn't apply to this
    schedule type · `409` resuming a `once` schedule that already fired, or
    a system schedule · `422` invalid value.
    """
    schedule = await _get_schedule_or_404(schedule_id)
    _reject_system(schedule)
    schedule_type = schedule["schedule_type"]
    changes = payload.model_dump(exclude_unset=True)
    updates: dict = {}

    if "interval_seconds" in changes:
        if schedule_type != "interval":
            raise HTTPException(
                status_code=400, detail=f"interval_seconds does not apply to a {schedule_type} schedule."
            )
        base = schedule["last_run_at"] or datetime.now(timezone.utc)
        updates["interval_seconds"] = changes["interval_seconds"]
        updates["next_run_at"] = base + timedelta(seconds=changes["interval_seconds"])

    if "cron_expr" in changes:
        if schedule_type != "cron":
            raise HTTPException(status_code=400, detail=f"cron_expr does not apply to a {schedule_type} schedule.")
        updates["cron_expr"] = changes["cron_expr"]
        updates["next_run_at"] = scheduling.next_cron_run(changes["cron_expr"], datetime.now(timezone.utc))

    if "enabled" in changes:
        if changes["enabled"] and schedule_type == "once" and schedule["run_count"] > 0:
            raise HTTPException(
                status_code=409, detail="This one-off schedule already fired; create a new one instead."
            )
        updates["enabled"] = changes["enabled"]

    if updates:
        await db.update_schedule(schedule_id, **updates)
        logger.info("Updated schedule id=%s fields=%s", schedule_id, sorted(updates))
    return ScheduleResponse.from_row(await db.get_schedule(schedule_id))


@router.delete(
    "/{schedule_id}",
    response_model=ScheduleDeleteResponse,
    responses=error_responses(404, 409),
    summary="Delete a schedule",
)
async def delete_schedule(schedule_id: int) -> ScheduleDeleteResponse:
    """
    Delete permanently. Jobs it already fired stay in the job history.

    **Errors:** `404` unknown id · `409` system schedule.
    """
    _reject_system(await _get_schedule_or_404(schedule_id))
    if not await db.delete_schedule(schedule_id):
        raise HTTPException(status_code=404, detail=f"Unknown schedule id: {schedule_id}")
    logger.info("Deleted schedule id=%s", schedule_id)
    return ScheduleDeleteResponse(schedule_id=schedule_id)
