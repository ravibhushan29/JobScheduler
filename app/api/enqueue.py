"""
Submitting a task from the API: record it (honouring idempotency keys), then publish it.

Idempotency: a client that retries a request (timeout, network error) with
the same `Idempotency-Key` header gets the original job back instead of a
second job. Reusing a key for a different task or arguments is a 409.
"""

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from celery import Task
from fastapi import HTTPException

from app import db, publisher
from app.config import settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Submission:
    task_id: str
    created: bool  # False when an earlier request with the same Idempotency-Key created it
    run_after: Optional[datetime] = None  # delayed start, if any


async def submit_task(
    task: Task,
    args: list,
    kwargs: Optional[dict] = None,
    retry_of: Optional[str] = None,
    idempotency_key: Optional[str] = None,
    *,
    tenant: Optional[str] = None,
    priority: str = "normal",
    run_after: Optional[datetime] = None,
    timeout_seconds: Optional[int] = None,
    max_retries: Optional[int] = None,
    callback_url: Optional[str] = None,
) -> Submission:
    """
    Record and publish one job on the task's own queue.

    The row is written BEFORE publishing (a fast worker must find it). If
    the broker is unreachable the job stays recorded but unpublished, and the
    scheduler's outbox relay publishes it once the broker is back — the
    request still succeeds, because the job is safely stored. A job with a
    future `run_after` isn't published now: the relay publishes it when due.

    `idempotency_key` must already be tenant-scoped (`security.scoped_key`).

    Raises:
        HTTPException(409): `idempotency_key` was already used for a different request.
    """
    args, kwargs = list(args), dict(kwargs or {})
    task_id = str(uuid.uuid4())
    queue = getattr(task, "queue", None) or settings.DEFAULT_QUEUE_NAME
    options = {
        "tenant": tenant,
        "priority": db.PRIORITIES[priority],
        "run_after": run_after,
        "timeout_seconds": timeout_seconds,
        "max_retries": max_retries,
        "callback_url": callback_url,
    }

    created = await db.create_job(
        task_id, task.name, args, kwargs, queue=queue, retry_of=retry_of, idempotency_key=idempotency_key, **options
    )
    if not created:
        return _replay(await db.get_job_by_idempotency_key(idempotency_key), task.name, args, kwargs)

    if run_after is not None:
        logger.info("Recorded delayed %s task_id=%s, starts after %s", task.name, task_id, run_after.isoformat())
        return Submission(task_id=task_id, created=True, run_after=run_after)
    if await publisher.publish_job(
        {"task_id": task_id, "task_name": task.name, "args": args, "kwargs": kwargs, "queue": queue, **options}
    ):
        logger.info("Queued %s task_id=%s queue=%s", task.name, task_id, queue)
    return Submission(task_id=task_id, created=True)


def _replay(existing: Optional[dict], task_name: str, args: list, kwargs: dict) -> Submission:
    if existing is None:  # the key's job was deleted by retention between insert and lookup
        raise HTTPException(status_code=409, detail="Idempotency key conflict; retry with a new key.")
    if (existing["task_name"], existing["args"], existing["kwargs"]) != (task_name, args, kwargs):
        raise HTTPException(
            status_code=409,
            detail="This Idempotency-Key was already used for a different request.",
        )
    logger.info("Idempotent replay of task_id=%s", existing["task_id"])
    return Submission(task_id=existing["task_id"], created=False, run_after=existing["run_after"])
