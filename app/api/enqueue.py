"""
Submitting a task from the API: record it (honouring idempotency keys), then publish it.

Idempotency: a client that retries a request (timeout, network error) with
the same `Idempotency-Key` header gets the original job back instead of a
second job. Reusing a key for a different task or arguments is a 409.
"""

import logging
import uuid
from dataclasses import dataclass
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


async def submit_task(
    task: Task,
    args: list,
    kwargs: Optional[dict] = None,
    retry_of: Optional[str] = None,
    idempotency_key: Optional[str] = None,
) -> Submission:
    """
    Record and publish one job on the task's own queue.

    The row is written BEFORE publishing (a fast worker must find it). If
    the broker is unreachable the job stays recorded but unpublished, and the
    scheduler's outbox relay publishes it once the broker is back — the
    request still succeeds, because the job is safely stored.

    Raises:
        HTTPException(409): `idempotency_key` was already used for a different request.
    """
    args, kwargs = list(args), dict(kwargs or {})
    task_id = str(uuid.uuid4())
    queue = getattr(task, "queue", None) or settings.DEFAULT_QUEUE_NAME

    created = await db.create_job(
        task_id, task.name, args, kwargs, queue=queue, retry_of=retry_of, idempotency_key=idempotency_key
    )
    if not created:
        return _replay(await db.get_job_by_idempotency_key(idempotency_key), task.name, args, kwargs)

    if await publisher.publish_job(
        {"task_id": task_id, "task_name": task.name, "args": args, "kwargs": kwargs, "queue": queue}
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
    return Submission(task_id=existing["task_id"], created=False)
