"""
Hand a recorded job to the broker. Used by the API, the scheduler and the outbox relay.

The job row must already exist (`db.create_job`). If publishing fails, the
row stays unpublished and the outbox relay in the scheduler retries it; if
publishing succeeds but the process dies before `mark_published`, the relay
publishes it again and the execution guard drops the extra copy. Either way
the job runs once.
"""

import asyncio
import logging

from app import db
from app.celery_app import celery_app
from app.config import settings

logger = logging.getLogger(__name__)


def _delivery_options(job: dict) -> dict:
    """Per-job priority and time limits, carried by the message (and kept on Celery retries)."""
    options = {"priority": job["priority"] if job.get("priority") is not None else db.PRIORITIES["normal"]}
    if job.get("timeout_seconds"):
        options["soft_time_limit"] = job["timeout_seconds"]
        options["time_limit"] = job["timeout_seconds"] + 10  # hard kill shortly after the soft limit
    return options


async def publish_job(job: dict) -> bool:
    """Publish `job` (a jobs row or the same keys) to its queue. Returns False if the broker refused."""
    try:
        # kombu blocks while it retries an unreachable broker; keep that off the event loop.
        await asyncio.to_thread(
            celery_app.send_task,
            job["task_name"],
            args=job["args"],
            kwargs=job["kwargs"],
            task_id=job["task_id"],
            queue=job["queue"] or settings.DEFAULT_QUEUE_NAME,
            retries=job.get("retries") or 0,  # a recovered job keeps its Celery retry count
            **_delivery_options(job),
        )
    except Exception:
        logger.warning(
            "Publish failed for task_id=%s (%s); the outbox relay will retry",
            job["task_id"],
            job["task_name"],
            exc_info=True,
        )
        return False
    await db.mark_published(job["task_id"])
    return True
