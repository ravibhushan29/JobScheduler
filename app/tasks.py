"""
Celery tasks, the public task registry, and the system schedules.

| Task               | Queue   | Shows                                                |
|--------------------|---------|------------------------------------------------------|
| add                | default | the simplest end-to-end job                          |
| long_running_task  | long    | slow work isolated on its own queue + progress       |
| flaky_task         | default | automatic retry with backoff and jitter              |
| send_notification  | default | manual retry + an idempotency key for side effects   |
| heartbeat          | default | system schedule, every 30 s                          |
| scheduled_cleanup  | default | system schedule, daily: prunes old jobs and results  |

Every task inherits `app.guard.GuardedTask`, so a job never runs twice even
if its message is delivered twice.

Adding a task: define it with an explicit `name=` and `queue=`, then add it
to `_PUBLIC_TASKS` if the API may submit, retry or schedule it.
"""

import inspect
import random
import time
from datetime import datetime, timedelta, timezone

from celery.utils.log import get_task_logger

from app import db
from app.celery_app import celery_app
from app.config import settings

logger = get_task_logger(__name__)


# ---------------------------------------------------------------------------
# Sample tasks
# ---------------------------------------------------------------------------
@celery_app.task(name="app.tasks.add", queue=settings.DEFAULT_QUEUE_NAME)
def add(x: float, y: float) -> float:
    """Return x + y."""
    return x + y


@celery_app.task(name="app.tasks.long_running_task", bind=True, queue=settings.LONG_TASK_QUEUE_NAME)
def long_running_task(self, duration_seconds: int = 5) -> str:
    """Sleep for `duration_seconds`, publishing a PROGRESS state (0-100) every second."""
    steps = max(duration_seconds, 1)
    for step in range(1, steps + 1):
        time.sleep(1)
        self.update_state(state="PROGRESS", meta={"progress": int(step / steps * 100)})
    return f"Completed after {duration_seconds}s"


@celery_app.task(
    name="app.tasks.flaky_task",
    bind=True,
    queue=settings.DEFAULT_QUEUE_NAME,
    autoretry_for=(Exception,),
    retry_backoff=settings.TASK_DEFAULT_RETRY_BACKOFF,  # 1st delay; doubles each retry
    retry_backoff_max=60,
    retry_jitter=True,  # randomise delays so many failures don't retry in lockstep
    max_retries=settings.TASK_DEFAULT_MAX_RETRIES,
)
def flaky_task(self, fail_probability: float = 0.7) -> str:
    """Fail with probability `fail_probability`; Celery retries it automatically."""
    logger.info("flaky_task attempt %d/%d", self.request.retries + 1, settings.TASK_DEFAULT_MAX_RETRIES + 1)
    if random.random() < fail_probability:
        raise RuntimeError("Simulated transient failure")
    return "flaky_task succeeded"


@celery_app.task(name="app.tasks.send_notification", bind=True, queue=settings.DEFAULT_QUEUE_NAME, max_retries=3)
def send_notification(self, email: str, message: str) -> str:
    """
    Pretend to call an email/SMS provider that is down 30% of the time.

    Two patterns worth copying for any task with external side effects:
      * Manual `self.retry()` (instead of `autoretry_for`), so only the
        errors you choose are retried — here ConnectionError, after 5/10/15 s.
      * An idempotency key. The guard ensures this job runs once, except when
        a worker dies mid-task and the job is re-run elsewhere. Passing
        `self.request.id` to the provider (most email/payment APIs accept an
        idempotency key) makes even that re-run send nothing extra.
    """
    idempotency_key = self.request.id  # e.g. provider.send(..., idempotency_key=idempotency_key)
    try:
        if random.random() < 0.3:
            raise ConnectionError("Upstream notification service unreachable")
        return f"Notification sent to {email} (idempotency key {idempotency_key})"
    except ConnectionError as exc:
        raise self.retry(exc=exc, countdown=5 * (self.request.retries + 1)) from exc


# ---------------------------------------------------------------------------
# Housekeeping
# ---------------------------------------------------------------------------
@celery_app.task(name="app.tasks.heartbeat", queue=settings.DEFAULT_QUEUE_NAME)
def heartbeat() -> str:
    """Prove the scheduler and a worker are both alive."""
    logger.info("heartbeat - scheduler and worker are alive")
    return "ok"


@celery_app.task(name="app.tasks.scheduled_cleanup", queue=settings.DEFAULT_QUEUE_NAME)
def scheduled_cleanup() -> str:
    """Delete finished jobs older than JOB_RETENTION_DAYS, old audit entries, and expired Celery results."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=settings.JOB_RETENTION_DAYS)
    deleted = db.run_sync(db.delete_finished_jobs_before(cutoff))
    audit_cutoff = datetime.now(timezone.utc) - timedelta(days=settings.AUDIT_RETENTION_DAYS)
    audit_deleted = db.run_sync(db.delete_audit_entries_before(audit_cutoff))
    celery_app.backend.cleanup()  # rows older than CELERY_RESULT_EXPIRES
    logger.info(
        "cleanup: deleted %d job rows finished before %s and %d audit entries",
        deleted,
        cutoff.isoformat(),
        audit_deleted,
    )
    return f"deleted {deleted} job rows"


# ---------------------------------------------------------------------------
# Public task registry — the single list of tasks the API may submit, retry
# or schedule. Nothing else can be triggered from outside.
# ---------------------------------------------------------------------------
_PUBLIC_TASKS = (add, long_running_task, flaky_task, send_notification, heartbeat, scheduled_cleanup)

TASK_REGISTRY: dict = {task.name: task for task in _PUBLIC_TASKS}
SCHEDULABLE_TASK_NAMES: frozenset[str] = frozenset(TASK_REGISTRY)

#: Periodic jobs defined in code (they replace Celery Beat's beat_schedule).
#: The scheduler upserts them by `name` on startup; they show up in
#: GET /schedules with `system: true` and can't be edited through the API.
SYSTEM_SCHEDULES = (
    {"name": "system:heartbeat", "task_name": heartbeat.name, "schedule_type": "interval", "interval_seconds": 30},
    {"name": "system:cleanup", "task_name": scheduled_cleanup.name, "schedule_type": "cron", "cron_expr": "0 3 * * *"},
)


def check_task_arguments(task_name: str, args: list, kwargs: dict) -> None:
    """
    Check that `args`/`kwargs` fit the task's signature, so a bad schedule is
    rejected when it is created rather than failing on every run.

    Raises:
        KeyError: `task_name` is not a public task.
        TypeError: the arguments don't fit the signature.
    """
    inspect.signature(TASK_REGISTRY[task_name].run).bind(*args, **kwargs)
