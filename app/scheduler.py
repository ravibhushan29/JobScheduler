"""
The scheduler service — highly available, safe to run with any number of replicas.

    python -m app.scheduler               # run (one or many replicas)
    python -m app.scheduler healthcheck   # exit 0 if the local loop is alive (liveness probe)

It replaces Celery Beat, which must run as exactly one process (a single
point of failure; two copies fire everything twice). Here every replica is
active, and the shared database decides who does what:

Each loop (every SCHEDULE_POLL_INTERVAL_SECONDS, plus jitter):
  1. **Fire due schedules.** For each due row, `db.fire_schedule` claims the
     run with an optimistic version check and records its job in the same
     transaction. Exactly one replica wins each due time; the job is then
     published. Losing replicas skip it.
  2. **Relay the outbox.** Jobs recorded but never published (a publisher
     crashed, or the broker was down) are published now.
  3. **Recover abandoned jobs.** A STARTED job whose worker stopped
     renewing its lease (crash, node loss) is re-published so a healthy
     worker finishes it; one Celery recorded as failed is marked FAILURE.

In parallel, a second loop sends due webhooks (`app.webhooks`) every
WEBHOOK_POLL_INTERVAL_SECONDS, so slow webhook receivers never delay schedules.

Losing a replica loses nothing: the others keep going on their next loop.
"""

import asyncio
import logging
import os
import random
import signal
import socket
import sys
import time
from datetime import datetime, timezone

import httpx

from app import db, publisher, scheduling, webhooks
from app.celery_app import celery_app
from app.config import settings
from app.logging_config import setup_logging
from app.tasks import SYSTEM_SCHEDULES, TASK_REGISTRY
from app.tracing import setup_tracing

logger = logging.getLogger(__name__)


async def seed_system_schedules() -> None:
    """Create or update the code-defined schedules. Idempotent; every replica may run it."""
    now = datetime.now(timezone.utc)
    for spec in SYSTEM_SCHEDULES:
        first_run = scheduling.first_run_at(spec["schedule_type"], now, cron_expr=spec.get("cron_expr"))
        await db.upsert_system_schedule(
            name=spec["name"],
            task_name=spec["task_name"],
            schedule_type=spec["schedule_type"],
            first_run_at=first_run,
            interval_seconds=spec.get("interval_seconds"),
            cron_expr=spec.get("cron_expr"),
        )


async def dispatch_due_schedules(now: datetime | None = None) -> list[str]:
    """Fire every due schedule this replica wins. Returns the task_ids it published."""
    now = now or datetime.now(timezone.utc)
    fired = []
    for schedule in await db.get_due_schedules(now):
        task = TASK_REGISTRY.get(schedule["task_name"])
        if task is None:
            logger.error("Disabling schedule id=%s: unknown task %r", schedule["id"], schedule["task_name"])
            await db.update_schedule(schedule["id"], enabled=False)
            continue

        next_run, stays_enabled = scheduling.next_run_after_fire(schedule, now)
        task_id = await db.fire_schedule(schedule, now, next_run, stays_enabled, task.name, task.queue)
        if task_id is None:
            continue  # another replica fired it, or it was paused/edited meanwhile

        logger.info("[SCHEDULE FIRED] schedule_id=%s task=%s task_id=%s", schedule["id"], task.name, task_id)
        await _count("schedule_fired")
        await publisher.publish_job(
            {
                "task_id": task_id,
                "task_name": task.name,
                "args": schedule["args"],
                "kwargs": schedule["kwargs"],
                "queue": task.queue,
            }
        )
        fired.append(task_id)
    return fired


async def relay_outbox() -> int:
    """Publish jobs that were recorded but never reached the broker. Returns how many were published."""
    published = 0
    for job in await db.claim_unpublished_jobs(settings.OUTBOX_REPUBLISH_AFTER_SECONDS):
        if job["run_after"] is not None:
            # A delayed job, or a workflow step whose dependencies just succeeded: expected.
            logger.info(
                "[RELEASED] publishing task_id=%s (%s), due at %s", job["task_id"], job["task_name"], job["run_after"]
            )
        else:
            logger.warning(
                "[OUTBOX] publishing task_id=%s (%s) that was never published", job["task_id"], job["task_name"]
            )
            await _count("outbox_relayed")
        published += await publisher.publish_job(job)
    return published


async def recover_abandoned_jobs() -> int:
    """
    Resume jobs whose worker died mid-run. Returns how many were handled.

    For each STARTED job whose lease lapsed (see `db.claim_abandoned_jobs`):
      * Celery's result backend says it finished (its own final write to the
        jobs table was lost) -> record SUCCESS, don't re-run it;
      * Celery recorded a final failure (e.g. the hard time limit killed it)
        -> mark it FAILURE with that reason;
      * it already lost MAX_WORKER_LOSS_RECOVERIES workers -> mark it FAILURE,
        so a task that crashes its worker can't loop forever;
      * otherwise -> re-publish it. A healthy worker claims it (the lease has
        lapsed) and runs it; if the broker also re-delivers the original
        message, the execution guard drops whichever copy comes second.

    This does not rely on the broker's own re-delivery, whose timing varies.
    """
    handled = 0
    for job in await db.claim_abandoned_jobs(settings.EXECUTION_LEASE_SECONDS):
        handled += 1
        task_id, attempt = job["task_id"], job["attempts"]
        meta = await asyncio.to_thread(celery_app.backend.get_task_meta, task_id)
        if meta.get("status") == "SUCCESS":
            # It finished, but its final write was lost (e.g. the DB was down at that moment).
            await db.update_job_status(task_id, "SUCCESS", result=meta.get("result"), finished=True, attempt=attempt)
            logger.warning("[RECOVER] task_id=%s had finished; recorded SUCCESS from the result backend", task_id)
            continue
        if meta.get("status") == "FAILURE":
            await db.update_job_status(
                task_id, "FAILURE", error=f"Worker lost: {meta.get('result')!r}", finished=True, attempt=attempt
            )
            logger.error("[ABANDONED] task_id=%s failed in its worker (%r)", task_id, meta.get("result"))
            continue

        worker_losses = attempt - job["retries"]  # executions that ended without a Celery result
        if worker_losses > settings.MAX_WORKER_LOSS_RECOVERIES:
            await db.update_job_status(
                task_id,
                "FAILURE",
                error=f"Worker lost {worker_losses} times; giving up (MAX_WORKER_LOSS_RECOVERIES).",
                finished=True,
                attempt=attempt,
            )
            logger.error("[ABANDONED] task_id=%s lost %d workers; marked FAILURE", task_id, worker_losses)
            continue

        logger.warning("[RECOVER] task_id=%s (%s): worker lost, re-publishing", task_id, job["task_name"])
        await _count("worker_lost_recovered")
        await publisher.publish_job(job)
    return handled


async def _count(name: str) -> None:
    try:
        await db.increment_counter(name)
    except Exception:
        logger.debug("Could not count event %s", name, exc_info=True)


async def run_once() -> None:
    """One scheduler loop. Errors are logged, never fatal — the next loop retries."""
    for step in (dispatch_due_schedules, relay_outbox, recover_abandoned_jobs):
        try:
            await step()
        except Exception:
            logger.exception("Scheduler step %s failed; will retry next loop", step.__name__)
    _touch_heartbeat()


async def run_forever(stop: asyncio.Event) -> None:
    if settings.AUTO_MIGRATE:
        await db.init_db()
    await seed_system_schedules()
    logger.info(
        "Scheduler %s started (poll every %ss)",
        f"{socket.gethostname()}:{os.getpid()}",
        settings.SCHEDULE_POLL_INTERVAL_SECONDS,
    )
    webhook_loop = asyncio.create_task(run_webhooks_forever(stop))
    while not stop.is_set():
        await run_once()
        # Jitter so replicas don't hit the database in lockstep.
        delay = settings.SCHEDULE_POLL_INTERVAL_SECONDS + random.uniform(0, 1)
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
        except TimeoutError:
            pass
    await webhook_loop
    logger.info("Scheduler stopped")


async def run_webhooks_forever(stop: asyncio.Event) -> None:
    """Send due webhooks until `stop` is set. Errors are logged; the next pass retries."""
    async with httpx.AsyncClient(timeout=settings.WEBHOOK_TIMEOUT_SECONDS, follow_redirects=False) as client:
        while not stop.is_set():
            try:
                await webhooks.deliver_due_webhooks(client)
            except Exception:
                logger.exception("Webhook delivery pass failed; will retry")
            try:
                await asyncio.wait_for(stop.wait(), timeout=settings.WEBHOOK_POLL_INTERVAL_SECONDS)
            except TimeoutError:
                pass


def _touch_heartbeat() -> None:
    try:
        with open(settings.SCHEDULER_HEARTBEAT_FILE, "a"):
            os.utime(settings.SCHEDULER_HEARTBEAT_FILE)
    except OSError:
        logger.warning("Could not touch %s", settings.SCHEDULER_HEARTBEAT_FILE, exc_info=True)


def healthcheck() -> int:
    """0 if the loop touched its heartbeat file recently, else 1."""
    max_age = settings.SCHEDULE_POLL_INTERVAL_SECONDS * 3 + 30
    try:
        age = time.time() - os.path.getmtime(settings.SCHEDULER_HEARTBEAT_FILE)
    except OSError:
        return 1
    return 0 if age < max_age else 1


def main() -> None:
    if sys.argv[1:] == ["healthcheck"]:
        sys.exit(healthcheck())

    setup_logging(log_filename="scheduler.log")
    setup_tracing("job-scheduler-scheduler")

    async def _main() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)  # finish the current loop, then exit
        await run_forever(stop)

    asyncio.run(_main())


if __name__ == "__main__":
    main()
