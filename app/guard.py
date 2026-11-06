"""
The execution guard: the base class of every task, which makes a job run
**once**, even though the broker may deliver its message more than once.

Why messages get duplicated
---------------------------
Celery + Redis deliver *at least once*. With `task_acks_late`, a message
comes back if its worker dies, loses its connection, or holds it longer than
the broker's visibility timeout. Publishers may also send the same job twice
(a scheduler retrying a publish, the outbox relay). So "no double execution"
has to be enforced where the work happens: in the worker.

What the guard does on every delivery
-------------------------------------
1. Atomically claims the job in the database (`db.acquire_execution`).
2. Claim won  -> run the task, renewing a short lease in the background
   until it returns. `attempts` (the claim's fencing token) is attached to
   the request, so status updates from a superseded attempt are ignored.
3. Job already finished or cancelled -> drop this copy.
4. Job running elsewhere with a live lease -> re-queue this copy to check
   again later. When the original finishes the copy is dropped; if the
   original's worker died, the lease runs out and the copy takes over.
5. Database unreachable -> hand the message back to the broker (never run
   a job without the claim, never drop it).

What it cannot do
-----------------
If a worker dies *during* a task, the task runs again elsewhere — the only
alternative is losing it. Tasks with external side effects should pass the
job's `task_id` to that system as an idempotency key (see
`send_notification`) so a re-run after a crash has no extra effect.
"""

import logging
import threading
import time
from datetime import datetime, timezone

from celery import Task
from celery.exceptions import Ignore, Reject
from sqlalchemy import exc as sa_exc

from app import db
from app.config import settings

logger = logging.getLogger(__name__)

#: Errors meaning "the database is unreachable right now" (not a bug in the task).
_DB_UNAVAILABLE = (sa_exc.OperationalError, sa_exc.InterfaceError, sa_exc.TimeoutError, OSError)

#: Pause before handing a message back while the database is down, so workers
#: don't spin on it.
DB_UNAVAILABLE_BACKOFF_SECONDS = 2


def count_event(name: str) -> None:
    """Best-effort bump of a cluster-wide event counter (see /metrics); never raises."""
    try:
        db.run_sync(db.increment_counter(name))
    except Exception:
        logger.debug("Could not count event %s", name, exc_info=True)


class _LeaseKeeper:
    """Renews a running attempt's lease every lease/3 seconds until stopped."""

    def __init__(self, task_id: str, attempt: int):
        self._task_id = task_id
        self._attempt = attempt
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name=f"lease-{task_id}", daemon=True)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc_info):
        self._stop.set()
        self._thread.join(timeout=5)

    def _run(self) -> None:
        interval = max(settings.EXECUTION_LEASE_SECONDS / 3, 1)
        while not self._stop.wait(interval):
            try:
                renewed = db.run_sync(db.renew_lease(self._task_id, self._attempt, settings.EXECUTION_LEASE_SECONDS))
            except Exception:
                logger.warning("Could not renew lease for task_id=%s", self._task_id, exc_info=True)
                continue
            if not renewed:
                count_event("lease_lost")
                logger.error(
                    "[LEASE LOST] task_id=%s attempt=%s: another worker took over this job",
                    self._task_id,
                    self._attempt,
                )
                return


class GuardedTask(Task):
    """Base class for all tasks (set as `task_cls` in app.celery_app)."""

    def __call__(self, *args, **kwargs):
        request = self.request
        if request.id is None or request.called_directly:
            return super().__call__(*args, **kwargs)  # a plain function call, not a queued job

        delivery_info = request.delivery_info or {}
        try:
            claim = db.run_sync(
                db.acquire_execution(
                    request.id,
                    self.name,
                    list(args),
                    dict(kwargs),
                    queue=delivery_info.get("routing_key") or getattr(self, "queue", None),
                    worker=request.hostname,
                    retries=request.retries or 0,
                    lease_seconds=settings.EXECUTION_LEASE_SECONDS,
                )
            )
        except _DB_UNAVAILABLE as exc:
            # Without the database we can't know whether the job already ran,
            # so never run it blind — and never drop it. Hand the message back
            # to the broker; it is retried until the database returns.
            logger.warning("[DB UNAVAILABLE] task=%s id=%s requeued: %s", self.name, request.id, exc)
            time.sleep(DB_UNAVAILABLE_BACKOFF_SECONDS)
            raise Reject(exc, requeue=True) from exc

        if claim.acquired:
            request.execution_attempt = claim.attempt  # read by the signal handlers
            request.job_max_retries = claim.max_retries  # per-job override, read by retry()
            with _LeaseKeeper(request.id, claim.attempt):
                return self.run(*args, **kwargs)

        if claim.status in db.TERMINAL_STATES:
            logger.warning("[DUPLICATE DROPPED] task=%s id=%s status=%s", self.name, request.id, claim.status)
            count_event("duplicate_dropped")
            raise Ignore()

        count_event("duplicate_deferred")

        delay = self._seconds_until(claim.lease_expires_at)
        logger.warning(
            "[DUPLICATE DEFERRED] task=%s id=%s is running elsewhere; re-checking in %ss", self.name, request.id, delay
        )
        self.signature_from_request(request, args, kwargs, countdown=delay).apply_async()
        raise Ignore()

    def retry(self, *args, max_retries=None, **kwargs):
        """Celery's retry, honouring a per-job `max_retries` given at submission."""
        if max_retries is None:
            max_retries = getattr(self.request, "job_max_retries", None)
        return super().retry(*args, max_retries=max_retries, **kwargs)

    @staticmethod
    def _seconds_until(lease_expires_at: datetime | None) -> int:
        if lease_expires_at is None:
            return 1
        remaining = (lease_expires_at - datetime.now(timezone.utc)).total_seconds()
        return int(min(max(remaining + 1, 1), settings.DUPLICATE_RECHECK_SECONDS))
