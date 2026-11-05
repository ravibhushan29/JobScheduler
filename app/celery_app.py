"""
The Celery application: configuration and the signal handlers that keep the
`jobs` table in step with every task's lifecycle.

Run any number of workers per queue:
    celery -A app.celery_app worker -Q default --loglevel=info
    celery -A app.celery_app worker -Q long -c 2 --loglevel=info

There is no Celery Beat: periodic and user-defined schedules are fired by the
scheduler service (`python -m app.scheduler`), which is safe to run with many
replicas. Every task uses `app.guard.GuardedTask`, which stops a job from
running twice.
"""

import logging
import os
import sqlite3
import sys
import time
from collections.abc import Callable, Coroutine

from celery import Celery, signals, states
from sqlalchemy import exc as sa_exc

from app import db
from app.config import settings
from app.logging_config import setup_logging

logger = logging.getLogger(__name__)


def _enable_sqlite_wal(db_path: str) -> None:
    """Put the SQLite result-backend file in WAL mode (persists in the file)."""
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    try:
        with sqlite3.connect(db_path, timeout=30) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
    except sqlite3.Error:
        logger.warning("Could not enable WAL on %s", db_path, exc_info=True)


if settings.CELERY_RESULT_BACKEND.startswith("db+sqlite"):
    _enable_sqlite_wal(settings.CELERY_RESULTS_DB_PATH)


def _broker_transport_options() -> dict:
    options = {"visibility_timeout": settings.BROKER_VISIBILITY_TIMEOUT}
    if settings.CELERY_BROKER_URL.startswith("sentinel://"):
        options["master_name"] = settings.REDIS_SENTINEL_MASTER
        if settings.REDIS_PASSWORD:
            options["sentinel_kwargs"] = {"password": settings.REDIS_PASSWORD}
    return options


_uses_database_backend = settings.CELERY_RESULT_BACKEND.startswith("db+")

celery_app = Celery(
    settings.APP_NAME,
    broker=settings.CELERY_BROKER_URL,
    # The SQLAlchemy backend, subclassed to retry through DB outages (app/result_backend.py).
    backend="app.result_backend:ResilientDatabaseBackend" if _uses_database_backend else settings.CELERY_RESULT_BACKEND,
    include=["app.tasks"],
    task_cls="app.guard:GuardedTask",
)

celery_app.conf.update(
    # --- Result backend (database via SQLAlchemy) ---
    result_expires=settings.CELERY_RESULT_EXPIRES,
    result_extended=True,  # also store task name, args, kwargs, worker, queue
    database_engine_options={"pool_pre_ping": True}
    | ({} if settings.IS_POSTGRES else {"connect_args": {"timeout": 30}}),
    database_url=settings.CELERY_RESULT_BACKEND.removeprefix("db+"),
    # Retry result writes through a database outage (backoff 0.2 s doubling to
    # 10 s, ~5 minutes in total) instead of failing a task that succeeded.
    result_backend_always_retry=True,
    result_backend_max_retries=36,
    result_backend_base_sleep_between_retries_ms=200,
    result_backend_max_sleep_between_retries_ms=10_000,
    task_track_started=True,
    task_send_sent_event=True,
    # --- Serialization: JSON only, so broker messages can never carry pickles ---
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    # --- Delivery: at least once (the execution guard removes duplicates) ---
    # Ack after the task finishes; requeue it if its worker dies.
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    # On broker connection loss, stop tasks whose messages will be re-delivered.
    worker_cancel_long_running_tasks_on_connection_loss=True,
    broker_connection_retry_on_startup=True,
    broker_transport_options=_broker_transport_options(),
    # --- Runtime limits ---
    task_soft_time_limit=settings.TASK_SOFT_TIME_LIMIT,
    task_time_limit=settings.TASK_TIME_LIMIT,
    # --- Queues and ordering (see FIFO_ENABLED in app/config.py) ---
    task_default_queue=settings.DEFAULT_QUEUE_NAME,
    worker_prefetch_multiplier=1 if settings.FIFO_ENABLED else 4,
    # --- Time: everything, including cron expressions, is UTC ---
    timezone="UTC",
    enable_utc=True,
)


# ---------------------------------------------------------------------------
# Logging: connecting to this signal makes Celery use our configuration.
# ---------------------------------------------------------------------------
@signals.setup_logging.connect
def _configure_celery_logging(**_) -> None:
    role = next((arg for arg in sys.argv if arg in ("worker", "flower")), "worker")
    setup_logging(log_filename=f"{role}.log")


# ---------------------------------------------------------------------------
# Lifecycle signals -> log lines + job-row updates.
#
# The execution guard records STARTED when it wins the claim and attaches the
# claim's attempt number to the request. Every update below is fenced by that
# number, so a superseded attempt can't overwrite a newer one, and copies the
# guard dropped (no attempt number) write nothing. A failed DB write is logged
# but never fails the task.
# ---------------------------------------------------------------------------
def _attempt_of(request) -> int | None:
    return getattr(request, "execution_attempt", None)


_DB_UNAVAILABLE = (sa_exc.OperationalError, sa_exc.InterfaceError, sa_exc.TimeoutError, OSError)


def _record(make_update: Callable[[], Coroutine], action: str, task_id: str, durable: bool = False) -> None:
    """
    Run a job-row update. With `durable=True` (final states), keep retrying
    while the database is unreachable, for up to FINAL_STATE_WRITE_TIMEOUT_SECONDS:
    losing a SUCCESS write would make the job look abandoned and re-run it.
    """
    deadline = time.monotonic() + (settings.FINAL_STATE_WRITE_TIMEOUT_SECONDS if durable else 0)
    delay = 1.0
    while True:
        try:
            db.run_sync(make_update())
            return
        except _DB_UNAVAILABLE:
            if time.monotonic() >= deadline:
                logger.exception("Could not record %s for task_id=%s", action, task_id)
                return
            logger.warning(
                "Database unavailable recording %s for task_id=%s; retrying in %.0fs", action, task_id, delay
            )
            time.sleep(delay)
            delay = min(delay * 2, 10)
        except Exception:
            logger.exception("Could not record %s for task_id=%s", action, task_id)
            return


@signals.task_prerun.connect
def _on_task_prerun(task_id, task, args, kwargs, **_) -> None:
    logger.info("[RECEIVED] task=%s id=%s", task.name, task_id)
    logger.debug("[RECEIVED] id=%s args=%r kwargs=%r", task_id, args, kwargs)


@signals.task_postrun.connect
def _on_task_postrun(task_id, task, retval, state, **_) -> None:
    attempt = _attempt_of(task.request)
    if attempt is None or state == states.RETRY:
        return  # a dropped duplicate, or a retry (recorded by _on_task_retry)
    logger.info("[END] task=%s id=%s state=%s attempt=%s", task.name, task_id, state, attempt)
    _record(
        lambda: db.update_job_status(
            task_id,
            status=state,
            result=retval if state == states.SUCCESS else None,
            retries=task.request.retries,
            finished=state in db.TERMINAL_STATES,
            attempt=attempt,
        ),
        state,
        task_id,
        durable=state in db.TERMINAL_STATES,
    )


@signals.task_retry.connect
def _on_task_retry(request, reason, **_) -> None:
    logger.warning("[RETRY] task=%s id=%s reason=%s", request.task, request.id, reason)
    attempt = _attempt_of(request)
    if attempt is None:
        return
    _record(
        lambda: db.update_job_status(request.id, status=states.RETRY, error=str(reason), attempt=attempt),
        states.RETRY,
        request.id,
        durable=True,
    )


@signals.task_failure.connect
def _on_task_failure(task_id, exception, einfo=None, sender=None, **_) -> None:
    request = getattr(sender, "request", None)
    logger.error(
        "[FAILURE] task=%s id=%s exception=%r",
        getattr(sender, "name", "?"),
        task_id,
        exception,
        exc_info=einfo.exc_info if einfo is not None else None,
    )
    attempt = _attempt_of(request)
    if attempt is None:
        return
    _record(
        lambda: db.update_job_status(
            task_id,
            status=states.FAILURE,
            error=str(exception),
            traceback=str(einfo.traceback) if einfo is not None else None,
            retries=getattr(request, "retries", None),
            finished=True,
            attempt=attempt,
        ),
        states.FAILURE,
        task_id,
        durable=True,
    )


@signals.worker_ready.connect
def _on_worker_ready(**_) -> None:
    if settings.AUTO_MIGRATE:
        db.run_sync(db.init_db())
    logger.info("Celery worker is ready and listening for tasks.")
