"""
Persistence for job history and schedules — the shared state that keeps
jobs from running twice across any number of pods.

Backends
--------
* **Postgres** (`DATABASE_URL=postgresql://...`) — required as soon as more
  than one machine is involved.
* **SQLite** (the default) — one machine only: local dev and tests.

The code is SQLAlchemy Core, so every query below runs unchanged on both.

Tables
------
* `jobs` — one row per job: the audit trail behind `GET /jobs`, the
  *outbox* (a row with `published_at IS NULL` has not reached the broker
  yet), and the *execution lock* (`status`, `attempts`, `lease_expires_at`).
* `schedules` — once / interval / cron schedules, including the system
  schedules that replace Celery Beat (rows with a `name`).

How double execution is prevented (details on each function):
* `fire_schedule` claims a due run with an optimistic version check, so
  exactly one scheduler replica fires each due time.
* `create_job` honours idempotency keys, so a retried HTTP request creates
  no second job.
* `acquire_execution` is the compare-and-set a worker must win before it may
  run a job; `attempts` doubles as a fencing token for later updates.

Calling from sync code
----------------------
Every function is `async`. Synchronous callers (Celery workers) use
`run_sync(coro)`, which runs it on a per-process background event loop so
connection pools are reused rather than rebuilt per call.
"""

import asyncio
import json
import logging
import os
import threading
import uuid
import weakref
from collections.abc import Coroutine
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from celery import states
from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    event,
    func,
    inspect,
    or_,
    select,
    text,
    true,
    update,
)
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.schema import CreateColumn
from sqlalchemy.types import TypeDecorator

from app.config import settings

logger = logging.getLogger(__name__)

#: Job statuses that never change again.
TERMINAL_STATES = frozenset(states.READY_STATES)  # SUCCESS, FAILURE, REVOKED

#: Statuses from which a worker may start (or restart) executing a job.
CLAIMABLE_STATES = ("QUEUED", states.RETRY)

#: Arbitrary constant: the Postgres advisory lock that serializes migrations.
_MIGRATION_LOCK_ID = 7_202_611


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
class UtcDateTime(TypeDecorator):
    """
    Timezone-aware UTC datetime.

    Postgres stores TIMESTAMPTZ. SQLite stores ISO-8601 text, the format
    earlier versions used, so existing files keep working and values still
    sort correctly as text.
    """

    impl = DateTime(timezone=True)
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "sqlite":
            return dialect.type_descriptor(String(40))
        return dialect.type_descriptor(DateTime(timezone=True))

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        value = value.astimezone(timezone.utc)
        return value.isoformat(timespec="microseconds") if dialect.name == "sqlite" else value

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        if isinstance(value, str):
            value = datetime.fromisoformat(value)
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


metadata = MetaData()

jobs = Table(
    "jobs",
    metadata,
    Column("task_id", String(255), primary_key=True),
    Column("task_name", String(255), nullable=False),
    Column("args", JSON, nullable=False),
    Column("kwargs", JSON, nullable=False),
    Column("queue", String(255)),
    Column("status", String(32), nullable=False, server_default="QUEUED"),
    Column("result", JSON(none_as_null=True)),
    Column("error", Text),  # short failure reason
    Column("traceback", Text),  # full formatted traceback
    Column("retries", Integer, nullable=False, server_default="0"),  # Celery retries so far
    Column("attempts", Integer, nullable=False, server_default="0"),  # executions started (fencing token)
    Column("lease_expires_at", UtcDateTime),  # while STARTED: when the running worker's claim lapses
    Column("worker", String(255)),
    Column("retry_of", String(255)),  # task_id this job manually re-submits
    Column("schedule_id", Integer),  # schedule that fired it
    Column("idempotency_key", String(255)),
    Column("published_at", UtcDateTime),  # NULL = not yet handed to the broker (outbox)
    Column("publish_attempted_at", UtcDateTime),
    Column("submitted_at", UtcDateTime, nullable=False),
    Column("started_at", UtcDateTime),
    Column("finished_at", UtcDateTime),
    # --- 5.0: per-job options ---
    Column("tenant", String(255)),  # owner, from the API key; NULL = no tenant
    Column("priority", Integer),  # broker priority: 0 high, 3 normal, 6 low (Redis: lower runs first)
    Column("run_after", UtcDateTime),  # not published before this time (delayed start)
    Column("timeout_seconds", Integer),  # per-job soft time limit (hard limit = +10 s)
    Column("max_retries", Integer),  # per-job override of the task's retry limit
    Column("callback_url", String(2048)),  # webhook called when the job finishes
    # --- 5.0: workflows ---
    Column("workflow_id", String(255)),
    Column("workflow_step", String(255)),  # step id, unique within its workflow
    Column("depends_on", JSON),  # step ids that must succeed first
)
Index("idx_jobs_status", jobs.c.status)
Index("idx_jobs_submitted_at", jobs.c.submitted_at)
Index("idx_jobs_finished_at", jobs.c.finished_at)
Index("idx_jobs_published_at", jobs.c.published_at)
Index("idx_jobs_lease", jobs.c.status, jobs.c.lease_expires_at)
Index("uq_jobs_idempotency_key", jobs.c.idempotency_key, unique=True)
Index("idx_jobs_tenant", jobs.c.tenant, jobs.c.submitted_at)
Index("idx_jobs_workflow", jobs.c.workflow_id)
Index("idx_jobs_retry_of", jobs.c.retry_of)

#: Status of a workflow step that is waiting for the steps it depends on.
WAITING = "WAITING"

#: API priority names -> broker priority (Redis consumes lower numbers first).
PRIORITIES = {"high": 0, "normal": 3, "low": 6}
PRIORITY_NAMES = {value: name for name, value in PRIORITIES.items()}

workflows = Table(
    "workflows",
    metadata,
    Column("id", String(255), primary_key=True),
    Column("name", String(255)),
    Column("tenant", String(255)),
    Column("status", String(32), nullable=False, server_default="RUNNING"),  # RUNNING | SUCCESS | FAILURE | REVOKED
    Column("callback_url", String(2048)),  # called once, when every step has finished
    Column("idempotency_key", String(255)),
    Column("created_at", UtcDateTime, nullable=False),
    Column("finished_at", UtcDateTime),
)
Index("uq_workflows_idempotency_key", workflows.c.idempotency_key, unique=True)
Index("idx_workflows_tenant", workflows.c.tenant, workflows.c.created_at)

webhook_deliveries = Table(
    "webhook_deliveries",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("event", String(64), nullable=False),  # job.finished | workflow.finished
    Column("subject_id", String(255), nullable=False),  # task_id or workflow id
    Column("url", String(2048), nullable=False),
    Column("payload", JSON, nullable=False),
    Column("status", String(16), nullable=False, server_default="PENDING"),  # PENDING | DELIVERED | FAILED
    Column("attempts", Integer, nullable=False, server_default="0"),
    Column("next_attempt_at", UtcDateTime, nullable=False),
    Column("last_status_code", Integer),
    Column("last_error", Text),
    Column("version", Integer, nullable=False, server_default="0"),  # optimistic claim
    Column("created_at", UtcDateTime, nullable=False),
    Column("delivered_at", UtcDateTime),
    sqlite_autoincrement=True,
)
Index("idx_webhooks_due", webhook_deliveries.c.status, webhook_deliveries.c.next_attempt_at)
Index("idx_webhooks_subject", webhook_deliveries.c.subject_id)

api_keys = Table(
    "api_keys",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String(255), nullable=False),
    Column("key_prefix", String(16), nullable=False),  # first characters, to recognise a key
    Column("key_hash", String(64), nullable=False),  # SHA-256 of the key; the key itself is never stored
    Column("scopes", JSON, nullable=False),
    Column("tenant", String(255)),
    Column("allowed_tasks", JSON),  # NULL = every public task
    Column("rate_limit_per_minute", Integer),
    Column("created_at", UtcDateTime, nullable=False),
    Column("last_used_at", UtcDateTime),
    Column("revoked_at", UtcDateTime),
    sqlite_autoincrement=True,
)
Index("uq_api_keys_hash", api_keys.c.key_hash, unique=True)
Index("uq_api_keys_name", api_keys.c.name, unique=True)

audit_log = Table(
    "audit_log",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("at", UtcDateTime, nullable=False),
    Column("actor", String(255), nullable=False),  # API key name
    Column("tenant", String(255)),
    Column("action", String(64), nullable=False),  # e.g. job.submit, schedule.delete
    Column("resource_type", String(32)),
    Column("resource_id", String(255)),
    Column("details", JSON),
    Column("client_ip", String(64)),
    sqlite_autoincrement=True,
)
Index("idx_audit_at", audit_log.c.at)
Index("idx_audit_actor", audit_log.c.actor, audit_log.c.at)

#: Cluster-wide event counters (duplicates dropped, jobs recovered, …) for /metrics.
event_counters = Table(
    "event_counters",
    metadata,
    Column("name", String(64), primary_key=True),
    Column("value", Integer, nullable=False, server_default="0"),
)

schedules = Table(
    "schedules",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String(255)),  # set only on system schedules (defined in code)
    Column("task_name", String(255), nullable=False),
    Column("args", JSON, nullable=False),
    Column("kwargs", JSON, nullable=False),
    Column("schedule_type", String(16), nullable=False),  # once | interval | cron
    Column("run_at", UtcDateTime),
    Column("interval_seconds", Integer),
    Column("cron_expr", String(255)),  # evaluated in UTC
    Column("enabled", Boolean, nullable=False, server_default=true()),
    Column("next_run_at", UtcDateTime, nullable=False),
    Column("last_run_at", UtcDateTime),
    Column("run_count", Integer, nullable=False, server_default="0"),
    Column("version", Integer, nullable=False, server_default="0"),  # optimistic lock
    Column("idempotency_key", String(255)),
    Column("created_at", UtcDateTime, nullable=False),
    Column("tenant", String(255)),  # 5.0
    Column("timezone", String(64)),  # 5.0: IANA zone for cron_expr; NULL = UTC
    sqlite_autoincrement=True,
)
Index("idx_schedules_due", schedules.c.enabled, schedules.c.next_run_at)
Index("idx_schedules_tenant", schedules.c.tenant)
Index("uq_schedules_name", schedules.c.name, unique=True)
Index("uq_schedules_idempotency_key", schedules.c.idempotency_key, unique=True)

#: Schedule columns `update_schedule` may change (never caller-chosen SQL).
_UPDATABLE_SCHEDULE_COLUMNS = frozenset({"enabled", "interval_seconds", "cron_expr", "next_run_at", "timezone"})


# ---------------------------------------------------------------------------
# Engines and the sync bridge
# ---------------------------------------------------------------------------
_engines: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, AsyncEngine]" = weakref.WeakKeyDictionary()


def _json_serializer(value: Any) -> str:
    return json.dumps(value, default=str)


def _create_engine() -> AsyncEngine:
    if settings.IS_POSTGRES:
        return create_async_engine(
            settings.DATABASE_URL,
            json_serializer=_json_serializer,
            pool_size=settings.DB_POOL_SIZE,
            max_overflow=settings.DB_MAX_OVERFLOW,
            pool_pre_ping=True,
            pool_recycle=1800,
        )

    sqlite_path = settings.DATABASE_URL.split("///", 1)[-1]
    os.makedirs(os.path.dirname(sqlite_path) or ".", exist_ok=True)
    engine = create_async_engine(
        settings.DATABASE_URL,
        json_serializer=_json_serializer,
        poolclass=NullPool,
        connect_args={"timeout": 30},
    )

    @event.listens_for(engine.sync_engine, "connect")
    def _sqlite_pragmas(dbapi_conn, _):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")  # readers don't block the writer
        cursor.execute("PRAGMA busy_timeout=30000")  # wait for a lock instead of failing
        cursor.close()

    return engine


def engine() -> AsyncEngine:
    """The engine for the running event loop (async pools can't be shared across loops)."""
    loop = asyncio.get_running_loop()
    if loop not in _engines:
        _engines[loop] = _create_engine()
    return _engines[loop]


_bridge_lock = threading.Lock()
_bridge: Optional[tuple[int, asyncio.AbstractEventLoop]] = None


def _bridge_loop() -> asyncio.AbstractEventLoop:
    """A background event loop owned by this process (re-created after fork)."""
    global _bridge
    with _bridge_lock:
        if _bridge is None or _bridge[0] != os.getpid():
            loop = asyncio.new_event_loop()
            threading.Thread(target=loop.run_forever, name="db-bridge", daemon=True).start()
            _bridge = (os.getpid(), loop)
        return _bridge[1]


def run_sync(coro: Coroutine) -> Any:
    """Run a coroutine from synchronous code (Celery tasks/signals) and return its result."""
    return asyncio.run_coroutine_threadsafe(coro, _bridge_loop()).result()


def _insert(table: Table):
    """A dialect-specific INSERT supporting ON CONFLICT."""
    return (postgresql if settings.IS_POSTGRES else sqlite).insert(table)


def _row(result) -> Optional[dict]:
    row = result.mappings().first()
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# Migrations
# ---------------------------------------------------------------------------
async def init_db() -> None:
    """
    Create missing tables, columns and indexes. Safe to run repeatedly and,
    on Postgres, from many pods at once (an advisory lock serializes runs).

    Only additive changes are made; existing data is never rewritten, except
    that pre-4.0 job rows are marked as already published so the outbox
    relay doesn't replay history.
    """
    async with engine().begin() as conn:
        if settings.IS_POSTGRES:
            await conn.execute(text("SELECT pg_advisory_xact_lock(:id)"), {"id": _MIGRATION_LOCK_ID})
        await conn.run_sync(_migrate)
    logger.info("Database schema is up to date (%s)", settings.DATABASE_URL.split("@")[-1])


def _migrate(sync_conn) -> None:
    inspector = inspect(sync_conn)
    existing_tables = set(inspector.get_table_names())

    for table in metadata.sorted_tables:
        if table.name not in existing_tables:
            table.create(sync_conn)
            logger.info("Created table %s", table.name)
            continue

        present = {col["name"] for col in inspector.get_columns(table.name)}
        for column in table.columns:
            if column.name in present:
                continue
            column_ddl = CreateColumn(column).compile(dialect=sync_conn.dialect)
            sync_conn.execute(text(f"ALTER TABLE {table.name} ADD COLUMN {column_ddl}"))
            logger.info("Migrated: added column %s.%s", table.name, column.name)
            if (table.name, column.name) == ("jobs", "published_at"):
                sync_conn.execute(text("UPDATE jobs SET published_at = submitted_at"))

        for index in table.indexes:
            index.create(sync_conn, checkfirst=True)


async def ping() -> None:
    """Raise if the database cannot be queried (readiness probe)."""
    async with engine().connect() as conn:
        await conn.execute(text("SELECT 1"))


# ---------------------------------------------------------------------------
# Jobs: creation, outbox, history
# ---------------------------------------------------------------------------
async def create_job(
    task_id: str,
    task_name: str,
    args: list,
    kwargs: dict,
    queue: Optional[str] = None,
    retry_of: Optional[str] = None,
    schedule_id: Optional[int] = None,
    idempotency_key: Optional[str] = None,
    conn: Optional[AsyncConnection] = None,
    *,
    tenant: Optional[str] = None,
    priority: Optional[int] = None,
    run_after: Optional[datetime] = None,
    timeout_seconds: Optional[int] = None,
    max_retries: Optional[int] = None,
    callback_url: Optional[str] = None,
    workflow_id: Optional[str] = None,
    workflow_step: Optional[str] = None,
    depends_on: Optional[list[str]] = None,
) -> bool:
    """
    Insert a not-yet-published job: QUEUED, or WAITING for a workflow step
    that depends on other steps. Returns False — inserting nothing — if
    `idempotency_key` (or `task_id`) already exists.

    Always call this BEFORE publishing; `published_at` stays NULL until
    `mark_published`, so the outbox relay can finish a publish that crashed.
    A job with `run_after` in the future is left for the relay to publish
    when it becomes due.
    """
    statement = (
        _insert(jobs)
        .values(
            task_id=task_id,
            task_name=task_name,
            args=list(args),
            kwargs=dict(kwargs),
            queue=queue,
            status=WAITING if depends_on else "QUEUED",
            retry_of=retry_of,
            schedule_id=schedule_id,
            idempotency_key=idempotency_key,
            submitted_at=utcnow(),
            tenant=tenant,
            priority=priority,
            run_after=run_after,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            callback_url=callback_url,
            workflow_id=workflow_id,
            workflow_step=workflow_step,
            depends_on=list(depends_on) if depends_on else None,
        )
        .on_conflict_do_nothing()
        .returning(jobs.c.task_id)  # rowcount isn't reliable for ON CONFLICT on every driver
    )
    if conn is not None:
        return (await conn.execute(statement)).scalar_one_or_none() is not None
    async with engine().begin() as own_conn:
        return (await own_conn.execute(statement)).scalar_one_or_none() is not None


async def mark_published(task_id: str) -> None:
    async with engine().begin() as conn:
        await conn.execute(
            update(jobs).where(jobs.c.task_id == task_id, jobs.c.published_at.is_(None)).values(published_at=utcnow())
        )


async def claim_unpublished_jobs(older_than_seconds: int, limit: int = 100) -> list[dict]:
    """
    Outbox relay: return jobs that were recorded but never published, and
    claim each so concurrent relays (one per scheduler replica) don't publish
    the same job at the same time. A job qualifies when either:

      * it has no `run_after` and is older than `older_than_seconds` (its
        publisher crashed, or the broker was down), or
      * its `run_after` has arrived (a delayed job, or a workflow step whose
        dependencies just succeeded) — published straight away.
    """
    now = utcnow()
    cutoff = now - timedelta(seconds=older_than_seconds)
    async with engine().begin() as conn:
        candidates = (
            (
                await conn.execute(
                    select(jobs)
                    .where(
                        jobs.c.published_at.is_(None),
                        jobs.c.status == "QUEUED",
                        or_(
                            jobs.c.run_after.is_(None) & (jobs.c.submitted_at < cutoff),
                            jobs.c.run_after <= now,
                        ),
                        or_(jobs.c.publish_attempted_at.is_(None), jobs.c.publish_attempted_at < cutoff),
                    )
                    .order_by(func.coalesce(jobs.c.run_after, jobs.c.submitted_at))
                    .limit(limit)
                )
            )
            .mappings()
            .all()
        )
    claimed = []
    for job in candidates:
        async with engine().begin() as conn:
            result = await conn.execute(
                update(jobs)
                .where(
                    jobs.c.task_id == job["task_id"],
                    jobs.c.published_at.is_(None),
                    # optimistic check: unchanged since we read it, so only one relay wins
                    jobs.c.publish_attempted_at.is_not_distinct_from(job["publish_attempted_at"]),
                )
                .values(publish_attempted_at=now)
            )
        if result.rowcount == 1:
            claimed.append(dict(job))
    return claimed


async def update_job_status(
    task_id: str,
    status: str,
    result: Any = None,
    error: Optional[str] = None,
    traceback: Optional[str] = None,
    retries: Optional[int] = None,
    worker: Optional[str] = None,
    finished: bool = False,
    attempt: Optional[int] = None,
) -> bool:
    """
    Move a job to `status`, setting whichever optional fields are given.

    `attempt` is a fencing token: when given, the update applies only if the
    job's `attempts` still equals it. A worker that lost its lease (and was
    replaced by another) therefore cannot overwrite the newer attempt's state.

    A final state (SUCCESS, FAILURE, REVOKED) is written only once: a job that
    has already finished keeps its first final state. In the same transaction
    the job's webhook (if any) is queued and its workflow (if any) advanced,
    so neither can be lost or happen twice. Returns False if nothing was updated.
    """
    now = utcnow()
    values: dict[str, Any] = {"status": status}
    for column, value in (
        ("result", result),
        ("error", error),
        ("traceback", traceback),
        ("retries", retries),
        ("worker", worker),
    ):
        if value is not None:
            values[column] = value
    if finished:
        values["finished_at"] = now
        values["lease_expires_at"] = None

    terminal = finished and status in TERMINAL_STATES
    statement = update(jobs).where(jobs.c.task_id == task_id).values(**values)
    if attempt is not None:
        statement = statement.where(jobs.c.attempts == attempt)
    if terminal:
        statement = statement.where(jobs.c.status.not_in(TERMINAL_STATES))
    statement = statement.returning(*jobs.c)
    async with engine().begin() as conn:
        row = _row(await conn.execute(statement))
        if row is None:
            return False
        if terminal:
            await _on_job_finished(conn, row, now)
        return True


def job_event_payload(job: dict) -> dict:
    """The JSON body of a `job.finished` webhook."""
    return {
        "event": "job.finished",
        "task_id": job["task_id"],
        "task_name": job["task_name"],
        "status": job["status"],
        "result": job["result"],
        "error": job["error"],
        "attempts": job["attempts"],
        "retries": job["retries"],
        "submitted_at": job["submitted_at"].isoformat() if job["submitted_at"] else None,
        "finished_at": job["finished_at"].isoformat() if job["finished_at"] else None,
        "workflow_id": job["workflow_id"],
        "workflow_step": job["workflow_step"],
        "schedule_id": job["schedule_id"],
    }


async def _on_job_finished(conn: AsyncConnection, job: dict, now: datetime) -> None:
    """Side effects of a job reaching a final state, inside its status transaction."""
    if job["callback_url"]:
        await _queue_webhook(conn, "job.finished", job["task_id"], job["callback_url"], job_event_payload(job), now)
    if job["workflow_id"]:
        await _advance_workflow(conn, job, now)


async def _queue_webhook(conn: AsyncConnection, event: str, subject_id: str, url: str, payload: dict, now) -> None:
    await conn.execute(
        webhook_deliveries.insert().values(
            event=event,
            subject_id=subject_id,
            url=url,
            payload=payload,
            status="PENDING",
            next_attempt_at=now,
            created_at=now,
        )
    )


async def get_job(task_id: str) -> Optional[dict]:
    async with engine().connect() as conn:
        return _row(await conn.execute(select(jobs).where(jobs.c.task_id == task_id)))


async def get_job_by_idempotency_key(key: str) -> Optional[dict]:
    async with engine().connect() as conn:
        return _row(await conn.execute(select(jobs).where(jobs.c.idempotency_key == key)))


def _job_filters(query, status=None, tenant=None, task_name=None, workflow_id=None):
    for column, value in (
        (jobs.c.status, status),
        (jobs.c.tenant, tenant),
        (jobs.c.task_name, task_name),
        (jobs.c.workflow_id, workflow_id),
    ):
        if value is not None:
            query = query.where(column == value)
    return query


async def list_jobs(
    limit: int = 50,
    offset: int = 0,
    status: Optional[str] = None,
    tenant: Optional[str] = None,
    task_name: Optional[str] = None,
    workflow_id: Optional[str] = None,
) -> list[dict]:
    """A page of jobs, newest first, optionally filtered. `tenant=None` means every tenant."""
    query = select(jobs).order_by(jobs.c.submitted_at.desc()).limit(limit).offset(offset)
    query = _job_filters(query, status or None, tenant, task_name, workflow_id)
    async with engine().connect() as conn:
        return [dict(row) for row in (await conn.execute(query)).mappings()]


async def count_jobs(
    status: Optional[str] = None,
    tenant: Optional[str] = None,
    task_name: Optional[str] = None,
    workflow_id: Optional[str] = None,
) -> int:
    query = _job_filters(select(func.count()).select_from(jobs), status or None, tenant, task_name, workflow_id)
    async with engine().connect() as conn:
        return (await conn.execute(query)).scalar_one()


async def delete_finished_jobs_before(cutoff: datetime) -> int:
    """Delete jobs (and their finished webhooks and workflows) that finished before `cutoff`."""
    async with engine().begin() as conn:
        result = await conn.execute(jobs.delete().where(jobs.c.finished_at.is_not(None), jobs.c.finished_at < cutoff))
        await conn.execute(
            webhook_deliveries.delete().where(
                webhook_deliveries.c.status != "PENDING", webhook_deliveries.c.created_at < cutoff
            )
        )
        await conn.execute(
            workflows.delete().where(workflows.c.finished_at.is_not(None), workflows.c.finished_at < cutoff)
        )
        return result.rowcount


async def failure_groups(tenant: Optional[str] = None, limit: int = 50) -> list[dict]:
    """
    Dead-letter view: FAILURE jobs not yet re-submitted, grouped by task and
    error, biggest group first.
    """
    retried = select(jobs.c.retry_of).where(jobs.c.retry_of.is_not(None)).scalar_subquery()
    query = (
        select(
            jobs.c.task_name,
            jobs.c.error,
            func.count().label("count"),
            func.min(jobs.c.finished_at).label("first_failed_at"),
            func.max(jobs.c.finished_at).label("last_failed_at"),
            func.max(jobs.c.task_id).label("example_task_id"),
        )
        .where(jobs.c.status == states.FAILURE, jobs.c.task_id.not_in(retried))
        .group_by(jobs.c.task_name, jobs.c.error)
        .order_by(func.count().desc(), func.max(jobs.c.finished_at).desc())
        .limit(limit)
    )
    if tenant is not None:
        query = query.where(jobs.c.tenant == tenant)
    async with engine().connect() as conn:
        return [dict(row) for row in (await conn.execute(query)).mappings()]


async def failed_jobs_to_retry(
    tenant: Optional[str] = None,
    task_name: Optional[str] = None,
    error: Optional[str] = None,
    limit: int = 100,
) -> list[dict]:
    """FAILURE jobs that haven't been re-submitted yet, oldest first, optionally filtered."""
    retried = select(jobs.c.retry_of).where(jobs.c.retry_of.is_not(None)).scalar_subquery()
    query = (
        select(jobs)
        .where(jobs.c.status == states.FAILURE, jobs.c.task_id.not_in(retried))
        .order_by(jobs.c.finished_at)
        .limit(limit)
    )
    for column, value in ((jobs.c.tenant, tenant), (jobs.c.task_name, task_name), (jobs.c.error, error)):
        if value is not None:
            query = query.where(column == value)
    async with engine().connect() as conn:
        return [dict(row) for row in (await conn.execute(query)).mappings()]


# ---------------------------------------------------------------------------
# Workflows: jobs that run after other jobs succeed (fan-out / fan-in)
# ---------------------------------------------------------------------------
async def create_workflow(
    workflow_id: str,
    name: Optional[str],
    steps: list[dict],
    tenant: Optional[str] = None,
    callback_url: Optional[str] = None,
    idempotency_key: Optional[str] = None,
) -> bool:
    """
    Record a workflow and one job per step, in one transaction. Steps with no
    dependencies are QUEUED (the caller publishes them); the rest are WAITING
    until `_advance_workflow` releases them. Returns False if
    `idempotency_key` already exists.

    Each step dict has: task_id, step, task_name, args, kwargs, queue,
    depends_on, and optionally priority, timeout_seconds, max_retries, callback_url.
    """
    async with engine().begin() as conn:
        inserted = await conn.execute(
            _insert(workflows)
            .values(
                id=workflow_id,
                name=name,
                tenant=tenant,
                status="RUNNING",
                callback_url=callback_url,
                idempotency_key=idempotency_key,
                created_at=utcnow(),
            )
            .on_conflict_do_nothing()
            .returning(workflows.c.id)
        )
        if inserted.scalar_one_or_none() is None:
            return False
        for step in steps:
            await create_job(
                step["task_id"],
                step["task_name"],
                step["args"],
                step["kwargs"],
                queue=step["queue"],
                conn=conn,
                tenant=tenant,
                priority=step.get("priority"),
                timeout_seconds=step.get("timeout_seconds"),
                max_retries=step.get("max_retries"),
                callback_url=step.get("callback_url"),
                workflow_id=workflow_id,
                workflow_step=step["step"],
                depends_on=step["depends_on"],
            )
    return True


async def get_workflow(workflow_id: str) -> Optional[dict]:
    """The workflow row plus `steps`: its jobs, in creation order."""
    async with engine().connect() as conn:
        workflow = _row(await conn.execute(select(workflows).where(workflows.c.id == workflow_id)))
        if workflow is None:
            return None
        steps = await conn.execute(
            select(jobs).where(jobs.c.workflow_id == workflow_id).order_by(jobs.c.submitted_at, jobs.c.task_id)
        )
        workflow["steps"] = [dict(row) for row in steps.mappings()]
    return workflow


async def get_workflow_by_idempotency_key(key: str) -> Optional[dict]:
    async with engine().connect() as conn:
        row = _row(await conn.execute(select(workflows.c.id).where(workflows.c.idempotency_key == key)))
    return await get_workflow(row["id"]) if row else None


async def list_workflows(limit: int = 50, offset: int = 0, tenant: Optional[str] = None) -> list[dict]:
    query = select(workflows).order_by(workflows.c.created_at.desc()).limit(limit).offset(offset)
    if tenant is not None:
        query = query.where(workflows.c.tenant == tenant)
    async with engine().connect() as conn:
        return [dict(row) for row in (await conn.execute(query)).mappings()]


async def skip_waiting_steps(workflow_id: str, reason: str) -> int:
    """Cancel every WAITING step of a workflow (they were never published). Returns how many."""
    now = utcnow()
    async with engine().begin() as conn:
        # Lock the workflow first, the same order `_advance_workflow` uses.
        await conn.execute(select(workflows.c.id).where(workflows.c.id == workflow_id).with_for_update())
        waiting = (
            await conn.execute(
                update(jobs)
                .where(jobs.c.workflow_id == workflow_id, jobs.c.status == WAITING)
                .values(status=states.REVOKED, error=reason, finished_at=now)
                .returning(*jobs.c)
            )
        ).mappings()
        skipped = [dict(row) for row in waiting]
        for job in skipped:
            if job["callback_url"]:
                await _queue_webhook(
                    conn, "job.finished", job["task_id"], job["callback_url"], job_event_payload(job), now
                )
        if skipped:
            await _finish_workflow_if_done(conn, workflow_id, now)
    return len(skipped)


async def cancel_waiting_step(task_id: str) -> bool:
    """
    Cancel one WAITING workflow step and skip the steps downstream of it.
    The workflow is locked first, the same order `_advance_workflow` uses,
    so this can't deadlock with a step finishing at the same moment.
    """
    now = utcnow()
    async with engine().begin() as conn:
        workflow_id = (
            await conn.execute(select(jobs.c.workflow_id).where(jobs.c.task_id == task_id))
        ).scalar_one_or_none()
        if workflow_id is None:
            return False
        await conn.execute(select(workflows.c.id).where(workflows.c.id == workflow_id).with_for_update())
        job = _row(
            await conn.execute(
                update(jobs)
                .where(jobs.c.task_id == task_id, jobs.c.status == WAITING)
                .values(status=states.REVOKED, error="Cancelled", finished_at=now)
                .returning(*jobs.c)
            )
        )
        if job is None:
            return False
        await _on_job_finished(conn, job, now)
    return True


async def _advance_workflow(conn: AsyncConnection, job: dict, now: datetime) -> None:
    """
    After a step finishes: release dependents whose dependencies have all
    succeeded, or skip (REVOKE) everything downstream of a step that failed or
    was cancelled; then close the workflow once every step has finished.

    The workflow row is locked first, so steps finishing at the same moment
    are processed one at a time. Each therefore sees the others' committed
    results, and a step that depends on several (fan-in) is released exactly once.
    """
    locked = await conn.execute(select(workflows.c.id).where(workflows.c.id == job["workflow_id"]).with_for_update())
    if locked.scalar_one_or_none() is None:
        return
    rows = await conn.execute(select(jobs).where(jobs.c.workflow_id == job["workflow_id"]))
    steps = [dict(row) for row in rows.mappings()]
    by_step = {step["workflow_step"]: step for step in steps}

    if job["status"] == states.SUCCESS:
        for step in steps:
            depends_on = step["depends_on"] or []
            if (
                step["status"] == WAITING
                and job["workflow_step"] in depends_on
                and all(by_step[dep]["status"] == states.SUCCESS for dep in depends_on)
            ):
                # run_after = now: the outbox relay publishes it on its next pass.
                await conn.execute(
                    update(jobs)
                    .where(jobs.c.task_id == step["task_id"], jobs.c.status == WAITING)
                    .values(status="QUEUED", run_after=now)
                )
                step["status"] = "QUEUED"
    else:
        ended = {job["workflow_step"]}
        changed = True
        while changed:  # walk downstream until nothing new is skipped
            changed = False
            for step in steps:
                blocked_by = sorted(set(step["depends_on"] or []) & ended)
                if step["status"] != WAITING or not blocked_by:
                    continue
                step.update(
                    status=states.REVOKED,
                    error=f"Skipped: upstream step(s) {blocked_by} did not succeed",
                    finished_at=now,
                )
                await conn.execute(
                    update(jobs)
                    .where(jobs.c.task_id == step["task_id"], jobs.c.status == WAITING)
                    .values(status=states.REVOKED, error=step["error"], finished_at=now)
                )
                if step["callback_url"]:
                    payload = job_event_payload(step)
                    await _queue_webhook(conn, "job.finished", step["task_id"], step["callback_url"], payload, now)
                ended.add(step["workflow_step"])
                changed = True

    await _finish_workflow_if_done(conn, job["workflow_id"], now, steps)


async def _finish_workflow_if_done(
    conn: AsyncConnection, workflow_id: str, now: datetime, steps: Optional[list[dict]] = None
) -> None:
    if steps is None:
        steps = [
            dict(r) for r in (await conn.execute(select(jobs).where(jobs.c.workflow_id == workflow_id))).mappings()
        ]
    if not steps or any(step["status"] not in TERMINAL_STATES for step in steps):
        return
    statuses = {step["status"] for step in steps}
    final = (
        states.SUCCESS
        if statuses == {states.SUCCESS}
        else states.FAILURE
        if states.FAILURE in statuses
        else states.REVOKED
    )
    closed = await conn.execute(
        update(workflows)
        .where(workflows.c.id == workflow_id, workflows.c.finished_at.is_(None))
        .values(status=final, finished_at=now)
        .returning(*workflows.c)
    )
    workflow = _row(closed)
    if workflow and workflow["callback_url"]:
        payload = {
            "event": "workflow.finished",
            "workflow_id": workflow_id,
            "name": workflow["name"],
            "status": final,
            "steps": {step["workflow_step"]: step["status"] for step in steps},
            "finished_at": now.isoformat(),
        }
        await _queue_webhook(conn, "workflow.finished", workflow_id, workflow["callback_url"], payload, now)


# ---------------------------------------------------------------------------
# Webhook deliveries (sent by the scheduler service; see app/webhooks.py)
# ---------------------------------------------------------------------------
async def claim_due_webhooks(lease_seconds: int, limit: int = 50) -> list[dict]:
    """
    Claim PENDING deliveries that are due. The claim pushes `next_attempt_at`
    forward by `lease_seconds` with an optimistic version check, so with many
    scheduler replicas each delivery is attempted by one of them at a time.
    """
    now = utcnow()
    async with engine().connect() as conn:
        candidates = (
            (
                await conn.execute(
                    select(webhook_deliveries)
                    .where(webhook_deliveries.c.status == "PENDING", webhook_deliveries.c.next_attempt_at <= now)
                    .order_by(webhook_deliveries.c.next_attempt_at)
                    .limit(limit)
                )
            )
            .mappings()
            .all()
        )
    claimed = []
    for delivery in candidates:
        async with engine().begin() as conn:
            result = await conn.execute(
                update(webhook_deliveries)
                .where(
                    webhook_deliveries.c.id == delivery["id"],
                    webhook_deliveries.c.version == delivery["version"],
                    webhook_deliveries.c.status == "PENDING",
                )
                .values(next_attempt_at=now + timedelta(seconds=lease_seconds), version=delivery["version"] + 1)
            )
        if result.rowcount == 1:
            claimed.append({**dict(delivery), "version": delivery["version"] + 1})
    return claimed


async def record_webhook_attempt(
    delivery: dict,
    delivered: bool,
    status_code: Optional[int],
    error: Optional[str],
    retry_at: Optional[datetime],
) -> bool:
    """Store one attempt's outcome. `retry_at=None` with `delivered=False` means give up (FAILED)."""
    now = utcnow()
    values: dict[str, Any] = {
        "attempts": webhook_deliveries.c.attempts + 1,
        "last_status_code": status_code,
        "last_error": error,
        "version": webhook_deliveries.c.version + 1,
    }
    if delivered:
        values.update(status="DELIVERED", delivered_at=now)
    elif retry_at is None:
        values.update(status="FAILED")
    else:
        values.update(next_attempt_at=retry_at)
    async with engine().begin() as conn:
        result = await conn.execute(
            update(webhook_deliveries)
            .where(webhook_deliveries.c.id == delivery["id"], webhook_deliveries.c.version == delivery["version"])
            .values(**values)
        )
        return result.rowcount == 1


async def list_webhook_deliveries(subject_id: str) -> list[dict]:
    async with engine().connect() as conn:
        result = await conn.execute(
            select(webhook_deliveries)
            .where(webhook_deliveries.c.subject_id == subject_id)
            .order_by(webhook_deliveries.c.id)
        )
        return [dict(row) for row in result.mappings()]


# ---------------------------------------------------------------------------
# Managed API keys and the audit log
# ---------------------------------------------------------------------------
async def create_api_key(
    name: str,
    key_hash: str,
    key_prefix: str,
    scopes: list[str],
    tenant: Optional[str],
    allowed_tasks: Optional[list[str]],
    rate_limit_per_minute: Optional[int],
) -> Optional[int]:
    """Insert a key and return its id, or None if `name` is taken."""
    async with engine().begin() as conn:
        result = await conn.execute(
            _insert(api_keys)
            .values(
                name=name,
                key_hash=key_hash,
                key_prefix=key_prefix,
                scopes=list(scopes),
                tenant=tenant,
                allowed_tasks=list(allowed_tasks) if allowed_tasks is not None else None,
                rate_limit_per_minute=rate_limit_per_minute,
                created_at=utcnow(),
            )
            .on_conflict_do_nothing()
            .returning(api_keys.c.id)
        )
        return result.scalar_one_or_none()


async def get_active_api_key(key_hash: str) -> Optional[dict]:
    async with engine().connect() as conn:
        return _row(
            await conn.execute(select(api_keys).where(api_keys.c.key_hash == key_hash, api_keys.c.revoked_at.is_(None)))
        )


async def get_api_key(key_id: int) -> Optional[dict]:
    async with engine().connect() as conn:
        return _row(await conn.execute(select(api_keys).where(api_keys.c.id == key_id)))


async def list_api_keys() -> list[dict]:
    async with engine().connect() as conn:
        return [dict(row) for row in (await conn.execute(select(api_keys).order_by(api_keys.c.id))).mappings()]


async def revoke_api_key(key_id: int) -> bool:
    async with engine().begin() as conn:
        result = await conn.execute(
            update(api_keys).where(api_keys.c.id == key_id, api_keys.c.revoked_at.is_(None)).values(revoked_at=utcnow())
        )
        return result.rowcount == 1


async def touch_api_key(key_id: int) -> None:
    async with engine().begin() as conn:
        await conn.execute(update(api_keys).where(api_keys.c.id == key_id).values(last_used_at=utcnow()))


async def add_audit_entry(
    actor: str,
    action: str,
    tenant: Optional[str] = None,
    resource_type: Optional[str] = None,
    resource_id: Optional[str] = None,
    details: Optional[dict] = None,
    client_ip: Optional[str] = None,
) -> None:
    async with engine().begin() as conn:
        await conn.execute(
            audit_log.insert().values(
                at=utcnow(),
                actor=actor,
                tenant=tenant,
                action=action,
                resource_type=resource_type,
                resource_id=resource_id,
                details=details,
                client_ip=client_ip,
            )
        )


async def list_audit_entries(
    limit: int = 50,
    offset: int = 0,
    actor: Optional[str] = None,
    action: Optional[str] = None,
    tenant: Optional[str] = None,
) -> list[dict]:
    query = select(audit_log).order_by(audit_log.c.id.desc()).limit(limit).offset(offset)
    for column, value in ((audit_log.c.actor, actor), (audit_log.c.action, action), (audit_log.c.tenant, tenant)):
        if value is not None:
            query = query.where(column == value)
    async with engine().connect() as conn:
        return [dict(row) for row in (await conn.execute(query)).mappings()]


async def delete_audit_entries_before(cutoff: datetime) -> int:
    async with engine().begin() as conn:
        return (await conn.execute(audit_log.delete().where(audit_log.c.at < cutoff))).rowcount


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------
async def increment_counter(name: str, by: int = 1) -> None:
    """Add `by` to a cluster-wide event counter (shown in /metrics as jobscheduler_events_total)."""
    statement = _insert(event_counters).values(name=name, value=by)
    statement = statement.on_conflict_do_update(
        index_elements=[event_counters.c.name], set_={"value": event_counters.c.value + by}
    )
    async with engine().begin() as conn:
        await conn.execute(statement)


async def metrics_snapshot(window_seconds: int) -> dict:
    """
    Everything /metrics reports, read in one connection. "Recent" values
    cover the last `window_seconds`.
    """
    now = utcnow()
    since = now - timedelta(seconds=window_seconds)
    snapshot: dict[str, Any] = {}
    async with engine().connect() as conn:
        snapshot["jobs"] = [
            dict(row)
            for row in (
                await conn.execute(
                    select(jobs.c.status, jobs.c.queue, jobs.c.task_name, func.count().label("count")).group_by(
                        jobs.c.status, jobs.c.queue, jobs.c.task_name
                    )
                )
            ).mappings()
        ]
        snapshot["finished_recent"] = [
            dict(row)
            for row in (
                await conn.execute(
                    select(jobs.c.task_name, jobs.c.status, func.count().label("count"))
                    .where(jobs.c.finished_at >= since)
                    .group_by(jobs.c.task_name, jobs.c.status)
                )
            ).mappings()
        ]
        snapshot["timings_recent"] = [
            dict(row)
            for row in (
                await conn.execute(
                    select(
                        jobs.c.task_name,
                        jobs.c.queue,
                        jobs.c.submitted_at,
                        jobs.c.run_after,
                        jobs.c.started_at,
                        jobs.c.finished_at,
                    )
                    .where(jobs.c.finished_at >= since, jobs.c.started_at.is_not(None))
                    .order_by(jobs.c.finished_at.desc())
                    .limit(20_000)
                )
            ).mappings()
        ]
        snapshot["oldest_queued"] = [
            dict(row)
            for row in (
                await conn.execute(
                    select(
                        jobs.c.queue,
                        func.min(func.coalesce(jobs.c.run_after, jobs.c.submitted_at)).label("oldest"),
                    )
                    .where(jobs.c.status == "QUEUED", jobs.c.published_at.is_not(None))
                    .group_by(jobs.c.queue)
                )
            ).mappings()
        ]
        outbox = (
            await conn.execute(
                select(func.count(), func.min(jobs.c.submitted_at)).where(
                    jobs.c.published_at.is_(None),
                    jobs.c.status == "QUEUED",
                    or_(jobs.c.run_after.is_(None), jobs.c.run_after <= now),
                )
            )
        ).one()
        snapshot["outbox_pending"], snapshot["outbox_oldest"] = outbox[0], outbox[1]
        snapshot["delayed"] = (
            await conn.execute(
                select(func.count()).where(
                    jobs.c.published_at.is_(None), jobs.c.status == "QUEUED", jobs.c.run_after > now
                )
            )
        ).scalar_one()
        snapshot["schedules"] = [
            dict(row)
            for row in (
                await conn.execute(
                    select(schedules.c.enabled, func.count().label("count")).group_by(schedules.c.enabled)
                )
            ).mappings()
        ]
        snapshot["most_overdue_schedule"] = (
            await conn.execute(
                select(func.min(schedules.c.next_run_at)).where(
                    schedules.c.enabled.is_(True), schedules.c.next_run_at <= now
                )
            )
        ).scalar_one()
        snapshot["webhooks"] = [
            dict(row)
            for row in (
                await conn.execute(
                    select(webhook_deliveries.c.status, func.count().label("count")).group_by(
                        webhook_deliveries.c.status
                    )
                )
            ).mappings()
        ]
        snapshot["workflows"] = [
            dict(row)
            for row in (
                await conn.execute(select(workflows.c.status, func.count().label("count")).group_by(workflows.c.status))
            ).mappings()
        ]
        snapshot["counters"] = {
            row["name"]: row["value"] for row in (await conn.execute(select(event_counters))).mappings()
        }
    snapshot["now"] = now
    return snapshot


# ---------------------------------------------------------------------------
# Jobs: the execution lock
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ExecutionClaim:
    acquired: bool
    attempt: Optional[int] = None  # fencing token, when acquired
    max_retries: Optional[int] = None  # per-job retry limit override, when acquired
    status: Optional[str] = None  # current status, when not acquired
    lease_expires_at: Optional[datetime] = None  # holder's lease, when not acquired


async def acquire_execution(
    task_id: str,
    task_name: str,
    args: list,
    kwargs: dict,
    queue: Optional[str],
    worker: Optional[str],
    retries: int,
    lease_seconds: int,
) -> ExecutionClaim:
    """
    Compare-and-set a job from "waiting" to STARTED for this worker.

    Succeeds only if one of these holds (checked atomically in one UPDATE):
      * the job is QUEUED or RETRY;
      * it is STARTED but the holder's lease has expired (the worker died);
      * it is STARTED by an earlier attempt and this delivery is a Celery
        retry of it (`retries` is higher), i.e. the earlier attempt ended.

    It never succeeds for SUCCESS, FAILURE or REVOKED jobs, so a finished or
    cancelled job can never run again. A job that wasn't created through
    the API or scheduler (e.g. `.delay()` from code) gets its row here.
    """
    now = utcnow()
    async with engine().begin() as conn:
        await create_job(task_id, task_name, args, kwargs, queue=queue, conn=conn)
        result = await conn.execute(
            update(jobs)
            .where(
                jobs.c.task_id == task_id,
                or_(
                    jobs.c.status.in_(CLAIMABLE_STATES),
                    (jobs.c.status == states.STARTED) & (jobs.c.lease_expires_at < now),
                    (jobs.c.status == states.STARTED) & (jobs.c.retries < retries),
                ),
            )
            .values(
                status=states.STARTED,
                attempts=jobs.c.attempts + 1,
                retries=retries,
                worker=worker,
                started_at=now,
                lease_expires_at=now + timedelta(seconds=lease_seconds),
                published_at=func.coalesce(jobs.c.published_at, now),
            )
            .returning(jobs.c.attempts, jobs.c.max_retries)
        )
        won = result.first()
        if won is not None:
            return ExecutionClaim(acquired=True, attempt=won.attempts, max_retries=won.max_retries)
        current = _row(
            await conn.execute(select(jobs.c.status, jobs.c.lease_expires_at).where(jobs.c.task_id == task_id))
        )
    return ExecutionClaim(acquired=False, status=current["status"], lease_expires_at=current["lease_expires_at"])


async def renew_lease(task_id: str, attempt: int, lease_seconds: int) -> bool:
    """Extend a running attempt's lease. False means the lease was lost to another worker."""
    async with engine().begin() as conn:
        result = await conn.execute(
            update(jobs)
            .where(jobs.c.task_id == task_id, jobs.c.attempts == attempt, jobs.c.status == states.STARTED)
            .values(lease_expires_at=utcnow() + timedelta(seconds=lease_seconds))
        )
        return result.rowcount == 1


async def claim_abandoned_jobs(grace_seconds: int, limit: int = 100) -> list[dict]:
    """
    Find STARTED jobs whose worker stopped renewing the lease more than
    `grace_seconds` ago (the worker crashed, its node died, or it hit the hard
    time limit), and claim each for recovery.

    The claim is an optimistic check on `publish_attempted_at`, so with many
    scheduler replicas each abandoned job is handled by one of them, at most
    once per grace period.
    """
    now = utcnow()
    cutoff = now - timedelta(seconds=grace_seconds)
    async with engine().connect() as conn:
        candidates = (
            (
                await conn.execute(
                    select(jobs)
                    .where(jobs.c.status == states.STARTED, jobs.c.lease_expires_at < cutoff)
                    .where(or_(jobs.c.publish_attempted_at.is_(None), jobs.c.publish_attempted_at < cutoff))
                    .order_by(jobs.c.lease_expires_at)
                    .limit(limit)
                )
            )
            .mappings()
            .all()
        )
    claimed = []
    for job in candidates:
        async with engine().begin() as conn:
            result = await conn.execute(
                update(jobs)
                .where(
                    jobs.c.task_id == job["task_id"],
                    jobs.c.status == states.STARTED,
                    jobs.c.attempts == job["attempts"],  # nobody took it over meanwhile
                    jobs.c.publish_attempted_at.is_not_distinct_from(job["publish_attempted_at"]),
                )
                .values(publish_attempted_at=now)
            )
        if result.rowcount == 1:
            claimed.append(dict(job))
    return claimed


# ---------------------------------------------------------------------------
# Schedules
# ---------------------------------------------------------------------------
async def create_schedule(
    task_name: str,
    args: list,
    kwargs: dict,
    schedule_type: str,
    next_run_at: datetime,
    run_at: Optional[datetime] = None,
    interval_seconds: Optional[int] = None,
    cron_expr: Optional[str] = None,
    idempotency_key: Optional[str] = None,
    tenant: Optional[str] = None,
    timezone_name: Optional[str] = None,
) -> Optional[int]:
    """Insert an enabled schedule and return its id, or None if `idempotency_key` already exists."""
    async with engine().begin() as conn:
        result = await conn.execute(
            _insert(schedules)
            .values(
                task_name=task_name,
                args=list(args),
                kwargs=dict(kwargs),
                schedule_type=schedule_type,
                run_at=run_at,
                interval_seconds=interval_seconds,
                cron_expr=cron_expr,
                enabled=True,
                next_run_at=next_run_at,
                idempotency_key=idempotency_key,
                created_at=utcnow(),
                tenant=tenant,
                timezone=timezone_name,
            )
            .on_conflict_do_nothing()
            .returning(schedules.c.id)
        )
        return result.scalar_one_or_none()


async def upsert_system_schedule(
    name: str,
    task_name: str,
    schedule_type: str,
    first_run_at: datetime,
    interval_seconds: Optional[int] = None,
    cron_expr: Optional[str] = None,
) -> None:
    """
    Create or update a schedule defined in code (keyed by `name`).

    Timing fields follow the code; run history (`next_run_at`, `run_count`,
    `last_run_at`) is kept, so restarting or adding replicas never re-fires.
    """
    statement = _insert(schedules).values(
        name=name,
        task_name=task_name,
        args=[],
        kwargs={},
        schedule_type=schedule_type,
        interval_seconds=interval_seconds,
        cron_expr=cron_expr,
        enabled=True,
        next_run_at=first_run_at,
        created_at=utcnow(),
    )
    statement = statement.on_conflict_do_update(
        index_elements=[schedules.c.name],
        set_={
            "task_name": statement.excluded.task_name,
            "schedule_type": statement.excluded.schedule_type,
            "interval_seconds": statement.excluded.interval_seconds,
            "cron_expr": statement.excluded.cron_expr,
        },
    )
    async with engine().begin() as conn:
        await conn.execute(statement)


async def get_schedule(schedule_id: int) -> Optional[dict]:
    async with engine().connect() as conn:
        return _row(await conn.execute(select(schedules).where(schedules.c.id == schedule_id)))


async def get_schedule_by_idempotency_key(key: str) -> Optional[dict]:
    async with engine().connect() as conn:
        return _row(await conn.execute(select(schedules).where(schedules.c.idempotency_key == key)))


async def list_schedules(
    limit: int = 50, offset: int = 0, enabled_only: bool = False, tenant: Optional[str] = None
) -> list[dict]:
    """A page of schedules, newest first. `tenant=None` means every tenant."""
    query = select(schedules).order_by(schedules.c.created_at.desc(), schedules.c.id.desc()).limit(limit).offset(offset)
    if enabled_only:
        query = query.where(schedules.c.enabled.is_(True))
    if tenant is not None:
        query = query.where(schedules.c.tenant == tenant)
    async with engine().connect() as conn:
        return [dict(row) for row in (await conn.execute(query)).mappings()]


async def count_active_schedules(tenant: str) -> int:
    async with engine().connect() as conn:
        return (
            await conn.execute(select(func.count()).where(schedules.c.tenant == tenant, schedules.c.enabled.is_(True)))
        ).scalar_one()


async def get_due_schedules(now: datetime, limit: int = 100) -> list[dict]:
    """Enabled schedules whose `next_run_at` is at or before `now`, oldest first."""
    async with engine().connect() as conn:
        result = await conn.execute(
            select(schedules)
            .where(schedules.c.enabled.is_(True), schedules.c.next_run_at <= now)
            .order_by(schedules.c.next_run_at)
            .limit(limit)
        )
        return [dict(row) for row in result.mappings()]


async def fire_schedule(
    schedule: dict,
    fired_at: datetime,
    next_run_at: Optional[datetime],
    enabled: bool,
    task_name: str,
    queue: Optional[str],
) -> Optional[str]:
    """
    Claim one due run of `schedule` and record its job, in ONE transaction.

    The claim is an optimistic lock: it applies only if the row's `version`
    is still the one the caller read. With many scheduler replicas reading
    the same due row, exactly one wins; the rest see 0 rows and skip it.
    Because the job row is written in the same transaction, a crash right
    after commit can't lose the run — the outbox relay publishes it.

    Returns the new job's task_id, or None if another replica won (or the
    schedule was paused/edited since it was read).
    """
    task_id = str(uuid.uuid4())
    async with engine().begin() as conn:
        claimed = await conn.execute(
            update(schedules)
            .where(
                schedules.c.id == schedule["id"],
                schedules.c.version == schedule["version"],
                schedules.c.enabled.is_(True),
            )
            .values(
                last_run_at=fired_at,
                next_run_at=next_run_at or schedules.c.next_run_at,
                enabled=enabled,
                run_count=schedules.c.run_count + 1,
                version=schedules.c.version + 1,
            )
        )
        if claimed.rowcount != 1:
            return None
        await create_job(
            task_id,
            task_name,
            schedule["args"],
            schedule["kwargs"],
            queue=queue,
            schedule_id=schedule["id"],
            conn=conn,
            tenant=schedule.get("tenant"),
        )
    return task_id


async def update_schedule(schedule_id: int, **fields: Any) -> bool:
    """
    Change the given columns of a schedule and bump its version (so an
    in-flight claim based on the old values fails). Returns False if the id
    doesn't exist.

    Raises:
        ValueError: an unknown or protected column name was passed.
    """
    unknown = set(fields) - _UPDATABLE_SCHEDULE_COLUMNS
    if unknown:
        raise ValueError(f"Cannot update schedule column(s): {sorted(unknown)}")
    async with engine().begin() as conn:
        result = await conn.execute(
            update(schedules).where(schedules.c.id == schedule_id).values(**fields, version=schedules.c.version + 1)
        )
        return result.rowcount == 1


async def delete_schedule(schedule_id: int) -> bool:
    async with engine().begin() as conn:
        return (await conn.execute(schedules.delete().where(schedules.c.id == schedule_id))).rowcount == 1
