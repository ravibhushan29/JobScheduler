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
)
Index("idx_jobs_status", jobs.c.status)
Index("idx_jobs_submitted_at", jobs.c.submitted_at)
Index("idx_jobs_finished_at", jobs.c.finished_at)
Index("idx_jobs_published_at", jobs.c.published_at)
Index("idx_jobs_lease", jobs.c.status, jobs.c.lease_expires_at)
Index("uq_jobs_idempotency_key", jobs.c.idempotency_key, unique=True)

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
    sqlite_autoincrement=True,
)
Index("idx_schedules_due", schedules.c.enabled, schedules.c.next_run_at)
Index("uq_schedules_name", schedules.c.name, unique=True)
Index("uq_schedules_idempotency_key", schedules.c.idempotency_key, unique=True)

#: Schedule columns `update_schedule` may change (never caller-chosen SQL).
_UPDATABLE_SCHEDULE_COLUMNS = frozenset({"enabled", "interval_seconds", "cron_expr", "next_run_at"})


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
) -> bool:
    """
    Insert a QUEUED, not-yet-published job. Returns False — inserting
    nothing — if `idempotency_key` (or `task_id`) already exists.

    Always call this BEFORE publishing; `published_at` stays NULL until
    `mark_published`, so the outbox relay can finish a publish that crashed.
    """
    statement = (
        _insert(jobs)
        .values(
            task_id=task_id,
            task_name=task_name,
            args=list(args),
            kwargs=dict(kwargs),
            queue=queue,
            status="QUEUED",
            retry_of=retry_of,
            schedule_id=schedule_id,
            idempotency_key=idempotency_key,
            submitted_at=utcnow(),
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
    Outbox relay: return jobs that were recorded but never published and
    are older than `older_than_seconds`, claiming each so concurrent relays
    (one per scheduler replica) don't publish the same job at the same time.
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
                        jobs.c.submitted_at < cutoff,
                        or_(jobs.c.publish_attempted_at.is_(None), jobs.c.publish_attempted_at < cutoff),
                    )
                    .order_by(jobs.c.submitted_at)
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
    Returns False if nothing was updated.
    """
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
        values["finished_at"] = utcnow()
        values["lease_expires_at"] = None

    statement = update(jobs).where(jobs.c.task_id == task_id).values(**values)
    if attempt is not None:
        statement = statement.where(jobs.c.attempts == attempt)
    async with engine().begin() as conn:
        return (await conn.execute(statement)).rowcount == 1


async def get_job(task_id: str) -> Optional[dict]:
    async with engine().connect() as conn:
        return _row(await conn.execute(select(jobs).where(jobs.c.task_id == task_id)))


async def get_job_by_idempotency_key(key: str) -> Optional[dict]:
    async with engine().connect() as conn:
        return _row(await conn.execute(select(jobs).where(jobs.c.idempotency_key == key)))


async def list_jobs(limit: int = 50, offset: int = 0, status: Optional[str] = None) -> list[dict]:
    """A page of jobs, newest first, optionally only those with `status`."""
    query = select(jobs).order_by(jobs.c.submitted_at.desc()).limit(limit).offset(offset)
    if status:
        query = query.where(jobs.c.status == status)
    async with engine().connect() as conn:
        return [dict(row) for row in (await conn.execute(query)).mappings()]


async def count_jobs(status: Optional[str] = None) -> int:
    query = select(func.count()).select_from(jobs)
    if status:
        query = query.where(jobs.c.status == status)
    async with engine().connect() as conn:
        return (await conn.execute(query)).scalar_one()


async def delete_finished_jobs_before(cutoff: datetime) -> int:
    """Delete jobs that finished before `cutoff`; return how many were removed."""
    async with engine().begin() as conn:
        result = await conn.execute(jobs.delete().where(jobs.c.finished_at.is_not(None), jobs.c.finished_at < cutoff))
        return result.rowcount


# ---------------------------------------------------------------------------
# Jobs: the execution lock
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ExecutionClaim:
    acquired: bool
    attempt: Optional[int] = None  # fencing token, when acquired
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
            .returning(jobs.c.attempts)
        )
        attempt = result.scalar_one_or_none()
        if attempt is not None:
            return ExecutionClaim(acquired=True, attempt=attempt)
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


async def list_schedules(limit: int = 50, offset: int = 0, enabled_only: bool = False) -> list[dict]:
    """A page of schedules, newest first."""
    query = select(schedules).order_by(schedules.c.created_at.desc(), schedules.c.id.desc()).limit(limit).offset(offset)
    if enabled_only:
        query = query.where(schedules.c.enabled.is_(True))
    async with engine().connect() as conn:
        return [dict(row) for row in (await conn.execute(query)).mappings()]


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
