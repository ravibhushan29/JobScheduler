"""
Shared test setup.

Settings are read from the environment at import time, so the environment is
set here, before any `app` module is imported:
  * database — a throwaway SQLite file, or Postgres if TEST_DATABASE_URL is set:
        TEST_DATABASE_URL=postgresql://postgres:test@localhost:55432/jobs pytest
  * broker   — kombu's in-memory transport (no Redis needed);
  * retries  — 1 retry with ~1 s backoff, so failure paths finish quickly;
  * lease    — 3 s, so takeover-after-crash tests run quickly.

Fixtures:
  * `clean_db` (autouse) — empty every table before each test.
  * `client`   — FastAPI TestClient with the app's lifespan running.
  * `worker`   — a real Celery worker in a background thread on both queues.
"""

import os
import tempfile
import time

_TMP = tempfile.mkdtemp(prefix="jobscheduler-tests-")
os.environ.update(
    {
        "DATABASE_URL": os.getenv("TEST_DATABASE_URL", f"sqlite:///{_TMP}/scheduler.db"),
        "DB_PATH": os.path.join(_TMP, "scheduler.db"),
        "CELERY_BROKER_URL": "memory://",
        "CELERY_RESULTS_DB_PATH": os.path.join(_TMP, "celery_results.db"),
        "SCHEDULER_HEARTBEAT_FILE": os.path.join(_TMP, "scheduler-heartbeat"),
        "LOG_TO_FILE": "false",
        "LOG_LEVEL": "WARNING",
        "API_KEYS": "",
        "TASK_DEFAULT_MAX_RETRIES": "1",
        "TASK_DEFAULT_RETRY_BACKOFF": "1",
        "EXECUTION_LEASE_SECONDS": "3",
        "DUPLICATE_RECHECK_SECONDS": "1",
    }
)

import pytest  # noqa: E402
from celery.contrib.testing.worker import start_worker  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import text  # noqa: E402

from app import db  # noqa: E402
from app.celery_app import celery_app  # noqa: E402
from app.config import settings  # noqa: E402
from app.main import app  # noqa: E402

run = db.run_sync  # run a coroutine from a synchronous test


@pytest.fixture(autouse=True)
def clean_db():
    async def reset() -> None:
        await db.init_db()
        async with db.engine().begin() as conn:
            for table in reversed(db.metadata.sorted_tables):
                await conn.execute(table.delete())
            # On Postgres, Celery's result table shares the database. Celery creates it
            # lazily on first use, so it doesn't exist yet on a fresh database.
            if settings.IS_POSTGRES and await conn.scalar(text("SELECT to_regclass('celery_taskmeta')")):
                await conn.execute(text("DELETE FROM celery_taskmeta WHERE true"))

    run(reset())


@pytest.fixture
def client():
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture(scope="session")
def worker():
    with start_worker(
        celery_app,
        pool="solo",
        perform_ping_check=False,
        queues=[settings.DEFAULT_QUEUE_NAME, settings.LONG_TASK_QUEUE_NAME],
        shutdown_timeout=30,
    ) as running_worker:
        yield running_worker


def wait_for_job(task_id: str, statuses: set[str], timeout: float = 30) -> dict:
    """Poll the job history until the job reaches one of `statuses`; return the row."""
    deadline = time.monotonic() + timeout
    job = None
    while time.monotonic() < deadline:
        job = run(db.get_job(task_id))
        if job and job["status"] in statuses:
            return job
        time.sleep(0.1)
    raise AssertionError(f"job {task_id} did not reach {statuses} within {timeout}s; last row: {job}")
