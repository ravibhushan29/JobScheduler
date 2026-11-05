"""
Attempts to make a job run twice. Each test is one way that happens in real
multi-pod deployments; each must still execute the job's body exactly once
(or, after a genuine worker crash, exactly once more).
"""

import asyncio
import threading
import time
from datetime import datetime, timedelta, timezone

import pytest
from conftest import run, wait_for_job

from app import db, publisher, scheduler, tasks
from app.celery_app import celery_app
from app.config import settings

ADD = tasks.add.name


def _record_and_publish(task_name: str, args: list, task_id: str, copies: int = 1) -> None:
    run(db.create_job(task_id, task_name, args, {}, queue=celery_app.tasks[task_name].queue))
    for _ in range(copies):
        celery_app.send_task(task_name, args=args, task_id=task_id, queue=celery_app.tasks[task_name].queue)


# ---------------------------------------------------------------------------
# Worker side: the execution guard
# ---------------------------------------------------------------------------
@pytest.mark.usefixtures("worker")
def test_duplicate_messages_execute_once():
    """The broker delivers the same message 3 times (re-delivery, double publish)."""
    _record_and_publish(ADD, [1, 1], "dup-1", copies=3)
    job = wait_for_job("dup-1", {"SUCCESS"})
    time.sleep(1.5)  # let the extra copies be processed (and dropped)
    assert run(db.get_job("dup-1"))["attempts"] == 1
    assert job["result"] == 2


@pytest.mark.usefixtures("worker")
def test_duplicate_of_running_job_waits_then_is_dropped():
    """A copy arrives while the first is still running (e.g. visibility-timeout re-delivery)."""
    task = tasks.long_running_task
    run(db.create_job("dup-long", task.name, [2], {}, queue=task.queue))
    celery_app.send_task(task.name, args=[2], task_id="dup-long", queue=task.queue)
    wait_for_job("dup-long", {"STARTED"})
    celery_app.send_task(task.name, args=[2], task_id="dup-long", queue=settings.DEFAULT_QUEUE_NAME)

    wait_for_job("dup-long", {"SUCCESS"})
    time.sleep(2.5)  # the deferred copy re-checks, sees SUCCESS, and is dropped
    job = run(db.get_job("dup-long"))
    assert (job["status"], job["attempts"]) == ("SUCCESS", 1)


@pytest.mark.usefixtures("worker")
def test_revoked_job_never_starts_even_if_delivered():
    """Cancel is enforced by every worker via the database, not only by the revoke broadcast."""
    run(db.create_job("cancelled-1", ADD, [1, 2], {}, queue="default"))
    run(db.update_job_status("cancelled-1", status="REVOKED", finished=True))
    celery_app.send_task(ADD, args=[1, 2], task_id="cancelled-1", queue="default")
    time.sleep(1.5)
    job = run(db.get_job("cancelled-1"))
    assert (job["status"], job["attempts"], job["result"]) == ("REVOKED", 0, None)


@pytest.mark.usefixtures("worker")
def test_job_waits_out_a_database_outage_instead_of_failing(monkeypatch):
    """While the DB is unreachable the guard requeues the message; the job then runs once."""
    from sqlalchemy.exc import OperationalError

    from app import guard

    monkeypatch.setattr(guard, "DB_UNAVAILABLE_BACKOFF_SECONDS", 0.2)
    real_acquire, outages = db.acquire_execution, iter([True, True])

    async def flaky_acquire(*args, **kwargs):
        if next(outages, False):
            raise OperationalError("UPDATE jobs", {}, ConnectionRefusedError("database is down"))
        return await real_acquire(*args, **kwargs)

    monkeypatch.setattr(db, "acquire_execution", flaky_acquire)
    _record_and_publish(ADD, [4, 4], "db-outage-1")
    job = wait_for_job("db-outage-1", {"SUCCESS"})
    assert (job["attempts"], job["result"]) == (1, 8)


def test_concurrent_claims_have_one_winner():
    """Many workers receive the same job at the same instant; exactly one may run it."""
    run(db.create_job("race-1", ADD, [1, 2], {}))
    results = []

    def claim():
        results.append(run(db.acquire_execution("race-1", ADD, [1, 2], {}, "default", "w", 0, 60)).acquired)

    threads = [threading.Thread(target=claim) for _ in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert results.count(True) == 1


def test_crashed_worker_is_taken_over_after_lease_expires():
    run(db.create_job("crash-1", ADD, [1, 2], {}))
    first = run(db.acquire_execution("crash-1", ADD, [1, 2], {}, "default", "w1", 0, lease_seconds=1))
    assert first.acquired

    second = run(db.acquire_execution("crash-1", ADD, [1, 2], {}, "default", "w2", 0, lease_seconds=60))
    assert not second.acquired and second.status == "STARTED"  # w1 is alive: must wait

    time.sleep(1.2)  # w1 dies: it stops renewing, the lease runs out
    third = run(db.acquire_execution("crash-1", ADD, [1, 2], {}, "default", "w2", 0, lease_seconds=60))
    assert third.acquired and third.attempt == 2

    # The zombie w1 comes back and reports: its stale attempt number is fenced off.
    assert run(db.update_job_status("crash-1", status="SUCCESS", attempt=first.attempt)) is False
    assert run(db.update_job_status("crash-1", status="SUCCESS", attempt=third.attempt)) is True


def test_celery_retry_of_running_attempt_may_claim():
    """A Celery retry (retries+1) of the attempt that just failed must run, even before its RETRY is recorded."""
    run(db.create_job("retry-1", ADD, [1], {}))
    assert run(db.acquire_execution("retry-1", ADD, [1], {}, "default", "w", 0, 60)).acquired
    assert not run(db.acquire_execution("retry-1", ADD, [1], {}, "default", "w", 0, 60)).acquired  # same try
    assert run(db.acquire_execution("retry-1", ADD, [1], {}, "default", "w", 1, 60)).acquired  # next try


def test_finished_jobs_can_never_be_claimed():
    for status in ("SUCCESS", "FAILURE", "REVOKED"):
        run(db.create_job(f"done-{status}", ADD, [], {}))
        run(db.update_job_status(f"done-{status}", status=status, finished=True))
        claim = run(db.acquire_execution(f"done-{status}", ADD, [], {}, "default", "w", 5, 60))
        assert not claim.acquired and claim.status == status


def _abandon(task_id: str, attempts_before: int = 0) -> None:
    """Simulate a worker that claimed the job, then died (stopped renewing its 1 s lease)."""
    run(db.create_job(task_id, ADD, [2, 3], {}, queue="default"))
    for _ in range(attempts_before + 1):
        run(db.update_job_status(task_id, "QUEUED"))
        assert run(
            db.acquire_execution(task_id, ADD, [2, 3], {}, "default", "dead-worker", 0, lease_seconds=1)
        ).acquired
    time.sleep(2.2)


@pytest.fixture
def short_grace(monkeypatch):
    monkeypatch.setattr(settings, "EXECUTION_LEASE_SECONDS", 1)


@pytest.mark.usefixtures("short_grace")
def test_abandoned_job_is_republished_once_per_grace_period(monkeypatch):
    sent = []
    monkeypatch.setattr(celery_app, "send_task", lambda name, **kw: sent.append(kw))
    monkeypatch.setattr(type(celery_app.backend), "get_task_meta", lambda self, task_id: {"status": "STARTED"})
    _abandon("lost-1")

    assert run(scheduler.recover_abandoned_jobs()) == 1
    assert run(scheduler.recover_abandoned_jobs()) == 0  # already handled this grace period
    assert [kw["task_id"] for kw in sent] == ["lost-1"]
    assert run(db.get_job("lost-1"))["status"] == "STARTED"  # left for a healthy worker to claim


@pytest.mark.usefixtures("short_grace")
def test_abandoned_job_that_celery_saw_fail_is_marked_failed(monkeypatch):
    # Celery's backend object is per-thread; patch the class so every thread sees it.
    monkeypatch.setattr(
        type(celery_app.backend),
        "get_task_meta",
        lambda self, task_id: {"status": "FAILURE", "result": "TimeLimitExceeded(360)"},
    )
    _abandon("timeout-1")
    run(scheduler.recover_abandoned_jobs())
    job = run(db.get_job("timeout-1"))
    assert job["status"] == "FAILURE" and "TimeLimitExceeded" in job["error"]


@pytest.mark.usefixtures("short_grace")
def test_finished_job_whose_final_write_was_lost_is_not_rerun(monkeypatch):
    sent = []
    monkeypatch.setattr(celery_app, "send_task", lambda name, **kw: sent.append(kw))
    monkeypatch.setattr(
        type(celery_app.backend), "get_task_meta", lambda self, task_id: {"status": "SUCCESS", "result": 5}
    )
    _abandon("lost-write-1")
    run(scheduler.recover_abandoned_jobs())
    job = run(db.get_job("lost-write-1"))
    assert (job["status"], job["result"], sent) == ("SUCCESS", 5, [])


@pytest.mark.usefixtures("short_grace")
def test_job_that_keeps_killing_workers_is_eventually_failed(monkeypatch):
    monkeypatch.setattr(celery_app, "send_task", lambda *a, **k: None)
    monkeypatch.setattr(type(celery_app.backend), "get_task_meta", lambda self, task_id: {"status": "STARTED"})
    _abandon("poison-1", attempts_before=settings.MAX_WORKER_LOSS_RECOVERIES)
    run(scheduler.recover_abandoned_jobs())
    job = run(db.get_job("poison-1"))
    assert job["status"] == "FAILURE" and "giving up" in job["error"]


@pytest.mark.usefixtures("worker", "short_grace")
def test_crashed_job_is_finished_by_another_worker():
    _abandon("crash-e2e")
    assert run(scheduler.recover_abandoned_jobs()) == 1
    job = wait_for_job("crash-e2e", {"SUCCESS"})
    assert (job["attempts"], job["result"], job["worker"] != "dead-worker") == (2, 5, True)


# ---------------------------------------------------------------------------
# Scheduler side: many replicas
# ---------------------------------------------------------------------------
def test_many_scheduler_replicas_fire_each_due_time_once(monkeypatch):
    """10 replicas poll at the same moment, 5 due schedules: exactly 5 jobs."""
    published = []

    async def fake_publish(job):
        published.append(job["task_id"])
        return True

    monkeypatch.setattr(publisher, "publish_job", fake_publish)
    now = datetime.now(timezone.utc)
    for _ in range(5):
        run(db.create_schedule(ADD, [1, 2], {}, "interval", now - timedelta(seconds=1), interval_seconds=60))

    results = []

    def replica():
        results.extend(asyncio.run(scheduler.dispatch_due_schedules(now)))

    threads = [threading.Thread(target=replica) for _ in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(results) == 5 and len(set(published)) == 5
    jobs = run(db.list_jobs(limit=100))
    assert len(jobs) == 5
    assert sorted(j["schedule_id"] for j in jobs) == sorted({j["schedule_id"] for j in jobs})
    assert all(s["run_count"] == 1 for s in run(db.list_schedules()))


def test_paused_or_edited_schedule_is_not_fired_by_stale_read(monkeypatch):
    monkeypatch.setattr(publisher, "publish_job", lambda job: asyncio.sleep(0, True))
    now = datetime.now(timezone.utc)
    schedule_id = run(db.create_schedule(ADD, [1, 2], {}, "interval", now, interval_seconds=60))
    stale = run(db.get_schedule(schedule_id))
    run(db.update_schedule(schedule_id, enabled=False))  # paused after the scheduler read it
    assert run(db.fire_schedule(stale, now, now + timedelta(minutes=1), True, ADD, "default")) is None
    assert run(db.list_jobs()) == []


def test_outbox_relay_publishes_jobs_that_never_reached_the_broker(monkeypatch):
    """The API (or a scheduler) recorded a job, then crashed or lost the broker before publishing."""
    sent = []
    monkeypatch.setattr(celery_app, "send_task", lambda name, **kw: sent.append(kw["task_id"]))
    monkeypatch.setattr(settings, "OUTBOX_REPUBLISH_AFTER_SECONDS", 0)
    run(db.create_job("orphan-1", ADD, [1, 2], {}, queue="default"))
    time.sleep(0.01)

    assert run(scheduler.relay_outbox()) == 1
    assert sent == ["orphan-1"]
    assert run(db.get_job("orphan-1"))["published_at"] is not None
    assert run(scheduler.relay_outbox()) == 0  # never twice


def test_concurrent_relays_publish_each_orphan_once(monkeypatch):
    """5 scheduler replicas relay the outbox at the same moment: each orphan is published once."""
    sent = []
    monkeypatch.setattr(celery_app, "send_task", lambda name, **kw: sent.append(kw["task_id"]))
    monkeypatch.setattr(settings, "OUTBOX_REPUBLISH_AFTER_SECONDS", 30)
    for i in range(5):
        run(db.create_job(f"orphan-{i}", ADD, [], {}, queue="default"))

    async def backdate():  # recorded a minute ago, never published
        async with db.engine().begin() as conn:
            await conn.execute(db.jobs.update().values(submitted_at=datetime.now(timezone.utc) - timedelta(minutes=1)))

    run(backdate())
    threads = [threading.Thread(target=lambda: asyncio.run(scheduler.relay_outbox())) for _ in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(sent) == [f"orphan-{i}" for i in range(5)]


def test_system_schedules_are_seeded_idempotently():
    for _ in range(3):  # three replicas starting
        run(scheduler.seed_system_schedules())
    names = sorted(s["name"] for s in run(db.list_schedules()))
    assert names == ["system:cleanup", "system:heartbeat"]


# ---------------------------------------------------------------------------
# API side: client retries
# ---------------------------------------------------------------------------
def test_idempotency_key_prevents_duplicate_jobs(client, monkeypatch):
    monkeypatch.setattr(celery_app, "send_task", lambda *a, **k: None)
    headers = {"Idempotency-Key": "order-42"}

    first = client.post("/tasks/add", json={"x": 1, "y": 2}, headers=headers)
    second = client.post("/tasks/add", json={"x": 1, "y": 2}, headers=headers)
    assert (first.status_code, second.status_code) == (201, 200)
    assert second.headers["Idempotent-Replayed"] == "true"
    assert first.json()["task_id"] == second.json()["task_id"]
    assert run(db.count_jobs()) == 1

    conflict = client.post("/tasks/add", json={"x": 9, "y": 9}, headers=headers)
    assert conflict.status_code == 409


def test_idempotency_key_prevents_duplicate_schedules(client):
    body = {"task_name": ADD, "args": [1, 2], "schedule_type": "interval", "interval_seconds": 60}
    headers = {"Idempotency-Key": "sched-7"}
    first = client.post("/schedules", json=body, headers=headers)
    second = client.post("/schedules", json=body, headers=headers)
    assert (first.status_code, second.status_code) == (201, 200)
    assert first.json()["id"] == second.json()["id"]
    assert len(run(db.list_schedules())) == 1
