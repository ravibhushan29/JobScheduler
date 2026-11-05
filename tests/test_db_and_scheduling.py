"""The persistence layer, migrations, the timing rules, and scheduler edge cases."""

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest
from conftest import run

from app import db, publisher, scheduler, scheduling, tasks
from app.celery_app import celery_app
from app.config import settings
from app.guard import GuardedTask

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


def _schedule(**overrides) -> int:
    fields = {
        "task_name": "app.tasks.add",
        "args": [1, 2],
        "kwargs": {},
        "schedule_type": "interval",
        "interval_seconds": 60,
        "next_run_at": NOW,
    }
    fields.update(overrides)
    return run(db.create_schedule(**fields))


# ---------------------------------------------------------------------------
# db
# ---------------------------------------------------------------------------
def test_fire_schedule_claims_once_and_records_job_atomically():
    schedule = run(db.get_schedule(_schedule()))
    next_run = NOW + timedelta(minutes=1)

    task_id = run(db.fire_schedule(schedule, NOW, next_run, True, "app.tasks.add", "default"))
    assert task_id is not None
    assert run(db.fire_schedule(schedule, NOW, next_run, True, "app.tasks.add", "default")) is None  # stale version

    row = run(db.get_schedule(schedule["id"]))
    assert (row["run_count"], row["next_run_at"], row["version"]) == (1, next_run, 1)
    job = run(db.get_job(task_id))
    assert (job["schedule_id"], job["args"], job["published_at"]) == (schedule["id"], [1, 2], None)


def test_fire_once_schedule_keeps_next_run_and_disables():
    schedule = run(db.get_schedule(_schedule(schedule_type="once")))
    assert run(db.fire_schedule(schedule, NOW, None, False, "app.tasks.add", "default"))
    row = run(db.get_schedule(schedule["id"]))
    assert row["enabled"] is False
    assert row["next_run_at"] == NOW


def test_update_schedule_rejects_unknown_columns_and_bumps_version():
    schedule_id = _schedule()
    with pytest.raises(ValueError, match="task_name"):
        run(db.update_schedule(schedule_id, task_name="app.tasks.evil"))
    assert run(db.update_schedule(schedule_id, enabled=False)) is True
    assert run(db.get_schedule(schedule_id))["version"] == 1
    assert run(db.update_schedule(999_999, enabled=False)) is False


def test_delete_finished_jobs_before_keeps_recent_and_unfinished():
    for task_id in ("old", "new", "running"):
        run(db.create_job(task_id, "app.tasks.add", [], {}))
    run(db.update_job_status("old", "SUCCESS", finished=True))
    run(db.update_job_status("new", "SUCCESS", finished=True))

    async def backdate():
        async with db.engine().begin() as conn:
            await conn.execute(
                db.jobs.update()
                .where(db.jobs.c.task_id == "old")
                .values(finished_at=datetime(2000, 1, 1, tzinfo=timezone.utc))
            )

    run(backdate())
    assert run(db.delete_finished_jobs_before(datetime(2001, 1, 1, tzinfo=timezone.utc))) == 1
    assert {j["task_id"] for j in run(db.list_jobs())} == {"new", "running"}


def test_scheduled_cleanup_uses_retention_setting(monkeypatch):
    seen = {}

    async def fake_delete(cutoff):
        seen["cutoff"] = cutoff
        return 4

    monkeypatch.setattr(db, "delete_finished_jobs_before", fake_delete)
    monkeypatch.setattr(celery_app.backend, "cleanup", lambda: seen.setdefault("results_cleaned", True))
    assert tasks.scheduled_cleanup.run() == "deleted 4 job rows"
    expected = datetime.now(timezone.utc) - timedelta(days=settings.JOB_RETENTION_DAYS)
    assert abs(seen["cutoff"] - expected) < timedelta(seconds=5)
    assert seen["results_cleaned"]


@pytest.mark.skipif(settings.IS_POSTGRES, reason="upgrades a legacy SQLite file")
def test_init_db_upgrades_a_v3_database(tmp_path, monkeypatch):
    legacy = tmp_path / "v3.db"
    with sqlite3.connect(legacy) as conn:
        conn.executescript(
            """
            CREATE TABLE jobs (task_id TEXT PRIMARY KEY, task_name TEXT NOT NULL, args TEXT NOT NULL DEFAULT '[]',
              kwargs TEXT NOT NULL DEFAULT '{}', queue TEXT, status TEXT NOT NULL DEFAULT 'QUEUED', result TEXT,
              error TEXT, traceback TEXT, retries INTEGER NOT NULL DEFAULT 0, worker TEXT, retry_of TEXT,
              schedule_id INTEGER, submitted_at TEXT NOT NULL, started_at TEXT, finished_at TEXT);
            CREATE TABLE schedules (id INTEGER PRIMARY KEY AUTOINCREMENT, task_name TEXT NOT NULL,
              args TEXT NOT NULL DEFAULT '[]', kwargs TEXT NOT NULL DEFAULT '{}', schedule_type TEXT NOT NULL,
              run_at TEXT, interval_seconds INTEGER, cron_expr TEXT, enabled INTEGER NOT NULL DEFAULT 1,
              next_run_at TEXT NOT NULL, last_run_at TEXT, run_count INTEGER NOT NULL DEFAULT 0,
              created_at TEXT NOT NULL);
            INSERT INTO jobs (task_id, task_name, args, status, result, submitted_at)
              VALUES ('legacy-1', 'app.tasks.add', '[3.0, 4.0]', 'QUEUED', '7.0', '2026-07-26T21:47:17+00:00');
            INSERT INTO schedules (task_name, args, schedule_type, interval_seconds, next_run_at, created_at)
              VALUES ('app.tasks.add', '[1, 2]', 'interval', 60, '2026-07-26T21:47:17.5+00:00',
                      '2026-07-26T21:47:00+00:00');
            """
        )
    monkeypatch.setattr(settings, "DB_PATH", str(legacy))
    monkeypatch.setattr(settings, "DATABASE_URL", f"sqlite+aiosqlite:///{legacy}")
    db._engines.clear()
    try:
        run(db.init_db())
        job = run(db.get_job("legacy-1"))
        assert job["args"] == [3.0, 4.0] and job["result"] == 7.0
        assert job["published_at"] is not None  # history isn't replayed by the outbox
        (schedule,) = run(db.list_schedules())
        assert schedule["next_run_at"] == datetime(2026, 7, 26, 21, 47, 17, 500000, tzinfo=timezone.utc)
        assert schedule["version"] == 0 and schedule["enabled"] is True
        # a legacy schedule can still be claimed
        assert run(db.fire_schedule(schedule, NOW, NOW, True, "app.tasks.add", "default"))
    finally:
        db._engines.clear()


# ---------------------------------------------------------------------------
# scheduling rules
# ---------------------------------------------------------------------------
def test_next_run_after_fire():
    assert scheduling.next_run_after_fire({"schedule_type": "once"}, NOW) == (None, False)
    assert scheduling.next_run_after_fire({"schedule_type": "interval", "interval_seconds": 90}, NOW) == (
        NOW + timedelta(seconds=90),
        True,
    )
    assert scheduling.next_run_after_fire({"schedule_type": "cron", "cron_expr": "30 12 * * *"}, NOW) == (
        NOW.replace(minute=30),
        True,
    )
    assert scheduling.next_run_after_fire({"schedule_type": "bogus"}, NOW) == (None, False)


@pytest.mark.parametrize(
    "expr, valid",
    [("*/5 * * * *", True), ("0 8 * * 1-5", True), ("0 0 * * * *", False), ("", False), ("nope", False)],
)
def test_is_valid_cron(expr, valid):
    assert scheduling.is_valid_cron(expr) is valid


def test_first_run_at():
    run_at = datetime(2030, 1, 1, 10, tzinfo=timezone(timedelta(hours=2)))
    assert scheduling.first_run_at("once", NOW, run_at=run_at) == datetime(2030, 1, 1, 8, tzinfo=timezone.utc)
    assert scheduling.first_run_at("interval", NOW) == NOW
    assert scheduling.first_run_at("cron", NOW, cron_expr="0 13 * * *") == NOW.replace(hour=13)


def test_check_task_arguments():
    tasks.check_task_arguments("app.tasks.add", [1, 2], {})
    tasks.check_task_arguments("app.tasks.long_running_task", [], {"duration_seconds": 3})
    with pytest.raises(TypeError):
        tasks.check_task_arguments("app.tasks.add", [1], {})
    with pytest.raises(TypeError):
        tasks.check_task_arguments("app.tasks.heartbeat", [], {"unexpected": 1})


def test_registry_and_system_schedules_are_consistent():
    assert tasks.SCHEDULABLE_TASK_NAMES == set(tasks.TASK_REGISTRY)
    assert all(name in celery_app.tasks for name in tasks.TASK_REGISTRY)
    assert all(spec["task_name"] in tasks.TASK_REGISTRY for spec in tasks.SYSTEM_SCHEDULES)
    assert all(isinstance(task, GuardedTask) for task in tasks.TASK_REGISTRY.values())


# ---------------------------------------------------------------------------
# scheduler edge cases (publishing stubbed; no worker involved)
# ---------------------------------------------------------------------------
def test_dispatch_disables_schedule_for_unknown_task():
    schedule_id = _schedule(task_name="app.tasks.always_fail_task")
    assert run(scheduler.dispatch_due_schedules()) == []
    assert run(db.get_schedule(schedule_id))["enabled"] is False


def test_dispatch_with_broker_down_leaves_job_for_the_outbox(monkeypatch):
    def broker_down(*_, **__):
        raise ConnectionError("redis unreachable")

    monkeypatch.setattr(celery_app, "send_task", broker_down)
    schedule_id = _schedule()
    (task_id,) = run(scheduler.dispatch_due_schedules())

    job = run(db.get_job(task_id))
    assert (job["schedule_id"], job["status"], job["published_at"]) == (schedule_id, "QUEUED", None)
    assert run(db.get_schedule(schedule_id))["run_count"] == 1  # claimed once; the relay delivers it


def test_scheduler_loop_survives_errors_and_touches_heartbeat(monkeypatch, tmp_path):
    async def broken():
        raise RuntimeError("database blip")

    heartbeat = tmp_path / "hb"
    monkeypatch.setattr(settings, "SCHEDULER_HEARTBEAT_FILE", str(heartbeat))
    monkeypatch.setattr(scheduler, "dispatch_due_schedules", broken)
    run(scheduler.run_once())  # must not raise
    assert heartbeat.exists()
    assert scheduler.healthcheck() == 0


def test_publish_job_reports_broker_failure(monkeypatch):
    run(db.create_job("p-1", "app.tasks.add", [], {}))

    def broker_down(*_, **__):
        raise ConnectionError("down")

    monkeypatch.setattr(celery_app, "send_task", broker_down)
    assert run(publisher.publish_job(run(db.get_job("p-1")))) is False
    assert run(db.get_job("p-1"))["published_at"] is None
