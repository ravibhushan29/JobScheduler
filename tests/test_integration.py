"""
End-to-end: API / scheduler -> broker -> real worker -> guard + signals -> job history.

These use the `worker` fixture (a Celery worker thread on an in-memory broker).
"""

import pytest
from conftest import run, wait_for_job

from app import scheduler, tasks
from app.config import settings

pytestmark = pytest.mark.usefixtures("worker")


def test_add_job_runs_once_and_is_recorded(client):
    task_id = client.post("/tasks/add", json={"x": 3, "y": 4}).json()["task_id"]

    job = wait_for_job(task_id, {"SUCCESS"})
    assert job["worker"] and job["started_at"] and job["finished_at"] and job["published_at"]
    assert job["attempts"] == 1
    assert job["lease_expires_at"] is None  # released when it finished

    record = client.get(f"/jobs/{task_id}").json()
    assert record["result"] == 7.0
    assert record["args"] == [3.0, 4.0]

    status = client.get(f"/jobs/{task_id}/status").json()
    assert status["state"] == "SUCCESS"
    assert status["source"] == "result_backend"
    assert status["result"] == 7.0


def test_every_sample_endpoint_runs(client, monkeypatch):
    monkeypatch.setattr(tasks.random, "random", lambda: 0.99)  # send_notification: provider "up"
    ids = client.post("/tasks/test-batch").json()

    assert wait_for_job(ids["add"], {"SUCCESS"})["result"] == 4
    assert wait_for_job(ids["long_running_task"], {"SUCCESS"})["queue"] == settings.LONG_TASK_QUEUE_NAME
    assert wait_for_job(ids["flaky_task_will_succeed"], {"SUCCESS"})["attempts"] == 1
    assert "test@example.com" in wait_for_job(ids["send_notification"], {"SUCCESS"})["result"]
    assert wait_for_job(ids["flaky_task_will_fail"], {"FAILURE"})["error"] == "Simulated transient failure"


def test_failing_job_records_reason_traceback_and_retries(client):
    task_id = client.post("/tasks/flaky", json={"fail_probability": 1.0}).json()["task_id"]

    job = wait_for_job(task_id, {"FAILURE"})
    assert job["error"] == "Simulated transient failure"
    assert "RuntimeError" in job["traceback"]
    assert job["retries"] == settings.TASK_DEFAULT_MAX_RETRIES
    assert job["attempts"] == settings.TASK_DEFAULT_MAX_RETRIES + 1  # one execution per try, no extras

    status = client.get(f"/jobs/{task_id}/status").json()
    assert status["state"] == "FAILURE"
    assert status["result"]["exc_type"] == "RuntimeError"


def test_manual_retry_creates_linked_job(client):
    failed_id = client.post("/tasks/flaky", json={"fail_probability": 1.0}).json()["task_id"]
    wait_for_job(failed_id, {"FAILURE"})

    response = client.post(f"/jobs/{failed_id}/retry")
    assert response.status_code == 201
    retried = wait_for_job(response.json()["task_id"], {"FAILURE"})
    assert retried["retry_of"] == failed_id


def test_notification_retries_itself_then_succeeds(client, monkeypatch):
    outcomes = iter([0.1, 0.99])  # first call: provider down -> self.retry(); second: up
    monkeypatch.setattr(tasks.random, "random", lambda: next(outcomes))
    monkeypatch.setattr(tasks.send_notification, "retry", _fast_retry(tasks.send_notification.retry))

    notify_id = client.post("/tasks/notify", json={"email": "a@example.com", "message": "hi"}).json()["task_id"]
    job = wait_for_job(notify_id, {"SUCCESS"})
    assert (job["retries"], job["attempts"]) == (1, 2)


def _fast_retry(original):
    def retry(*args, countdown=None, **kwargs):
        return original(*args, countdown=0.2, **kwargs)

    return retry


def test_jobs_published_directly_are_still_recorded():
    """`.delay()` from code bypasses the API; the guard creates the row."""
    result = tasks.scheduled_cleanup.apply_async()
    job = wait_for_job(result.id, {"SUCCESS"})
    assert job["task_name"] == "app.tasks.scheduled_cleanup"
    assert job["attempts"] == 1


def test_interval_schedule_fires_once_per_due_time(client):
    schedule = client.post(
        "/schedules",
        json={"task_name": "app.tasks.add", "args": [1, 2], "schedule_type": "interval", "interval_seconds": 3600},
    ).json()

    assert len(run(scheduler.dispatch_due_schedules())) == 1
    assert run(scheduler.dispatch_due_schedules()) == []  # next run is an hour away

    fired = [j for j in client.get("/jobs").json()["jobs"] if j["schedule_id"] == schedule["id"]]
    assert len(fired) == 1
    assert wait_for_job(fired[0]["task_id"], {"SUCCESS"})["result"] == 3

    after = client.get(f"/schedules/{schedule['id']}").json()
    assert after["run_count"] == 1
    assert after["next_run_at"] > after["last_run_at"]


def test_scheduled_long_task_is_routed_to_long_queue(client):
    client.post(
        "/schedules",
        json={
            "task_name": "app.tasks.long_running_task",
            "args": [1],
            "schedule_type": "interval",
            "interval_seconds": 60,
        },
    )
    (task_id,) = run(scheduler.dispatch_due_schedules())
    assert wait_for_job(task_id, {"SUCCESS"})["queue"] == settings.LONG_TASK_QUEUE_NAME


def test_progress_state_is_visible_while_running(client):
    task_id = client.post("/tasks/long", json={"duration_seconds": 3}).json()["task_id"]
    wait_for_job(task_id, {"STARTED"})
    import time

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        status = client.get(f"/jobs/{task_id}/status").json()
        if status["state"] == "PROGRESS":
            assert 0 < status["result"]["progress"] <= 100
            break
        time.sleep(0.2)
    else:
        raise AssertionError("never saw PROGRESS")
    wait_for_job(task_id, {"SUCCESS"})
