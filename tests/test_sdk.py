"""The Python SDK (sdk/python), run against the real app in-process and a real worker."""

import sys
import time
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sdk" / "python"))

from jobscheduler_client import JobScheduler, JobSchedulerError, verify_webhook  # noqa: E402

from app import webhooks  # noqa: E402


@pytest.fixture
def js(client):
    return JobScheduler(http=client)


def test_submit_wait_and_events(js, worker):
    job = js.submit("app.tasks.add", args=[3, 4], priority="high")
    assert js.wait(job.task_id, timeout=30)["result"] == 7.0
    assert js.job(job.task_id)["priority"] == "high"

    slow = js.submit("app.tasks.long_running_task", args=[2])
    states = [event["state"] for event in js.events(slow.task_id)]
    assert states[-1] == "SUCCESS" and "PROGRESS" in states


def test_submit_is_idempotent(js, monkeypatch):
    from app.celery_app import celery_app

    monkeypatch.setattr(celery_app, "send_task", lambda *a, **k: None)
    first = js.submit("app.tasks.add", args=[1, 1], idempotency_key="sdk-1")
    again = js.submit("app.tasks.add", args=[1, 1], idempotency_key="sdk-1")
    assert again.task_id == first.task_id and again.replayed and not first.replayed


def test_errors_are_typed(js):
    with pytest.raises(JobSchedulerError) as info:
        js.submit("app.tasks.add", args=[1])
    assert info.value.status == 422 and info.value.type == "validation_error" and info.value.details
    with pytest.raises(JobSchedulerError) as info:
        js.status("nope")
    assert info.value.type == "not_found" and not info.value.retryable


def test_retries_reuse_the_same_idempotency_key():
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("Idempotency-Key"))
        if len(seen) < 3:
            return httpx.Response(503, json={"error": {"type": "service_unavailable", "message": "db down"}})
        return httpx.Response(201, json={"task_id": "t-1", "status": "queued"})

    http = httpx.Client(base_url="http://x", transport=httpx.MockTransport(handler))
    js = JobScheduler(http=http, retries=4)
    start = time.monotonic()
    assert js.submit("app.tasks.add", args=[1, 1]).task_id == "t-1"
    assert len(seen) == 3 and len(set(seen)) == 1 and seen[0]  # one key, three attempts
    assert time.monotonic() - start >= 1.4  # backed off 0.5 s + 1 s


def test_writes_without_a_key_are_not_retried():
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(503, json={"error": {"type": "service_unavailable", "message": "x"}})

    js = JobScheduler(http=httpx.Client(base_url="http://x", transport=httpx.MockTransport(handler)))
    with pytest.raises(JobSchedulerError):
        js.cancel("t-1")  # DELETE without an idempotency key: not safe to repeat blindly
    assert len(calls) == 1


def test_workflow_and_schedule_helpers(js, worker):
    wf = js.create_workflow(
        [
            {"id": "a", "task_name": "app.tasks.add", "args": [1, 2]},
            {"id": "b", "task_name": "app.tasks.add", "args": [3, 4], "depends_on": ["a"]},
        ],
        name="sdk",
    )
    from conftest import run

    from app import scheduler

    deadline = time.monotonic() + 30
    while js.workflow(wf["id"])["status"] == "RUNNING" and time.monotonic() < deadline:
        run(scheduler.relay_outbox())
        time.sleep(0.2)
    assert js.wait_workflow(wf["id"], timeout=5)["status"] == "SUCCESS"

    schedule = js.create_schedule("app.tasks.heartbeat", "cron", cron_expr="0 9 * * *", timezone="Europe/London")
    assert js.update_schedule(schedule["id"], enabled=False)["enabled"] is False
    js.delete_schedule(schedule["id"])


def test_verify_webhook_matches_the_server_signature():
    body, ts = b'{"event":"job.finished"}', str(int(time.time()))
    headers = {"X-JobScheduler-Timestamp": ts, "X-JobScheduler-Signature": webhooks.sign(body, ts, "k")}
    assert verify_webhook(body, headers, "k")
    assert not verify_webhook(body + b" ", headers, "k")
    old = {**headers, "X-JobScheduler-Timestamp": str(int(time.time()) - 3600)}
    assert not verify_webhook(body, old, "k")
