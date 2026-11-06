"""
Tests for the 5.0 features: /v1 + legacy aliases, the generic submit endpoint
(priority, delayed start, per-job retries/timeout), webhooks, workflows,
managed API keys (scopes, tenants, allowed tasks, rate limits, audit),
the dead-letter view, cron time zones, /metrics, the event stream and the console.
"""

import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from celery import states
from conftest import run, wait_for_job
from sqlalchemy import update

from app import db, publisher, scheduler, scheduling, webhooks
from app.api import security
from app.celery_app import celery_app
from app.config import settings

BOOT = "bootstrap-test-key"


@pytest.fixture
def auth(monkeypatch):
    """Turn auth on with one bootstrap (admin) key; return its headers."""
    monkeypatch.setattr(settings, "API_KEYS", [BOOT])
    security.clear_key_cache()
    yield {"X-API-Key": BOOT}
    security.clear_key_cache()


def make_key(client, admin_headers, **fields) -> dict:
    body = {"name": fields.pop("name"), "scopes": fields.pop("scopes", ["jobs:read", "jobs:write"]), **fields}
    response = client.post("/v1/admin/api-keys", json=body, headers=admin_headers)
    assert response.status_code == 201, response.text
    return response.json()


def no_publish(monkeypatch) -> list:
    sent = []
    monkeypatch.setattr(celery_app, "send_task", lambda name, **kw: sent.append({"name": name, **kw}))
    return sent


# ---------------------------------------------------------------------------
# Versioning
# ---------------------------------------------------------------------------
def test_v1_paths_and_deprecated_aliases(client, monkeypatch):
    no_publish(monkeypatch)
    v1 = client.post("/v1/tasks/add", json={"x": 1, "y": 2})
    assert v1.status_code == 201 and "Deprecation" not in v1.headers

    legacy = client.post("/tasks/add", json={"x": 1, "y": 2})
    assert legacy.status_code == 201
    assert legacy.headers["Deprecation"] == "true"
    assert legacy.headers["Link"] == '</v1/tasks/add>; rel="successor-version"'

    paths = client.get("/openapi.json").json()["paths"]
    assert "/v1/jobs" in paths and "/v1/workflows" in paths and "/v1/admin/api-keys" in paths
    assert "/jobs" not in paths and "/tasks/add" not in paths  # aliases are hidden


# ---------------------------------------------------------------------------
# Generic submit: priority, delayed start, per-job options
# ---------------------------------------------------------------------------
def test_generic_submit_with_priority_and_timeout(client, monkeypatch):
    sent = no_publish(monkeypatch)
    response = client.post(
        "/v1/jobs",
        json={
            "task_name": "app.tasks.add",
            "args": [2, 3],
            "priority": "high",
            "timeout_seconds": 30,
            "max_retries": 2,
        },
    )
    assert response.status_code == 201, response.text
    task_id = response.json()["task_id"]
    assert sent[0]["priority"] == 0 and sent[0]["soft_time_limit"] == 30 and sent[0]["time_limit"] == 40

    job = client.get(f"/v1/jobs/{task_id}").json()
    assert (job["priority"], job["timeout_seconds"], job["max_retries"]) == ("high", 30, 2)


def test_generic_submit_validates_task_and_arguments(client):
    assert client.post("/v1/jobs", json={"task_name": "os.system", "args": ["ls"]}).status_code == 422
    assert client.post("/v1/jobs", json={"task_name": "app.tasks.add", "args": [1]}).status_code == 422
    both = {"task_name": "app.tasks.add", "args": [1, 2], "delay_seconds": 5, "run_at": "2099-01-01T00:00:00Z"}
    assert client.post("/v1/jobs", json=both).status_code == 422


def test_delayed_job_is_published_only_when_due(client, monkeypatch):
    sent = no_publish(monkeypatch)
    response = client.post("/v1/jobs", json={"task_name": "app.tasks.add", "args": [1, 1], "delay_seconds": 3600})
    body = response.json()
    assert response.status_code == 201 and body["run_after"] is not None
    assert sent == []  # not published yet

    assert run(scheduler.relay_outbox()) == 0  # not due: the relay leaves it alone
    past = datetime.now(timezone.utc) - timedelta(seconds=1)

    async def make_due():
        async with db.engine().begin() as conn:
            await conn.execute(update(db.jobs).where(db.jobs.c.task_id == body["task_id"]).values(run_after=past))

    run(make_due())
    assert run(scheduler.relay_outbox()) == 1  # due: published at once, no outbox delay
    assert [m["task_id"] for m in sent] == [body["task_id"]]


def test_per_job_max_retries_overrides_the_task(client, worker):
    # flaky_task retries TASK_DEFAULT_MAX_RETRIES (1 in tests) times by default; 0 here.
    response = client.post("/v1/jobs", json={"task_name": "app.tasks.flaky_task", "args": [1.0], "max_retries": 0})
    job = wait_for_job(response.json()["task_id"], {states.FAILURE})
    assert job["retries"] == 0 and job["attempts"] == 1


# ---------------------------------------------------------------------------
# Webhooks
# ---------------------------------------------------------------------------
def _mock_client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def test_webhook_is_signed_and_delivered(client, worker, monkeypatch):
    monkeypatch.setattr(settings, "WEBHOOK_SIGNING_SECRET", "s3cret")
    response = client.post(
        "/v1/jobs",
        json={"task_name": "app.tasks.add", "args": [2, 2], "callback_url": "https://hooks.example.com/done"},
    )
    task_id = response.json()["task_id"]
    wait_for_job(task_id, {states.SUCCESS})

    received = []

    def handler(request: httpx.Request) -> httpx.Response:
        received.append(request)
        return httpx.Response(204)

    async def deliver():
        async with _mock_client(handler) as http:
            return await webhooks.deliver_due_webhooks(http)

    assert run(deliver()) == 1
    request = received[0]
    body = json.loads(request.content)
    assert body["event"] == "job.finished" and body["task_id"] == task_id and body["status"] == "SUCCESS"
    assert body["result"] == 4.0
    assert webhooks.verify_signature(
        request.content,
        request.headers[webhooks.TIMESTAMP_HEADER],
        request.headers[webhooks.SIGNATURE_HEADER],
        "s3cret",
    )
    assert not webhooks.verify_signature(
        request.content, request.headers[webhooks.TIMESTAMP_HEADER], "sha256=bad", "s3cret"
    )

    deliveries = client.get(f"/v1/jobs/{task_id}/webhooks").json()
    assert [(d["status"], d["attempts"], d["last_status_code"]) for d in deliveries] == [("DELIVERED", 1, 204)]
    assert run(deliver()) == 0  # nothing left to send


def test_webhook_retries_with_backoff_then_gives_up(client, monkeypatch):
    monkeypatch.setattr(settings, "WEBHOOK_MAX_ATTEMPTS", 2)
    no_publish(monkeypatch)
    task_id = client.post(
        "/v1/jobs", json={"task_name": "app.tasks.add", "args": [1, 1], "callback_url": "https://hooks.example.com/x"}
    ).json()["task_id"]
    run(db.update_job_status(task_id, states.SUCCESS, result=2, finished=True))

    async def deliver():
        async with _mock_client(lambda request: httpx.Response(500, text="boom")) as http:
            return await webhooks.deliver_due_webhooks(http)

    assert run(deliver()) == 1
    [delivery] = run(db.list_webhook_deliveries(task_id))
    assert delivery["status"] == "PENDING" and delivery["attempts"] == 1
    assert delivery["next_attempt_at"] > datetime.now(timezone.utc) + timedelta(seconds=5)  # backed off
    assert run(deliver()) == 0  # not due yet

    async def make_due():
        async with db.engine().begin() as conn:
            await conn.execute(update(db.webhook_deliveries).values(next_attempt_at=datetime.now(timezone.utc)))

    run(make_due())
    assert run(deliver()) == 1
    [delivery] = run(db.list_webhook_deliveries(task_id))
    assert (delivery["status"], delivery["attempts"]) == ("FAILED", 2)
    assert "HTTP 500" in delivery["last_error"]


def test_final_state_and_webhook_are_written_once(client, monkeypatch):
    no_publish(monkeypatch)
    task_id = client.post(
        "/v1/jobs", json={"task_name": "app.tasks.add", "args": [1, 1], "callback_url": "https://hooks.example.com/x"}
    ).json()["task_id"]
    assert run(db.update_job_status(task_id, states.SUCCESS, result=2, finished=True)) is True
    # A second final write (a late duplicate, a racing cancel) changes nothing and queues no second webhook.
    assert run(db.update_job_status(task_id, states.REVOKED, finished=True)) is False
    assert run(db.get_job(task_id))["status"] == states.SUCCESS
    assert len(run(db.list_webhook_deliveries(task_id))) == 1


def test_callback_allow_list(client, monkeypatch):
    monkeypatch.setattr(settings, "WEBHOOK_ALLOWED_HOSTS", ["*.example.com"])
    ok = {"task_name": "app.tasks.add", "args": [1, 1], "callback_url": "https://hooks.example.com/x"}
    no_publish(monkeypatch)
    assert client.post("/v1/jobs", json=ok).status_code == 201
    for url in ("https://169.254.169.254/latest", "ftp://hooks.example.com/x", "https://u:p@hooks.example.com/"):
        assert client.post("/v1/jobs", json={**ok, "callback_url": url}).status_code == 422, url


# ---------------------------------------------------------------------------
# Workflows
# ---------------------------------------------------------------------------
def test_workflow_fan_out_fan_in(client, worker):
    response = client.post(
        "/v1/workflows",
        json={
            "name": "diamond",
            "steps": [
                {"id": "a", "task_name": "app.tasks.add", "args": [1, 2]},
                {"id": "b", "task_name": "app.tasks.add", "args": [3, 4]},
                {"id": "c", "task_name": "app.tasks.add", "args": [5, 6], "depends_on": ["a", "b"]},
                {"id": "d", "task_name": "app.tasks.add", "args": [7, 8], "depends_on": ["c"]},
            ],
        },
    )
    assert response.status_code == 201, response.text
    workflow = response.json()
    steps = {s["id"]: s for s in workflow["steps"]}
    assert steps["c"]["status"] == "WAITING" and steps["d"]["status"] == "WAITING"

    # The relay releases dependents; run it the way the scheduler loop does.
    for _ in range(100):
        run(scheduler.relay_outbox())
        workflow = client.get(f"/v1/workflows/{workflow['id']}").json()
        if workflow["status"] != "RUNNING":
            break
        __import__("time").sleep(0.2)

    assert workflow["status"] == "SUCCESS"
    steps = {s["id"]: s for s in workflow["steps"]}
    assert {s["status"] for s in steps.values()} == {"SUCCESS"}
    assert steps["c"]["started_at"] >= max(steps["a"]["finished_at"], steps["b"]["finished_at"])
    assert steps["d"]["started_at"] >= steps["c"]["finished_at"]
    jobs = client.get(f"/v1/jobs?workflow_id={workflow['id']}").json()
    assert jobs["total"] == 4 and all(j["attempts"] == 1 for j in jobs["jobs"])  # each step ran once


def test_fan_in_step_is_released_once_and_only_when_all_parents_succeed(client, monkeypatch):
    sent = no_publish(monkeypatch)
    workflow = client.post(
        "/v1/workflows",
        json={
            "callback_url": "https://hooks.example.com/wf",
            "steps": [
                {"id": "a", "task_name": "app.tasks.add", "args": [1, 1]},
                {"id": "b", "task_name": "app.tasks.add", "args": [1, 1]},
                {"id": "c", "task_name": "app.tasks.add", "args": [1, 1], "depends_on": ["a", "b"]},
            ],
        },
    ).json()
    ids = {s["id"]: s["task_id"] for s in workflow["steps"]}
    assert sorted(m["task_id"] for m in sent) == sorted([ids["a"], ids["b"]])  # roots published at once

    run(db.update_job_status(ids["a"], states.SUCCESS, finished=True))
    assert run(db.get_job(ids["c"]))["status"] == "WAITING"  # b hasn't finished
    run(db.update_job_status(ids["b"], states.SUCCESS, finished=True))
    released = run(db.get_job(ids["c"]))
    assert released["status"] == "QUEUED" and released["run_after"] is not None

    run(db.update_job_status(ids["c"], states.SUCCESS, finished=True))
    assert client.get(f"/v1/workflows/{workflow['id']}").json()["status"] == "SUCCESS"
    events = [d["event"] for d in run(db.list_webhook_deliveries(workflow["id"]))]
    assert events == ["workflow.finished"]  # exactly once


def test_failed_step_skips_everything_downstream(client, monkeypatch):
    no_publish(monkeypatch)
    workflow = client.post(
        "/v1/workflows",
        json={
            "steps": [
                {"id": "extract", "task_name": "app.tasks.add", "args": [1, 1]},
                {"id": "other", "task_name": "app.tasks.add", "args": [1, 1]},
                {"id": "transform", "task_name": "app.tasks.add", "args": [1, 1], "depends_on": ["extract"]},
                {"id": "load", "task_name": "app.tasks.add", "args": [1, 1], "depends_on": ["transform", "other"]},
            ]
        },
    ).json()
    ids = {s["id"]: s["task_id"] for s in workflow["steps"]}
    run(db.update_job_status(ids["extract"], states.FAILURE, error="boom", finished=True))
    steps = {s["id"]: s for s in client.get(f"/v1/workflows/{workflow['id']}").json()["steps"]}
    assert steps["transform"]["status"] == "REVOKED" and "extract" in steps["transform"]["error"]
    assert steps["load"]["status"] == "REVOKED"
    assert steps["other"]["status"] == "QUEUED"  # independent branch keeps going

    run(db.update_job_status(ids["other"], states.SUCCESS, finished=True))
    assert client.get(f"/v1/workflows/{workflow['id']}").json()["status"] == "FAILURE"


def test_cancel_workflow(client, monkeypatch):
    no_publish(monkeypatch)
    monkeypatch.setattr(celery_app.control, "revoke", lambda *a, **k: None)
    workflow = client.post(
        "/v1/workflows",
        json={
            "steps": [
                {"id": "a", "task_name": "app.tasks.add", "args": [1, 1]},
                {"id": "b", "task_name": "app.tasks.add", "args": [1, 1], "depends_on": ["a"]},
            ]
        },
    ).json()
    cancelled = client.delete(f"/v1/workflows/{workflow['id']}").json()
    assert cancelled["status"] == "REVOKED" and {s["status"] for s in cancelled["steps"]} == {"REVOKED"}
    assert client.delete(f"/v1/workflows/{workflow['id']}").status_code == 409


def test_cancel_one_waiting_step(client, monkeypatch):
    no_publish(monkeypatch)
    monkeypatch.setattr(celery_app.control, "revoke", lambda *a, **k: None)
    workflow = client.post(
        "/v1/workflows",
        json={
            "steps": [
                {"id": "a", "task_name": "app.tasks.add", "args": [1, 1]},
                {"id": "b", "task_name": "app.tasks.add", "args": [1, 1], "depends_on": ["a"]},
                {"id": "c", "task_name": "app.tasks.add", "args": [1, 1], "depends_on": ["b"]},
            ]
        },
    ).json()
    ids = {s["id"]: s["task_id"] for s in workflow["steps"]}
    assert client.delete(f"/v1/jobs/{ids['b']}").status_code == 200
    assert run(db.get_job(ids["b"]))["status"] == "REVOKED"
    assert run(db.get_job(ids["c"]))["status"] == "REVOKED"  # downstream skipped
    run(db.update_job_status(ids["a"], states.SUCCESS, finished=True))
    assert client.get(f"/v1/workflows/{workflow['id']}").json()["status"] == "REVOKED"


@pytest.mark.parametrize(
    "steps, message",
    [
        ([{"id": "a", "task_name": "app.tasks.add", "args": [1, 1], "depends_on": ["b"]}], "unknown step"),
        (
            [
                {"id": "a", "task_name": "app.tasks.add", "args": [1, 1], "depends_on": ["b"]},
                {"id": "b", "task_name": "app.tasks.add", "args": [1, 1], "depends_on": ["a"]},
            ],
            "cycle",
        ),
        (
            [
                {"id": "a", "task_name": "app.tasks.add", "args": [1, 1]},
                {"id": "a", "task_name": "app.tasks.add", "args": [1, 1]},
            ],
            "duplicate",
        ),
        ([{"id": "a", "task_name": "app.tasks.add", "args": [1]}], "don't match"),
    ],
)
def test_invalid_workflows_are_rejected(client, steps, message):
    response = client.post("/v1/workflows", json={"steps": steps})
    assert response.status_code == 422
    assert message in json.dumps(response.json())


def test_workflow_idempotency(client, monkeypatch):
    no_publish(monkeypatch)
    body = {"steps": [{"id": "a", "task_name": "app.tasks.add", "args": [1, 1]}]}
    first = client.post("/v1/workflows", json=body, headers={"Idempotency-Key": "wf-1"})
    again = client.post("/v1/workflows", json=body, headers={"Idempotency-Key": "wf-1"})
    assert first.status_code == 201 and again.status_code == 200
    assert again.json()["id"] == first.json()["id"] and again.headers["Idempotent-Replayed"] == "true"
    other = {"steps": [{"id": "z", "task_name": "app.tasks.add", "args": [1, 1]}]}
    assert client.post("/v1/workflows", json=other, headers={"Idempotency-Key": "wf-1"}).status_code == 409


# ---------------------------------------------------------------------------
# Managed keys: scopes, tenants, allowed tasks, rate limits, audit
# ---------------------------------------------------------------------------
def test_managed_key_lifecycle_and_scopes(client, auth, monkeypatch):
    no_publish(monkeypatch)
    created = make_key(client, auth, name="reader", scopes=["jobs:read"])
    assert created["key"].startswith("jsk_") and created["key_prefix"] == created["key"][:10]
    reader = {"X-API-Key": created["key"]}

    assert client.get("/v1/jobs", headers=reader).status_code == 200
    denied = client.post("/v1/tasks/add", json={"x": 1, "y": 1}, headers=reader)
    assert denied.status_code == 403 and denied.json()["error"]["type"] == "forbidden"
    assert client.get("/v1/schedules", headers=reader).status_code == 403
    assert client.get("/v1/admin/api-keys", headers=reader).status_code == 403

    listed = client.get("/v1/admin/api-keys", headers=auth).json()
    assert [k["name"] for k in listed] == ["reader"] and "key" not in listed[0]  # secret never shown again

    assert client.delete(f"/v1/admin/api-keys/{created['id']}", headers=auth).status_code == 200
    assert client.get("/v1/jobs", headers=reader).status_code == 401  # revoked
    assert (
        client.post("/v1/admin/api-keys", json={"name": "reader", "scopes": ["jobs:read"]}, headers=auth).status_code
        == 409
    )
    assert client.post("/v1/admin/api-keys", json={"name": "x", "scopes": ["root"]}, headers=auth).status_code == 422


def test_tenants_are_isolated(client, auth, monkeypatch):
    no_publish(monkeypatch)
    acme = {
        "X-API-Key": make_key(
            client,
            auth,
            name="acme",
            tenant="acme",
            scopes=["jobs:read", "jobs:write", "schedules:read", "schedules:write"],
        )["key"]
    }
    globex = {
        "X-API-Key": make_key(
            client,
            auth,
            name="globex",
            tenant="globex",
            scopes=["jobs:read", "jobs:write", "schedules:read", "schedules:write"],
        )["key"]
    }

    acme_job = client.post("/v1/jobs", json={"task_name": "app.tasks.add", "args": [1, 1]}, headers=acme).json()[
        "task_id"
    ]
    assert client.get(f"/v1/jobs/{acme_job}", headers=acme).json()["tenant"] == "acme"
    assert client.get(f"/v1/jobs/{acme_job}", headers=globex).status_code == 404
    assert client.delete(f"/v1/jobs/{acme_job}", headers=globex).status_code == 404
    assert client.get("/v1/jobs", headers=globex).json()["total"] == 0
    assert client.get("/v1/jobs", headers=auth).json()["total"] == 1  # admin sees every tenant

    # The same Idempotency-Key in two tenants makes two different jobs.
    same_key = {"Idempotency-Key": "order-7"}
    one = client.post("/v1/jobs", json={"task_name": "app.tasks.add", "args": [2, 2]}, headers={**acme, **same_key})
    two = client.post("/v1/jobs", json={"task_name": "app.tasks.add", "args": [2, 2]}, headers={**globex, **same_key})
    assert one.status_code == two.status_code == 201 and one.json()["task_id"] != two.json()["task_id"]
    assert client.get(f"/v1/jobs/{one.json()['task_id']}", headers=acme).json()["idempotency_key"] == "order-7"

    schedule = client.post(
        "/v1/schedules",
        json={"task_name": "app.tasks.heartbeat", "schedule_type": "interval", "interval_seconds": 60},
        headers=acme,
    ).json()
    assert schedule["tenant"] == "acme"
    assert client.get(f"/v1/schedules/{schedule['id']}", headers=globex).status_code == 404
    assert [s["id"] for s in client.get("/v1/schedules", headers=globex).json()] == []


def test_allowed_tasks_and_schedule_quota(client, auth, monkeypatch):
    no_publish(monkeypatch)
    monkeypatch.setattr(settings, "TENANT_MAX_ACTIVE_SCHEDULES", 1)
    key = make_key(
        client,
        auth,
        name="notifier",
        tenant="t1",
        scopes=["jobs:write", "schedules:write"],
        allowed_tasks=["app.tasks.heartbeat"],
    )
    headers = {"X-API-Key": key["key"]}
    assert (
        client.post("/v1/jobs", json={"task_name": "app.tasks.add", "args": [1, 1]}, headers=headers).status_code == 403
    )
    assert client.post("/v1/jobs", json={"task_name": "app.tasks.heartbeat"}, headers=headers).status_code == 201

    body = {"task_name": "app.tasks.heartbeat", "schedule_type": "interval", "interval_seconds": 60}
    assert client.post("/v1/schedules", json=body, headers=headers).status_code == 201
    over = client.post("/v1/schedules", json=body, headers=headers)
    assert over.status_code == 409 and "TENANT_MAX_ACTIVE_SCHEDULES" in over.json()["error"]["message"]


def test_rate_limit(client, auth):
    key = make_key(client, auth, name="limited", scopes=["jobs:read"], rate_limit_per_minute=2)
    headers = {"X-API-Key": key["key"]}
    first = client.get("/v1/jobs", headers=headers)
    assert first.status_code == 200 and first.headers["X-RateLimit-Limit"] == "2"
    assert first.headers["X-RateLimit-Remaining"] == "1"
    assert client.get("/v1/jobs", headers=headers).status_code == 200
    blocked = client.get("/v1/jobs", headers=headers)
    assert blocked.status_code == 429 and blocked.json()["error"]["type"] == "rate_limited"
    assert int(blocked.headers["Retry-After"]) >= 1
    assert client.get("/v1/jobs", headers=auth).status_code == 200  # other keys unaffected


def test_audit_log_records_who_did_what(client, auth, monkeypatch):
    no_publish(monkeypatch)
    monkeypatch.setattr(celery_app.control, "revoke", lambda *a, **k: None)
    writer = {"X-API-Key": make_key(client, auth, name="svc")["key"]}
    task_id = client.post("/v1/jobs", json={"task_name": "app.tasks.add", "args": [1, 1]}, headers=writer).json()[
        "task_id"
    ]
    client.delete(f"/v1/jobs/{task_id}", headers=writer)

    entries = client.get("/v1/admin/audit-log?actor=svc", headers=auth).json()
    assert [(e["action"], e["resource_id"]) for e in entries] == [("job.cancel", task_id), ("job.submit", task_id)]
    created = client.get("/v1/admin/audit-log?action=apikey.create", headers=auth).json()
    assert created[0]["actor"] == "bootstrap-key-1" and created[0]["details"]["name"] == "svc"


# ---------------------------------------------------------------------------
# Dead-letter view
# ---------------------------------------------------------------------------
def test_failure_groups_and_bulk_retry(client, monkeypatch):
    sent = no_publish(monkeypatch)
    failed = []
    for _ in range(3):
        task_id = client.post("/v1/jobs", json={"task_name": "app.tasks.flaky_task", "args": [1.0]}).json()["task_id"]
        run(db.update_job_status(task_id, states.FAILURE, error="Simulated transient failure", finished=True))
        failed.append(task_id)
    other = client.post("/v1/jobs", json={"task_name": "app.tasks.add", "args": [1, 1]}).json()["task_id"]
    run(db.update_job_status(other, states.FAILURE, error="other", finished=True))

    groups = client.get("/v1/jobs/failures").json()["groups"]
    assert [(g["task_name"], g["count"]) for g in groups] == [("app.tasks.flaky_task", 3), ("app.tasks.add", 1)]

    sent.clear()
    body = {"task_name": "app.tasks.flaky_task", "error": "Simulated transient failure"}
    result = client.post("/v1/jobs/failures/retry", json=body).json()
    assert result["retried"] == 3 and sorted(j["retry_of"] for j in result["jobs"]) == sorted(failed)
    assert len(sent) == 3
    assert client.post("/v1/jobs/failures/retry", json=body).json()["retried"] == 0  # never twice
    assert [g["task_name"] for g in client.get("/v1/jobs/failures").json()["groups"]] == ["app.tasks.add"]


# ---------------------------------------------------------------------------
# Cron time zones
# ---------------------------------------------------------------------------
def test_cron_schedule_in_a_time_zone(client):
    body = {
        "task_name": "app.tasks.heartbeat",
        "schedule_type": "cron",
        "cron_expr": "0 9 * * *",
        "timezone": "Asia/Kolkata",
    }
    schedule = client.post("/v1/schedules", json=body).json()
    next_run = datetime.fromisoformat(schedule["next_run_at"])
    assert schedule["timezone"] == "Asia/Kolkata" and (next_run.hour, next_run.minute) == (3, 30)  # 09:00 IST

    changed = client.patch(f"/v1/schedules/{schedule['id']}", json={"timezone": "UTC"}).json()
    assert changed["timezone"] is None and datetime.fromisoformat(changed["next_run_at"]).hour == 9

    assert client.post("/v1/schedules", json={**body, "timezone": "Mars/Olympus"}).status_code == 422
    interval = {
        "task_name": "app.tasks.heartbeat",
        "schedule_type": "interval",
        "interval_seconds": 5,
        "timezone": "UTC",
    }
    assert client.post("/v1/schedules", json=interval).status_code == 422


def test_cron_time_zone_follows_daylight_saving():
    new_york = "America/New_York"
    winter = scheduling.next_cron_run("0 9 * * *", datetime(2026, 1, 10, tzinfo=timezone.utc), new_york)
    summer = scheduling.next_cron_run("0 9 * * *", datetime(2026, 7, 10, tzinfo=timezone.utc), new_york)
    assert (winter.hour, summer.hour) == (14, 13)
    # 01:30 happens twice when clocks go back (2026-11-01): fire once, not twice.
    first = scheduling.next_cron_run("30 1 * * *", datetime(2026, 11, 1, 4, tzinfo=timezone.utc), new_york)
    second = scheduling.next_cron_run("30 1 * * *", first, new_york)
    assert first == datetime(2026, 11, 1, 5, 30, tzinfo=timezone.utc)
    assert second == datetime(2026, 11, 2, 6, 30, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Metrics, event stream, console
# ---------------------------------------------------------------------------
def test_metrics_endpoint(client, monkeypatch):
    no_publish(monkeypatch)
    monkeypatch.setattr(settings, "METRICS_CACHE_SECONDS", 0)
    client.post("/v1/jobs", json={"task_name": "app.tasks.add", "args": [1, 1]})
    run(db.increment_counter("duplicate_dropped"))
    response = client.get("/metrics")
    assert response.status_code == 200 and response.headers["content-type"].startswith("text/plain")
    text = response.text
    assert 'jobscheduler_jobs{queue="default",status="QUEUED",task="app.tasks.add"} 1.0' in text
    assert 'jobscheduler_events_total{event="duplicate_dropped"} 1.0' in text
    assert "jobscheduler_schedule_max_overdue_seconds" in text
    assert 'jobscheduler_http_requests_total{method="POST",route="/v1/jobs",status="201"}' in text
    assert "jobscheduler_metrics_up 1.0" in text


def test_event_stream_ends_on_final_state(client, worker):
    task_id = client.post("/v1/jobs", json={"task_name": "app.tasks.add", "args": [20, 22]}).json()["task_id"]
    events = []
    with client.stream("GET", f"/v1/jobs/{task_id}/events") as response:
        assert response.headers["content-type"].startswith("text/event-stream")
        for line in response.iter_lines():
            if line.startswith("data: "):
                events.append(json.loads(line[6:]))
    assert events[-1]["state"] == "SUCCESS" and events[-1]["result"] == 42.0
    assert client.get("/v1/jobs/unknown/events").status_code == 404


def test_console_page(client):
    response = client.get("/ui")
    assert response.status_code == 200 and "<title>Job Scheduler Console</title>" in response.text
    assert "frame-ancestors 'none'" in response.headers["Content-Security-Policy"]


def test_publisher_carries_job_options():
    options = publisher._delivery_options({"priority": 6, "timeout_seconds": 5})
    assert options == {"priority": 6, "soft_time_limit": 5, "time_limit": 15}
    assert publisher._delivery_options({}) == {"priority": db.PRIORITIES["normal"]}
