"""HTTP behaviour that doesn't need a worker: validation, errors, auth, state rules."""

from datetime import datetime, timedelta, timezone

import pytest
from conftest import run
from fastapi.testclient import TestClient

from app import db, scheduler
from app.api.routes import jobs as jobs_routes
from app.celery_app import celery_app
from app.config import settings
from app.main import app


def _future(seconds: int = 3600) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


def _assert_error(response, status_code: int, error_type: str) -> dict:
    assert response.status_code == status_code, response.text
    error = response.json()["error"]
    assert error["status_code"] == status_code
    assert error["type"] == error_type
    assert error["message"]
    return error


# ---------------------------------------------------------------------------
# Error envelope
# ---------------------------------------------------------------------------
def test_unknown_route_uses_error_envelope(client):
    _assert_error(client.get("/does-not-exist"), 404, "not_found")


def test_wrong_method_uses_error_envelope(client):
    _assert_error(client.put("/jobs"), 405, "method_not_allowed")


def test_validation_error_uses_error_envelope(client):
    error = _assert_error(client.post("/tasks/add", json={"x": "not a number"}), 422, "validation_error")
    assert isinstance(error["details"], list) and error["details"]


def test_unhandled_exception_becomes_clean_500(monkeypatch):
    async def boom(**_):
        raise RuntimeError("secret internals")

    monkeypatch.setattr(db, "list_jobs", boom)
    with TestClient(app, raise_server_exceptions=False) as client:
        error = _assert_error(client.get("/jobs"), 500, "internal_error")
    assert "secret" not in error["message"]


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------
def test_api_key_required_when_configured(client, monkeypatch):
    monkeypatch.setattr(settings, "API_KEYS", ["key-1", "key-2"])

    missing = client.get("/jobs")
    _assert_error(missing, 401, "unauthorized")
    assert missing.headers["WWW-Authenticate"] == "ApiKey"
    _assert_error(client.get("/jobs", headers={"X-API-Key": "wrong"}), 401, "unauthorized")

    assert client.get("/jobs", headers={"X-API-Key": "key-2"}).status_code == 200
    assert client.get("/health").status_code == 200  # probes stay open


def test_auth_disabled_when_no_keys(client):
    assert client.get("/jobs").status_code == 200


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------
def test_readiness_ok(client):
    body = client.get("/health/ready").json()
    assert body == {"status": "ready", "checks": {"database": "ok", "broker": "ok"}}


def test_readiness_reports_failed_dependency(client, monkeypatch):
    async def down():
        raise OSError("disk gone")

    monkeypatch.setattr(db, "ping", down)
    error = _assert_error(client.get("/health/ready"), 503, "service_unavailable")
    assert error["details"]["database"].startswith("error")
    assert error["details"]["broker"] == "ok"


# ---------------------------------------------------------------------------
# Submitting tasks
# ---------------------------------------------------------------------------
def test_job_row_exists_before_task_is_published(client, monkeypatch):
    seen = {}

    def fake_send_task(name, *, args, kwargs, task_id, queue, retries):
        seen["row"] = run(db.get_job(task_id))
        seen["queue"] = queue

    monkeypatch.setattr(celery_app, "send_task", fake_send_task)
    task_id = client.post("/tasks/long", json={"duration_seconds": 2}).json()["task_id"]

    assert seen["row"]["task_id"] == task_id
    assert seen["row"]["status"] == "QUEUED"
    assert seen["row"]["published_at"] is None  # marked only after the broker accepted it
    assert seen["queue"] == settings.LONG_TASK_QUEUE_NAME
    assert run(db.get_job(task_id))["published_at"] is not None


def test_broker_down_job_is_kept_and_published_later(client, monkeypatch):
    """The job is durably accepted; the outbox relay publishes it once the broker is back."""

    def broker_down(*_, **__):
        raise ConnectionError("redis unreachable")

    monkeypatch.setattr(celery_app, "send_task", broker_down)
    response = client.post("/tasks/add", json={"x": 1, "y": 2})
    assert response.status_code == 201
    task_id = response.json()["task_id"]
    assert run(db.get_job(task_id))["published_at"] is None

    sent = []
    monkeypatch.setattr(celery_app, "send_task", lambda name, **kw: sent.append(kw["task_id"]))
    monkeypatch.setattr(settings, "OUTBOX_REPUBLISH_AFTER_SECONDS", 0)
    assert run(scheduler.relay_outbox()) == 1
    assert sent == [task_id]


def test_database_down_is_503(monkeypatch):
    from sqlalchemy.exc import OperationalError

    async def down(**_):
        raise OperationalError("SELECT 1", {}, Exception("connection refused"))

    monkeypatch.setattr(db, "list_jobs", down)
    with TestClient(app, raise_server_exceptions=False) as client:
        _assert_error(client.get("/jobs"), 503, "service_unavailable")


# ---------------------------------------------------------------------------
# Jobs: status fallback, retry rules, cancel rules
# ---------------------------------------------------------------------------
def _insert_job(task_id: str, status: str, task_name: str = "app.tasks.add") -> None:
    run(db.create_job(task_id, task_name, [1, 2], {}, queue="default"))
    if status != "QUEUED":
        run(db.update_job_status(task_id, status=status, result=3, finished=status in db.TERMINAL_STATES))


def test_unknown_job_is_404(client):
    _assert_error(client.get("/jobs/nope"), 404, "not_found")
    _assert_error(client.get("/jobs/nope/status"), 404, "not_found")


def test_status_falls_back_to_job_record_when_result_expired(client):
    _insert_job("expired-1", "SUCCESS")
    body = client.get("/jobs/expired-1/status").json()
    assert body["source"] == "job_record"
    assert body["state"] == "SUCCESS"
    assert body["result"] == 3


def test_retry_rejects_non_failed_job_unless_forced(client, monkeypatch):
    monkeypatch.setattr(celery_app, "send_task", lambda *a, **k: None)
    _insert_job("ok-1", "SUCCESS")

    _assert_error(client.post("/jobs/ok-1/retry"), 409, "conflict")
    forced = client.post("/jobs/ok-1/retry", params={"force": "true"})
    assert forced.status_code == 201
    assert run(db.get_job(forced.json()["task_id"]))["retry_of"] == "ok-1"


def test_retry_unregistered_task_is_400(client):
    _insert_job("gone-1", "FAILURE", task_name="app.tasks.removed_long_ago")
    _assert_error(client.post("/jobs/gone-1/retry"), 400, "bad_request")


def test_cancel_finished_job_is_409(client):
    _insert_job("done-1", "SUCCESS")
    _assert_error(client.delete("/jobs/done-1"), 409, "conflict")
    assert run(db.get_job("done-1"))["status"] == "SUCCESS"  # unchanged


def test_cancel_queued_job_revokes_it(client, monkeypatch):
    revoked = []
    monkeypatch.setattr(jobs_routes.celery_app.control, "revoke", lambda task_id, **kw: revoked.append(task_id))
    _insert_job("queued-1", "QUEUED")

    assert client.delete("/jobs/queued-1").json() == {"task_id": "queued-1", "status": "revoked"}
    assert revoked == ["queued-1"]
    assert run(db.get_job("queued-1"))["status"] == "REVOKED"


# ---------------------------------------------------------------------------
# Schedules: create validation
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "body, fragment",
    [
        ({"task_name": "os.system", "schedule_type": "interval", "interval_seconds": 5}, "task_name must be one of"),
        ({"task_name": "app.tasks.heartbeat", "schedule_type": "interval"}, "interval_seconds is required"),
        (
            {
                "task_name": "app.tasks.heartbeat",
                "schedule_type": "interval",
                "interval_seconds": 5,
                "cron_expr": "* * * * *",
            },
            "cannot be used",
        ),
        ({"task_name": "app.tasks.heartbeat", "schedule_type": "cron", "cron_expr": "not cron"}, "Invalid"),
        ({"task_name": "app.tasks.heartbeat", "schedule_type": "cron", "cron_expr": "0 * * * * *"}, "Invalid"),
        (
            {"task_name": "app.tasks.heartbeat", "schedule_type": "once", "run_at": "2020-01-01T00:00:00+00:00"},
            "future",
        ),
        ({"task_name": "app.tasks.heartbeat", "schedule_type": "once", "run_at": "2099-01-01T00:00:00"}, "timezone"),
        (
            {"task_name": "app.tasks.add", "args": [1], "schedule_type": "interval", "interval_seconds": 5},
            "don't match",
        ),
    ],
)
def test_create_schedule_rejects_invalid_input(client, body, fragment):
    error = _assert_error(client.post("/schedules", json=body), 422, "validation_error")
    assert fragment in str(error["details"])


def test_create_once_schedule_normalises_run_at_to_utc(client):
    body = client.post(
        "/schedules",
        json={
            "task_name": "app.tasks.add",
            "args": [1, 2],
            "schedule_type": "once",
            "run_at": "2099-01-01T10:00:00+02:00",
        },
    ).json()
    assert body["next_run_at"] == "2099-01-01T08:00:00Z"
    assert body["run_at"] == "2099-01-01T08:00:00Z"
    assert body["args"] == [1, 2]
    assert body["enabled"] is True


# ---------------------------------------------------------------------------
# Schedules: updates
# ---------------------------------------------------------------------------
def _create(client, **body) -> dict:
    response = client.post("/schedules", json={"task_name": "app.tasks.heartbeat", **body})
    assert response.status_code == 201, response.text
    return response.json()


def test_patch_interval_recomputes_next_run(client):
    schedule = _create(client, schedule_type="interval", interval_seconds=3600)
    before = datetime.now(timezone.utc)
    body = client.patch(f"/schedules/{schedule['id']}", json={"interval_seconds": 120}).json()

    next_run = datetime.fromisoformat(body["next_run_at"])
    assert body["interval_seconds"] == 120
    assert timedelta(seconds=110) < next_run - before < timedelta(seconds=130)


def test_patch_cron_recomputes_next_run(client):
    schedule = _create(client, schedule_type="cron", cron_expr="0 0 1 1 *")  # once a year
    body = client.patch(f"/schedules/{schedule['id']}", json={"cron_expr": "*/5 * * * *"}).json()
    next_run = datetime.fromisoformat(body["next_run_at"])
    assert next_run - datetime.now(timezone.utc) <= timedelta(minutes=5)
    assert next_run.minute % 5 == 0


def test_patch_field_of_other_schedule_type_is_400(client):
    schedule = _create(client, schedule_type="interval", interval_seconds=60)
    _assert_error(client.patch(f"/schedules/{schedule['id']}", json={"cron_expr": "* * * * *"}), 400, "bad_request")


def test_patch_rejects_invalid_cron_and_nulls(client):
    schedule = _create(client, schedule_type="cron", cron_expr="* * * * *")
    _assert_error(client.patch(f"/schedules/{schedule['id']}", json={"cron_expr": "nope"}), 422, "validation_error")
    _assert_error(client.patch(f"/schedules/{schedule['id']}", json={"cron_expr": None}), 422, "validation_error")


def test_pause_and_resume(client):
    schedule = _create(client, schedule_type="interval", interval_seconds=60)
    assert client.patch(f"/schedules/{schedule['id']}", json={"enabled": False}).json()["enabled"] is False
    assert client.patch(f"/schedules/{schedule['id']}", json={"enabled": True}).json()["enabled"] is True


def test_cannot_resume_once_schedule_that_already_fired(client, monkeypatch):
    monkeypatch.setattr(celery_app, "send_task", lambda *a, **k: None)
    schedule = _create(client, schedule_type="once", run_at=_future(1))
    run(db.update_schedule(schedule["id"], next_run_at=datetime.now(timezone.utc)))
    run(scheduler.dispatch_due_schedules())

    fired = client.get(f"/schedules/{schedule['id']}").json()
    assert fired["enabled"] is False and fired["run_count"] == 1
    _assert_error(client.patch(f"/schedules/{schedule['id']}", json={"enabled": True}), 409, "conflict")


def test_delete_schedule(client):
    schedule = _create(client, schedule_type="interval", interval_seconds=60)
    assert client.delete(f"/schedules/{schedule['id']}").json() == {"schedule_id": schedule["id"], "status": "deleted"}
    _assert_error(client.get(f"/schedules/{schedule['id']}"), 404, "not_found")
    _assert_error(client.delete(f"/schedules/{schedule['id']}"), 404, "not_found")


def test_system_schedules_are_read_only(client):
    run(scheduler.seed_system_schedules())
    system = next(s for s in client.get("/schedules").json() if s["system"])
    _assert_error(client.patch(f"/schedules/{system['id']}", json={"enabled": False}), 409, "conflict")
    _assert_error(client.delete(f"/schedules/{system['id']}"), 409, "conflict")


def test_list_schedules_filters_enabled(client):
    active = _create(client, schedule_type="interval", interval_seconds=60)
    paused = _create(client, schedule_type="interval", interval_seconds=60)
    client.patch(f"/schedules/{paused['id']}", json={"enabled": False})

    assert {s["id"] for s in client.get("/schedules").json()} == {active["id"], paused["id"]}
    assert [s["id"] for s in client.get("/schedules", params={"enabled_only": "true"}).json()] == [active["id"]]
