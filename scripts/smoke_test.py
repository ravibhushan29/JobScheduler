"""
Smoke test against a RUNNING stack (API + both workers + scheduler + Redis + database).

Uses only the standard library, so it runs anywhere Python does:

    python scripts/smoke_test.py                          # http://localhost:8000
    API_URL=https://jobs.example.com API_KEY=... python scripts/smoke_test.py

Checks readiness, runs each kind of job end to end, retries a failure, and
waits for a one-off schedule to fire. Exits non-zero if any check fails.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

API_URL = os.getenv("API_URL", "http://localhost:8000").rstrip("/")
API_KEY = os.getenv("API_KEY", "")


def call(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    request = urllib.request.Request(
        API_URL + path,
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json", **({"X-API-Key": API_KEY} if API_KEY else {})},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b"{}")


def wait_for(path: str, done, timeout: float) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status, body = call("GET", path)
        if status == 200 and done(body):
            return body
        time.sleep(0.5)
    raise AssertionError(f"timed out after {timeout}s waiting on {path}; last: {body}")


def wait_for_job(task_id: str, status: str, timeout: float = 30) -> dict:
    return wait_for(f"/jobs/{task_id}", lambda job: job["status"] == status, timeout)


def check_ready():
    status, body = call("GET", "/health/ready")
    assert status == 200, body


def check_add():
    _, body = call("POST", "/tasks/add", {"x": 3, "y": 4})
    assert wait_for_job(body["task_id"], "SUCCESS")["result"] == 7.0


def check_long_queue():
    _, body = call("POST", "/tasks/long", {"duration_seconds": 2})
    assert wait_for_job(body["task_id"], "SUCCESS")["queue"] == "long"


def check_failure_and_retry():
    _, body = call("POST", "/tasks/flaky", {"fail_probability": 1.0})
    job = wait_for_job(body["task_id"], "FAILURE", timeout=180)  # waits out automatic retries
    assert job["error"] and job["traceback"]
    status, retried = call("POST", f"/jobs/{job['task_id']}/retry")
    assert status == 201, retried


def check_once_schedule():
    run_at = (datetime.now(timezone.utc) + timedelta(seconds=3)).isoformat()
    status, schedule = call(
        "POST", "/schedules", {"task_name": "app.tasks.add", "args": [1, 2], "schedule_type": "once", "run_at": run_at}
    )
    assert status == 201, schedule
    wait_for(f"/schedules/{schedule['id']}", lambda s: s["run_count"] == 1, timeout=60)
    _, jobs = call("GET", "/jobs?limit=200")
    fired = [j for j in jobs["jobs"] if j["schedule_id"] == schedule["id"]]
    assert len(fired) == 1, fired
    wait_for_job(fired[0]["task_id"], "SUCCESS")
    call("DELETE", f"/schedules/{schedule['id']}")


def check_error_envelope():
    status, body = call("GET", "/no-such-route")
    assert status == 404 and body["error"]["type"] == "not_found", body


CHECKS = [check_ready, check_add, check_long_queue, check_error_envelope, check_once_schedule, check_failure_and_retry]


def main() -> int:
    print(f"Smoke testing {API_URL}")
    failures = 0
    for check in CHECKS:
        started = time.monotonic()
        try:
            check()
            print(f"  PASS  {check.__name__} ({time.monotonic() - started:.1f}s)")
        except Exception as exc:
            failures += 1
            print(f"  FAIL  {check.__name__}: {exc}")
    print("OK" if not failures else f"{failures} check(s) failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
