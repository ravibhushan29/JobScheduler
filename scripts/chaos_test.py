"""
Chaos test for the docker compose stack: tries to make jobs run twice or get
lost while pods are killed. Needs Docker and a stack started with several
replicas and short timeouts, e.g.:

    export API_KEYS=k REDIS_PASSWORD=p POSTGRES_PASSWORD=p FLOWER_BASIC_AUTH=a:b \\
           SCHEDULE_POLL_INTERVAL_SECONDS=2 EXECUTION_LEASE_SECONDS=10 BROKER_VISIBILITY_TIMEOUT=30 \\
           SCHEDULER_REPLICAS=3 WORKER_REPLICAS=3
    docker compose -p jsha -f docker-compose.yml up -d --build
    python scripts/chaos_test.py --project jsha --api http://localhost:8000 --api http://localhost:8001 --key k

Scenarios (each asserts "every job ran exactly once" via the job's `attempts`):
  1. load          — 300 jobs spread over every API replica, submitted concurrently
  2. idempotency   — the same request with one Idempotency-Key, 20x concurrently on all replicas
  3. schedulers    — 5 fast interval schedules on 3 scheduler replicas; one replica is killed mid-test
  4. worker crash  — a worker is SIGKILLed mid-task; the job must finish on another worker
  5. cancel        — cancel through a different API replica than the one that submitted
  6. broker outage — Redis is stopped; jobs submitted meanwhile must run once it is back
  7. db outage     — Postgres is stopped while jobs are running and queued
"""

import argparse
import json
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

ARGS = argparse.Namespace()


def call(method: str, path: str, body=None, api: str | None = None, headers=None) -> tuple[int, dict]:
    request = urllib.request.Request(
        (api or ARGS.api[0]) + path,
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json", "X-API-Key": ARGS.key, **(headers or {})},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b"{}")


def job(task_id: str) -> dict:
    return call("GET", f"/jobs/{task_id}")[1]


def wait_jobs(task_ids, statuses=("SUCCESS",), timeout=180) -> list[dict]:
    deadline = time.monotonic() + timeout
    pending = set(task_ids)
    rows = {}
    while pending and time.monotonic() < deadline:
        for task_id in list(pending):
            row = job(task_id)
            if row.get("status") in statuses:
                rows[task_id] = row
                pending.discard(task_id)
        time.sleep(0.5)
    assert not pending, f"{len(pending)} job(s) never reached {statuses}: {list(pending)[:3]}"
    return [rows[t] for t in task_ids]


def docker(*args: str) -> str:
    return subprocess.run(["docker", *args], check=True, capture_output=True, text=True).stdout.strip()


def container_for_hostname(hostname: str) -> str:
    short = hostname.split("@")[-1]
    for name in docker("ps", "--format", "{{.Names}}", "--filter", f"name={ARGS.project}-").splitlines():
        if docker("inspect", "-f", "{{.Config.Hostname}}", name) == short:
            return name
    raise AssertionError(f"no container with hostname {short}")


# ---------------------------------------------------------------------------
def scenario_load():
    def submit(i):
        api = ARGS.api[i % len(ARGS.api)]
        status, body = call("POST", "/tasks/add", {"x": i, "y": 1}, api=api)
        assert status == 201, body
        return body["task_id"]

    with ThreadPoolExecutor(max_workers=30) as pool:
        ids = list(pool.map(submit, range(300)))
    rows = wait_jobs(ids)
    attempts = Counter(r["attempts"] for r in rows)
    assert attempts == {1: 300}, attempts
    assert sorted(r["result"] for r in rows) == sorted(float(i + 1) for i in range(300))
    return f"300 jobs on {len(ARGS.api)} API replicas, every one executed exactly once"


def scenario_idempotency():
    headers = {"Idempotency-Key": f"chaos-{time.time()}"}

    def submit(i):
        return call("POST", "/tasks/add", {"x": 5, "y": 5}, api=ARGS.api[i % len(ARGS.api)], headers=headers)

    with ThreadPoolExecutor(max_workers=20) as pool:
        responses = list(pool.map(submit, range(20)))
    ids = {body["task_id"] for _, body in responses}
    codes = Counter(status for status, _ in responses)
    assert len(ids) == 1, ids
    assert codes[201] == 1 and codes[200] == 19, codes
    (row,) = wait_jobs(list(ids))
    assert row["attempts"] == 1
    return "20 concurrent identical requests -> 1 job (1x201, 19x200 replayed), run once"


def scenario_schedulers():
    schedule_ids = []
    for i in range(5):
        status, body = call(
            "POST",
            "/schedules",
            {"task_name": "app.tasks.add", "args": [i, 0], "schedule_type": "interval", "interval_seconds": 2},
        )
        assert status == 201, body
        schedule_ids.append(body["id"])

    time.sleep(12)
    victim = f"{ARGS.project}-scheduler-1"
    docker("kill", victim)
    killed_at = {sid: call("GET", f"/schedules/{sid}")[1]["run_count"] for sid in schedule_ids}
    time.sleep(16)
    for sid in schedule_ids:
        call("PATCH", f"/schedules/{sid}", {"enabled": False})
    time.sleep(3)
    docker("start", victim)

    _, listing = call("GET", "/jobs?limit=200")
    total_runs = 0
    for sid in schedule_ids:
        schedule = call("GET", f"/schedules/{sid}")[1]
        jobs = [j for j in listing["jobs"] if j["schedule_id"] == sid]
        assert len(jobs) == schedule["run_count"], (sid, len(jobs), schedule["run_count"])
        assert schedule["run_count"] > killed_at[sid], f"schedule {sid} stopped firing after a replica was killed"
        fired = sorted(datetime.fromisoformat(j["submitted_at"].replace("Z", "+00:00")) for j in jobs)
        gaps = [(b - a).total_seconds() for a, b in zip(fired, fired[1:], strict=False)]
        assert min(gaps) >= 1.9, f"schedule {sid} fired twice for one due time (gap {min(gaps):.2f}s)"
        rows = wait_jobs([j["task_id"] for j in jobs])
        assert all(r["attempts"] == 1 for r in rows)
        total_runs += len(jobs)
    return f"3 replicas (1 killed mid-test): {total_runs} runs, one job per due time, none missed after the kill"


def scenario_worker_crash():
    status, body = call("POST", "/tasks/long", {"duration_seconds": 25})
    assert status == 201, body
    task_id = body["task_id"]
    (running,) = wait_jobs([task_id], statuses=("STARTED",), timeout=30)
    time.sleep(2)
    victim = container_for_hostname(running["worker"])
    docker("kill", victim)  # SIGKILL: no graceful shutdown, message never acked

    (done,) = wait_jobs([task_id], timeout=240)
    docker("start", victim)
    assert done["attempts"] == 2, done
    assert done["worker"] != running["worker"], "expected another worker to take over"
    return f"{victim} SIGKILLed mid-task; job taken over and finished on another worker (attempts=2)"


def scenario_cancel():
    status, body = call("POST", "/tasks/long", {"duration_seconds": 30}, api=ARGS.api[0])
    assert status == 201, body
    task_id = body["task_id"]
    wait_jobs([task_id], statuses=("STARTED",), timeout=30)
    status, body = call("DELETE", f"/jobs/{task_id}", api=ARGS.api[-1])
    assert status == 200, body
    time.sleep(5)
    row = job(task_id)
    assert row["status"] == "REVOKED" and row["attempts"] == 1, row
    return "cancelled through another API replica; job stopped and never re-ran"


def scenario_broker_outage():
    redis = f"{ARGS.project}-redis-1"
    docker("stop", redis)
    try:
        ids = []
        for i in range(5):
            status, body = call("POST", "/tasks/add", {"x": i, "y": 100})
            assert status == 201, (status, body)  # accepted and stored, not lost
            ids.append(body["task_id"])
        assert all(job(t)["published_at"] is None for t in ids)
    finally:
        docker("start", redis)
    rows = wait_jobs(ids, timeout=180)
    assert all(r["attempts"] == 1 for r in rows)
    return "Redis down: 5 jobs accepted; published by the outbox when Redis returned; each ran once"


def scenario_database_outage():
    ids = []
    for _ in range(12):
        status, body = call("POST", "/tasks/long", {"duration_seconds": 2})
        assert status == 201, body
        ids.append(body["task_id"])
    time.sleep(1)
    postgres = f"{ARGS.project}-postgres-1"
    docker("stop", postgres)  # with jobs running AND queued
    time.sleep(15)
    docker("start", postgres)
    rows = wait_jobs(ids, timeout=240)
    attempts = Counter(r["attempts"] for r in rows)
    assert attempts == {1: 12}, attempts
    return "Postgres down 15 s with 12 jobs in flight/queued: all finished, none re-run, none lost"


SCENARIOS = [
    scenario_load,
    scenario_idempotency,
    scenario_schedulers,
    scenario_cancel,
    scenario_worker_crash,
    scenario_broker_outage,
    scenario_database_outage,
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--project", required=True, help="docker compose project name (-p)")
    parser.add_argument("--api", action="append", required=True, help="API base URL; repeat for each replica")
    parser.add_argument("--key", required=True, help="an API key from API_KEYS")
    parser.add_argument("--only", help="run one scenario, e.g. worker_crash")
    parser.parse_args(namespace=ARGS)

    failures = 0
    for scenario in SCENARIOS:
        name = scenario.__name__.removeprefix("scenario_")
        if ARGS.only and ARGS.only != name:
            continue
        started = time.monotonic()
        try:
            detail = scenario()
            print(f"  PASS  {name:<15} ({time.monotonic() - started:5.1f}s)  {detail}", flush=True)
        except Exception as exc:
            failures += 1
            print(f"  FAIL  {name:<15} ({time.monotonic() - started:5.1f}s)  {exc!r}", flush=True)
    print("ALL PASSED" if not failures else f"{failures} scenario(s) FAILED")
    return 1 if failures else 0


if __name__ == "__main__":
    threading.excepthook = lambda args: None
    sys.exit(main())
