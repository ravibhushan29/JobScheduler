# Manual API Testing Guide

A walkthrough that exercises **every endpoint by hand**, in an order where each
step sets up the data the next one needs. Run each step in **Swagger UI**
(browser only) or with the **curl** command underneath — they send the same
request. Responses shown are examples: ids and timestamps will differ.

> Want an automated version? `python scripts/smoke_test.py` runs the core of
> this guide against a running stack in about 30 seconds.

---

## 0. Before you start

Start the stack (see the README for details). Either:

```bash
cp .env.example .env          # then set the four secrets
docker compose up --build     # one replica of each; -f docker-compose.yml for the multi-replica stack
```

or locally, one command per terminal (Redis must be running: `redis-cli ping` → `PONG`):

```bash
export API_KEYS=dev-key
uvicorn app.main:app --reload --port 8000
celery -A app.celery_app worker -Q default --loglevel=info
celery -A app.celery_app worker -Q long -c 2 --loglevel=info
python -m app.scheduler       # fires schedules; run as many copies as you like
```

Set these two shell variables; every curl below uses them:

```bash
API=http://localhost:8000
KEY=dev-key                   # one of the values in API_KEYS
```

> The API only *queues* tasks. If a job stays `QUEUED`, no worker is
> consuming its queue. If a schedule never fires, no scheduler is running.

---

## 1. Open Swagger UI and authorize

Go to **http://localhost:8000/docs**. Click **Authorize** (top right), paste
your key into `X-API-Key`, and click **Authorize**. Swagger now sends it on
every request.

For each endpoint: expand it → **Try it out** → edit or keep the example →
**Execute** → read **Server response**.

---

## 2. Health — no key needed

```bash
curl $API/health
curl $API/health/ready
```

**Expected — `200`:**
```json
{ "status": "ok", "service": "job-scheduler", "version": "4.0.0" }
{ "status": "ready", "checks": { "database": "ok", "broker": "ok" } }
```

`/health` only means the process is up. `/health/ready` also checks the
database and Redis; stop Redis and it returns **`503 service_unavailable`**
with the failing check in `error.details`.

---

## 3. Auth

```bash
curl -i $API/jobs                          # no key
curl -i $API/jobs -H "X-API-Key: wrong"    # bad key
```

**Expected — `401`:**
```json
{ "error": { "status_code": 401, "type": "unauthorized",
  "message": "Missing or invalid API key. Send it in the X-API-Key header.", "details": null } }
```

---

## 4. Submit tasks (`Tasks`)

Each call returns a `task_id` **immediately**; the task runs on a worker. Keep the ids.

```bash
# 4a. simplest task
curl -X POST $API/tasks/add    -H "X-API-Key: $KEY" -H "Content-Type: application/json" -d '{"x": 3, "y": 4}'
# 4b. slow job on the `long` queue (1-120 s)
curl -X POST $API/tasks/long   -H "X-API-Key: $KEY" -H "Content-Type: application/json" -d '{"duration_seconds": 5}'
# 4c. fails randomly, retried automatically with backoff
curl -X POST $API/tasks/flaky  -H "X-API-Key: $KEY" -H "Content-Type: application/json" -d '{"fail_probability": 0.7}'
# 4d. simulated provider call, retried manually via self.retry()
curl -X POST $API/tasks/notify -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"email": "someone@example.com", "message": "Your report is ready."}'
```

**Expected — `201`:** `{ "task_id": "d282f4cf-…", "status": "queued" }`

**4e. One of everything:**
```bash
curl -X POST $API/tasks/test-batch -H "X-API-Key: $KEY"
```
```json
{
  "add": "3a1f…",
  "long_running_task": "9c0b…",
  "flaky_task_will_fail": "51fccdae-…",
  "flaky_task_will_succeed": "7b22…",
  "send_notification": "e4d5…"
}
```
👉 Save `flaky_task_will_fail`. It reaches `FAILURE` after its automatic
retries (about a minute with default settings); §5d and §5e use it.

---

## 5. Inspect jobs (`Jobs`)

### 5a. `GET /jobs` — newest first, optional `status` filter

```bash
curl "$API/jobs?limit=5"              -H "X-API-Key: $KEY"
curl "$API/jobs?status=FAILURE"       -H "X-API-Key: $KEY"
```
```json
{
  "total": 12, "limit": 5, "offset": 0,
  "jobs": [{
    "task_id": "d282f4cf-…",
    "task_name": "app.tasks.add",
    "status": "SUCCESS",
    "args": [3.0, 4.0],
    "kwargs": {},
    "queue": "default",
    "result": 7.0,
    "error": null,
    "traceback": null,
    "retries": 0,
    "attempts": 1,
    "worker": "celery@your-host",
    "retry_of": null,
    "schedule_id": null,
    "idempotency_key": null,
    "submitted_at": "2026-07-25T12:27:52.100000Z",
    "published_at": "2026-07-25T12:27:52.110000Z",
    "started_at":   "2026-07-25T12:27:52.400000Z",
    "finished_at":  "2026-07-25T12:27:52.680000Z"
  }]
}
```

Every job appears here, whether it came from the API, a schedule
(`schedule_id` links it), or `.delay()` in code. `attempts` is how many times
a worker started it: `retries + 1` normally, and one more only if a worker
died mid-run and another finished it.

### 5b. `GET /jobs/{task_id}` — one job's history record

```bash
curl $API/jobs/<add-task-id> -H "X-API-Key: $KEY"
```
Same shape as one item above. Kept for `JOB_RETENTION_DAYS` (default 30).

### 5c. `GET /jobs/{task_id}/status` — live state

```bash
curl $API/jobs/<add-task-id>/status -H "X-API-Key: $KEY"
```
```json
{
  "task_id": "d282f4cf-…",
  "state": "SUCCESS",
  "source": "result_backend",
  "result": 7.0,
  "error": null, "traceback": null,
  "name": "app.tasks.add", "args": [3.0, 4.0], "kwargs": {},
  "retries": 0, "worker": "celery@your-host", "queue": "default",
  "date_done": "2026-07-25T12:27:52.673172", "children": null
}
```
Call it on the **long** job while it runs to see `"state": "PROGRESS"` and
`"result": {"progress": 60}`. Once Celery's result expires
(`CELERY_RESULT_EXPIRES`, default 24 h) the answer comes from the job history
instead and `source` is `"job_record"`.

### 5d. A failed job — reason and traceback

```bash
curl $API/jobs/<flaky_task_will_fail> -H "X-API-Key: $KEY"
```
```json
{
  "status": "FAILURE",
  "error": "Simulated transient failure",
  "traceback": "Traceback (most recent call last):\n  …\nRuntimeError: Simulated transient failure\n",
  "retries": 3,
  "…": "…"
}
```
`/status` on the same id shows `"result": {"exc_type": "RuntimeError", "exc_message": "…"}`.

### 5e. `POST /jobs/{task_id}/retry`

```bash
curl -X POST $API/jobs/<flaky_task_will_fail>/retry -H "X-API-Key: $KEY"
```
**Expected — `201`** with a new `task_id`; `GET /jobs/<new-id>` shows `"retry_of": "<original id>"`.

Retrying a job that did **not** fail is refused:
```bash
curl -X POST $API/jobs/<add-task-id>/retry -H "X-API-Key: $KEY"              # 409 conflict
curl -X POST "$API/jobs/<add-task-id>/retry?force=true" -H "X-API-Key: $KEY" # 201
```

### 5f. `DELETE /jobs/{task_id}` — cancel

```bash
NEW=$(curl -s -X POST $API/tasks/long -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"duration_seconds": 30}' | python3 -c "import sys,json;print(json.load(sys.stdin)['task_id'])")
curl -X DELETE $API/jobs/$NEW -H "X-API-Key: $KEY"
```
**Expected — `200`:** `{ "task_id": "…", "status": "revoked" }`

Cancelling works cluster-wide: the job is marked REVOKED in the database, and
every worker refuses to start a REVOKED job — even one that never received the
revoke broadcast. With several API replicas, you can cancel through any of them.

Cancelling a job that already finished returns **`409 conflict`** and leaves
its status unchanged:
```bash
curl -X DELETE $API/jobs/<add-task-id> -H "X-API-Key: $KEY"
```

---

## 6. Dynamic schedules (`Schedules`)

Any public task (listed in the `task_name` description in Swagger) can run
once, on an interval, or on a cron expression. **Cron is evaluated in UTC.**

### 6a. `POST /schedules`

```bash
# every 2 hours (fires on the next poll, then every 7200 s)
curl -X POST $API/schedules -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"task_name": "app.tasks.heartbeat", "schedule_type": "interval", "interval_seconds": 7200}'

# weekdays at 08:00 UTC
curl -X POST $API/schedules -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"task_name": "app.tasks.scheduled_cleanup", "schedule_type": "cron", "cron_expr": "0 8 * * 1-5"}'

# once, 10 seconds from now — watch it fire in §6b
RUN_AT=$(python3 -c "from datetime import datetime,timedelta,timezone;print((datetime.now(timezone.utc)+timedelta(seconds=10)).isoformat())")
curl -X POST $API/schedules -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d "{\"task_name\": \"app.tasks.add\", \"args\": [10, 20], \"schedule_type\": \"once\", \"run_at\": \"$RUN_AT\"}"
```
**Expected — `201`:**
```json
{
  "id": 5, "task_name": "app.tasks.add", "args": [10, 20], "kwargs": {},
  "name": null, "system": false,
  "schedule_type": "once", "run_at": "2026-07-25T12:34:28Z",
  "interval_seconds": null, "cron_expr": null,
  "enabled": true, "next_run_at": "2026-07-25T12:34:28Z",
  "last_run_at": null, "run_count": 0, "created_at": "2026-07-25T12:34:18Z"
}
```

### 6b. Watch the `once` schedule fire

The scheduler polls every `SCHEDULE_POLL_INTERVAL_SECONDS` (default 5). Wait ~15 s:
```bash
curl $API/schedules/<once-id> -H "X-API-Key: $KEY"   # run_count: 1, enabled: false
curl "$API/jobs?limit=5" -H "X-API-Key: $KEY"        # one job with that schedule_id, result 30.0
```

### 6c. List and get

```bash
curl "$API/schedules?enabled_only=true" -H "X-API-Key: $KEY"
curl $API/schedules/<interval-id> -H "X-API-Key: $KEY"
```

The list also contains two **system schedules** (`"system": true`,
`system:heartbeat` every 30 s and `system:cleanup` daily at 03:00 UTC). They
are defined in code and seeded by the scheduler; `PATCH`/`DELETE` on them
returns `409 conflict`.

### 6d. `PATCH /schedules/{id}` — pause, resume, retime

```bash
curl -X PATCH $API/schedules/<interval-id> -H "X-API-Key: $KEY" -H "Content-Type: application/json" -d '{"enabled": false}'
curl -X PATCH $API/schedules/<interval-id> -H "X-API-Key: $KEY" -H "Content-Type: application/json" -d '{"interval_seconds": 120}'
```
**Expected — `200`** with `next_run_at` recalculated straight away (last run + 120 s).

Refused changes:

| Request | Result |
|---|---|
| `{"cron_expr": "* * * * *"}` on an interval schedule | `400 bad_request` |
| `{"cron_expr": "nope"}` or `{"interval_seconds": null}` | `422 validation_error` |
| `{"enabled": true}` on a `once` schedule that already fired | `409 conflict` |
| anything on a system schedule | `409 conflict` |

### 6e. `DELETE /schedules/{id}`

```bash
curl -X DELETE $API/schedules/<interval-id> -H "X-API-Key: $KEY"
```
**Expected — `200`:** `{ "schedule_id": …, "status": "deleted" }`. Jobs it already fired stay in `/jobs`.

---

## 7. Errors — always the same shape

```bash
curl -i $API/jobs/does-not-exist -H "X-API-Key: $KEY"   # 404 not_found
curl -i $API/no-such-route                              # 404 not_found (framework errors too)
curl -i -X PUT $API/jobs -H "X-API-Key: $KEY"           # 405 method_not_allowed
curl -i -X POST $API/tasks/add -H "X-API-Key: $KEY" -H "Content-Type: application/json" -d '{"x": "nope", "y": 4}'
```
```json
{ "error": { "status_code": 422, "type": "validation_error",
  "message": "Request validation failed; see 'details' for each field error.",
  "details": [ { "type": "float_parsing", "loc": ["body", "x"], "msg": "…" } ] } }
```

More `422`s for schedules: an unknown `task_name`, a `run_at` in the past or
without a timezone, a 6-field cron, a timing field that doesn't match
`schedule_type`, or `args` that don't fit the task (e.g. `add` with one argument).

| `type` | Status |
|---|---|
| `bad_request` | 400 |
| `unauthorized` | 401 |
| `not_found` | 404 |
| `method_not_allowed` | 405 |
| `conflict` | 409 |
| `validation_error` | 422 |
| `internal_error` | 500 |
| `service_unavailable` | 503 (broker or DB down) |

Branch on `type`, never on `message`.

---

## 8. Safe client retries — `Idempotency-Key`

Send the same key twice (as a client would after a timeout):
```bash
for i in 1 2; do
  curl -i -X POST $API/tasks/add -H "X-API-Key: $KEY" -H "Idempotency-Key: order-42" \
    -H "Content-Type: application/json" -d '{"x": 1, "y": 2}'
done
```
**Expected:** first `201`, second `200` with header `Idempotent-Replayed: true`,
**the same `task_id`**, and only one job in `/jobs`. The same key with a
different body returns `409 conflict`. `POST /schedules` and
`POST /jobs/{id}/retry` accept the header too.

---

## 9. (Optional) Look at the data directly

Results and job history live in the database, never only in Redis:
```bash
# compose stack (Postgres)
docker compose exec postgres psql -U jobs -d jobs -c "SELECT task_id, status, attempts FROM jobs ORDER BY submitted_at DESC LIMIT 5;"
# local run (SQLite)
sqlite3 data/scheduler.db "SELECT task_id, status, attempts FROM jobs ORDER BY submitted_at DESC LIMIT 5;"
```

---

## Test order at a glance

| # | Request | Expect |
|---|---|---|
| 1 | `GET /health`, `GET /health/ready` | 200 |
| 2 | `GET /jobs` without a key | 401 |
| 3 | `POST /tasks/add` `/long` `/flaky` `/notify` | 201 |
| 4 | `POST /tasks/test-batch` | 201 (save the fail id) |
| 5 | `GET /jobs`, `GET /jobs/{id}` | 200 |
| 6 | `GET /jobs/{id}/status` (on the long job while it runs) | 200, PROGRESS |
| 7 | `GET /jobs/{fail-id}` | FAILURE with error + traceback |
| 8 | `POST /jobs/{fail-id}/retry` | 201, `retry_of` set |
| 9 | `POST /jobs/{ok-id}/retry` | 409 (201 with `?force=true`) |
| 10 | `DELETE /jobs/{running-id}` / `{finished-id}` | 200 / 409 |
| 11 | `POST /schedules` × 3 | 201 |
| 12 | wait, `GET /schedules/{once-id}` | run_count 1, disabled |
| 13 | `PATCH /schedules/{id}` | 200 / 400 / 409 / 422 |
| 14 | `DELETE /schedules/{id}` | 200 |
| 15 | error cases (§7) | uniform envelope |
| 16 | same `Idempotency-Key` twice (§8) | 201 then 200, one job |
