# Job Scheduler & Queue

A highly available background-job service: **FastAPI** to submit and inspect
jobs, **Celery** workers to run them, an **active-active scheduler** for
one-off, interval and cron schedules, **Redis** as the queue, **Postgres** as
the source of truth, and **Flower** for live monitoring.

Run any number of replicas of every component — API, workers, scheduler —
and **each job, and each scheduled run, executes once.** Losing a pod loses
no work.

## Contents

- [Guarantees](#guarantees) · [How it works](#how-it-works) · [Quick start](#quick-start)
- [Deploying to Kubernetes](#deploying-to-kubernetes) · [High availability](#high-availability)
- [API overview](#api-overview) · [Configuration](#configuration) · [Testing](#testing)
- [Operations](#operations) · [Extending](#extending)
- Manual walkthrough of every endpoint: [USER_TESTING.md](USER_TESTING.md) · Changes: [CHANGELOG.md](CHANGELOG.md)

---

## Guarantees

| Situation | What happens |
|---|---|
| 3 scheduler pods poll the same due schedule at the same moment | Exactly one fires it |
| A scheduler pod dies | The others keep firing on their next poll (≤ 5 s); no failover, no leader election |
| The broker delivers a job's message twice (re-delivery, double publish) | The job runs once; the extra copy is dropped |
| A copy arrives while the job is still running elsewhere | It waits, then is dropped when the job finishes |
| A client retries `POST /tasks/*` or `POST /schedules` (same `Idempotency-Key`) | The original job/schedule is returned; nothing new is created |
| A job is cancelled | No worker will ever start it, even if it never got the revoke broadcast |
| The API or scheduler crashes after recording a job but before publishing it | The outbox relay publishes it |
| Redis is down when a job is submitted | The job is accepted and stored, then published when Redis is back |
| A worker is killed mid-task (OOM, node failure, `SIGKILL`) | Its lease lapses and the job is re-run on another worker (`attempts: 2`) |
| A task keeps crashing its worker | Re-run at most `MAX_WORKER_LOSS_RECOVERIES` times, then FAILURE |
| Postgres goes down with jobs running and queued | Workers hand messages back instead of running them blind; results are written once the DB returns. No job is lost or re-run |

**The one honest caveat.** No distributed queue can deliver *exactly once*:
if a worker dies *in the middle of* a task, the only choices are to re-run it
or to lose it. This system re-runs it. For tasks with external side effects
(emails, payments), pass the job's `task_id` to that system as an idempotency
key — see `send_notification` in [app/tasks.py](app/tasks.py) — so the
re-run is harmless too. Everything else in the table is enforced by the
database and holds with no caveat.

Each guarantee is tested: [tests/test_no_double_execution.py](tests/test_no_double_execution.py)
covers them one by one, and [scripts/chaos_test.py](scripts/chaos_test.py) checks them against a
multi-replica Docker stack while it kills pods.

---

## How it works

```
 clients ──HTTP──▶ API ×N ──┐                           ┌──▶ worker ×N (default queue)
   Idempotency-Key          │ 1. record job (outbox)    │
                            ▼                           │       execution guard:
                      ┌───────────┐  2. publish   ┌─────┴─┐     claim job in DB → run
 scheduler ×N ───────▶│ Postgres  │──────────────▶│ Redis │     (lease, fencing token)
  claim due run,      │ jobs      │               └─────┬─┘
  relay outbox,       │ schedules │◀── status ──────────┤
  recover lost jobs   └───────────┘                     └──▶ worker ×N (long queue)
```

Four mechanisms, all built on atomic updates in the shared database:

1. **Active-active scheduler** ([app/scheduler.py](app/scheduler.py)). It replaces Celery Beat, which must run as a single process. Every replica polls for due schedules. Firing one is an optimistic-lock `UPDATE … WHERE version = :read_version` that writes the job row **in the same transaction**, so exactly one replica wins each due time. Periodic system jobs (heartbeat, daily cleanup) are ordinary schedule rows seeded from code.
2. **Transactional outbox** ([app/publisher.py](app/publisher.py)). A job row is written *before* the message is sent, and `published_at` is set only after the broker accepts it. The scheduler re-publishes anything left unpublished, so a crash or a Redis outage between "recorded" and "sent" can't lose a job.
3. **Execution guard** ([app/guard.py](app/guard.py)), the base class of every task. Before running, a worker must win a compare-and-set that moves the job to `STARTED` with a 60-second lease, renewed every 20 seconds while it runs.
   - Finished or cancelled jobs can't be claimed.
   - A copy that arrives while the lease is live is deferred.
   - Every later status write is fenced by the claim's `attempts` number, so a worker that lost its lease can't overwrite a newer attempt.
4. **Lease-based crash recovery.** If a lease lapses (the worker died), the scheduler re-publishes the job and a healthy worker takes it over. This doesn't wait for the broker's own re-delivery, whose timing is unreliable.

On top of that, **`Idempotency-Key`** headers deduplicate client retries through unique indexes.

### Code layout

```
app/
  config.py           every setting, from env vars
  db.py               SQLAlchemy Core schema + queries (Postgres or SQLite), migrations,
                      the claim/lease/fencing primitives, sync bridge for Celery
  guard.py            GuardedTask: the execution guard every task inherits
  celery_app.py       Celery config + lifecycle signals (attempt-fenced status updates)
  tasks.py            tasks, public task registry, SYSTEM_SCHEDULES
  scheduler.py        the HA scheduler service (python -m app.scheduler)
  publisher.py        publish a recorded job; mark it published
  scheduling.py       once / interval / cron timing rules
  result_backend.py   Celery's DB result backend, retrying through DB outages
  migrate.py          python -m app.migrate (lock-protected, idempotent)
  logging_config.py   stdout + optional file; text or JSON
  main.py             FastAPI app
  api/                errors, auth, schemas, enqueue (idempotency), routes/
deploy/k8s/           Kubernetes manifests (kustomize)
tests/                pytest: unit, end-to-end with a real worker, double-execution attempts
scripts/              smoke_test.py (any deployment), chaos_test.py (compose, kills pods)
```

### Sample tasks

| Task | Queue | Shows |
|---|---|---|
| `add(x, y)` | default | The simplest end-to-end job |
| `long_running_task(duration_seconds)` | long | Slow work on its own queue, with PROGRESS updates |
| `flaky_task(fail_probability)` | default | Automatic retry with exponential backoff and jitter |
| `send_notification(email, message)` | default | Manual `self.retry()` + an idempotency key for side effects |
| `heartbeat()` | default | System schedule, every 30 s |
| `scheduled_cleanup()` | default | System schedule, 03:00 UTC: prunes old jobs and Celery results |

---

## Quick start

### Docker: the full multi-replica stack

```bash
cp .env.example .env    # set API_KEYS, REDIS_PASSWORD, POSTGRES_PASSWORD, FLOWER_BASIC_AUTH
docker compose -f docker-compose.yml up -d --build
```

This starts Postgres and Redis, then runs `migrate` once. After that come 2 API, 2 worker, 2 long-worker and 2 scheduler replicas, plus Flower. Scale any app service freely:

```bash
docker compose -f docker-compose.yml up -d --scale worker=4 --scale scheduler=3
```

For development, `docker compose up --build` runs one replica of each with hot reload and text logs.

| URL | What |
|---|---|
| http://localhost:8000/docs (and :8001) | Swagger UI per API replica (click **Authorize**, paste a key) |
| http://localhost:5555 | Flower (login: `FLOWER_BASIC_AUTH`) |

```bash
curl -X POST localhost:8000/tasks/add -H "X-API-Key: <key>" -H "Idempotency-Key: demo-1" \
     -H "Content-Type: application/json" -d '{"x": 3, "y": 4}'
curl "localhost:8000/jobs?limit=5" -H "X-API-Key: <key>"
```

### Local, without Docker

Needs Python 3.11+ and Redis. Without `DATABASE_URL` it uses a local SQLite
file, which is fine for one machine.

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements-dev.txt
export API_KEYS=dev-key
uvicorn app.main:app --reload --port 8000                       # API
celery -A app.celery_app worker -Q default --loglevel=info      # quick tasks
celery -A app.celery_app worker -Q long -c 2 --loglevel=info    # slow tasks
python -m app.scheduler                                         # scheduler (run as many as you like)
```

---

## Deploying to Kubernetes

[deploy/k8s/](deploy/k8s/) is a kustomize bundle: Deployments for `api` (3), `worker` (3), `worker-long` (2), `scheduler` (2) and `flower` (1), plus Services, PodDisruptionBudgets, HPAs, NetworkPolicies, a ConfigMap and a non-token ServiceAccount.

```bash
docker build -t <registry>/job-scheduler:4.0.0 . && docker push <registry>/job-scheduler:4.0.0
# set images[0].newName/newTag in deploy/k8s/kustomization.yaml
kubectl create namespace job-scheduler
kubectl -n job-scheduler create secret generic job-scheduler-secrets \
  --from-literal=DATABASE_URL='postgresql://jobs:…@db:5432/jobs?sslmode=require' \
  --from-literal=CELERY_BROKER_URL='rediss://:…@redis:6379/0' \
  --from-literal=API_KEYS='…' --from-literal=FLOWER_BASIC_AUTH='admin:…'
kubectl apply -k deploy/k8s
```

What the manifests do for availability:

| Concern | How |
|---|---|
| Schema changes | `migrate` init container in every pod; idempotent and serialized by a Postgres advisory lock |
| Spread | `topologySpreadConstraints` across nodes **and** zones |
| Rollouts | API and scheduler: `maxUnavailable: 0`. Workers: warm shutdown within `terminationGracePeriodSeconds` (330 s), longer than the task soft limit |
| Node drains | PDBs: API `minAvailable: 2`, scheduler `minAvailable: 1`, workers `maxUnavailable: 1` |
| Probes | API: liveness `/health`, readiness `/health/ready` (DB + broker). Scheduler: heartbeat-file check. Workers: `celery inspect ping` |
| Scaling | HPAs on CPU. For queue-length autoscaling, swap the worker HPAs for KEDA's `redis` trigger |
| Hardening | Pod Security `restricted`, non-root, read-only root FS, all capabilities dropped, no SA token, default-deny ingress |

All 19 rendered resources pass strict schema validation (`kubeconform`, Kubernetes 1.30). `preStop.sleep` needs Kubernetes ≥ 1.29.

---

## High availability

The app tier has no single point of failure: every component runs as several
interchangeable replicas. The remaining SPOFs are the two stateful services,
and they must be HA themselves:

| Dependency | Production choice | Notes |
|---|---|---|
| **Postgres** | Managed with a synchronous standby and automatic failover (RDS/Aurora Multi-AZ, Cloud SQL HA, Azure Flexible HA), or Patroni / CloudNativePG | It is the source of truth and enforces every "once" guarantee. Use `sslmode=require`. Put PgBouncer in front if you run many pods (each process opens up to `DB_POOL_SIZE + DB_MAX_OVERFLOW` connections) |
| **Redis** | Managed with a replica and automatic failover (ElastiCache Multi-AZ, Memorystore Standard), or Sentinel (`CELERY_BROKER_URL=sentinel://…`, `REDIS_SENTINEL_MASTER`) | Enable AOF persistence. A failover can lose the last messages: the outbox and lease recovery re-publish them, and the guard dedupes |

Failure behaviour (the chaos test stops Redis and Postgres for real; the API
responses are covered by unit tests):
- **Postgres down:**
  - The API returns `503 service_unavailable`, and readiness fails so the load balancer stops routing to it.
  - Workers hand each new message back to the broker rather than running it without a claim.
  - Running tasks keep retrying their final writes, both the job row and Celery's result (via [app/result_backend.py](app/result_backend.py)), for up to `FINAL_STATE_WRITE_TIMEOUT_SECONDS`.
  - The scheduler logs the error and retries on its next poll.
  - Keep `EXECUTION_LEASE_SECONDS × 2` longer than the database failovers you expect. A task still running when its lease lapses could otherwise be resumed elsewhere.
- **Redis down:** submissions are still accepted (status 201, stored in the outbox) and published when Redis returns.

---

## API overview

Every route except `/health*` needs `X-API-Key`. Full schemas and examples are in `/docs`.

| Method & path | Purpose |
|---|---|
| `GET /health` · `GET /health/ready` | Liveness · readiness (DB + broker, 503 otherwise) |
| `POST /tasks/add` · `/long` · `/flaky` · `/notify` | Queue one task; `Idempotency-Key` supported |
| `POST /tasks/test-batch` | Queue one of each, including a guaranteed failure |
| `GET /jobs?status=&limit=&offset=` | Job history, newest first |
| `GET /jobs/{id}` | One job: args, result, error, traceback, `retries`, `attempts`, worker, `retry_of`, `schedule_id`, `published_at` |
| `GET /jobs/{id}/status` | Live Celery state (incl. PROGRESS); falls back to the job history after expiry |
| `POST /jobs/{id}/retry[?force=true]` | Re-submit as a new job linked by `retry_of` (409 unless it failed); `Idempotency-Key` supported |
| `DELETE /jobs/{id}` | Cancel cluster-wide (409 if already finished) |
| `POST /schedules` | Create a `once` / `interval` / `cron` schedule; `Idempotency-Key` supported |
| `GET /schedules`, `GET /schedules/{id}` | List / get (system schedules have `system: true`) |
| `PATCH /schedules/{id}` · `DELETE /schedules/{id}` | Pause, resume, retime · delete (409 for system schedules) |

**Idempotency:** repeating a POST with the same `Idempotency-Key` returns the original with **200** and `Idempotent-Replayed: true`. Reusing a key for a different request is **409**.

**Errors** always look like `{"error": {"status_code", "type", "message", "details"}}`. `type` is one of `bad_request`, `unauthorized`, `not_found`, `method_not_allowed`, `conflict`, `validation_error`, `internal_error`, `service_unavailable`.

**Schedules:** `once` needs a future `run_at` with a timezone. `interval` fires on the next poll, then every N s. `cron` is 5-field and evaluated in **UTC**. Arguments are checked against the task signature when the schedule is created, and missed runs are not replayed.

**Timestamps** are UTC ISO-8601 (`2026-01-01T08:00:00Z`).

---

## Configuration

Everything is environment variables. [`.env.example`](.env.example) lists them all, and [`app/config.py`](app/config.py) explains each. The important ones:

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | `sqlite:///data/scheduler.db` | `postgresql://…` for anything multi-pod |
| `CELERY_BROKER_URL` | from `REDIS_*` | `redis://`, `rediss://` or `sentinel://` |
| `API_KEYS` | *(empty: no auth)* | Comma-separated; several allow key rotation |
| `AUTO_MIGRATE` | `true` | `false` when `python -m app.migrate` runs separately (compose, k8s) |
| `EXECUTION_LEASE_SECONDS` | `60` | Lease on a running job; a dead worker's job is resumed after about 2× this |
| `MAX_WORKER_LOSS_RECOVERIES` | `3` | Re-runs after worker crashes before a job is marked FAILURE |
| `FINAL_STATE_WRITE_TIMEOUT_SECONDS` | `300` | How long a finished task keeps retrying its final write while the DB is down |
| `SCHEDULE_POLL_INTERVAL_SECONDS` | `5` | Scheduler resolution |
| `OUTBOX_REPUBLISH_AFTER_SECONDS` | `30` | When an unpublished job is re-published |
| `TASK_SOFT_TIME_LIMIT` / `TASK_TIME_LIMIT` | `300` / `360` | Per-task runtime limits |
| `JOB_RETENTION_DAYS` | `30` | History kept for finished jobs |

---

## Testing

```bash
pip install -r requirements-dev.txt
pytest                                                           # 83 tests on SQLite, ~50 s, no services needed
TEST_DATABASE_URL=postgresql://user:pw@localhost:5432/test pytest  # the same suite on Postgres
ruff check . && ruff format --check .
```

| File | Covers |
|---|---|
| `tests/test_no_double_execution.py` | Duplicate deliveries, duplicates of running jobs, cancelled jobs, DB outage at claim time, 10 concurrent claims, lease takeover + fencing, Celery retries, 10 racing scheduler replicas, stale reads, outbox relay races, crash recovery (re-publish, retry cap, Celery-recorded success/failure), idempotency keys |
| `tests/test_integration.py` | A real Celery worker thread: every sample task end to end, retries, PROGRESS, queues, schedules |
| `tests/test_api.py` | Validation, error envelope, auth, readiness, 503 on DB outage, outbox on broker outage, retry/cancel rules, schedules incl. system ones |
| `tests/test_db_and_scheduling.py` | Claims, versioning, retention, v3 → v4 SQLite migration, timing rules, scheduler resilience |

Against running deployments:

```bash
API_URL=https://jobs.example.com API_KEY=… python scripts/smoke_test.py         # any deployment
python scripts/chaos_test.py --project <compose-project> --api http://localhost:8000 \
       --api http://localhost:8001 --key <key>                                 # compose stack; kills pods
```

The chaos test's seven scenarios:
1. 300 concurrent jobs across 2 API replicas.
2. 20 concurrent requests with the same `Idempotency-Key`.
3. 5 fast schedules on 3 scheduler replicas, with one replica killed mid-test.
4. Cancelling through a different API replica.
5. `SIGKILL` of a worker mid-task.
6. Redis stopped.
7. Postgres stopped with jobs in flight.

Afterwards it checks every job in the database for extra runs, gaps, stuck jobs and unpublished jobs.

---

## Operations

- **Health at a glance:**
  - `GET /jobs?status=STARTED` shows what's running.
  - A job with `attempts > retries + 1` was re-run after a worker loss.
  - `published_at: null` on an old job means the broker was unreachable.
- **Log lines to alert on:** `[FAILURE]`, `[RECOVER]` (a worker was lost), `[ABANDONED]`, `[LEASE LOST]` (a worker was cut off from the DB longer than its lease), `[OUTBOX]` (publish was delayed).
- **Duplicates being suppressed** show up as `[DUPLICATE DROPPED]` and `[DUPLICATE DEFERRED]`. That's the guard working, not an error.
- **Upgrades:** roll out normally. The `migrate` init container applies additive schema changes before new pods start, so old and new pods can run side by side.
- **Backups:** back up Postgres (point-in-time recovery). Redis holds only in-flight messages, and anything lost there is recovered from the database.

---

## Extending

- **New task:** define it in `app/tasks.py` with an explicit `name=` and `queue=`. It inherits the execution guard automatically. Add it to `_PUBLIC_TASKS` if the API may submit or schedule it. If it has external side effects, pass `self.request.id` to that system as an idempotency key.
- **New periodic job:** add an entry to `SYSTEM_SCHEDULES` in `app/tasks.py`. Every scheduler replica seeds it on startup, and it fires once per due time.
- **New runtime schedule:** `POST /schedules`; no code change needed.
- **New column:** add it to the table in `app/db.py`. Migrations add missing columns and indexes on the next start.
