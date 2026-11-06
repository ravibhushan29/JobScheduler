# 11. Developer guide

> **Who this is for:** developers changing or extending the code.
> **Before reading:** [3. Architecture](03-architecture.md) and [7. Reliability](07-reliability.md).

---

## 11.1 Repository map

```
JobScheduler/
├── app/                        ← all application code
│   ├── config.py               every setting (env vars + defaults + explanations)
│   ├── db.py                   database schema, queries, claims/leases, migrations
│   ├── guard.py                GuardedTask — the execution guard every task inherits
│   ├── celery_app.py           Celery configuration + lifecycle signal handlers
│   ├── result_backend.py       Celery result storage that survives DB outages
│   ├── tasks.py                the tasks, the public task registry, SYSTEM_SCHEDULES
│   ├── scheduler.py            the scheduler service (python -m app.scheduler)
│   ├── publisher.py            send a saved job to the queue
│   ├── scheduling.py           once / interval / cron timing maths
│   ├── migrate.py              python -m app.migrate
│   ├── logging_config.py       text/JSON logging to stdout (+ optional files)
│   ├── main.py                 builds the FastAPI app
│   └── api/
│       ├── errors.py           the error envelope + exception handlers
│       ├── security.py         X-API-Key check
│       ├── schemas.py          request/response models (validation lives here)
│       ├── enqueue.py          submit_task(): save → publish, idempotency
│       └── routes/             health.py, submit.py (/tasks), jobs.py, schedules.py
├── tests/                      pytest suite (runs on SQLite and Postgres)
├── scripts/                    smoke_test.py, chaos_test.py
├── deploy/k8s/                 Kubernetes manifests
├── docs/                       this documentation
├── Dockerfile, docker-compose.yml, docker-compose.override.yml
├── requirements.txt, requirements-dev.txt, pyproject.toml
└── README.md, USER_TESTING.md, CHANGELOG.md, .env.example
```

## 11.2 How the modules depend on each other

```mermaid
flowchart TD
    config[config.py] --> db[db.py]
    config --> celery[celery_app.py]
    db --> celery
    guard[guard.py] --> db
    celery -. "task_cls = GuardedTask" .-> guard
    celery --> tasks[tasks.py]
    tasks --> db
    publisher[publisher.py] --> celery
    publisher --> db
    scheduling[scheduling.py]
    scheduler[scheduler.py] --> tasks & publisher & scheduling & db
    migrate[migrate.py] --> db & celery
    resultb[result_backend.py] -. used by .-> celery
    subgraph API
        main[main.py] --> routes[api/routes/*]
        routes --> enqueue[api/enqueue.py] & schemas[api/schemas.py] & errors[api/errors.py] & security[api/security.py]
        enqueue --> publisher
        schemas --> tasks & scheduling
    end
    routes --> db
```

Rule of thumb: **`db.py` is the only module that talks to the database**, and
**`publisher.py` is the only place that puts jobs on the queue**. Keep it that way.

## 11.3 Following a request through the code

```mermaid
sequenceDiagram
    participant R as routes/submit.py<br/>submit_add()
    participant S as security.py
    participant V as schemas.py<br/>AddRequest
    participant E as enqueue.py<br/>submit_task()
    participant D as db.py
    participant P as publisher.py
    participant G as guard.py<br/>GuardedTask.__call__
    participant T as tasks.py<br/>add()
    participant C as celery_app.py<br/>signals

    S->>S: require_api_key (dependency)
    V->>V: validate body
    R->>E: submit_task(tasks.add, [x, y], idempotency_key)
    E->>D: create_job(...) → inserted?
    E->>P: publish_job(job)
    P->>P: celery_app.send_task(...)
    P->>D: mark_published()
    Note over G: …later, in a worker process…
    G->>D: acquire_execution() → ExecutionClaim
    G->>T: run add(x, y) (lease renewed in background)
    C->>D: task_postrun → update_job_status(..., attempt=n)
```

## 11.4 Key building blocks in `db.py`

| Function | What it does |
|---|---|
| `init_db()` | Creates missing tables/columns/indexes. Takes a Postgres advisory lock. |
| `create_job()` | Inserts a QUEUED, unpublished job; returns `False` if the idempotency key exists. |
| `mark_published()` | Sets `published_at`. |
| `claim_unpublished_jobs()` | Outbox relay: finds and claims unsent jobs. |
| `acquire_execution()` | **The claim.** Returns `ExecutionClaim(acquired, attempt, status, lease_expires_at)`. |
| `renew_lease()` | Extends a running attempt's lease (fenced by attempt). |
| `update_job_status(..., attempt=n)` | Updates a job; with `attempt`, only if it's still that attempt. |
| `claim_abandoned_jobs()` | Finds STARTED jobs with expired leases, claims them for recovery. |
| `fire_schedule()` | **Version-checked claim** of a due run + job insert, in one transaction. |
| `upsert_system_schedule()` | Creates/updates a code-defined schedule by name. |
| `update_schedule()` | Changes allowed columns only, bumps `version`. |
| `run_sync(coro)` | Runs an async DB call from sync code (Celery) on a per-process background loop. |

Everything is SQLAlchemy Core, so the same queries run on Postgres and SQLite.
Timestamps use the `UtcDateTime` type (TIMESTAMPTZ on Postgres, ISO text on SQLite).

## 11.5 Adding a new task

```mermaid
flowchart LR
    A["① Write the function<br/>in app/tasks.py"] --> B["② Decorate it with<br/>@celery_app.task(name=..., queue=...)"]
    B --> C{"③ Should the API<br/>run or schedule it?"}
    C -->|yes| D["Add to _PUBLIC_TASKS"]
    C -->|no| E[Leave it out]
    D --> F{"④ Needs its own<br/>endpoint?"}
    F -->|yes| G["Add a request model (schemas.py)<br/>and a route (routes/submit.py)"]
    F -->|no| H["Schedules can already use it"]
    G --> I["⑤ Write tests"]
    H --> I
    E --> I
```

Example — a task that emails an invoice:

```python
# app/tasks.py
@celery_app.task(name="app.tasks.email_invoice", bind=True, queue=settings.DEFAULT_QUEUE_NAME, max_retries=5)
def email_invoice(self, invoice_id: int) -> str:
    """Render invoice `invoice_id` and email it to the customer."""
    try:
        pdf = render_invoice(invoice_id)                       # your code
        email_provider.send(pdf, idempotency_key=self.request.id)  # ← makes a crash re-run harmless
        return f"invoice {invoice_id} sent"
    except TemporaryEmailError as exc:
        raise self.retry(exc=exc, countdown=30 * (self.request.retries + 1)) from exc

_PUBLIC_TASKS = (add, long_running_task, flaky_task, send_notification, heartbeat, scheduled_cleanup, email_invoice)
```

Rules for tasks:
1. **Always give an explicit `name=` and `queue=`.** Slow work goes on `settings.LONG_TASK_QUEUE_NAME`.
2. **Inputs and outputs must be JSON** (numbers, strings, lists, dicts). Pass IDs, not big objects.
3. **External side effects need an idempotency key** — use `self.request.id`.
4. **Retry only what's temporary** (network errors), with `self.retry()` or `autoretry_for=`.
5. **Stay under the time limits** (300 s soft / 360 s hard) or split the work.
6. You don't need to do anything for "runs once" — the guard is inherited automatically.

### Adding a periodic job
Add an entry to `SYSTEM_SCHEDULES` in `app/tasks.py`:
```python
{"name": "system:nightly-report", "task_name": email_invoice.name, "schedule_type": "cron", "cron_expr": "0 6 * * *"},
```
Every scheduler creates it on start-up (by name, so never twice).

### Adding a database column
Add a `Column(...)` to the table in `app/db.py`. `init_db()` adds missing
columns and indexes automatically on the next start. Keep changes **additive**
(no renames/drops) so old and new versions can run side by side.

## 11.6 Running the tests

```bash
pip install -r requirements-dev.txt
pytest                                   # SQLite, no services needed (~50 s)
TEST_DATABASE_URL=postgresql://user:pw@localhost:5432/test pytest   # Postgres
pytest tests/test_no_double_execution.py -v                         # one file
ruff check . && ruff format --check .    # lint & formatting
```

A throwaway Postgres for the tests:
```bash
docker run -d --name test-pg -e POSTGRES_PASSWORD=test -e POSTGRES_DB=jobs -p 55432:5432 postgres:16-alpine
TEST_DATABASE_URL=postgresql://postgres:test@localhost:55432/jobs pytest
docker rm -f test-pg
```

### How the test setup works (`tests/conftest.py`)
- Settings are set as environment variables **before** importing `app`: a temporary database, the in-memory queue (`memory://`, so no Redis is needed), short retries and a 3-second lease.
- `clean_db` empties every table before each test.
- `client` is a FastAPI test client.
- `worker` starts a **real Celery worker in a background thread** — integration tests go through the real guard, signals and queues.
- `wait_for_job(task_id, {"SUCCESS"})` polls until a job reaches a status.

| Test file | Focus |
|---|---|
| `test_api.py` | HTTP behaviour: validation, errors, auth, health, outbox, retry/cancel/schedule rules |
| `test_db_and_scheduling.py` | Database functions, migrations, timing rules, scheduler resilience |
| `test_integration.py` | Real worker, every sample task end to end |
| `test_no_double_execution.py` | Every way a job could run twice — and proof it doesn't |

## 11.7 Conventions

- **Formatting & lint:** `ruff` (line length 120). Run `ruff format .` before committing.
- **Logging:** `logger.info("text %s", value)` (not f-strings). Start important events with a `[TAG]`. Don't log personal data at INFO.
- **Errors in routes:** `raise HTTPException(status_code=…, detail="…")` — the handlers turn it into the envelope.
- **Database access:** only through `app/db.py`; from sync code use `db.run_sync(...)`.
- **Time:** always timezone-aware UTC (`datetime.now(timezone.utc)`).
- **Docstrings:** explain *why*, not just *what*. Each module starts with a short overview.

**Next:** [12. FAQ & troubleshooting →](12-faq-troubleshooting.md)
