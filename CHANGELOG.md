# Changelog

## 5.0.0 — 2026-11-06

Features for building on the service, running it as a shared platform, and
seeing what it does. All database changes are additive (new tables and
nullable columns), so 4.0 and 5.0 pods can run side by side during a rolling upgrade.

### Upgrade notes

- **Roll out the schedulers first if you'll use delayed jobs right away.** A
  4.0 scheduler would publish a delayed job early. Everything else is safe in
  any order: priorities use the same Redis lists 4.0 workers already read.
- **New dependencies** in `requirements.txt` (`httpx`, `prometheus-client`,
  `tzdata`, OpenTelemetry). Rebuild the image.
- **Set `WEBHOOK_SIGNING_SECRET` and `WEBHOOK_ALLOWED_HOSTS`** before anyone uses `callback_url`.
- **Unversioned paths are deprecated.** `/tasks/*`, `/jobs…` and `/schedules…` still work exactly as before,
  but now answer with `Deprecation: true` and a `Link` to the `/v1` path, and are hidden from `/docs`.
  Move clients to `/v1` (a find-and-replace). Policy: [docs/18-api-versioning.md](docs/18-api-versioning.md).
- **A job's final state is now written once.** Before, a late write could replace it, for example
  SUCCESS arriving after a cancel. The first final state now always wins. This is what makes webhooks and
  workflow steps fire exactly once.

### Added

- **API versioning:** every endpoint under `/v1`.
- **`POST /v1/jobs`:** submit any public task, with per-job options:
  - `priority` (`high`, `normal` or `low`, using Redis priority lists);
  - a delayed start (`delay_seconds` or `run_at`), stored durably and released by the scheduler;
  - `timeout_seconds`;
  - `max_retries`;
  - `callback_url`.
- **Webhooks:**
  - recorded in the same transaction as the job's final state (an outbox), and sent by any scheduler replica;
  - signed with HMAC-SHA256 and a timestamp; retried with backoff;
  - delivery status at `GET /v1/jobs/{id}/webhooks`;
  - host allow-list (`WEBHOOK_ALLOWED_HOSTS`).
- **Live event stream:** `GET /v1/jobs/{id}/events` (server-sent events).
- **Workflows:** `POST /v1/workflows`, and `GET` / `DELETE` on them.
  - Steps run in dependency order: fan-out and fan-in.
  - When a step fails, everything downstream of it is skipped.
  - A step that depends on several others is released exactly once, even when its parents finish at the same moment on different workers (the workflow row is locked).
  - A workflow can have its own webhook, and supports `Idempotency-Key`.
  - New job status `WAITING`.
- **Failures view:** `GET /v1/jobs/failures` groups failed jobs by task and error; `POST /v1/jobs/failures/retry` re-submits them in bulk, at most once per failed job.
- **Cron time zones:** a `timezone` field on cron schedules, following daylight-saving changes. A local time that repeats when clocks go back fires once.
- **Managed API keys:**
  - `POST`, `GET` and `DELETE /v1/admin/api-keys`;
  - scopes (`jobs:read`, `jobs:write`, `schedules:read`, `schedules:write`, `admin`);
  - optional `tenant`, `allowed_tasks` and `rate_limit_per_minute`;
  - stored as SHA-256 hashes.
  - Existing `API_KEYS` become full-access bootstrap keys.
- **Tenants:**
  - a tenant key sees only its own jobs, schedules and workflows (anything else answers 404);
  - idempotency keys are separate per tenant;
  - `TENANT_MAX_ACTIVE_SCHEDULES` quota.
- **Rate limits** per key and minute, counted in Redis across API replicas. Over the limit: `429` with `Retry-After` and `X-RateLimit-*` headers. New error types `forbidden` and `rate_limited`.
- **Audit log:** `GET /v1/admin/audit-log` records every submit, retry, cancel, bulk retry, schedule change, workflow change and key change.
- **Metrics** at `GET /metrics` (Prometheus).
  - Cluster-wide, from the database: jobs, recent finishes, wait and run-time quantiles, oldest queued job, Redis queue depth, outbox backlog, schedule lateness, webhooks, workflows.
  - Reliability event counters: duplicates dropped or deferred, workers lost and recovered, leases lost, outbox relays, schedules fired, webhooks, rate limits.
  - Per API process: HTTP metrics.
- **Monitoring kit** in `deploy/monitoring/`:
  - a Grafana dashboard (17 panels);
  - 9 Prometheus alert rules (checked with `promtool`);
  - Prometheus config;
  - `docker-compose.monitoring.yml` with Prometheus, Grafana and Jaeger.
- **OpenTelemetry tracing** (turned on by `OTEL_EXPORTER_OTLP_ENDPOINT`) for FastAPI, SQLAlchemy, Celery publish and run, and httpx. One trace spans API → worker.
- **Operator console** at `/ui`: overview, jobs, failures (retry a whole group), schedules (pause, resume, delete) and workflows (step detail, cancel).
- **SDKs:**
  - `sdk/python` (httpx);
  - `sdk/typescript`, with no dependencies and types generated from `sdk/openapi.json`.
  - Both retry safely with idempotency keys, wait, stream events, cover schedules and workflows, and verify webhooks.
- **Helm chart** in `deploy/helm/job-scheduler`, at parity with the kustomize manifests. It has a values schema, an optional ServiceMonitor and PrometheusRule, and `examples/monitoring-values.yaml`.
- **Supply chain:**
  - a weekly and per-push `pip-audit` and Trivy scan, plus an SBOM artifact;
  - a release workflow that pushes a multi-arch image to GHCR, with provenance and an SBOM, signed with cosign (keyless);
  - Dependabot.
- **CI:** a check that `sdk/openapi.json` matches the API, and TypeScript SDK build and tests.
- **Scripts:**
  - `scripts/load_test.py`, which measures throughput and latency and checks exactly-once;
  - `scripts/backup_restore_drill.sh`, which dumps, restores into a throwaway Postgres, compares every table and runs the app on the copy;
  - `scripts/export_openapi.py`.
- **Docs:** new chapters 15 (security & tenants), 16 (webhooks, workflows & job options), 17 (observability, performance & DR) and 18 (API versioning). The integration guide, the configuration chapter and the runbooks are updated. 66 diagrams.

### Tested

- **Automated tests,** on SQLite and on Postgres: 31 new feature tests and 7 SDK tests, plus the TypeScript SDK's own tests.
- **End-to-end checks against the multi-replica Docker stack (20 of 20 pass):**
  - webhooks are signed, retried and sent once;
  - per-job timeouts are enforced by real workers;
  - priority order is followed (5 high-priority jobs ran before 5 low-priority ones);
  - delayed start works;
  - a workflow with fan-in and a failed branch behaves as described;
  - a rate limit holds across 2 API replicas;
  - tenant isolation holds across replicas;
  - the event stream works;
  - Prometheus scrapes, the alert rules load, the Grafana dashboard loads, and Jaeger receives traces spanning API → worker;
  - the console works.
- **Chaos test:** all 7 scenarios still pass.
- **Load test:** 5,000 jobs; each ran exactly once, with no duplicates.
- **Backup and restore drill:** passes.

## 4.0.0 — 2026-11-05

Multi-pod, highly available operation: every component can run as many
replicas, and each job and each scheduled run executes once.

### Breaking changes

- **Celery Beat is gone.** Run the new scheduler service, `python -m app.scheduler`, with as many replicas as you like. Static periodic tasks are now `SYSTEM_SCHEDULES` in `app/tasks.py`, which appear in `GET /schedules` with `system: true` and are read-only through the API (409).
- **Postgres is required for any multi-pod deployment** (`DATABASE_URL=postgresql://…`). SQLite remains the default for a single machine and for tests. The Celery result backend now lives in the same Postgres database by default.
- **Migrations run separately in compose and Kubernetes** (`python -m app.migrate`; `AUTO_MIGRATE=false`).
- **Submitting while Redis is down** now returns **201** (the job is stored and published later by the outbox) instead of 503.
- **Timestamps** in responses are UTC with a `Z` suffix (`2026-01-01T08:00:00Z`).
- **New fields:**
  - jobs gain `attempts`, `published_at` and `idempotency_key`;
  - schedules gain `name` and `system`.
- **Removed settings:** `DB_PATH`-only configuration and `BEAT_SCHEDULE_PATH`. The default `BROKER_VISIBILITY_TIMEOUT` is now 300 s.

### Added

- **Active-active scheduler.** Each due run is claimed with an optimistic version check, in the same transaction that records its job, so exactly one replica fires it. There is no leader election and no failover delay.
- **Execution guard** (`app/guard.py`), the base class of every task:
  - a worker must atomically claim a job, with a renewable lease, before running it;
  - duplicate deliveries are dropped, or deferred while the original is running;
  - status writes are fenced by attempt number;
  - cancelled jobs never start on any worker.
- **Transactional outbox.** Jobs are recorded before publishing, and the scheduler re-publishes anything left unpublished.
- **Crash recovery.** A job whose worker died (its lease lapsed) is re-published and finished elsewhere. A job Celery recorded as finished is reconciled instead of re-run. Re-runs are capped by `MAX_WORKER_LOSS_RECOVERIES`.
- **Surviving database outages:**
  - workers hand messages back instead of running them without a claim;
  - final status writes retry (`FINAL_STATE_WRITE_TIMEOUT_SECONDS`);
  - the Celery result backend retries connection errors (`app/result_backend.py`);
  - the API returns 503 `service_unavailable`.
- **`Idempotency-Key`** header on `POST /tasks/*`, `POST /jobs/{id}/retry` and `POST /schedules`. A repeat returns the original with 200 and `Idempotent-Replayed: true`; reusing a key for a different request is 409.
- Redis Sentinel support (`sentinel://` broker URL, `REDIS_SENTINEL_MASTER`).
- Kubernetes manifests (`deploy/k8s/`): replicas, zone/node spread, PDBs, HPAs, probes, migrate init containers, restricted Pod Security, NetworkPolicies.
- Compose now runs Postgres, a migrate step and 2 replicas of every app service.
- `scripts/chaos_test.py` (7 scenarios that kill pods, Redis and Postgres) and `tests/test_no_double_execution.py`. 83 tests pass on both SQLite and Postgres.

### Fixed (found by the new tests)

- **Postgres reported a new job as an idempotent replay.** `INSERT … ON CONFLICT DO NOTHING` row counts are unreliable there, so the API took every new job for a replay and never published it. Inserts are now detected with `RETURNING`.
- **Concurrent outbox relays could publish the same job twice.** The relay's claim is now an exact optimistic check.
- **A crashed worker's job could be marked FAILURE** before the broker re-delivered it. Recovery no longer depends on broker re-delivery timing.
- **A task that succeeded while Postgres was down was recorded as FAILURE** ("failed to store result on the backend").

## 3.1.0 — 2026-11-05

A production-readiness pass: bug fixes, security, tests, and documentation.

### Breaking changes for API clients

- **Auth:** when `API_KEYS` is set, every route except `/health*` needs an `X-API-Key` header (401 otherwise).
- `POST /jobs/{id}/retry` on a job that didn't fail returns **409 `conflict`** (was 400 `bad_request`).
- `DELETE /jobs/{id}` on a finished job returns **409 `conflict`** and leaves its status alone (previously it overwrote SUCCESS/FAILURE with REVOKED).
- Job `result`, `args` and `kwargs` are returned as JSON values (`7.0`), not strings containing JSON (`"7.0"`).
- `POST /schedules` rejects (422): a `run_at` in the past or without a timezone, a timing field that doesn't match `schedule_type`, 6-field cron expressions, and `args`/`kwargs` that don't fit the task's signature.
- `PATCH /schedules/{id}` rejects null values (422), a field that doesn't apply to the schedule's type (400), and resuming a `once` schedule that already fired (409).
- `GET /health` returns `{"status", "service", "version"}`; the service name is now `APP_NAME`.

### Fixed

- **Lost status updates:** the API published a task before inserting its job row, so a fast worker could finish before the row existed, leaving it stuck at QUEUED. The row is now inserted first.
- **Duplicate schedule runs:** overlapping dispatcher runs, or a crash between publishing and recording, could fire one due time twice. Each run is now claimed atomically before publishing.
- **Wrong queue:** scheduled tasks were sent with `send_task`, which ignored their queue, so scheduled `long_running_task` ran on `default`. They are now published to the task's own queue.
- **Phantom task:** `app.tasks.always_fail_task` could be scheduled but didn't exist. The schedulable list is now derived from the task registry; a schedule pointing at an unknown task is disabled.
- **Untracked jobs:** jobs fired by Beat and dynamic schedules never appeared in `/jobs`. Now they do, linked by the new `schedule_id` column.
- **PATCH didn't take effect:** changing `interval_seconds` / `cron_expr` didn't recalculate `next_run_at`, and `cron_expr` wasn't validated.
- **Retry marked jobs finished:** a job being retried got a `finished_at` timestamp.
- **`/jobs/{id}/status` said PENDING** for finished jobs once Celery's result expired. It now falls back to the job history (`source: "job_record"`).
- **Errors had two shapes:** unknown routes and 405s returned FastAPI's default `{"detail": …}`. All errors now use the envelope; 401/405/409/503 get their own `type`.
- **API logs went to `celery.log`** and `api.log` was never created; the worker and Beat also shared one rotating file. Each process type now has its own file, and Celery uses our logging config.
- **Docker workers could not start:** kombu's Redis transport needs `packaging`, which wasn't in `requirements.txt` (it only worked locally because a dev venv had it). Now pinned.
- The broker publish no longer blocks the API's event loop.
- Stale docs and docstrings that described Redis as the result backend.

### Added

- API-key auth with constant-time comparison; several keys for rotation.
- `GET /health/ready` readiness probe (database + broker).
- Real `scheduled_cleanup`: deletes job rows older than `JOB_RETENTION_DAYS`.
- Redis password support (`REDIS_PASSWORD`), `BROKER_VISIBILITY_TIMEOUT`, JSON log format, `LOG_TO_FILE`.
- Test suite (58 tests: unit + end-to-end with a real worker), `scripts/smoke_test.py`, ruff config, `requirements-dev.txt`, `pyproject.toml`.
- `.env.example`, `.dockerignore`, `CHANGELOG.md`.

### Changed

- `app/main.py` is split into `app/api/` (errors, security, schemas, enqueue, one router per group) and `app/scheduling.py`.
- `docker-compose.yml` is now production-oriented: required secrets, Redis not published and password-protected, Flower on localhost with mandatory login, restart policies, worker health checks, stop grace periods, JSON logs, multi-process API. Dev settings (reload, bind-mount, text logs) moved to `docker-compose.override.yml`, which `docker compose up` loads automatically.
- Beat's state file moved to `data/celerybeat-schedule` (`BEAT_SCHEDULE_PATH`).
- Logging uses lazy `%s` formatting; task args (which can contain email addresses) are logged only at DEBUG.
- `@app.on_event("startup")` replaced with a lifespan handler.
- `test_tasks.py` replaced by `scripts/smoke_test.py` and `tests/`.
