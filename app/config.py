"""
Central, environment-driven configuration for every process
(API, worker, scheduler, migrate).

The same code runs unchanged on a laptop, in docker compose and in
Kubernetes — only environment variables differ. Every setting is read once at
import time; `.env.example` lists them all.
"""

import os


def _bool_env(name: str, default: bool) -> bool:
    """Parse a boolean-ish env var: '1', 'true', 'yes', 'on' (any case) mean True."""
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


def _int_env(name: str, default: int) -> int:
    return int(os.getenv(name, default))


def _list_env(name: str) -> list[str]:
    """Parse a comma-separated env var into a list, ignoring blanks."""
    return [item.strip() for item in os.getenv(name, "").split(",") if item.strip()]


def _normalize_database_url(url: str) -> str:
    """Pick the async driver for each database: psycopg 3 for Postgres, aiosqlite for SQLite."""
    for prefix in ("postgresql://", "postgres://", "postgresql+psycopg2://"):
        if url.startswith(prefix):
            return "postgresql+psycopg://" + url[len(prefix) :]
    if url.startswith("sqlite:///"):
        return "sqlite+aiosqlite:///" + url[len("sqlite:///") :]
    return url


class Settings:
    # ------------------------------------------------------------------
    # Database: job history, schedules, and (on Postgres) Celery results.
    #
    #   Multi-pod / production:  postgresql://user:pass@host:5432/jobs
    #   Single machine / tests:  sqlite:///data/scheduler.db  (the default)
    #
    # SQLite cannot be shared between hosts; every multi-pod deployment needs
    # Postgres. All guarantees against double execution rely on this shared
    # database.
    # ------------------------------------------------------------------
    DB_PATH: str = os.getenv("DB_PATH", "data/scheduler.db")  # SQLite only
    DATABASE_URL: str = _normalize_database_url(os.getenv("DATABASE_URL", f"sqlite:///{DB_PATH}"))
    IS_POSTGRES: bool = DATABASE_URL.startswith("postgresql")
    DB_POOL_SIZE: int = _int_env("DB_POOL_SIZE", 5)  # per process, Postgres only
    DB_MAX_OVERFLOW: int = _int_env("DB_MAX_OVERFLOW", 5)
    # Create/upgrade the schema when a process starts. Convenient locally.
    # In Kubernetes set false and run `python -m app.migrate` as a Job, so
    # many pods starting at once never race on DDL.
    AUTO_MIGRATE: bool = _bool_env("AUTO_MIGRATE", True)

    # ------------------------------------------------------------------
    # Broker (Redis). Only the message queue — results live in the database.
    #
    # For HA use a managed Redis with automatic failover, or Sentinel:
    #   CELERY_BROKER_URL="sentinel://:pass@s1:26379;sentinel://:pass@s2:26379"
    #   REDIS_SENTINEL_MASTER=mymaster
    # ------------------------------------------------------------------
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = _int_env("REDIS_PORT", 6379)
    REDIS_PASSWORD: str = os.getenv("REDIS_PASSWORD", "")
    CELERY_BROKER_URL: str = os.getenv(
        "CELERY_BROKER_URL",
        f"redis://{':' + REDIS_PASSWORD + '@' if REDIS_PASSWORD else ''}{REDIS_HOST}:{REDIS_PORT}/0",
    )
    REDIS_SENTINEL_MASTER: str = os.getenv("REDIS_SENTINEL_MASTER", "mymaster")
    # Seconds before Redis re-delivers a message whose worker vanished without
    # acking. Crash recovery doesn't wait for this (the scheduler resumes jobs
    # whose lease lapsed); the execution guard makes either re-delivery harmless.
    BROKER_VISIBILITY_TIMEOUT: int = _int_env("BROKER_VISIBILITY_TIMEOUT", 300)

    # ------------------------------------------------------------------
    # Celery result backend (live task state for GET /jobs/{id}/status).
    # Defaults to the main Postgres database, or a separate SQLite file.
    # ------------------------------------------------------------------
    CELERY_RESULTS_DB_PATH: str = os.getenv("CELERY_RESULTS_DB_PATH", "data/celery_results.db")
    CELERY_RESULT_BACKEND: str = os.getenv(
        "CELERY_RESULT_BACKEND",
        f"db+{DATABASE_URL}" if IS_POSTGRES else f"db+sqlite:///{CELERY_RESULTS_DB_PATH}",
    )
    CELERY_RESULT_EXPIRES: int = _int_env("CELERY_RESULT_EXPIRES", 60 * 60 * 24)

    # ------------------------------------------------------------------
    # Retries and runtime limits (sample tasks use these; any task may override)
    # ------------------------------------------------------------------
    TASK_DEFAULT_MAX_RETRIES: int = _int_env("TASK_DEFAULT_MAX_RETRIES", 3)
    TASK_DEFAULT_RETRY_BACKOFF: int = _int_env("TASK_DEFAULT_RETRY_BACKOFF", 5)
    TASK_SOFT_TIME_LIMIT: int = _int_env("TASK_SOFT_TIME_LIMIT", 300)  # raises SoftTimeLimitExceeded
    TASK_TIME_LIMIT: int = _int_env("TASK_TIME_LIMIT", 360)  # worker process is killed

    # ------------------------------------------------------------------
    # Execution guard (see app/guard.py)
    #
    # A worker must hold a lease on a job to run it, renewed every
    # LEASE/3 seconds while it runs. A second copy of the message is deferred
    # while the lease is live and dropped once the job has finished. If the
    # worker dies, the lease runs out and a re-delivered copy may take over.
    # ------------------------------------------------------------------
    EXECUTION_LEASE_SECONDS: int = _int_env("EXECUTION_LEASE_SECONDS", 60)
    # Max seconds a deferred duplicate waits before checking the lease again.
    DUPLICATE_RECHECK_SECONDS: int = _int_env("DUPLICATE_RECHECK_SECONDS", 15)
    # A job whose worker dies is re-run on another worker, at most this many
    # times; then it is marked FAILURE (guards against tasks that crash workers).
    MAX_WORKER_LOSS_RECOVERIES: int = _int_env("MAX_WORKER_LOSS_RECOVERIES", 3)
    # How long a worker keeps retrying a job's final status write while the
    # database is down. Keep EXECUTION_LEASE_SECONDS x 2 above the longest DB
    # outage you want to ride out without re-running tasks that were in flight.
    FINAL_STATE_WRITE_TIMEOUT_SECONDS: int = _int_env("FINAL_STATE_WRITE_TIMEOUT_SECONDS", 300)

    # ------------------------------------------------------------------
    # Queues and ordering
    #
    # Each Redis queue is FIFO. What can reorder work is *prefetch*: with
    # FIFO_ENABLED=true a worker reserves one task at a time, so tasks are
    # dispatched in arrival order (add --concurrency=1 for strict completion
    # order too). Retries with a countdown lose their place. false prefetches
    # 4 per process for throughput.
    # ------------------------------------------------------------------
    FIFO_ENABLED: bool = _bool_env("FIFO_ENABLED", True)
    DEFAULT_QUEUE_NAME: str = os.getenv("DEFAULT_QUEUE_NAME", "default")
    LONG_TASK_QUEUE_NAME: str = os.getenv("LONG_TASK_QUEUE_NAME", "long")

    # ------------------------------------------------------------------
    # Scheduler service (app/scheduler.py) — run any number of replicas.
    # ------------------------------------------------------------------
    SCHEDULE_POLL_INTERVAL_SECONDS: int = _int_env("SCHEDULE_POLL_INTERVAL_SECONDS", 5)
    # A job whose row exists but was never published (its publisher crashed,
    # or the broker was down) is re-published after this many seconds.
    OUTBOX_REPUBLISH_AFTER_SECONDS: int = _int_env("OUTBOX_REPUBLISH_AFTER_SECONDS", 30)
    # The scheduler touches this file every loop; the liveness probe checks its age.
    SCHEDULER_HEARTBEAT_FILE: str = os.getenv("SCHEDULER_HEARTBEAT_FILE", "/tmp/scheduler-heartbeat")
    # Finished jobs older than this are deleted by the daily cleanup.
    JOB_RETENTION_DAYS: int = _int_env("JOB_RETENTION_DAYS", 30)

    # ------------------------------------------------------------------
    # API security
    #
    # Comma-separated keys accepted in the `X-API-Key` header. Empty disables
    # auth (a warning is logged) — local dev only.
    # ------------------------------------------------------------------
    API_KEYS: list[str] = _list_env("API_KEYS")

    # ------------------------------------------------------------------
    # App and logging
    # ------------------------------------------------------------------
    APP_NAME: str = os.getenv("APP_NAME", "job-scheduler")
    APP_VERSION: str = "4.0.0"
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()
    LOG_FORMAT: str = os.getenv("LOG_FORMAT", "text").lower()  # "text" or "json"
    # Also write rotating files under LOG_DIR (local dev). In containers keep
    # this false and let the platform collect stdout.
    LOG_TO_FILE: bool = _bool_env("LOG_TO_FILE", True)
    LOG_DIR: str = os.getenv("LOG_DIR", "logs")

    # ------------------------------------------------------------------
    # Flower dashboard — https://github.com/mher/flower
    # ------------------------------------------------------------------
    FLOWER_PORT: int = _int_env("FLOWER_PORT", 5555)
    FLOWER_BASIC_AUTH: str = os.getenv("FLOWER_BASIC_AUTH", "")  # "user:pass"


settings = Settings()
