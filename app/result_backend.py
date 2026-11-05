"""
Celery's SQLAlchemy result backend, made to survive database outages.

Stock behaviour: a result write that hits a dropped connection is retried
3 times within milliseconds, then the task is marked FAILURE with
"failed to store result on the backend" — even though the task itself
succeeded. Celery's `result_backend_always_retry` would fix this, but the
database backend never declares any error safe to retry.

This subclass declares connection-level errors (not data errors) safe, so
Celery retries them with exponential backoff (`result_backend_*` settings in
app.celery_app) and a task that finishes during a database failover keeps
its real outcome.
"""

from celery.backends.database import DatabaseBackend
from sqlalchemy import exc as sa_exc

_CONNECTION_ERRORS = (sa_exc.OperationalError, sa_exc.InterfaceError, sa_exc.TimeoutError, ConnectionError)


class ResilientDatabaseBackend(DatabaseBackend):
    def exception_safe_to_retry(self, exc: Exception) -> bool:
        return isinstance(exc, _CONNECTION_ERRORS)
