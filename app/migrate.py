"""
Create or upgrade every table this service needs, then exit.

    python -m app.migrate

Run it before starting a new version (Kubernetes runs it as an init
container in every pod; compose runs it as the `migrate` service). It is
idempotent, and on Postgres concurrent runs are serialized by advisory locks,
so many pods starting at once are safe.
"""

import asyncio
import logging

from sqlalchemy import create_engine, text

from app import db
from app.config import settings
from app.logging_config import setup_logging

logger = logging.getLogger(__name__)

_CELERY_TABLES_LOCK_ID = 7_202_612


def _create_celery_result_tables() -> None:
    """Create Celery's result tables now, instead of lazily (and racily) in the first worker."""
    from app.celery_app import celery_app

    backend = celery_app.backend
    if not hasattr(backend, "ResultSession"):
        return  # not the SQLAlchemy backend
    if not settings.IS_POSTGRES:
        backend.ResultSession().close()
        return

    lock_engine = create_engine(settings.CELERY_RESULT_BACKEND.removeprefix("db+"))
    try:
        with lock_engine.connect() as conn:
            conn.execute(text("SELECT pg_advisory_lock(:id)"), {"id": _CELERY_TABLES_LOCK_ID})
            try:
                backend.ResultSession().close()
            finally:
                conn.execute(text("SELECT pg_advisory_unlock(:id)"), {"id": _CELERY_TABLES_LOCK_ID})
    finally:
        lock_engine.dispose()


def main() -> None:
    setup_logging(log_filename="migrate.log")
    asyncio.run(db.init_db())
    _create_celery_result_tables()
    logger.info("Migrations complete")


if __name__ == "__main__":
    main()
