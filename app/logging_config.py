"""
Logging setup shared by every process: API, Celery workers, scheduler, migrate.

Each process calls `setup_logging(<file name>)` exactly once:
  * the API from `app.main`,
  * Celery through its `setup_logging` signal (see `app.celery_app`), which
    also stops Celery from replacing these handlers with its own.

Output always goes to stdout. When `LOG_TO_FILE` is on, it is also written to
a rotating file under `LOG_DIR`, one file per process type (api / worker /
scheduler). Python's RotatingFileHandler is not multi-process safe, and a worker's
pool children inherit their parent's file, so in production set
`LOG_TO_FILE=false` and let the platform collect stdout.
"""

import json
import logging
import logging.handlers
import os
from datetime import datetime, timezone

from app.config import settings

TEXT_FORMAT = "%(asctime)s | %(levelname)-8s | %(processName)s | %(name)s | %(message)s"

_configured = False


class JsonFormatter(logging.Formatter):
    """Render each record as one JSON object per line, for log shippers."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "time": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "process": record.processName,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def setup_logging(log_filename: str) -> None:
    """Configure the root logger. Later calls in the same process do nothing."""
    global _configured
    if _configured:
        return
    _configured = True

    formatter: logging.Formatter = JsonFormatter() if settings.LOG_FORMAT == "json" else logging.Formatter(TEXT_FORMAT)

    root = logging.getLogger()
    root.setLevel(settings.LOG_LEVEL)
    root.handlers.clear()

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    root.addHandler(console)

    if settings.LOG_TO_FILE:
        os.makedirs(settings.LOG_DIR, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            os.path.join(settings.LOG_DIR, log_filename),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
        )
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    logging.getLogger("kombu").setLevel(logging.WARNING)
