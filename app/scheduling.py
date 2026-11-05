"""
Timing rules for dynamic schedules, shared by the API (create/update) and
the dispatcher task so both compute run times the same way.

All times are timezone-aware UTC. Cron expressions are standard 5-field
expressions evaluated in UTC.

Semantics:
  * once     — fires a single time at `run_at`, then disables itself.
  * interval — fires, then again `interval_seconds` after the moment it
               actually fired (so a late poll pushes later runs back a little).
  * cron     — fires at each matching minute.
  * Missed runs (e.g. every scheduler replica was down) are NOT replayed: a late schedule fires
    once, then resumes its normal timing.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional

from croniter import croniter

SCHEDULE_TYPES = ("once", "interval", "cron")


def is_valid_cron(expr: str) -> bool:
    """True if `expr` is a valid 5-field cron expression."""
    return bool(expr) and croniter.is_valid(expr) and len(expr.split()) == 5


def next_cron_run(expr: str, after: datetime) -> datetime:
    """The first time strictly after `after` that matches `expr` (UTC)."""
    return croniter(expr, after.astimezone(timezone.utc)).get_next(datetime)


def first_run_at(
    schedule_type: str,
    now: datetime,
    run_at: Optional[datetime] = None,
    cron_expr: Optional[str] = None,
) -> datetime:
    """When a newly created schedule should fire for the first time."""
    if schedule_type == "once":
        return run_at.astimezone(timezone.utc)
    if schedule_type == "interval":
        return now  # fire on the next poll, then every interval
    if schedule_type == "cron":
        return next_cron_run(cron_expr, now)
    raise ValueError(f"Unknown schedule_type: {schedule_type!r}")


def next_run_after_fire(schedule: dict, fired_at: datetime) -> tuple[Optional[datetime], bool]:
    """
    For a schedule row that is firing at `fired_at`, return
    (next run time, whether it stays enabled).

    'once' returns (None, False). An unknown type also returns (None, False),
    so a corrupt row is switched off rather than retried forever.
    """
    schedule_type = schedule["schedule_type"]
    if schedule_type == "interval":
        return fired_at + timedelta(seconds=schedule["interval_seconds"]), True
    if schedule_type == "cron":
        return next_cron_run(schedule["cron_expr"], fired_at), True
    return None, False
