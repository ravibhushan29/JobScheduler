"""
Request and response models for the HTTP API.

All timestamps are timezone-aware UTC and serialize as ISO-8601 (`...Z`).
"""

from datetime import datetime, timezone
from typing import Any, Literal, Optional

from pydantic import BaseModel, EmailStr, Field, model_validator

from app import scheduling
from app.tasks import SCHEDULABLE_TASK_NAMES, check_task_arguments

#: Request header for safe client retries (see app.api.enqueue).
IDEMPOTENCY_HEADER = "Idempotency-Key"


# ---------------------------------------------------------------------------
# Task submission
# ---------------------------------------------------------------------------
class AddRequest(BaseModel):
    x: float = Field(..., examples=[3])
    y: float = Field(..., examples=[4])


class LongTaskRequest(BaseModel):
    duration_seconds: int = Field(default=5, ge=1, le=120, examples=[5])


class FlakyTaskRequest(BaseModel):
    fail_probability: float = Field(
        default=0.7, ge=0.0, le=1.0, examples=[0.7], description="0.0 never fails, 1.0 always fails."
    )


class NotificationRequest(BaseModel):
    email: EmailStr = Field(..., examples=["someone@example.com"])
    message: str = Field(..., min_length=1, max_length=2000, examples=["Your report is ready."])


class TaskSubmitResponse(BaseModel):
    """Returned as soon as the task is queued; it has not run yet."""

    task_id: str = Field(..., examples=["8f14e45f-fceb-4dc9-9a0f-example"])
    status: Literal["queued"] = "queued"


class BatchTestResponse(BaseModel):
    """task_id of each job queued by POST /tasks/test-batch."""

    add: str
    long_running_task: str
    flaky_task_will_fail: str
    flaky_task_will_succeed: str
    send_notification: str


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------
class TaskStatusResponse(BaseModel):
    """Live task state, as stored by Celery's result backend."""

    task_id: str
    state: str = Field(
        ..., examples=["SUCCESS"], description="PENDING | STARTED | PROGRESS | RETRY | SUCCESS | FAILURE | REVOKED"
    )
    source: Literal["result_backend", "job_record"] = Field(
        ...,
        description="`job_record` when Celery's result has expired and the state comes from the job history instead.",
    )
    result: Optional[Any] = Field(
        default=None, description="Return value on SUCCESS, progress on PROGRESS, exception info on FAILURE."
    )
    error: Optional[str] = Field(default=None, description="Short failure reason, on FAILURE.")
    traceback: Optional[str] = Field(default=None, description="Full formatted traceback, on FAILURE.")
    name: Optional[str] = Field(default=None, examples=["app.tasks.flaky_task"])
    args: Optional[list] = None
    kwargs: Optional[dict] = None
    retries: Optional[int] = None
    worker: Optional[str] = Field(default=None, description="Hostname of the worker that ran it.")
    queue: Optional[str] = None
    date_done: Optional[datetime] = Field(default=None, description="When the task reached a final state.")
    children: Optional[list] = Field(default=None, description="Results of sub-tasks it started.")


class JobRecord(BaseModel):
    """One row of the job history. Kept for JOB_RETENTION_DAYS after it finishes."""

    task_id: str
    task_name: str = Field(..., examples=["app.tasks.add"])
    status: str = Field(..., examples=["SUCCESS"], description="QUEUED | STARTED | RETRY | SUCCESS | FAILURE | REVOKED")
    args: list = Field(default_factory=list)
    kwargs: dict = Field(default_factory=dict)
    queue: Optional[str] = None
    result: Optional[Any] = Field(default=None, description="Return value, on SUCCESS.")
    error: Optional[str] = Field(default=None, description="Short failure reason.")
    traceback: Optional[str] = Field(default=None, description="Full formatted traceback, on FAILURE.")
    retries: int = Field(0, description="Automatic retries Celery made for this job.")
    attempts: int = Field(
        0,
        description="Times a worker started executing it: 1 + retries normally; "
        "higher only if a worker died mid-run and another took over.",
    )
    worker: Optional[str] = None
    retry_of: Optional[str] = Field(default=None, description="task_id this job manually re-submits.")
    schedule_id: Optional[int] = Field(default=None, description="Schedule that fired this job.")
    idempotency_key: Optional[str] = None
    submitted_at: datetime
    published_at: Optional[datetime] = Field(default=None, description="When it reached the broker.")
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None

    @classmethod
    def from_row(cls, row: dict) -> "JobRecord":
        return cls.model_validate(row)


class JobListResponse(BaseModel):
    total: int = Field(..., description="Matching jobs in total, ignoring limit/offset.")
    limit: int
    offset: int
    jobs: list[JobRecord]


class CancelResponse(BaseModel):
    task_id: str
    status: Literal["revoked"] = "revoked"


# ---------------------------------------------------------------------------
# Schedules
# ---------------------------------------------------------------------------
_TIMING_FIELD = {"once": "run_at", "interval": "interval_seconds", "cron": "cron_expr"}


class ScheduleCreateRequest(BaseModel):
    """
    A dynamic schedule. Give exactly the timing field that matches `schedule_type`:

    | schedule_type | required field     | behaviour                                      |
    |---------------|--------------------|------------------------------------------------|
    | `once`        | `run_at`           | fires once at that time, then disables itself  |
    | `interval`    | `interval_seconds` | fires on the next poll, then every N seconds   |
    | `cron`        | `cron_expr`        | 5-field cron, evaluated in **UTC**             |

    One task may have any number of schedules.
    """

    task_name: str = Field(
        ..., examples=["app.tasks.heartbeat"], description=f"One of: {sorted(SCHEDULABLE_TASK_NAMES)}"
    )
    args: list = Field(default_factory=list)
    kwargs: dict = Field(default_factory=dict)
    schedule_type: Literal["once", "interval", "cron"] = Field(..., examples=["interval"])
    run_at: Optional[datetime] = Field(
        default=None,
        examples=["2026-07-26T09:50:00+00:00"],
        description="schedule_type='once' only. ISO-8601 with a timezone offset, in the future.",
    )
    interval_seconds: Optional[int] = Field(
        default=None, ge=1, examples=[7200], description="schedule_type='interval' only."
    )
    cron_expr: Optional[str] = Field(
        default=None, examples=["0 8 * * 1-5"], description="schedule_type='cron' only. Evaluated in UTC."
    )

    @model_validator(mode="after")
    def _validate(self) -> "ScheduleCreateRequest":
        if self.task_name not in SCHEDULABLE_TASK_NAMES:
            raise ValueError(f"task_name must be one of {sorted(SCHEDULABLE_TASK_NAMES)}")

        required = _TIMING_FIELD[self.schedule_type]
        if getattr(self, required) is None:
            raise ValueError(f"{required} is required when schedule_type={self.schedule_type!r}")
        extra = [f for f in _TIMING_FIELD.values() if f != required and getattr(self, f) is not None]
        if extra:
            raise ValueError(f"{extra} cannot be used with schedule_type={self.schedule_type!r}")

        if self.run_at is not None:
            if self.run_at.tzinfo is None:
                raise ValueError("run_at must include a timezone offset, e.g. '+00:00'")
            if self.run_at <= datetime.now(timezone.utc):
                raise ValueError("run_at must be in the future")
        if self.cron_expr is not None and not scheduling.is_valid_cron(self.cron_expr):
            raise ValueError(f"Invalid 5-field cron expression: {self.cron_expr!r}")

        try:
            check_task_arguments(self.task_name, self.args, self.kwargs)
        except TypeError as exc:
            raise ValueError(f"args/kwargs don't match {self.task_name}: {exc}") from exc
        return self


class ScheduleUpdateRequest(BaseModel):
    """Fields to change; omitted fields stay as they are. Values may not be null."""

    enabled: Optional[bool] = Field(default=None, description="false pauses, true resumes.")
    interval_seconds: Optional[int] = Field(default=None, ge=1, description="Interval schedules only.")
    cron_expr: Optional[str] = Field(default=None, description="Cron schedules only.")

    @model_validator(mode="after")
    def _validate(self) -> "ScheduleUpdateRequest":
        nulls = sorted(name for name in self.model_fields_set if getattr(self, name) is None)
        if nulls:
            raise ValueError(f"{nulls} cannot be null; omit a field to leave it unchanged")
        if self.cron_expr is not None and not scheduling.is_valid_cron(self.cron_expr):
            raise ValueError(f"Invalid 5-field cron expression: {self.cron_expr!r}")
        return self


class ScheduleResponse(BaseModel):
    id: int
    name: Optional[str] = Field(default=None, description="Set on system schedules, e.g. `system:cleanup`.")
    system: bool = Field(..., description="Defined in code; can't be changed or deleted through the API.")
    task_name: str
    args: list
    kwargs: dict
    schedule_type: str
    run_at: Optional[datetime] = None
    interval_seconds: Optional[int] = None
    cron_expr: Optional[str] = None
    enabled: bool
    next_run_at: datetime = Field(..., description="When it fires next (UTC). Not meaningful once disabled.")
    last_run_at: Optional[datetime] = None
    run_count: int = Field(..., description="How many times it has fired.")
    created_at: datetime

    @classmethod
    def from_row(cls, row: dict) -> "ScheduleResponse":
        return cls.model_validate({**row, "system": row["name"] is not None})


class ScheduleDeleteResponse(BaseModel):
    schedule_id: int
    status: Literal["deleted"] = "deleted"
