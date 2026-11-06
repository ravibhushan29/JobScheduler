"""
Request and response models for the HTTP API.

All timestamps are timezone-aware UTC and serialize as ISO-8601 (`...Z`).
"""

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Optional

from pydantic import BaseModel, EmailStr, Field, field_validator, model_validator

from app import db, scheduling, webhooks
from app.api.security import SCOPES
from app.config import settings
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
    run_after: Optional[datetime] = Field(
        default=None, description="Set for a delayed job: it won't start before this time (UTC)."
    )


Priority = Literal["high", "normal", "low"]

#: Longest per-job timeout a caller may ask for.
MAX_JOB_TIMEOUT_SECONDS = 24 * 3600


def _check_task_name(name: str) -> str:
    if name not in SCHEDULABLE_TASK_NAMES:
        raise ValueError(f"task_name must be one of {sorted(SCHEDULABLE_TASK_NAMES)}")
    return name


def _check_callback(url: Optional[str]) -> Optional[str]:
    if url is not None:
        problem = webhooks.check_callback_url(url)
        if problem:
            raise ValueError(problem)
    return url


class JobOptions(BaseModel):
    """Per-job options, shared by POST /v1/jobs and workflow steps."""

    priority: Priority = Field(
        default="normal", description="`high` jobs are taken from the queue before `normal`, then `low`."
    )
    timeout_seconds: Optional[int] = Field(
        default=None,
        ge=1,
        le=MAX_JOB_TIMEOUT_SECONDS,
        description="Stop the job after this long (SoftTimeLimitExceeded; killed 10 s later). "
        f"Default: the worker's limit ({settings.TASK_SOFT_TIME_LIMIT} s).",
    )
    max_retries: Optional[int] = Field(
        default=None, ge=0, le=50, description="Override the task's automatic retry limit for this job."
    )
    callback_url: Optional[str] = Field(
        default=None,
        max_length=2048,
        examples=["https://example.com/hooks/job-finished"],
        description="Called with a signed POST when the job finishes (see the integration guide).",
    )

    @field_validator("callback_url")
    @classmethod
    def _callback(cls, value: Optional[str]) -> Optional[str]:
        return _check_callback(value)


class JobSubmitRequest(JobOptions):
    """Submit any public task. The generic alternative to the `/tasks/*` endpoints."""

    task_name: str = Field(..., examples=["app.tasks.add"], description=f"One of: {sorted(SCHEDULABLE_TASK_NAMES)}")
    args: list = Field(default_factory=list, examples=[[3, 4]])
    kwargs: dict = Field(default_factory=dict)
    delay_seconds: Optional[int] = Field(
        default=None, ge=1, le=366 * 24 * 3600, description="Start no earlier than this many seconds from now."
    )
    run_at: Optional[datetime] = Field(
        default=None, description="Start no earlier than this time (ISO-8601 with a timezone). Not with delay_seconds."
    )

    @model_validator(mode="after")
    def _validate(self) -> "JobSubmitRequest":
        _check_task_name(self.task_name)
        if self.delay_seconds is not None and self.run_at is not None:
            raise ValueError("Use delay_seconds or run_at, not both")
        if self.run_at is not None and self.run_at.tzinfo is None:
            raise ValueError("run_at must include a timezone offset, e.g. '+00:00'")
        try:
            check_task_arguments(self.task_name, self.args, self.kwargs)
        except TypeError as exc:
            raise ValueError(f"args/kwargs don't match {self.task_name}: {exc}") from exc
        return self

    def start_time(self) -> Optional[datetime]:
        """When the job may start, or None for "now"."""
        now = datetime.now(timezone.utc)
        if self.delay_seconds:
            return now + timedelta(seconds=self.delay_seconds)
        if self.run_at is not None and self.run_at > now:
            return self.run_at.astimezone(timezone.utc)
        return None


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
    tenant: Optional[str] = None
    priority: Optional[Priority] = None
    run_after: Optional[datetime] = Field(default=None, description="Delayed start: not before this time.")
    timeout_seconds: Optional[int] = None
    max_retries: Optional[int] = None
    callback_url: Optional[str] = None
    workflow_id: Optional[str] = None
    workflow_step: Optional[str] = None
    depends_on: Optional[list[str]] = None

    @classmethod
    def from_row(cls, row: dict) -> "JobRecord":
        row = dict(row)
        row["priority"] = db.PRIORITY_NAMES.get(row.get("priority"))
        if row.get("idempotency_key") and row.get("tenant"):  # show the client's own key, not the stored form
            row["idempotency_key"] = row["idempotency_key"].removeprefix(f"tenant:{row['tenant']}:")
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
        default=None,
        examples=["0 8 * * 1-5"],
        description="schedule_type='cron' only. Evaluated in UTC, or in `timezone` if given.",
    )
    timezone: Optional[str] = Field(
        default=None,
        examples=["Asia/Kolkata"],
        description="schedule_type='cron' only. IANA time zone the cron expression is read in "
        "(follows daylight-saving changes). Default UTC.",
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
        if self.timezone is not None:
            if self.schedule_type != "cron":
                raise ValueError("timezone can only be used with schedule_type='cron'")
            if not scheduling.is_valid_timezone(self.timezone):
                raise ValueError(f"Unknown time zone: {self.timezone!r} (use an IANA name like 'Europe/London')")

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
    timezone: Optional[str] = Field(default=None, description="Cron schedules only. IANA time zone, e.g. 'UTC'.")

    @model_validator(mode="after")
    def _validate(self) -> "ScheduleUpdateRequest":
        nulls = sorted(name for name in self.model_fields_set if getattr(self, name) is None)
        if nulls:
            raise ValueError(f"{nulls} cannot be null; omit a field to leave it unchanged")
        if self.cron_expr is not None and not scheduling.is_valid_cron(self.cron_expr):
            raise ValueError(f"Invalid 5-field cron expression: {self.cron_expr!r}")
        if self.timezone is not None and not scheduling.is_valid_timezone(self.timezone):
            raise ValueError(f"Unknown time zone: {self.timezone!r}")
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
    timezone: Optional[str] = Field(default=None, description="Zone the cron expression is read in; null = UTC.")
    tenant: Optional[str] = None
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


# ---------------------------------------------------------------------------
# Dead-letter view
# ---------------------------------------------------------------------------
class FailureGroup(BaseModel):
    """FAILURE jobs not yet re-submitted, with the same task and error."""

    task_name: str
    error: Optional[str]
    count: int
    first_failed_at: Optional[datetime]
    last_failed_at: Optional[datetime]
    example_task_id: str


class FailureGroupsResponse(BaseModel):
    groups: list[FailureGroup]


class BulkRetryRequest(BaseModel):
    """Which failed jobs to re-submit. Omitted filters match everything."""

    task_name: Optional[str] = Field(default=None, examples=["app.tasks.flaky_task"])
    error: Optional[str] = Field(default=None, description="Exact error text, as shown in a failure group.")
    limit: int = Field(default=100, ge=1, le=1000)


class BulkRetryResponse(BaseModel):
    retried: int
    jobs: list[dict] = Field(..., description='{"retry_of": old task_id, "task_id": new task_id} for each.')


# ---------------------------------------------------------------------------
# Webhook deliveries
# ---------------------------------------------------------------------------
class WebhookDelivery(BaseModel):
    id: int
    event: str
    url: str
    status: str = Field(..., description="PENDING | DELIVERED | FAILED")
    attempts: int
    next_attempt_at: datetime
    last_status_code: Optional[int]
    last_error: Optional[str]
    created_at: datetime
    delivered_at: Optional[datetime]


# ---------------------------------------------------------------------------
# Workflows
# ---------------------------------------------------------------------------
_STEP_ID = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")


class WorkflowStep(JobOptions):
    """One step: a task, plus the steps that must succeed before it starts."""

    id: str = Field(..., examples=["extract"], description="Unique within the workflow: letters, digits, _ . -")
    task_name: str = Field(..., examples=["app.tasks.add"])
    args: list = Field(default_factory=list)
    kwargs: dict = Field(default_factory=dict)
    depends_on: list[str] = Field(default_factory=list, examples=[["extract"]])

    @model_validator(mode="after")
    def _validate(self) -> "WorkflowStep":
        if not _STEP_ID.match(self.id):
            raise ValueError(f"step id {self.id!r} must be 1-64 letters, digits, '_', '.' or '-'")
        _check_task_name(self.task_name)
        try:
            check_task_arguments(self.task_name, self.args, self.kwargs)
        except TypeError as exc:
            raise ValueError(f"step {self.id!r}: args/kwargs don't match {self.task_name}: {exc}") from exc
        return self


class WorkflowCreateRequest(BaseModel):
    """
    A set of steps that run in dependency order. Steps with no `depends_on`
    start at once (fan-out); a step with several dependencies starts when all
    of them succeed (fan-in). If a step fails or is cancelled, everything
    downstream of it is skipped (REVOKED).
    """

    name: Optional[str] = Field(default=None, max_length=255, examples=["nightly-report"])
    steps: list[WorkflowStep] = Field(..., min_length=1, max_length=200)
    callback_url: Optional[str] = Field(
        default=None, max_length=2048, description="Called once when every step has finished."
    )

    @field_validator("callback_url")
    @classmethod
    def _callback(cls, value: Optional[str]) -> Optional[str]:
        return _check_callback(value)

    @model_validator(mode="after")
    def _validate(self) -> "WorkflowCreateRequest":
        ids = [step.id for step in self.steps]
        duplicates = sorted({i for i in ids if ids.count(i) > 1})
        if duplicates:
            raise ValueError(f"duplicate step ids: {duplicates}")
        known = set(ids)
        for step in self.steps:
            unknown = sorted(set(step.depends_on) - known)
            if unknown:
                raise ValueError(f"step {step.id!r} depends on unknown step(s) {unknown}")
            if step.id in step.depends_on:
                raise ValueError(f"step {step.id!r} depends on itself")
        cycle = _find_cycle({step.id: step.depends_on for step in self.steps})
        if cycle:
            raise ValueError(f"steps form a cycle: {' -> '.join(cycle)}")
        return self


def _find_cycle(graph: dict[str, list[str]]) -> Optional[list[str]]:
    """Return one dependency cycle as a list of step ids, or None."""
    visiting, done = set(), set()

    def visit(node: str, path: list[str]) -> Optional[list[str]]:
        if node in done:
            return None
        if node in visiting:
            return path[path.index(node) :] + [node]
        visiting.add(node)
        for dep in graph[node]:
            found = visit(dep, path + [node])
            if found:
                return found
        visiting.discard(node)
        done.add(node)
        return None

    for node in graph:
        found = visit(node, [])
        if found:
            return found
    return None


class WorkflowStepStatus(BaseModel):
    id: str
    task_id: str
    task_name: str
    status: str
    depends_on: list[str]
    result: Optional[Any] = None
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None


class WorkflowResponse(BaseModel):
    id: str
    name: Optional[str]
    status: str = Field(..., description="RUNNING | SUCCESS | FAILURE | REVOKED")
    tenant: Optional[str] = None
    callback_url: Optional[str] = None
    created_at: datetime
    finished_at: Optional[datetime] = None
    steps: list[WorkflowStepStatus]

    @classmethod
    def from_row(cls, workflow: dict) -> "WorkflowResponse":
        steps = [
            WorkflowStepStatus(
                id=s["workflow_step"],
                task_id=s["task_id"],
                task_name=s["task_name"],
                status=s["status"],
                depends_on=s["depends_on"] or [],
                result=s["result"],
                error=s["error"],
                started_at=s["started_at"],
                finished_at=s["finished_at"],
            )
            for s in workflow.get("steps", [])
        ]
        return cls.model_validate({**workflow, "steps": steps})


class WorkflowSummary(BaseModel):
    id: str
    name: Optional[str]
    status: str
    tenant: Optional[str] = None
    created_at: datetime
    finished_at: Optional[datetime] = None


# ---------------------------------------------------------------------------
# Admin: managed API keys and the audit log
# ---------------------------------------------------------------------------
class ApiKeyCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=255, examples=["billing-service"])
    scopes: list[str] = Field(..., min_length=1, examples=[["jobs:read", "jobs:write"]], description=str(SCOPES))
    tenant: Optional[str] = Field(
        default=None, max_length=255, examples=["acme"], description="Restrict the key to one tenant's data."
    )
    allowed_tasks: Optional[list[str]] = Field(
        default=None,
        examples=[["app.tasks.send_notification"]],
        description="Only these tasks; null = all public tasks.",
    )
    rate_limit_per_minute: Optional[int] = Field(default=None, ge=1, le=1_000_000)

    @model_validator(mode="after")
    def _validate(self) -> "ApiKeyCreateRequest":
        unknown = sorted(set(self.scopes) - set(SCOPES))
        if unknown:
            raise ValueError(f"unknown scopes {unknown}; valid: {sorted(SCOPES)}")
        if self.allowed_tasks is not None:
            bad = sorted(set(self.allowed_tasks) - SCHEDULABLE_TASK_NAMES)
            if bad:
                raise ValueError(f"unknown tasks {bad}")
        return self


class ApiKeyResponse(BaseModel):
    id: int
    name: str
    key_prefix: str = Field(..., description="First characters of the key, to recognise it.")
    scopes: list[str]
    tenant: Optional[str]
    allowed_tasks: Optional[list[str]]
    rate_limit_per_minute: Optional[int]
    created_at: datetime
    last_used_at: Optional[datetime]
    revoked_at: Optional[datetime]


class ApiKeyCreatedResponse(ApiKeyResponse):
    key: str = Field(..., description="The secret key. Shown only once: store it now.")


class AuditEntry(BaseModel):
    id: int
    at: datetime
    actor: str
    tenant: Optional[str]
    action: str
    resource_type: Optional[str]
    resource_id: Optional[str]
    details: Optional[dict]
    client_ip: Optional[str]
