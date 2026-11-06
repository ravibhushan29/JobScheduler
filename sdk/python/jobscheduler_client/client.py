"""The client. Only dependency: httpx."""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, Optional

import httpx

#: Job states after which nothing changes.
FINISHED = frozenset({"SUCCESS", "FAILURE", "REVOKED"})

_RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class JobSchedulerError(Exception):
    """An error answer from the API. Branch on `type` (e.g. "validation_error"), not on the message."""

    def __init__(self, status: int, type: str, message: str, details: Any = None, retry_after: Optional[float] = None):
        super().__init__(f"{status} {type}: {message}")
        self.status, self.type, self.message, self.details, self.retry_after = (
            status,
            type,
            message,
            details,
            retry_after,
        )

    @property
    def retryable(self) -> bool:
        return self.status == 0 or self.status in _RETRYABLE_STATUS


@dataclass(frozen=True)
class Submitted:
    task_id: str
    replayed: bool  # True: this Idempotency-Key was used before; the original job was returned
    run_after: Optional[str] = None


class JobScheduler:
    """
    A synchronous client. Thread-safe (httpx.Client is). Use as a context
    manager, or call `close()`.

    Args:
        base_url: e.g. "https://jobs.example.com" (without /v1).
        api_key: sent as X-API-Key.
        timeout: per-request timeout in seconds.
        retries: attempts for retryable failures (network, 429, 5xx).
        http: an existing httpx.Client to use instead (its base_url is respected).
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        api_key: Optional[str] = None,
        *,
        timeout: float = 10.0,
        retries: int = 4,
        http: Optional[httpx.Client] = None,
    ):
        self._owns_http = http is None
        self._http = http or httpx.Client(base_url=base_url.rstrip("/"), timeout=timeout)
        self._headers = {"X-API-Key": api_key} if api_key else {}
        self._retries = max(retries, 1)

    def __enter__(self) -> "JobScheduler":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_http:
            self._http.close()

    # ------------------------------------------------------------------ core
    def request(
        self,
        method: str,
        path: str,
        *,
        json_body: Any = None,
        params: Optional[dict] = None,
        idempotency_key: Optional[str] = None,
    ) -> httpx.Response:
        """Send one API call (path relative to /v1), retrying retryable failures with backoff."""
        headers = dict(self._headers)
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        params = {k: v for k, v in (params or {}).items() if v is not None}
        delay = 0.5
        for attempt in range(1, self._retries + 1):
            try:
                response = self._http.request(method, "/v1" + path, json=json_body, params=params, headers=headers)
            except httpx.TransportError as exc:
                error = JobSchedulerError(0, "network_error", str(exc))
            else:
                if response.is_success:
                    return response
                error = self._error(response)
            # Only retry what is safe: reads, and writes that carry an idempotency key.
            safe = method == "GET" or idempotency_key is not None
            if not (error.retryable and safe) or attempt == self._retries:
                raise error
            time.sleep(error.retry_after if error.retry_after else delay)
            delay = min(delay * 2, 8)
        raise AssertionError("unreachable")

    @staticmethod
    def _error(response: httpx.Response) -> JobSchedulerError:
        try:
            body = response.json().get("error", {})
        except (ValueError, AttributeError):
            body = {}
        retry_after = response.headers.get("Retry-After")
        return JobSchedulerError(
            response.status_code,
            body.get("type", "http_error"),
            body.get("message", response.text[:200]),
            body.get("details"),
            float(retry_after) if retry_after and retry_after.isdigit() else None,
        )

    # ------------------------------------------------------------------ jobs
    def submit(
        self,
        task_name: str,
        args: Optional[list] = None,
        kwargs: Optional[dict] = None,
        *,
        priority: str = "normal",
        delay_seconds: Optional[int] = None,
        run_at: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        max_retries: Optional[int] = None,
        callback_url: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> Submitted:
        """Queue a task. Safe to call again with the same `idempotency_key`."""
        body = {
            "task_name": task_name,
            "args": args or [],
            "kwargs": kwargs or {},
            "priority": priority,
            "delay_seconds": delay_seconds,
            "run_at": run_at,
            "timeout_seconds": timeout_seconds,
            "max_retries": max_retries,
            "callback_url": callback_url,
        }
        response = self.request(
            "POST",
            "/jobs",
            json_body={k: v for k, v in body.items() if v is not None},
            idempotency_key=idempotency_key or str(uuid.uuid4()),
        )
        data = response.json()
        return Submitted(data["task_id"], response.headers.get("Idempotent-Replayed") == "true", data.get("run_after"))

    def status(self, task_id: str) -> dict:
        """Live state: PENDING | STARTED | PROGRESS | RETRY | SUCCESS | FAILURE | REVOKED, plus result/error."""
        return self.request("GET", f"/jobs/{task_id}/status").json()

    def job(self, task_id: str) -> dict:
        """The full job record (history, options, attempts, timestamps)."""
        return self.request("GET", f"/jobs/{task_id}").json()

    def jobs(self, **filters) -> dict:
        """One page of jobs: filters status, task_name, workflow_id, limit, offset."""
        return self.request("GET", "/jobs", params=filters).json()

    def cancel(self, task_id: str) -> dict:
        return self.request("DELETE", f"/jobs/{task_id}").json()

    def retry(self, task_id: str, *, force: bool = False, idempotency_key: Optional[str] = None) -> Submitted:
        response = self.request(
            "POST",
            f"/jobs/{task_id}/retry",
            params={"force": str(force).lower()},
            idempotency_key=idempotency_key or str(uuid.uuid4()),
        )
        return Submitted(response.json()["task_id"], response.headers.get("Idempotent-Replayed") == "true")

    def wait(self, task_id: str, *, timeout: float = 600, poll_max: float = 5.0) -> dict:
        """Poll `status` (0.5 s doubling to `poll_max`) until the job finishes. Raises TimeoutError."""
        deadline = time.monotonic() + timeout
        delay = 0.5
        while True:
            state = self.status(task_id)
            if state["state"] in FINISHED:
                return state
            if time.monotonic() + delay > deadline:
                raise TimeoutError(f"job {task_id} still {state['state']} after {timeout} s")
            time.sleep(delay)
            delay = min(delay * 2, poll_max)

    def events(self, task_id: str, *, timeout: float = 3600) -> Iterator[dict]:
        """Yield each state change from the server-sent event stream until the job finishes."""
        with self._http.stream(
            "GET", f"/v1/jobs/{task_id}/events", headers=self._headers, timeout=httpx.Timeout(timeout, connect=10)
        ) as response:
            if not response.is_success:
                response.read()
                raise self._error(response)
            for line in response.iter_lines():
                if line.startswith("data: "):
                    yield json.loads(line[6:])

    def failures(self, limit: int = 50) -> list[dict]:
        """Failed jobs not yet re-submitted, grouped by task and error."""
        return self.request("GET", "/jobs/failures", params={"limit": limit}).json()["groups"]

    def retry_failures(self, task_name: Optional[str] = None, error: Optional[str] = None, limit: int = 100) -> dict:
        body = {"task_name": task_name, "error": error, "limit": limit}
        return self.request(
            "POST", "/jobs/failures/retry", json_body={k: v for k, v in body.items() if v is not None}
        ).json()

    # ------------------------------------------------------------------ schedules
    def create_schedule(
        self, task_name: str, schedule_type: str, *, idempotency_key: Optional[str] = None, **fields
    ) -> dict:
        """fields: args, kwargs, run_at, interval_seconds, cron_expr, timezone."""
        body = {"task_name": task_name, "schedule_type": schedule_type, **fields}
        return self.request(
            "POST", "/schedules", json_body=body, idempotency_key=idempotency_key or str(uuid.uuid4())
        ).json()

    def schedules(self, **filters) -> list[dict]:
        return self.request("GET", "/schedules", params=filters).json()

    def update_schedule(self, schedule_id: int, **changes) -> dict:
        """changes: enabled, interval_seconds, cron_expr, timezone."""
        return self.request("PATCH", f"/schedules/{schedule_id}", json_body=changes).json()

    def delete_schedule(self, schedule_id: int) -> dict:
        return self.request("DELETE", f"/schedules/{schedule_id}").json()

    # ------------------------------------------------------------------ workflows
    def create_workflow(
        self,
        steps: list[dict],
        *,
        name: Optional[str] = None,
        callback_url: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> dict:
        """steps: [{"id", "task_name", "args", "kwargs", "depends_on", ...options}]."""
        body = {"steps": steps, "name": name, "callback_url": callback_url}
        return self.request(
            "POST",
            "/workflows",
            json_body={k: v for k, v in body.items() if v is not None},
            idempotency_key=idempotency_key or str(uuid.uuid4()),
        ).json()

    def workflow(self, workflow_id: str) -> dict:
        return self.request("GET", f"/workflows/{workflow_id}").json()

    def cancel_workflow(self, workflow_id: str) -> dict:
        return self.request("DELETE", f"/workflows/{workflow_id}").json()

    def wait_workflow(self, workflow_id: str, *, timeout: float = 3600, poll_max: float = 5.0) -> dict:
        deadline = time.monotonic() + timeout
        delay = 0.5
        while True:
            workflow = self.workflow(workflow_id)
            if workflow["status"] != "RUNNING":
                return workflow
            if time.monotonic() + delay > deadline:
                raise TimeoutError(f"workflow {workflow_id} still running after {timeout} s")
            time.sleep(delay)
            delay = min(delay * 2, poll_max)
