"""
Prometheus metrics, served by the API at GET /metrics.

Two kinds:

* **Cluster-wide** (prefix `jobscheduler_`, no per-process labels): read from
  the shared database (and Redis queue lengths) when Prometheus scrapes,
  cached for METRICS_CACHE_SECONDS. Every API replica reports the same values,
  so dashboards use `max()` across instances rather than `sum()`.
* **Per API process**: HTTP request counts and latencies
  (`jobscheduler_http_*`), summed across instances as usual.

Nothing is collected inside workers or schedulers: what they do is visible in
the database (job rows) or in `event_counters`, which they increment.
"""

import logging
import threading
import time
from collections import defaultdict
from typing import Optional

from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Counter, Histogram, generate_latest
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, InfoMetricFamily

from app import db
from app.config import settings

logger = logging.getLogger(__name__)

registry = CollectorRegistry()

HTTP_REQUESTS = Counter(
    "jobscheduler_http_requests",
    "HTTP requests handled by this API process.",
    ["method", "route", "status"],
    registry=registry,
)
HTTP_LATENCY = Histogram(
    "jobscheduler_http_request_duration_seconds",
    "Time to handle an HTTP request in this API process.",
    ["method", "route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
    registry=registry,
)

#: Event counters the services increment (see db.increment_counter), with their help text.
EVENTS = {
    "schedule_fired": "A schedule fired (one job created).",
    "duplicate_dropped": "A duplicate message for a finished job was dropped by the execution guard.",
    "duplicate_deferred": "A duplicate message for a running job was deferred by the execution guard.",
    "worker_lost_recovered": "A job whose worker died was re-published for another worker.",
    "lease_lost": "A running worker lost its lease to another worker.",
    "outbox_relayed": "A job saved but never published was published by the outbox relay.",
    "webhook_delivered": "A webhook was delivered (2xx).",
    "webhook_failed": "A webhook gave up after its last attempt.",
    "rate_limited": "A request was rejected with 429.",
}


def _quantiles(values: list[float], qs=(0.5, 0.95, 0.99)) -> dict[float, float]:
    if not values:
        return {}
    ordered = sorted(values)
    return {q: ordered[min(int(q * len(ordered)), len(ordered) - 1)] for q in qs}


def _queue_lengths() -> dict[str, int]:
    """Messages waiting in Redis per queue (all priority levels), or {} if the broker isn't Redis."""
    from app import ratelimit  # reuses its Redis client

    client = ratelimit._redis()
    if client is None:
        return {}
    sep, steps = "\x06\x16", (0, 3, 6, 9)  # kombu's defaults (see app.db.PRIORITIES)
    lengths = {}
    try:
        pipe = client.pipeline()
        queues = (settings.DEFAULT_QUEUE_NAME, settings.LONG_TASK_QUEUE_NAME)
        for queue in queues:
            for step in steps:
                pipe.llen(queue if step == 0 else f"{queue}{sep}{step}")
        sizes = pipe.execute()
        for i, queue in enumerate(queues):
            lengths[queue] = sum(sizes[i * len(steps) : (i + 1) * len(steps)])
    except Exception as exc:
        logger.warning("Could not read queue lengths from Redis: %s", exc)
    return lengths


class ClusterCollector:
    """Builds the cluster-wide metrics from a cached database snapshot."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cached: Optional[tuple[float, list]] = None

    def describe(self):
        return []  # don't query the database when the collector is registered (at import)

    def collect(self):
        with self._lock:
            if self._cached and time.monotonic() - self._cached[0] < settings.METRICS_CACHE_SECONDS:
                return iter(self._cached[1])
            try:
                families = self._build()
            except Exception:
                logger.exception("Could not collect cluster metrics")
                up = GaugeMetricFamily("jobscheduler_metrics_up", "1 if cluster metrics could be read.")
                up.add_metric([], 0)
                return iter([up])
            self._cached = (time.monotonic(), families)
            return iter(families)

    def _build(self) -> list:
        snap = db.run_sync(db.metrics_snapshot(settings.METRICS_WINDOW_SECONDS))
        now = snap["now"]
        window = settings.METRICS_WINDOW_SECONDS
        out = []

        info = InfoMetricFamily("jobscheduler_build", "Version of the service reporting these metrics.")
        info.add_metric([], {"version": settings.APP_VERSION})
        out.append(info)
        up = GaugeMetricFamily("jobscheduler_metrics_up", "1 if cluster metrics could be read.")
        up.add_metric([], 1)
        out.append(up)

        jobs = GaugeMetricFamily(
            "jobscheduler_jobs",
            "Jobs in the job history, by status, queue and task.",
            labels=["status", "queue", "task"],
        )
        for row in snap["jobs"]:
            jobs.add_metric([row["status"], row["queue"] or "", row["task_name"]], row["count"])
        out.append(jobs)

        finished = GaugeMetricFamily(
            "jobscheduler_jobs_finished_recent",
            f"Jobs that finished in the last {window} s, by task and final status.",
            labels=["task", "status"],
        )
        for row in snap["finished_recent"]:
            finished.add_metric([row["task_name"], row["status"]], row["count"])
        out.append(finished)

        durations, waits = defaultdict(list), defaultdict(list)
        for row in snap["timings_recent"]:
            durations[row["task_name"]].append((row["finished_at"] - row["started_at"]).total_seconds())
            ready_at = row["run_after"] or row["submitted_at"]
            waits[row["queue"] or ""].append(max((row["started_at"] - ready_at).total_seconds(), 0))
        duration = GaugeMetricFamily(
            "jobscheduler_job_duration_seconds",
            f"Run time of jobs finished in the last {window} s (quantiles), by task.",
            labels=["task", "quantile"],
        )
        for task, values in durations.items():
            for q, value in _quantiles(values).items():
                duration.add_metric([task, str(q)], value)
        out.append(duration)
        wait = GaugeMetricFamily(
            "jobscheduler_job_wait_seconds",
            f"Time from ready to started, for jobs finished in the last {window} s (quantiles), by queue.",
            labels=["queue", "quantile"],
        )
        for queue, values in waits.items():
            for q, value in _quantiles(values).items():
                wait.add_metric([queue, str(q)], value)
        out.append(wait)

        oldest = GaugeMetricFamily(
            "jobscheduler_oldest_queued_job_age_seconds",
            "Age of the oldest published job no worker has started yet, by queue.",
            labels=["queue"],
        )
        for row in snap["oldest_queued"]:
            oldest.add_metric([row["queue"] or ""], max((now - row["oldest"]).total_seconds(), 0))
        out.append(oldest)

        outbox = GaugeMetricFamily("jobscheduler_outbox_pending", "Due jobs saved but not yet published.")
        outbox.add_metric([], snap["outbox_pending"])
        out.append(outbox)
        outbox_age = GaugeMetricFamily(
            "jobscheduler_outbox_oldest_age_seconds", "Age of the oldest due job saved but not yet published."
        )
        outbox_age.add_metric([], (now - snap["outbox_oldest"]).total_seconds() if snap["outbox_oldest"] else 0)
        out.append(outbox_age)
        delayed = GaugeMetricFamily("jobscheduler_jobs_delayed", "Jobs waiting for their delayed start time.")
        delayed.add_metric([], snap["delayed"])
        out.append(delayed)

        schedules = GaugeMetricFamily("jobscheduler_schedules", "Schedules, by enabled state.", labels=["enabled"])
        for row in snap["schedules"]:
            schedules.add_metric([str(bool(row["enabled"])).lower()], row["count"])
        out.append(schedules)
        overdue = GaugeMetricFamily(
            "jobscheduler_schedule_max_overdue_seconds",
            "How late the most overdue enabled schedule is (0 = none overdue). "
            "A few seconds is normal; minutes means no scheduler is running.",
        )
        most_overdue = snap["most_overdue_schedule"]
        overdue.add_metric([], max((now - most_overdue).total_seconds(), 0) if most_overdue else 0)
        out.append(overdue)

        webhooks = GaugeMetricFamily("jobscheduler_webhooks", "Webhook deliveries, by status.", labels=["status"])
        for row in snap["webhooks"]:
            webhooks.add_metric([row["status"]], row["count"])
        out.append(webhooks)
        workflows = GaugeMetricFamily("jobscheduler_workflows", "Workflows, by status.", labels=["status"])
        for row in snap["workflows"]:
            workflows.add_metric([row["status"]], row["count"])
        out.append(workflows)

        events = CounterMetricFamily(
            "jobscheduler_events",
            "Cluster-wide events: " + "; ".join(f"{k}: {v}" for k, v in EVENTS.items()),
            labels=["event"],
        )
        for name in EVENTS:
            events.add_metric([name], snap["counters"].get(name, 0))
        out.append(events)

        queue_messages = GaugeMetricFamily(
            "jobscheduler_queue_messages",
            "Messages waiting in the broker, by queue (all priorities).",
            labels=["queue"],
        )
        for queue, length in _queue_lengths().items():
            queue_messages.add_metric([queue], length)
        out.append(queue_messages)
        return out


registry.register(ClusterCollector())


def render() -> tuple[bytes, str]:
    """The /metrics response body and content type."""
    return generate_latest(registry), CONTENT_TYPE_LATEST
