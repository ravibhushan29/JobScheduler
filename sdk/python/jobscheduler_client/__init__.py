"""
Python client for the Job Scheduler API (v1).

    from jobscheduler_client import JobScheduler

    js = JobScheduler("https://jobs.example.com", api_key="jsk_...")
    job = js.submit("app.tasks.add", args=[3, 4], priority="high")
    final = js.wait(job.task_id)          # polls with backoff until SUCCESS / FAILURE / REVOKED
    print(final["state"], final["result"])

Every write sends an Idempotency-Key (generated if you don't pass one) and is
retried on network errors, 429, 500 and 503 with that same key, so a retry
never creates a second job.
"""

from jobscheduler_client.client import FINISHED, JobScheduler, JobSchedulerError, Submitted
from jobscheduler_client.webhooks import verify_webhook

__all__ = ["FINISHED", "JobScheduler", "JobSchedulerError", "Submitted", "verify_webhook"]
__version__ = "5.0.0"
