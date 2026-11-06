# jobscheduler-client (Python)

A small, typed client for the Job Scheduler API v1. One dependency: `httpx`.

```bash
pip install ./sdk/python          # from this repository
```

```python
from jobscheduler_client import JobScheduler, JobSchedulerError

with JobScheduler("https://jobs.example.com", api_key="jsk_...") as js:
    job = js.submit("app.tasks.add", args=[3, 4], priority="high")
    final = js.wait(job.task_id)                 # or: for event in js.events(job.task_id): ...
    print(final["state"], final["result"])       # SUCCESS 7.0

    js.create_schedule("app.tasks.heartbeat", "cron", cron_expr="0 9 * * *", timezone="Europe/London")

    wf = js.create_workflow([
        {"id": "a", "task_name": "app.tasks.add", "args": [1, 2]},
        {"id": "b", "task_name": "app.tasks.add", "args": [3, 4]},
        {"id": "c", "task_name": "app.tasks.add", "args": [5, 6], "depends_on": ["a", "b"]},
    ])
    print(js.wait_workflow(wf["id"])["status"])
```

**Safe by default.** Every write sends an `Idempotency-Key` (a new UUID unless
you pass one) and retries network errors, `429` (honouring `Retry-After`),
`500` and `503` with the *same* key, so a retry never creates a duplicate.

**Errors** raise `JobSchedulerError` with `.status`, `.type` (branch on this),
`.message`, `.details` (field errors on `422`) and `.retryable`.

**Webhook receivers** can check signatures with `verify_webhook(raw_body, headers, secret)`.

Full guide: [docs/14-integration-guide.md](../../docs/14-integration-guide.md).
