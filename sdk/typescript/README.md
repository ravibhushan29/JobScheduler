# @jobscheduler/client (TypeScript)

A typed client for the Job Scheduler API v1, with **no runtime dependencies**.
It uses the built-in `fetch` (Node 18+, Deno, Bun). Request and response types
are generated from the API's OpenAPI document (`src/schema.ts`), so they
can't drift from the server.

> **Server-side only.** It needs an API key, which must never be shipped to a
> browser. Your frontend calls your backend, and your backend uses this client
> (see the [integration guide](../../docs/14-integration-guide.md)).

```bash
cd sdk/typescript && npm install && npm run build      # then depend on it from your backend
```

```ts
import { JobScheduler, JobSchedulerError } from "@jobscheduler/client";

const js = new JobScheduler({ baseUrl: process.env.JOB_SCHEDULER_URL, apiKey: process.env.JOB_SCHEDULER_API_KEY });

const { taskId } = await js.submit("app.tasks.add", { args: [3, 4], priority: "high", callback_url: "https://api.example.com/hooks/done" });
const final = await js.wait(taskId);                 // or: for await (const s of js.events(taskId)) { … }
console.log(final.state, final.result);              // SUCCESS 7

await js.createSchedule({ task_name: "app.tasks.heartbeat", schedule_type: "cron", cron_expr: "0 9 * * *", timezone: "Asia/Kolkata" });

const wf = await js.createWorkflow([
  { id: "a", task_name: "app.tasks.add", args: [1, 2] },
  { id: "b", task_name: "app.tasks.add", args: [3, 4] },
  { id: "c", task_name: "app.tasks.add", args: [5, 6], depends_on: ["a", "b"] },
]);
console.log((await js.waitWorkflow(wf.id)).status);
```

**Safe by default.** Every write sends an `Idempotency-Key` (a new UUID unless
you pass `idempotencyKey`). Network errors, `429` (honouring `Retry-After`)
and `5xx` are retried with the *same* key. Writes without a key (cancel, schedule
updates) are not retried blindly.

**Errors** are `JobSchedulerError` with `.status`, `.type` (branch on this),
`.details` (field errors on `422`) and `.retryable`.

**Webhook receivers:** `await verifyWebhook(rawBody, headers, secret)` checks
the signature and rejects replays older than 5 minutes.

| Script | |
|---|---|
| `npm run generate` | Regenerate `src/schema.ts` from `../openapi.json` (`python scripts/export_openapi.py` refreshes that) |
| `npm run build` | Compile to `dist/` |
| `npm test` | Unit tests; set `JOB_SCHEDULER_URL` and `JOB_SCHEDULER_API_KEY` to also run the live test |
