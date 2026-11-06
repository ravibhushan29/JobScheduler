/**
 * TypeScript client for the Job Scheduler API (v1). No dependencies: uses the
 * built-in `fetch` (Node 18+, Deno, Bun, modern browsers behind your backend).
 *
 *   const js = new JobScheduler({ baseUrl: "https://jobs.example.com", apiKey: process.env.JOB_SCHEDULER_API_KEY });
 *   const { taskId } = await js.submit("app.tasks.add", { args: [3, 4], priority: "high" });
 *   const final = await js.wait(taskId);        // SUCCESS / FAILURE / REVOKED
 *
 * Every write sends an Idempotency-Key (a new UUID unless you pass one) and is
 * retried on network errors, 429, 500 and 503 with that same key, so a retry
 * never creates a second job. Keep the API key on the server: never ship it to a browser.
 */
import type { components } from "./schema.js";

export type JobStatus = components["schemas"]["TaskStatusResponse"];
export type JobRecord = components["schemas"]["JobRecord"];
export type JobList = components["schemas"]["JobListResponse"];
export type Schedule = components["schemas"]["ScheduleResponse"];
export type ScheduleCreate = components["schemas"]["ScheduleCreateRequest"];
export type ScheduleUpdate = components["schemas"]["ScheduleUpdateRequest"];
export type Workflow = components["schemas"]["WorkflowResponse"];
export type WorkflowStep = components["schemas"]["WorkflowStep"];
export type FailureGroup = components["schemas"]["FailureGroup"];
export type JobSubmit = components["schemas"]["JobSubmitRequest"];
export type { components };

export type JobState = "PENDING" | "STARTED" | "PROGRESS" | "RETRY" | "SUCCESS" | "FAILURE" | "REVOKED";
export const FINISHED: ReadonlySet<string> = new Set(["SUCCESS", "FAILURE", "REVOKED"]);

/** An error answer from the API. Branch on `type`, not on the message. */
export class JobSchedulerError extends Error {
  readonly status: number;
  readonly type: string;
  readonly details: unknown;
  readonly retryAfter: number | null;

  constructor(status: number, type: string, message: string, details: unknown = null, retryAfter: number | null = null) {
    super(`${status} ${type}: ${message}`);
    this.name = "JobSchedulerError";
    this.status = status;
    this.type = type;
    this.details = details;
    this.retryAfter = retryAfter;
  }

  /** Network errors, 429 and 5xx may succeed if tried again later. */
  get retryable(): boolean {
    return this.status === 0 || this.status === 429 || this.status >= 500;
  }
}

export interface Submitted {
  taskId: string;
  /** True: this Idempotency-Key was used before and the original job was returned. */
  replayed: boolean;
  runAfter: string | null;
}

export interface Options {
  baseUrl?: string;
  apiKey?: string;
  /** Per-request timeout in ms (default 10 s). */
  timeoutMs?: number;
  /** Attempts for retryable failures (default 4). */
  retries?: number;
  /** Inject a fetch implementation (tests, custom agents). */
  fetch?: typeof fetch;
}

type SubmitOptions = Partial<Omit<JobSubmit, "task_name" | "args" | "kwargs">> & {
  args?: unknown[];
  kwargs?: Record<string, unknown>;
  idempotencyKey?: string;
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const newKey = () => globalThis.crypto.randomUUID();

export class JobScheduler {
  private readonly baseUrl: string;
  private readonly headers: Record<string, string>;
  private readonly timeoutMs: number;
  private readonly retries: number;
  private readonly fetchImpl: typeof fetch;

  constructor(options: Options = {}) {
    this.baseUrl = (options.baseUrl ?? "http://localhost:8000").replace(/\/+$/, "");
    this.headers = options.apiKey ? { "X-API-Key": options.apiKey } : {};
    this.timeoutMs = options.timeoutMs ?? 10_000;
    this.retries = Math.max(options.retries ?? 4, 1);
    this.fetchImpl = options.fetch ?? globalThis.fetch.bind(globalThis);
  }

  /** One API call (path relative to /v1), retrying retryable failures when that is safe. */
  async request<T>(
    method: string,
    path: string,
    init: { body?: unknown; query?: Record<string, string | number | boolean | undefined>; idempotencyKey?: string } = {},
  ): Promise<{ data: T; headers: Headers }> {
    const url = new URL(this.baseUrl + "/v1" + path);
    for (const [k, v] of Object.entries(init.query ?? {})) if (v !== undefined) url.searchParams.set(k, String(v));
    const headers: Record<string, string> = { ...this.headers };
    if (init.body !== undefined) headers["Content-Type"] = "application/json";
    if (init.idempotencyKey) headers["Idempotency-Key"] = init.idempotencyKey;
    // Repeating is safe for reads, and for writes that carry an idempotency key.
    const safe = method === "GET" || Boolean(init.idempotencyKey);

    let delay = 500;
    for (let attempt = 1; ; attempt++) {
      let error: JobSchedulerError;
      try {
        const res = await this.fetchImpl(url, {
          method,
          headers,
          body: init.body === undefined ? undefined : JSON.stringify(init.body),
          signal: AbortSignal.timeout(this.timeoutMs),
        });
        const data: any = await res.json().catch(() => null);
        if (res.ok) return { data: data as T, headers: res.headers };
        const e = data?.error ?? {};
        const retryAfter = Number(res.headers.get("Retry-After"));
        error = new JobSchedulerError(res.status, e.type ?? "http_error", e.message ?? res.statusText, e.details ?? null, retryAfter || null);
      } catch (cause) {
        if (cause instanceof JobSchedulerError) throw cause;
        error = new JobSchedulerError(0, "network_error", String(cause));
      }
      if (!error.retryable || !safe || attempt >= this.retries) throw error;
      await sleep(error.retryAfter ? error.retryAfter * 1000 : delay);
      delay = Math.min(delay * 2, 8000);
    }
  }

  // ------------------------------------------------------------------ jobs
  async submit(taskName: string, options: SubmitOptions = {}): Promise<Submitted> {
    const { args = [], kwargs = {}, idempotencyKey = newKey(), ...rest } = options;
    const { data, headers } = await this.request<{ task_id: string; run_after?: string | null }>("POST", "/jobs", {
      body: { task_name: taskName, args, kwargs, ...rest },
      idempotencyKey,
    });
    return { taskId: data.task_id, replayed: headers.get("Idempotent-Replayed") === "true", runAfter: data.run_after ?? null };
  }

  async status(taskId: string): Promise<JobStatus> {
    return (await this.request<JobStatus>("GET", `/jobs/${encodeURIComponent(taskId)}/status`)).data;
  }

  async job(taskId: string): Promise<JobRecord> {
    return (await this.request<JobRecord>("GET", `/jobs/${encodeURIComponent(taskId)}`)).data;
  }

  async jobs(query: { status?: string; task_name?: string; workflow_id?: string; limit?: number; offset?: number } = {}): Promise<JobList> {
    return (await this.request<JobList>("GET", "/jobs", { query })).data;
  }

  async cancel(taskId: string): Promise<void> {
    await this.request("DELETE", `/jobs/${encodeURIComponent(taskId)}`);
  }

  async retry(taskId: string, { force = false, idempotencyKey = newKey() } = {}): Promise<Submitted> {
    const { data, headers } = await this.request<{ task_id: string }>("POST", `/jobs/${encodeURIComponent(taskId)}/retry`, {
      query: { force },
      idempotencyKey,
    });
    return { taskId: data.task_id, replayed: headers.get("Idempotent-Replayed") === "true", runAfter: null };
  }

  /** Poll until the job finishes (0.5 s doubling to 5 s). Network blips while polling are ignored. */
  async wait(taskId: string, { timeoutMs = 600_000, onUpdate }: { timeoutMs?: number; onUpdate?: (s: JobStatus) => void } = {}): Promise<JobStatus> {
    const deadline = Date.now() + timeoutMs;
    let delay = 500;
    for (;;) {
      try {
        const status = await this.status(taskId);
        onUpdate?.(status);
        if (FINISHED.has(status.state)) return status;
      } catch (err) {
        if (!(err instanceof JobSchedulerError) || !err.retryable) throw err;
      }
      if (Date.now() + delay > deadline) throw new Error(`Job ${taskId} still running after ${timeoutMs} ms`);
      await sleep(delay);
      delay = Math.min(delay * 2, 5000);
    }
  }

  /** Each state change from the server-sent event stream, until the job finishes. */
  async *events(taskId: string): AsyncGenerator<JobStatus> {
    const res = await this.fetchImpl(`${this.baseUrl}/v1/jobs/${encodeURIComponent(taskId)}/events`, { headers: this.headers });
    if (!res.ok || !res.body) {
      const e = ((await res.json().catch(() => null)) as any)?.error ?? {};
      throw new JobSchedulerError(res.status, e.type ?? "http_error", e.message ?? res.statusText);
    }
    const reader = res.body.pipeThrough(new TextDecoderStream()).getReader();
    let buffer = "";
    for (;;) {
      const { value, done } = await reader.read();
      if (done) return;
      buffer += value;
      let end: number;
      while ((end = buffer.indexOf("\n\n")) >= 0) {
        const chunk = buffer.slice(0, end);
        buffer = buffer.slice(end + 2);
        const data = chunk.split("\n").find((line) => line.startsWith("data: "));
        if (data) yield JSON.parse(data.slice(6)) as JobStatus;
      }
    }
  }

  async failures(limit = 50): Promise<FailureGroup[]> {
    return (await this.request<{ groups: FailureGroup[] }>("GET", "/jobs/failures", { query: { limit } })).data.groups;
  }

  async retryFailures(filter: { task_name?: string; error?: string; limit?: number } = {}) {
    return (await this.request<{ retried: number; jobs: { retry_of: string; task_id: string }[] }>("POST", "/jobs/failures/retry", { body: filter })).data;
  }

  // ------------------------------------------------------------------ schedules
  async createSchedule(body: ScheduleCreate, idempotencyKey: string = newKey()): Promise<Schedule> {
    return (await this.request<Schedule>("POST", "/schedules", { body, idempotencyKey })).data;
  }

  async schedules(query: { limit?: number; offset?: number; enabled_only?: boolean } = {}): Promise<Schedule[]> {
    return (await this.request<Schedule[]>("GET", "/schedules", { query })).data;
  }

  async updateSchedule(id: number, changes: ScheduleUpdate): Promise<Schedule> {
    return (await this.request<Schedule>("PATCH", `/schedules/${id}`, { body: changes })).data;
  }

  async deleteSchedule(id: number): Promise<void> {
    await this.request("DELETE", `/schedules/${id}`);
  }

  // ------------------------------------------------------------------ workflows
  async createWorkflow(
    steps: WorkflowStep[] | Array<Partial<WorkflowStep> & Pick<WorkflowStep, "id" | "task_name">>,
    { name, callbackUrl, idempotencyKey = newKey() }: { name?: string; callbackUrl?: string; idempotencyKey?: string } = {},
  ): Promise<Workflow> {
    const body = { steps, ...(name ? { name } : {}), ...(callbackUrl ? { callback_url: callbackUrl } : {}) };
    return (await this.request<Workflow>("POST", "/workflows", { body, idempotencyKey })).data;
  }

  async workflow(id: string): Promise<Workflow> {
    return (await this.request<Workflow>("GET", `/workflows/${encodeURIComponent(id)}`)).data;
  }

  async cancelWorkflow(id: string): Promise<Workflow> {
    return (await this.request<Workflow>("DELETE", `/workflows/${encodeURIComponent(id)}`)).data;
  }

  async waitWorkflow(id: string, { timeoutMs = 3_600_000 } = {}): Promise<Workflow> {
    const deadline = Date.now() + timeoutMs;
    let delay = 500;
    for (;;) {
      const wf = await this.workflow(id);
      if (wf.status !== "RUNNING") return wf;
      if (Date.now() + delay > deadline) throw new Error(`Workflow ${id} still running after ${timeoutMs} ms`);
      await sleep(delay);
      delay = Math.min(delay * 2, 5000);
    }
  }
}

/**
 * Verify a webhook (Node). `rawBody` must be the exact bytes received.
 * Rejects bad signatures and anything older than `toleranceSeconds` (replays).
 */
export async function verifyWebhook(
  rawBody: string | Uint8Array,
  headers: Record<string, string | string[] | undefined> | Headers,
  secret: string,
  toleranceSeconds = 300,
): Promise<boolean> {
  const get = (name: string): string => {
    if (headers instanceof Headers) return headers.get(name) ?? "";
    const value = headers[name] ?? headers[name.toLowerCase()];
    return Array.isArray(value) ? value[0] ?? "" : value ?? "";
  };
  const timestamp = get("X-JobScheduler-Timestamp");
  const signature = get("X-JobScheduler-Signature");
  if (!/^\d+$/.test(timestamp) || Math.abs(Date.now() / 1000 - Number(timestamp)) > toleranceSeconds) return false;
  const { createHmac, timingSafeEqual } = await import("node:crypto");
  const body = typeof rawBody === "string" ? Buffer.from(rawBody) : Buffer.from(rawBody);
  const expected = "sha256=" + createHmac("sha256", secret).update(Buffer.concat([Buffer.from(timestamp + "."), body])).digest("hex");
  return expected.length === signature.length && timingSafeEqual(Buffer.from(expected), Buffer.from(signature));
}
