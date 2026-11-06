// Unit tests with a fake fetch, plus a live test when JOB_SCHEDULER_URL is set.
import { test } from "node:test";
import assert from "node:assert/strict";
import { createHmac } from "node:crypto";
import { JobScheduler, JobSchedulerError, verifyWebhook } from "../dist/index.js";

const json = (status, body, headers = {}) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json", ...headers } });

test("retries 503 with the same idempotency key", async () => {
  const keys = [];
  const fetch = async (url, init) => {
    keys.push(init.headers["Idempotency-Key"]);
    return keys.length < 3 ? json(503, { error: { type: "service_unavailable", message: "down" } }) : json(201, { task_id: "t1" });
  };
  const js = new JobScheduler({ fetch, retries: 4 });
  const out = await js.submit("app.tasks.add", { args: [1, 2] });
  assert.equal(out.taskId, "t1");
  assert.equal(keys.length, 3);
  assert.equal(new Set(keys).size, 1);
});

test("honours Retry-After on 429", async () => {
  let calls = 0;
  const fetch = async () => (++calls === 1 ? json(429, { error: { type: "rate_limited", message: "slow" } }, { "Retry-After": "1" }) : json(201, { task_id: "t2" }));
  const started = Date.now();
  await new JobScheduler({ fetch }).submit("app.tasks.add", { args: [1, 2] });
  assert.ok(Date.now() - started >= 900);
});

test("does not retry 4xx, and errors are typed", async () => {
  let calls = 0;
  const fetch = async () => (calls++, json(422, { error: { type: "validation_error", message: "bad", details: [{ loc: ["body", "y"] }] } }));
  await assert.rejects(new JobScheduler({ fetch }).submit("app.tasks.add", { args: [1] }), (err) => {
    assert.ok(err instanceof JobSchedulerError);
    assert.equal(err.type, "validation_error");
    assert.equal(err.retryable, false);
    return true;
  });
  assert.equal(calls, 1);
});

test("DELETE without a key is not retried", async () => {
  let calls = 0;
  const fetch = async () => (calls++, json(503, { error: { type: "service_unavailable", message: "x" } }));
  await assert.rejects(new JobScheduler({ fetch }).cancel("t1"));
  assert.equal(calls, 1);
});

test("verifyWebhook", async () => {
  const body = '{"event":"job.finished"}';
  const ts = String(Math.floor(Date.now() / 1000));
  const sig = "sha256=" + createHmac("sha256", "k").update(`${ts}.${body}`).digest("hex");
  const headers = { "x-jobscheduler-timestamp": ts, "x-jobscheduler-signature": sig };
  assert.equal(await verifyWebhook(body, headers, "k"), true);
  assert.equal(await verifyWebhook(body + " ", headers, "k"), false);
  assert.equal(await verifyWebhook(body, { ...headers, "x-jobscheduler-timestamp": String(Number(ts) - 3600) }, "k"), false);
});

test("live: submit, events, wait, workflow", { skip: !process.env.JOB_SCHEDULER_URL }, async () => {
  const js = new JobScheduler({ baseUrl: process.env.JOB_SCHEDULER_URL, apiKey: process.env.JOB_SCHEDULER_API_KEY });
  const { taskId } = await js.submit("app.tasks.add", { args: [3, 4], priority: "high" });
  assert.equal((await js.wait(taskId)).result, 7);
  const slow = await js.submit("app.tasks.long_running_task", { args: [2] });
  const states = [];
  for await (const s of js.events(slow.taskId)) states.push(s.state);
  assert.equal(states.at(-1), "SUCCESS");
  assert.ok(states.includes("PROGRESS"));
  const wf = await js.createWorkflow([
    { id: "a", task_name: "app.tasks.add", args: [1, 2] },
    { id: "b", task_name: "app.tasks.add", args: [3, 4], depends_on: ["a"] },
  ]);
  assert.equal((await js.waitWorkflow(wf.id, { timeoutMs: 60000 })).status, "SUCCESS");
});
