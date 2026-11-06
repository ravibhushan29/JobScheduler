# Job Scheduler — Documentation

Welcome! This folder explains the whole project, from "what is this?" to
"how do I run it in production?". It is written so that **anyone** can follow
it — you don't need to know Python, Celery or Kubernetes to read the first
chapters. Technical detail comes later, and every technical word is explained
the first time it appears (and again in the [glossary](02-key-concepts.md)).

---

## Start here: pick your path

| You are… | Read these, in order |
|---|---|
| **New to the project, any background** (manager, tester, student) | [1. What is this?](01-what-is-this.md) → [2. Key concepts](02-key-concepts.md) → [7. How "runs exactly once" works](07-reliability.md) (first half) |
| **A user of the API** (you want to submit jobs or create schedules) | [1](01-what-is-this.md) → [4. Getting started](04-getting-started.md) → [5. Using the API](05-using-the-api.md) → [6. Scheduling](06-scheduling.md) |
| **A developer** joining the project | [1](01-what-is-this.md) → [2](02-key-concepts.md) → [3. Architecture](03-architecture.md) → [4](04-getting-started.md) → [11. Developer guide](11-developer-guide.md) → [7](07-reliability.md) |
| **Operations / DevOps / SRE** | [3](03-architecture.md) → [8. Configuration](08-configuration.md) → [9. Deployment](09-deployment.md) → [10. Operations & runbooks](10-operations.md) → [7](07-reliability.md) |
| **A visual learner** | [13. Diagram gallery](13-diagrams.md) — every picture, each linked to its explanation |
| **Something is broken right now** | [12. FAQ & troubleshooting](12-faq-troubleshooting.md) → [10. Runbooks](10-operations.md#105-runbooks) |

---

## All chapters

| # | Chapter | What you'll learn |
|---|---|---|
| 1 | [What is this?](01-what-is-this.md) | The problem it solves, explained with a restaurant analogy. No jargon. |
| 2 | [Key concepts & glossary](02-key-concepts.md) | Every term used in the project — job, queue, worker, schedule, lease, idempotency… |
| 3 | [Architecture](03-architecture.md) | The pieces, how they talk, and the life of a job step by step (with diagrams). |
| 4 | [Getting started](04-getting-started.md) | Install, run, and submit your first job in about 10 minutes. |
| 5 | [Using the API](05-using-the-api.md) | Every endpoint with copy-paste examples and the answers you'll get back. |
| 6 | [Scheduling](06-scheduling.md) | Running things once, every N seconds, or on a calendar (cron) — with a cron primer. |
| 7 | [How "runs exactly once" works](07-reliability.md) | The guarantees, the mechanisms behind them, and their honest limits. |
| 8 | [Configuration reference](08-configuration.md) | Every setting: what it does, its default, and when to change it. |
| 9 | [Deployment](09-deployment.md) | Docker Compose, Kubernetes, high availability, production checklist. |
| 10 | [Operations & runbooks](10-operations.md) | Monitoring, logs, alerts, and step-by-step fixes for common incidents. |
| 11 | [Developer guide](11-developer-guide.md) | A tour of the code, how to add a task, testing, and conventions. |
| 12 | [FAQ & troubleshooting](12-faq-troubleshooting.md) | Quick answers to common questions and problems. |
| 13 | [Diagram gallery](13-diagrams.md) | All 17 key diagrams in one place — system, flows, states, database, deployment. |

Other documents in the repository:
- [README.md](../README.md) — the one-page project summary.
- [USER_TESTING.md](../USER_TESTING.md) — a hands-on walkthrough that exercises every endpoint in order.
- [CHANGELOG.md](../CHANGELOG.md) — what changed in each version.

---

## The project in one paragraph

This is a **background job system**. Some work is too slow, too unreliable,
or too repetitive to do while a person waits — sending emails, generating
reports, cleaning up old data every night. You hand that work to this system
over a simple web API; it puts the work in a queue, a pool of workers does it,
and you can check on it at any time. It can also run work **on a schedule**
(once at 9:00, every 2 hours, every weekday morning). It is built to run on
many machines at once, keep working when any one of them fails, and — most
importantly — **never do the same piece of work twice by accident**.
