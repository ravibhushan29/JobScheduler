# 13. Diagram gallery

All the project's key diagrams in one place. Each links back to the chapter
that explains it. (Diagrams are written in [Mermaid](https://mermaid.js.org/)
and render automatically on GitHub, GitLab and in VS Code's Markdown preview.)

| # | Diagram | Type |
|---|---|---|
| 1 | [System context](#1-system-context) | Who uses the system and what it depends on |
| 2 | [Components](#2-components) | The parts and how they connect |
| 3 | [Network & ports](#3-network--ports) | Who talks to whom, on which port |
| 4 | [Job status lifecycle](#4-job-status-lifecycle) | State diagram |
| 5 | [Submitting and running a job](#5-submitting-and-running-a-job) | Sequence |
| 6 | [Firing a schedule with many schedulers](#6-firing-a-schedule-with-many-schedulers) | Sequence |
| 7 | [Scheduler loop](#7-scheduler-loop) | Flowchart |
| 8 | [Execution guard decision](#8-execution-guard-decision) | Flowchart |
| 9 | [Outbox](#9-outbox) | Sequence |
| 10 | [Crash recovery](#10-crash-recovery) | Sequence |
| 11 | [Fencing an outdated worker](#11-fencing-an-outdated-worker) | Sequence |
| 12 | [Idempotency key](#12-idempotency-key) | Flowchart |
| 13 | [Database tables](#13-database-tables) | Entity-relationship |
| 14 | [Schedule lifecycle](#14-schedule-lifecycle) | State diagram |
| 15 | [Kubernetes deployment](#15-kubernetes-deployment) | Deployment |
| 16 | [High availability across zones](#16-high-availability-across-zones) | Deployment |
| 17 | [Incident triage](#17-incident-triage) | Decision tree |

---

## 1. System context
```mermaid
flowchart TB
    Users(["People<br/>(via Swagger UI / curl)"])
    Apps(["Your applications<br/>(call the API)"])
    Ops(["Operators<br/>(Flower, logs, SQL)"])
    SYS["🗂️ Job Scheduler<br/>runs background jobs and schedules,<br/>each exactly once"]
    PG[(Postgres<br/>source of truth)]
    RD[(Redis<br/>queue)]
    EXT["External services your tasks call<br/>(email, payments, storage…)"]
    Users --> SYS
    Apps --> SYS
    Ops --> SYS
    SYS --> PG
    SYS --> RD
    SYS --> EXT
```
*Explained in [chapter 1](01-what-is-this.md).*

## 2. Components
```mermaid
flowchart LR
    C[Client] -- HTTPS + X-API-Key --> API
    subgraph App["Application (replicas)"]
        API[API]
        S[Scheduler]
        W1[Workers: default]
        W2[Workers: long]
        F[Flower]
    end
    DB[(Postgres)]
    R[(Redis)]
    API -- record --> DB
    API -- publish --> R
    S -- claim / record --> DB
    S -- publish --> R
    R -- deliver --> W1 & W2
    W1 & W2 -- claim / lease / result --> DB
    F -. watch .-> R
```
*Explained in [chapter 3.1](03-architecture.md#31-the-big-picture).*

## 3. Network & ports
```mermaid
flowchart LR
    Client -->|"443 (TLS) → 8000"| API[api :8000]
    Admin -->|"port-forward 5555"| FL[flower :5555]
    API -->|5432| PG[(postgres :5432)]
    API -->|6379| RD[(redis :6379)]
    WK[workers] -->|5432| PG
    WK -->|6379| RD
    SC[scheduler] -->|5432| PG
    SC -->|6379| RD
    FL -->|6379| RD
```
Only the API accepts traffic from outside. Workers and schedulers make outgoing connections only.

## 4. Job status lifecycle
```mermaid
stateDiagram-v2
    [*] --> QUEUED: submitted
    QUEUED --> STARTED: worker claims it
    STARTED --> SUCCESS: finished
    STARTED --> RETRY: error, will retry
    RETRY --> STARTED: next attempt
    STARTED --> FAILURE: retries exhausted
    STARTED --> STARTED: worker died → another claims it<br/>(attempts +1)
    QUEUED --> REVOKED: cancelled
    STARTED --> REVOKED: cancelled
    SUCCESS --> [*]
    FAILURE --> [*]
    REVOKED --> [*]
```
*Explained in [chapter 2.2](02-key-concepts.md#22-the-life-of-a-job-statuses).*

## 5. Submitting and running a job
```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant A as API
    participant D as Postgres
    participant R as Redis
    participant W as Worker
    C->>A: POST /tasks/add
    A->>D: INSERT job (QUEUED, unpublished)
    A->>R: publish
    A->>D: published_at = now
    A-->>C: 201 task_id
    R->>W: deliver
    W->>D: claim (STARTED, attempt 1, lease)
    W->>W: run task (renew lease every 20 s)
    W->>D: SUCCESS + result (if still attempt 1)
    W->>R: acknowledge
```
*Explained in [chapter 3.3](03-architecture.md#33-life-of-a-job-step-by-step).*

## 6. Firing a schedule with many schedulers
```mermaid
sequenceDiagram
    participant S1 as Scheduler 1
    participant S2 as Scheduler 2
    participant D as Postgres
    participant R as Redis
    S1->>D: due schedules? → #7 (version 12)
    S2->>D: due schedules? → #7 (version 12)
    S1->>D: UPDATE … WHERE version = 12 (+ INSERT job)
    D-->>S1: 1 row → won
    S2->>D: UPDATE … WHERE version = 12
    D-->>S2: 0 rows → skip
    S1->>R: publish job
```
*Explained in [chapter 3.4](03-architecture.md#34-life-of-a-scheduled-run).*

## 7. Scheduler loop
```mermaid
flowchart TD
    A([every ~5 s]) --> B[Fire due schedules<br/>version-checked claim]
    B --> C[Relay outbox<br/>send saved-but-unsent jobs]
    C --> D[Recover abandoned jobs<br/>expired leases]
    D --> E[Touch heartbeat file]
    E --> A
```
*Explained in [chapter 6.6](06-scheduling.md#66-how-schedules-actually-fire).*

## 8. Execution guard decision
```mermaid
flowchart TD
    M[Message for job J] --> T{Claim J?}
    T -->|won| RUN[Run, renewing lease] --> DONE[Record result, fenced by attempt]
    T -->|J finished or cancelled| DROP[Drop copy]
    T -->|J running, lease alive| DEFER[Re-check later]
    T -->|DB unreachable| BACK[Hand back to queue]
```
*Explained in [chapter 7.7](07-reliability.md#77-the-execution-guard--one-worker-runs-each-job).*

## 9. Outbox
```mermaid
sequenceDiagram
    participant A as API / scheduler
    participant D as Postgres
    participant S as Scheduler relay
    participant R as Redis
    A->>D: INSERT job (published_at NULL)
    A-xR: publish fails / crash
    S->>D: unsent jobs > 30 s old?
    S->>D: claim (if unchanged)
    S->>R: publish
    S->>D: published_at = now
```
*Explained in [chapter 7.5](07-reliability.md#75-the-outbox--no-lost-jobs-between-saved-and-sent).*

## 10. Crash recovery
```mermaid
sequenceDiagram
    participant W1 as Worker 1
    participant D as Postgres
    participant S as Scheduler
    participant W2 as Worker 2
    W1->>D: claim → attempt 1
    Note over W1: 💥 dies
    Note over D: lease expires + grace
    S->>D: expired STARTED jobs → J
    S->>S: did J actually finish? (result store)
    S->>W2: re-publish J (via Redis)
    W2->>D: claim → attempt 2
    W2->>D: SUCCESS (attempt 2)
```
*Explained in [chapter 7.8](07-reliability.md#78-lease-based-recovery--finishing-jobs-whose-worker-died).*

## 11. Fencing an outdated worker
```mermaid
sequenceDiagram
    participant W1 as Worker 1 (frozen)
    participant D as Postgres
    participant W2 as Worker 2
    W1->>D: claim → attempt 1
    Note over D: lease expires
    W2->>D: claim → attempt 2
    W2->>D: SUCCESS where attempts = 2 ✅
    W1->>D: SUCCESS where attempts = 1 ❌ ignored
```
*Explained in [chapter 7.7](07-reliability.md#the-fencing-token).*

## 12. Idempotency key
```mermaid
flowchart TD
    A["POST + Idempotency-Key K"] --> B{K seen before?}
    B -->|no| C[Create → 201]
    B -->|yes| D{Same request?}
    D -->|yes| E[Return original → 200<br/>Idempotent-Replayed: true]
    D -->|no| F[409 conflict]
```
*Explained in [chapter 5.6](05-using-the-api.md#56-safe-retries-with-idempotency-key).*

## 13. Database tables
```mermaid
erDiagram
    SCHEDULES ||--o{ JOBS : "fires (schedule_id)"
    JOBS ||--o{ JOBS : "manual retry (retry_of)"
    JOBS ||--o| CELERY_TASKMETA : "live result (same task_id)"

    SCHEDULES {
        int id PK
        string name "unique; system schedules only"
        string task_name
        json args
        json kwargs
        string schedule_type "once | interval | cron"
        datetime run_at
        int interval_seconds
        string cron_expr
        bool enabled
        datetime next_run_at
        datetime last_run_at
        int run_count
        int version "optimistic lock"
        string idempotency_key "unique"
        datetime created_at
    }
    JOBS {
        string task_id PK
        string task_name
        json args
        json kwargs
        string queue
        string status
        json result
        text error
        text traceback
        int retries
        int attempts "fencing token"
        datetime lease_expires_at
        string worker
        string retry_of FK
        int schedule_id FK
        string idempotency_key "unique"
        datetime published_at "NULL = in outbox"
        datetime publish_attempted_at
        datetime submitted_at
        datetime started_at
        datetime finished_at
    }
    CELERY_TASKMETA {
        string task_id
        string status
        blob result
        datetime date_done
        text traceback
    }
```
*Explained in [chapter 3.5](03-architecture.md#35-where-data-lives).*

## 14. Schedule lifecycle
```mermaid
stateDiagram-v2
    [*] --> Active: created
    Active --> Paused: enabled = false
    Paused --> Active: enabled = true
    Active --> Active: interval / cron fires
    Active --> Finished: once fires
    Active --> [*]: deleted
    Paused --> [*]: deleted
    Finished --> [*]: deleted
```
*Explained in [chapter 6.4](06-scheduling.md#64-managing-schedules).*

## 15. Kubernetes deployment
```mermaid
flowchart TB
    U([Clients]) --> ING[Ingress + TLS] --> SVC[Service api]
    subgraph NS[namespace job-scheduler]
        SVC --> A[api × 3]
        W[worker × 3]
        WL[worker-long × 2]
        S[scheduler × 2]
        F[flower × 1]
    end
    A & W & WL & S --> DB[(Managed Postgres)]
    A & W & WL & S --> R[(Managed Redis)]
```
*Explained in [chapter 9.2](09-deployment.md#92-kubernetes).*

## 16. High availability across zones
```mermaid
flowchart LR
    subgraph ZA[Zone A]
        A1[api]
        W1[worker]
        S1[scheduler]
        PGP[(Postgres primary)]
    end
    subgraph ZB[Zone B]
        A2[api]
        W2[worker]
        S2[scheduler]
        PGS[(Postgres standby)]
    end
    subgraph ZC[Zone C]
        A3[api]
        W3[worker]
    end
    PGP -- replication --> PGS
```
*Explained in [chapter 9.3](09-deployment.md#93-high-availability-the-full-picture).*

## 17. Incident triage
```mermaid
flowchart TD
    S([Something's wrong]) --> R{/health/ready?}
    R -->|database error| A[Runbook A]
    R -->|broker error| B[Runbook B]
    R -->|OK| J{Symptom}
    J -->|stays QUEUED| C[Runbook C]
    J -->|stays STARTED| D[Runbook D]
    J -->|schedules silent| E[Runbook E]
    J -->|many failures| F[Runbook F]
    J -->|slow| G[Runbook G]
```
*Explained in [chapter 10.5](10-operations.md#105-runbooks).*
