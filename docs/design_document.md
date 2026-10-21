# Distributed Job Processing Platform Architecture & Design Document

## 1. Executive Summary & Architecture Overview

This platform implements an enterprise-grade, distributed asynchronous job processing and real-time event broadcasting platform in Go (Golang) with Gin, PostgreSQL, Redis Streams, and WebSockets.

### High-Level Architecture Diagram

```
 [Clients / Frontend]
      │           │
  (HTTP REST) (WebSockets /api/v1/ws/jobs)
      │           │
      ▼           ▼
┌──────────────────────────────────────────────┐
│                  API Server                  │
│  - JWT Auth & Multi-Tenant Enforcement       │
│  - Idempotency Gate (PostgreSQL Unique Keys) │
│  - WebSocket Connection Hub                  │
│  - Redis Job Cache (TTL & Invalidation)      │
└──────────────┬───────────────────────────────┘
               │ (ACID Transaction)
               ▼
┌──────────────────────────────────────────────┐
│             PostgreSQL 16 (DB)               │
│  - jobs (Optimistic locking & state machine) │
│  - outbox_events (Transactional Outbox)      │
│  - job_events (Audit & reconnect history)    │
│  - idempotency_keys & refresh_tokens         │
└──────────────┬───────────────────────────────┘
               │ (Async Polling / Relaying)
               ▼
┌──────────────────────────────────────────────┐
│           Outbox Publisher Daemon            │
│  - Reads undispatched outbox_events          │
│  - Dispatches job creation to Redis Stream   │
│  - Broadcasts state events to Redis Pub/Sub  │
└──────────────┬───────────────────────┬───────┘
               │ (XADD)                │ (PUBLISH events:org:*)
               ▼                       ▼
┌───────────────────────────┐   ┌──────────────────────────┐
│ Redis Streams & ZSet      │   │   WebSocket Hub Clients  │
│ - jobs:stream (Group)     │   │   (Tenant Scoped Rooms)  │
│ - jobs:scheduled (ZSet)   │   └──────────────────────────┘
│ - jobs:dlq (Dead Letter)  │
└──────────────┬────────────┘
               │ (XREADGROUP / XAUTOCLAIM)
               ▼
┌──────────────────────────────────────────────┐
│      Distributed Worker Daemons (xN)         │
│  - SELECT FOR UPDATE + Optimistic Lock Claim │
│  - Exponential Backoff with Jitter Retries   │
│  - Dead-Letter Queue (DLQ) Exhaustion        │
│  - Periodic Crash Recovery (XAUTOCLAIM PEL)  │
└──────────────────────────────────────────────┘
```

---

## 2. Core Reliability Patterns & Technical Trade-offs

### A. Transactional Outbox Pattern
**Problem**: In distributed systems, writing to a database and publishing to a message broker in dual-write steps without 2PC can lead to catastrophic inconsistencies (e.g. DB commits, but Redis publish fails due to network partition or worker crash).

**Solution**:
1. When a client submits a job, the job record, initial `job_events` record, and `outbox_events` record are written in a single **PostgreSQL ACID transaction**.
2. A lightweight background `OutboxPublisher` polls `outbox_events WHERE dispatched = FALSE`.
3. It pushes the message to Redis Streams (for workers) and Redis Pub/Sub (for WebSockets), marking `dispatched = TRUE` upon success.
4. If the application crashes before publishing, on restart the un-dispatched outbox events are automatically recovered and dispatched without losing any job.

### B. Why "Exactly-Once" Execution Cannot Generally Be Guaranteed
In distributed computing, the **Two Generals' Problem** and **FLM Impossibility Theorem** prove that arbitrary external side effects (such as sending an email, charging a credit card, or mutating external microservices) cannot be guaranteed to execute exactly once under arbitrary network partitions:
- An external service can receive the request, execute it, but crash before sending the network acknowledgement.
- Re-trying is necessary to ensure progress (at-least-once delivery).
- Therefore, our system is designed around **at-least-once delivery with idempotent consumers**.

### C. Idempotency & Deduplication Engine
1. Clients provide an optional `Idempotency-Key` header with requests.
2. The database enforces a unique constraint `(organization_id, key)`.
3. An atomic `INSERT ... ON CONFLICT DO NOTHING` reserves the key. If an ongoing operation or completed response exists, duplicate calls safely return the original `202 Accepted` response with header `X-Deduplicated: true` without creating redundant processing.

### D. Concurrency & Outdated Worker Race Prevention
1. **Row-Level Lock & Optimistic Versioning**: When workers claim a job via `ClaimJobAtomic`, they acquire a `SELECT ... FOR UPDATE` lock and increment the `version` column.
2. **Outdated Worker Overwrite Protection**:
   ```sql
   UPDATE jobs
   SET status = 'completed', version = version + 1
   WHERE id = $1 AND version = $2 AND status = 'processing';
   ```
   If a worker hangs or partitions and another worker recovers and completes the job, the delayed worker's write fails harmlessly due to version mismatch.

### E. Worker Crash Recovery (Redis Stream PEL & XAUTOCLAIM)
- When a worker takes a message from a consumer group with `XREADGROUP`, Redis marks it in the **Pending Entries List (PEL)**.
- If a worker crashes before sending `XACK`, the message remains unacknowledged.
- Active workers run a periodic sweep with `XAUTOCLAIM` with `min-idle-time=30s`. Any message pending longer than the threshold is automatically reclaimed by surviving workers.

### F. Exponential Backoff with Jitter & Dead-Letter Queue (DLQ)
When a job fails:
- Backoff delay formula:
  $$\text{Delay} = \min(\text{MaxBackoff}, \text{Base} \times 2^{\text{attempt}-1}) \pm \text{Jitter}$$
- The job is placed in Redis Sorted Set `jobs:scheduled` with score equal to execution timestamp. A background sweeper moves matured jobs back to `jobs:stream`.
- Once `attempt_count >= max_attempts`, the job status transitions to `dead_letter` and the job message is routed to `jobs:dlq`.
- Operators can inspect DLQ errors and trigger manual retry via `POST /api/v1/jobs/:id/retry`.

### G. Real-Time WebSockets & Cross-Instance Scalability
- WebSockets authenticate via JWT in the handshake query parameter or Authorization header.
- Connections are grouped strictly by `organization_id`.
- Multiple API server instances communicate via Redis Pub/Sub channel pattern `events:org:<org_id>`.
- Slow clients with full buffers are isolated with bounded channels (`chan []byte, 256`) and dropped if they block, preventing slow clients from causing backpressure on workers or API throughput.
- Clients that disconnect can recover missed events by querying `GET /api/v1/jobs/:id/events`.

### H. Redis Caching & Outage Resilience
- Frequently accessed jobs are cached in Redis with short TTLs (1 minute).
- Status changes immediately call `cache.Invalidate(orgID, jobID)`.
- If Redis becomes unavailable, PostgreSQL acts as the authoritative store: read and write requests continue working, while caching and asynchronous queuing gracefully log warnings or fallback to PostgreSQL polling until Redis reconnects.
