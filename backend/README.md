# Distributed Job Processing & Real-Time Event Platform

[![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat&logo=go)](https://golang.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?style=flat&logo=postgresql)](https://www.postgresql.org)
[![Redis](https://img.shields.io/badge/Redis-7.0+-DC382D?style=flat&logo=redis)](https://redis.io)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker)](https://www.docker.com)

A distributed backend platform built in Go that accepts asynchronous jobs through REST APIs, processes them using concurrent background workers, retries failed jobs with exponential backoff and jitter, routes exhausted jobs to a Dead-Letter Queue (DLQ), recovers from worker crashes using Redis Streams PEL auto-claiming, and streams real-time status events to authorized clients via WebSockets and Redis Pub/Sub.

---

## Architecture Diagram

```
 [Clients / UI / Microservices]
       │                  │
 (HTTP REST)     (Authenticated WebSockets)
       │                  │
       ▼                  ▼
 ┌──────────────────────────────────────────────┐
 │             API Server (Gin)                 │
 │  - JWT Auth & Multi-Tenant Enforcement       │
 │  - Idempotency Gate (Idempotency-Key Header) │
 │  - WebSocket Hub (Tenant Scoped Rooms)       │
 │  - Redis Job Cache (TTL + Invalidation)      │
 └──────────────┬───────────────────────────────┘
                │ (ACID Transaction)
                ▼
 ┌──────────────────────────────────────────────┐
 │              PostgreSQL 16                   │
 │  - jobs (Optimistic locking & status guards) │
 │  - outbox_events (Transactional Outbox)      │
 │  - job_events (Audit & reconnect history)    │
 │  - users, organizations, idempotency_keys    │
 └──────────────┬───────────────────────────────┘
                │ (Outbox Polling & Relay)
                ▼
 ┌──────────────────────────────────────────────┐
 │         Transactional Outbox Publisher       │
 │  - Relays job creation to Redis Stream       │
 │  - Broadcasts status to Redis Pub/Sub        │
 └──────────────┬───────────────────────┬───────┘
                │ (XADD)                │ (PUBLISH)
                ▼                       ▼
 ┌───────────────────────────┐   ┌──────────────────────────┐
 │ Redis Streams & ZSet      │   │   WebSocket Hub Clients  │
 │ - jobs:stream (Group)     │   │   (Real-Time Streaming)  │
 │ - jobs:scheduled (ZSet)   │   └──────────────────────────┘
 │ - jobs:dlq (Dead Letter)  │
 └──────────────┬────────────┘
                │ (XREADGROUP / XAUTOCLAIM)
                ▼
 ┌──────────────────────────────────────────────┐
 │         Distributed Workers (xN)             │
 │  - Atomic SELECT FOR UPDATE Claiming         │
 │  - Exponential Backoff with Jitter Retries   │
 │  - Automatic DLQ Routing                     │
 │  - Dead Worker Crash Recovery (XAUTOCLAIM)   │
 └──────────────────────────────────────────────┘
```

---

## Key Features

1. **Multi-Tenancy & Security**:
   - Secure password hashing with `bcrypt`.
   - JWT access tokens with configurable expiration.
   - Refresh token rotation with single-use enforcement and logout revocation.
   - Organization-level scoping on all endpoints preventing cross-tenant access.

2. **Asynchronous 202 Accepted & Transactional Outbox**:
   - Job submission returns `202 Accepted` immediately once durably persisted in PostgreSQL.
   - Transactional outbox pattern relays queued, retry, and dead-letter events after commit; leased claims let multiple API instances relay concurrently and recover after a publisher crash.

3. **Distributed Job Queue with Redis Streams**:
   - Multiple concurrent workers consuming via Redis Consumer Groups (`XREADGROUP`).
   - Scheduled and delayed execution via Redis Sorted Sets (`jobs:scheduled`).
   - Exponential backoff with full jitter to avoid the thundering herd problem.
   - Dead-Letter Queue (`jobs:dlq`) for jobs that exhaust their maximum attempts.
   - Crash recovery using `XAUTOCLAIM` to reclaim abandoned messages in the Pending Entries List (PEL).

4. **Idempotency & Concurrency Safety**:
   - `Idempotency-Key` reservation, job creation, outbox record, and cached response commit in one PostgreSQL transaction; request fingerprints include type, canonical payload, priority, schedule, and retry limit.
   - Optimistic concurrency control (`version` column) and PostgreSQL row locks (`SELECT ... FOR UPDATE`) prevent concurrent workers from claiming the same job or outdated workers from overwriting completed jobs.
   - Abandoned Redis pending entries can reclaim stale `processing` rows only after the configured idle threshold.

5. **Real-Time WebSockets & Cross-Instance Event Bus**:
   - Authenticated WebSocket endpoint at `/api/v1/ws/jobs`.
   - Redis Pub/Sub distributes events across multiple API instances.
   - Heartbeats (ping/pong), disconnect cleanup, and bounded buffers to prevent slow clients from blocking worker throughput.
   - Missed events can be retrieved upon reconnection via `/api/v1/jobs/:id/events`.

6. **Redis Caching with Graceful Fallback**:
   - Caches frequently accessed job responses.
   - Immediate cache invalidation on job state transitions.
   - If Redis goes down, database operations continue smoothly with PostgreSQL as the authoritative source.

---

## Quickstart with Docker Compose

### Prerequisites
- [Docker](https://docs.docker.com/get-docker/) & [Docker Compose v2.20+](https://docs.docker.com/compose/)

### Start the entire platform
From the repository root, create the backend environment file:
```bash
cp backend/.env.example backend/.env
# For deployment, replace the example PostgreSQL password and JWT secret.
docker compose up --build
```
The root Compose file includes `backend/docker-compose.yml`, so the same command works from the repository root. To run it from `backend/` instead, use `cp .env.example .env` and then `docker compose up --build`.
If a host port is already in use, change `POSTGRES_HOST_PORT`, `REDIS_HOST_PORT`, or `API_HOST_PORT` in `backend/.env` (or `.env` when running from `backend/`).
This starts:
- **PostgreSQL 16** on port `3000`
- **Redis 7** on port `6379`
- **API Server** on port `8080`
- **Worker 1** (Concurrency: 5)
- **Worker 2** (Concurrency: 5)

---

## REST API Reference

### Authentication
- `POST /api/v1/auth/register` — Register a new organization and its first admin.
- `POST /api/v1/auth/login` — Login and receive JWT access & refresh tokens.
- `POST /api/v1/auth/refresh` — Rotate refresh token for a new access token.
- `POST /api/v1/auth/logout` — Revoke user sessions.
- `GET /api/v1/admin/users` — Admin only: list accounts in the organization.
- `POST /api/v1/admin/users` — Admin only: create a member account with an email and password.

### Jobs
- `POST /api/v1/jobs` — Admin or member: submit a job (`202 Accepted`). Supports `Idempotency-Key` header.
- `GET /api/v1/my/jobs` — Member only: list jobs submitted by the authenticated account.
- `GET /api/v1/jobs` — Admin only: list and filter jobs by status with pagination (`?status=queued&limit=20&offset=0`).
- `GET /api/v1/jobs/:id` — Admin only: get job details (checks cache first).
- `POST /api/v1/jobs/:id/cancel` — Admin only: cancel an eligible job.
- `POST /api/v1/jobs/:id/retry` — Admin only: manually retry a dead_letter or failed job.
- `GET /api/v1/jobs/:id/events` — Admin only: retrieve chronological job event history for reconnect recovery.

### Real-Time & Metrics
- `GET /api/v1/ws/jobs?token=<JWT_TOKEN>` — Admin-only WebSocket event stream.
- `GET /api/v1/metrics` — Admin-only queue, DLQ, stream length, and active worker metrics.

---

## Testing & Verification

### Running Unit Tests
```bash
go test ./... -v
```

### Running Integration Test Suite
The integration test suite validates all required failure scenarios:
```bash
go test -v ./tests/integration -run TestScenario_
```
Covered scenarios:
1. Concurrently submit 100 jobs and verify all complete.
2. Concurrent duplicate idempotency key deduplication.
3. Killing a worker mid-execution and recovering via PEL claim.
4. Forcing repeated failures to route to the Dead-Letter Queue.
5. Verifying cross-organization requests are rejected.

### Running Load Tests
A load testing tool is included in `tests/load/main.go`:
```bash
go run ./tests/load -url http://localhost:8080 -c 20 -n 100
```

---

## Deliverables & Documentation
- **Project overview and API reference**: this README
- **Postman API Collection**: [`postman/collection.json`](file:///c:/Users/Hashim%20Traders/Documents/Task%20%28Go%20lang%29/job%20processing%20%20background/postman/collection.json)
- **Database Schema**: [`migrations/000001_init_schema.sql`](file:///c:/Users/Hashim%20Traders/Documents/Task%20%28Go%20lang%29/job%20processing%20%20background/migrations/000001_init_schema.sql)
- **Docker Compose Setup**: [`docker-compose.yml`](file:///c:/Users/Hashim%20Traders/Documents/Task%20%28Go%20lang%29/job%20processing%20%20background/docker-compose.yml)
- **Environment Template**: [`.env.example`](file:///c:/Users/Hashim%20Traders/Documents/Task%20%28Go%20lang%29/job%20processing%20%20background/.env.example)

Docker Compose requires `POSTGRES_PASSWORD`, `DATABASE_URL` and `JWT_SECRET` to be set in `.env` or the process environment. Use a unique random JWT secret (at least 32 characters) and configure `ALLOWED_ORIGINS` as a comma-separated list of trusted browser origins. The development JWT fallback is disabled in production.
