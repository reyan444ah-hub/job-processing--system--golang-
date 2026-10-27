# Frontend API integration

The frontend consumes the existing Go/Gin API; it does not implement or mock backend routes.

## Development

From the repository root, start the existing backend services:

```powershell
docker compose -f backend\docker-compose.yml up --build -d
```

Then, from `frontend/`:

```powershell
Copy-Item .env.example .env
npm.cmd install
npm.cmd run dev
```

Vite proxies `/api` and WebSocket upgrades to `http://localhost:8080` by default. This is a development-only same-origin proxy; configure `VITE_BACKEND_PROXY_TARGET` if the API runs elsewhere. Set `VITE_API_BASE_URL` to `/api/v1` for the proxy or an absolute API URL ending in `/api/v1` for direct requests.

For direct browser-to-API requests, set the backend's `ALLOWED_ORIGINS` to the frontend origin (for example `http://localhost:5173`) and restart the backend. Production should use HTTPS and either a same-origin reverse proxy or an explicit backend origin allow-list.

## Routes consumed

| Method | Endpoint | Usage |
| --- | --- | --- |
| POST | `/auth/register` | Create organization and first admin; expects `organization_name`, `email`, `password`. |
| POST | `/auth/login` | Login; stores returned access and refresh tokens and user. |
| POST | `/auth/refresh` | Rotate refresh token on API `401`; both tokens are replaced together. |
| POST | `/auth/logout` | Revoke refresh tokens, then clear the local session. |
| POST | `/jobs` | Admin or member: submit `{type,payload,priority,max_attempts,scheduled_at?}` with `Idempotency-Key`. |
| GET | `/my/jobs?status=&limit=&offset=` | Member only: list jobs submitted by the authenticated account. |
| GET | `/jobs?status=&limit=&offset=` | Admin only: organization-scoped list and pagination. |
| GET | `/jobs/:id` | Admin only: organization-scoped job details. |
| POST | `/jobs/:id/cancel` | Admin only: cancel queued/retrying jobs. |
| POST | `/jobs/:id/retry` | Admin only: manually retry failed/dead-letter jobs. |
| GET | `/jobs/:id/events` | Admin only: organization-scoped event history. |
| GET | `/admin/users` | Admin only: list accounts in the organization. |
| POST | `/admin/users` | Admin only: create a member account with `email` and `password`. |
| GET | `/metrics` | Admin only: platform-level Redis queue and worker metrics; these counters are not organization-scoped. |
| WS | `/ws/jobs?token=<access-token>` | Admin only: authenticated tenant-scoped broadcast events; reconnect uses bounded exponential backoff. |

The first account created by `/auth/register` is an organization admin. Admins use the operations dashboard to inspect/control jobs and create member accounts. Members are routed to a separate portal to submit jobs and view only jobs submitted by their own account; the backend independently restricts these APIs by role, organization, and submitting user.

## Authentication and data handling

The backend returns refresh tokens as JSON and requires a token in the WebSocket query string. To match that existing contract, both tokens are held in `sessionStorage` (not `localStorage`) and are cleared on logout or refresh rejection. This is browser-accessible token storage, not HttpOnly-cookie protection; production deployments should mitigate XSS and use HTTPS.

The backend JWT contains the user's organization ID. The frontend never supplies or overrides tenant IDs for list or state-changing operations; tenant enforcement remains on the authenticated backend. Dashboard status totals are obtained from actual filtered job-list responses. System-wide metrics are labelled separately to avoid implying tenant scope.

Job list responses have `{jobs,total,limit,offset}`. Mutations return the updated `{job}` envelope, and submission returns `{job,deduplicated}` with HTTP 202. Error messages are taken from the backend's `{error}` response where available.

The current backend list endpoint supports status filtering and pagination, but not name search. The Jobs page therefore keeps name search separate from the status filter and searches the authenticated organization's jobs by their displayed job type, loading the API's paginated results as needed.

## Commands and checks

```powershell
npm.cmd test
npm.cmd run build
```
