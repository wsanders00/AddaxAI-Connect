# Deployment health checks

The server-admin System health page reads `GET /api/health/services`. The endpoint is admin-only and read-only: it checks database and Redis connectivity, verifies access to the configured raw-image S3 bucket with `HeadBucket`, reads worker heartbeats and selected status snapshots, and checks the configured frontend URL. It does not change storage or service state.

## Configure health expectations

Set these variables in the API process environment through the deployment's existing configuration mechanism:

| Variable | Purpose |
| --- | --- |
| `HEALTH_ENABLED_WORKERS` | Comma-separated worker names expected to run in this deployment. |
| `FRONTEND_HEALTH_URL` | A private, internal URL that returns HTTP 200 directly, without credentials or an authentication redirect. |
| `BACKUP_ENABLED` | Set to `true` only when scheduled backups are configured and expected. |
| `COLD_TIER_ENABLED` | Set to `true` only when cold-tier storage and its watchdog are configured and expected. |

`HEALTH_ENABLED_WORKERS` accepts these exact names: `ingestion`, `bulk-upload`, `detection`, `classification`, `notifications`, `notifications-email`, `notifications-telegram`, `notifications-earthranger`, and `notifications-sensingclues`. Keep the list aligned with the workers this deployment actually runs. An unset, empty, malformed, or unknown list is a configuration failure; it does not silently mark workers disabled.

The frontend check uses `FRONTEND_HEALTH_URL` as a private service-to-service probe. Point it at a URL that serves the application directly with status 200. Do not point it at a login page or a URL that redirects; redirects are considered failed checks. The URL must not contain credentials, a query string, or a fragment.

## Read the health states

- **Healthy** means the check completed and the configured service passed.
- **Unhealthy** means an enabled check failed, timed out, returned invalid or stale status, or could not verify the service. For workers, the heartbeat is written by the worker's own consume loop; a missing or stale heartbeat remains a failure when that worker is enabled.
- **Disabled** means the feature or worker is not expected in this deployment. Disabled rows are informational and are excluded from the healthy/failed service counts. If every row is disabled, the page reports that there are no configured services being checked; it does not claim the system is healthy.

When `BACKUP_ENABLED=true`, health requires a recent, valid successful backup status. When `COLD_TIER_ENABLED=true`, health requires a recent, valid successful watchdog status. With either option disabled, its row is neutral and is not counted as a health failure. Configure the flags only after the corresponding scheduled job or watchdog is actually installed and operating.

The bulk-upload worker has its own loop heartbeat and also refreshes it after completed per-file inspection or processing. A stalled current file receives no timer refresh. Progress heartbeat writes use a separate bounded Redis client, and write failures do not interrupt imports. Its status is independent of queue depth: queued work is not evidence that the worker is dead. Likewise, pipeline activity is shown separately from service health. A pending-image count describes work waiting to be processed; worker heartbeat checks determine whether enabled consumers are alive. If the pipeline activity request itself fails, the page shows that check failure instead of omitting the information.

Health checks use finite, bounded deadlines. The storage and Redis probes run in an isolated child process, and timed-out work is stopped and reaped so an unavailable dependency cannot hold the health request indefinitely. Keep these checks read-only and bounded when extending them.
