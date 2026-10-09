"""Health checks for deployed services (server admins only)."""
import asyncio
import json
import math
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import List, Literal, Optional
from urllib.parse import urlsplit

import httpx
from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from auth.permissions import require_server_admin
from shared.database import get_async_session
from shared.logger import get_logger
from shared.models import User
from shared.queue import (
    DEVICE_KEY_CLASSIFICATION,
    DEVICE_KEY_DETECTION,
    HEARTBEAT_KEY_BULK_UPLOAD,
    HEARTBEAT_KEY_CLASSIFICATION,
    HEARTBEAT_KEY_DETECTION,
    HEARTBEAT_KEY_INGESTION,
    HEARTBEAT_KEY_NOTIFICATIONS,
    HEARTBEAT_KEY_NOTIFICATIONS_EARTHRANGER,
    HEARTBEAT_KEY_NOTIFICATIONS_EMAIL,
    HEARTBEAT_KEY_NOTIFICATIONS_SENSINGCLUES,
    HEARTBEAT_KEY_NOTIFICATIONS_TELEGRAM,
    HEARTBEAT_STALE_AFTER_MINUTES,
    QUEUE_DETECTION_COMPLETE,
    QUEUE_IMAGE_INGESTED,
    QUEUE_NOTIFICATION_EARTHRANGER,
    QUEUE_NOTIFICATION_EMAIL,
    QUEUE_NOTIFICATION_EVENTS,
    QUEUE_NOTIFICATION_SENSINGCLUES,
    QUEUE_NOTIFICATION_TELEGRAM,
    parse_heartbeat,
)

logger = get_logger("api.health")

HEALTH_CHECK_TIMEOUT_SECONDS = 2.5
HEALTH_ROUTE_TIMEOUT_SECONDS = 8.0
HEALTH_CHILD_TIMEOUT_SECONDS = 4.0
HEALTH_FRONTEND_TOTAL_TIMEOUT_SECONDS = 2.5
BACKUP_STATUS_MAX_AGE = timedelta(days=3)
HEALTH_PROBE_MODULE = "shared.health_probe"
WORKER_HEARTBEATS = {
    "ingestion": (HEARTBEAT_KEY_INGESTION, None, None),
    "bulk-upload": (HEARTBEAT_KEY_BULK_UPLOAD, None, None),
    "detection": (HEARTBEAT_KEY_DETECTION, QUEUE_IMAGE_INGESTED, DEVICE_KEY_DETECTION),
    "classification": (HEARTBEAT_KEY_CLASSIFICATION, QUEUE_DETECTION_COMPLETE, DEVICE_KEY_CLASSIFICATION),
    "notifications": (HEARTBEAT_KEY_NOTIFICATIONS, QUEUE_NOTIFICATION_EVENTS, None),
    "notifications-email": (HEARTBEAT_KEY_NOTIFICATIONS_EMAIL, QUEUE_NOTIFICATION_EMAIL, None),
    "notifications-telegram": (HEARTBEAT_KEY_NOTIFICATIONS_TELEGRAM, QUEUE_NOTIFICATION_TELEGRAM, None),
    "notifications-earthranger": (HEARTBEAT_KEY_NOTIFICATIONS_EARTHRANGER, QUEUE_NOTIFICATION_EARTHRANGER, None),
    "notifications-sensingclues": (HEARTBEAT_KEY_NOTIFICATIONS_SENSINGCLUES, QUEUE_NOTIFICATION_SENSINGCLUES, None),
}
_health_child_slots = asyncio.Semaphore(1)


class HealthProbeRoute(APIRoute):
    """Put one deadline around the whole health request, including auth deps."""

    def get_route_handler(self):
        original_handler = super().get_route_handler()

        async def bounded_handler(request: Request):
            try:
                async with asyncio.timeout(HEALTH_ROUTE_TIMEOUT_SECONDS):
                    return await original_handler(request)
            except TimeoutError:
                return JSONResponse(
                    status_code=503,
                    content={"detail": "Health request timed out"},
                )

        return bounded_handler


# The route class has to be defined before constructing its router.
router = APIRouter(prefix="/api/health", tags=["health"], route_class=HealthProbeRoute)


class ServiceStatus(BaseModel):
    """Status information for one service."""

    name: str
    status: Literal["healthy", "unhealthy", "disabled"]
    message: str
    # "cpu" or "cuda" for healthy ML workers; absent otherwise.
    device: Optional[str] = None


class ServicesHealthResponse(BaseModel):
    services: List[ServiceStatus]


def _status(name: str, status: str, message: str, device: Optional[str] = None) -> ServiceStatus:
    return ServiceStatus(name=name, status=status, message=message, device=device)


def _probe_ok(probe: Optional[dict], key: str) -> bool:
    return isinstance(probe, dict) and probe.get(key) is True


async def check_postgres(db: AsyncSession) -> ServiceStatus:
    """Use server-side statement timeout and an async deadline for SELECT 1."""
    try:
        async with asyncio.timeout(HEALTH_CHECK_TIMEOUT_SECONDS):
            # This transaction-scoped setting does not affect other sessions
            # or application queries and also bounds the database-side work.
            await db.execute(text("SELECT set_config('statement_timeout', '2000ms', true)"))
            await db.execute(text("SELECT 1"))
        return _status("postgres", "healthy", "Database connection successful")
    except Exception as exc:
        logger.warning("PostgreSQL health check failed", error_type=type(exc).__name__)
        return _status("postgres", "unhealthy", "Database check failed or timed out")


def _frontend_health_url() -> Optional[str]:
    raw = os.environ.get("FRONTEND_HEALTH_URL", "").strip()
    if not raw:
        return None
    try:
        parsed = urlsplit(raw)
        _ = parsed.port  # Access validates malformed port text.
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            return None
    except ValueError:
        return None
    return raw


async def check_frontend() -> ServiceStatus:
    url = _frontend_health_url()
    if url is None:
        return _status("frontend", "unhealthy", "Frontend health URL is missing or invalid")
    try:
        timeout = httpx.Timeout(2.0, connect=1.0)
        async with asyncio.timeout(HEALTH_FRONTEND_TOTAL_TIMEOUT_SECONDS):
            async with httpx.AsyncClient(timeout=timeout, follow_redirects=False) as client:
                response = await client.get(url)
        if response.status_code == 200:
            return _status("frontend", "healthy", "Frontend returned HTTP 200")
        return _status("frontend", "unhealthy", f"Frontend returned HTTP {response.status_code}")
    except Exception as exc:
        logger.warning("Frontend health check failed", error_type=type(exc).__name__)
        return _status("frontend", "unhealthy", "Frontend connection failed or timed out")


def _enabled_workers() -> tuple[Optional[set[str]], Optional[str]]:
    raw = os.environ.get("HEALTH_ENABLED_WORKERS")
    if raw is None or not raw.strip():
        return None, "Worker expectation configuration is missing"
    names = [part.strip() for part in raw.split(",")]
    if any(not name for name in names) or len(names) != len(set(names)):
        return None, "Worker expectation configuration is malformed"
    unknown = set(names) - set(WORKER_HEARTBEATS)
    if unknown:
        return None, "Worker expectation configuration contains unknown names"
    return set(names), None


def _disabled(name: str, reason: str) -> ServiceStatus:
    return _status(name, "disabled", reason)


async def _run_probe_process(argv: list[str], payload: bytes, timeout: float) -> Optional[bytes]:
    """Run and reap an owned subprocess; never leave timed-out work behind."""
    async def kill_and_reap(process):
        if process.returncode is None:
            process.kill()
        cleanup = asyncio.create_task(process.wait())
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await cleanup

    process = None
    try:
        process = await asyncio.create_subprocess_exec(
            *argv,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await asyncio.wait_for(process.communicate(payload), timeout=timeout)
        if process.returncode != 0 or len(stdout) > 16_384:
            return None
        return stdout
    except TimeoutError:
        if process is not None:
            await kill_and_reap(process)
        return None
    except BaseException:
        if process is not None:
            # Reap the owned child even when the request task is cancelled.
            await kill_and_reap(process)
        raise


async def _run_health_probe(workers: set[str], backup: bool, cold_tier: bool) -> Optional[dict]:
    """Run bounded sync network probes in a disposable child process."""
    acquired = False
    try:
        await asyncio.wait_for(_health_child_slots.acquire(), timeout=0.05)
        acquired = True
    except TimeoutError:
        return None

    try:
        payload = json.dumps(
            {"workers": sorted(workers), "backup": backup, "cold_tier": cold_tier},
            separators=(",", ":"),
        ).encode("utf-8")
        stdout = await _run_probe_process(
            [sys.executable, "-m", HEALTH_PROBE_MODULE],
            payload,
            HEALTH_CHILD_TIMEOUT_SECONDS,
        )
        if stdout is None:
            return None
        result = json.loads(stdout)
        if not isinstance(result, dict) or result.get("probe_failed"):
            return None
        return result
    except Exception as exc:
        logger.warning("Health probe process failed", error_type=type(exc).__name__)
        return None
    finally:
        if acquired:
            _health_child_slots.release()


def _heartbeat_status(name: str, snapshot: Optional[dict], redis_ok: bool) -> ServiceStatus:
    if not redis_ok or not isinstance(snapshot, dict):
        return _status(name, "unhealthy", "Redis health snapshot unavailable")
    stamp = parse_heartbeat(snapshot.get("stamp"))
    if stamp is None:
        return _status(name, "unhealthy", "No heartbeat recorded (worker never started)")
    age = datetime.now(timezone.utc) - stamp
    if age < timedelta(seconds=-60):
        return _status(name, "unhealthy", "Worker heartbeat is in the future")
    if age > timedelta(minutes=HEARTBEAT_STALE_AFTER_MINUTES):
        return _status(
            name,
            "unhealthy",
            f"Heartbeat stale (threshold {HEARTBEAT_STALE_AFTER_MINUTES} minutes)",
        )
    age_seconds = max(0, int(age.total_seconds()))
    age_label = f"{age_seconds} seconds ago" if age_seconds < 120 else f"{age_seconds // 60} minutes ago"
    depth = snapshot.get("depth")
    depth_label = f"; queue depth {depth}" if isinstance(depth, int) and not isinstance(depth, bool) else ""
    device = snapshot.get("device") if snapshot.get("device") in ("cpu", "cuda") else None
    return _status(name, "healthy", f"Heartbeat {age_label}{depth_label}", device)


def _worker_statuses(enabled: Optional[set[str]], config_error: Optional[str], probe: Optional[dict]) -> list[ServiceStatus]:
    if config_error or enabled is None:
        reason = config_error or "Worker expectation configuration is invalid"
        return [_status(name, "unhealthy", reason) for name in WORKER_HEARTBEATS]
    redis_ok = _probe_ok(probe, "redis_ok")
    snapshots = probe.get("workers", {}) if probe else {}
    results = []
    for name in WORKER_HEARTBEATS:
        if name not in enabled:
            results.append(_disabled(name, "Not deployed by this configuration"))
        else:
            snapshot = snapshots.get(name) if isinstance(snapshots, dict) else None
            results.append(_heartbeat_status(name, snapshot, redis_ok))
    return results


def _parse_timestamp(value) -> Optional[datetime]:
    if not isinstance(value, str):
        return None
    try:
        stamp = datetime.fromisoformat(value)
    except ValueError:
        return None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return stamp


def _recent_timestamp(value, max_age: timedelta) -> bool:
    stamp = _parse_timestamp(value)
    if stamp is None:
        return False
    age = datetime.now(timezone.utc) - stamp
    return timedelta(0) <= age <= max_age


def _feature_snapshot(probe: Optional[dict], key: str, redis_ok: bool):
    if not redis_ok or not isinstance(probe, dict):
        return None
    value = probe.get(key)
    return value if isinstance(value, dict) else None


def _valid_number(value) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def check_cold_tier_watchdog(probe: Optional[dict]) -> ServiceStatus:
    if os.environ.get("COLD_TIER_ENABLED", "false").lower() != "true":
        return _disabled("cold-tier-watchdog", "Cold tier is disabled")
    redis_ok = _probe_ok(probe, "redis_ok")
    snapshot = _feature_snapshot(probe, "cold_tier", redis_ok)
    if not snapshot or not snapshot.get("present") or not snapshot.get("valid"):
        return _status("cold-tier-watchdog", "unhealthy", "Watchdog status is missing or invalid")
    try:
        max_age = timedelta(seconds=max(300, int(os.environ.get("COLD_TIER_TICK_SECONDS", "86400")) * 3))
    except ValueError:
        return _status("cold-tier-watchdog", "unhealthy", "Watchdog interval configuration is invalid")
    if not _recent_timestamp(snapshot.get("timestamp"), max_age):
        return _status("cold-tier-watchdog", "unhealthy", "Watchdog status is stale or has an invalid timestamp")
    if snapshot.get("status") != "ok":
        return _status("cold-tier-watchdog", "unhealthy", "Last watchdog check failed")
    fields = ("hot_gb", "budget_gb", "objects_hot", "objects_cold")
    if any(
        not _valid_number(snapshot.get(field))
        for field in fields
    ):
        return _status("cold-tier-watchdog", "unhealthy", "Watchdog status is invalid")
    return _status("cold-tier-watchdog", "healthy", "Recent watchdog check succeeded")


def check_backup(probe: Optional[dict]) -> ServiceStatus:
    if os.environ.get("BACKUP_ENABLED", "false").lower() != "true":
        return _disabled("backup", "Automated backups are disabled")
    redis_ok = _probe_ok(probe, "redis_ok")
    snapshot = _feature_snapshot(probe, "backup", redis_ok)
    if not snapshot or not snapshot.get("present") or not snapshot.get("valid"):
        return _status("backup", "unhealthy", "Backup status is missing or invalid")
    if not _recent_timestamp(snapshot.get("timestamp"), BACKUP_STATUS_MAX_AGE):
        return _status("backup", "unhealthy", "Backup status is stale or has an invalid timestamp")
    if snapshot.get("status") != "ok":
        return _status("backup", "unhealthy", "Last backup did not complete successfully")
    duration = snapshot.get("duration_s")
    if not _valid_number(duration):
        return _status("backup", "unhealthy", "Backup status is invalid")
    return _status("backup", "healthy", "Recent backup completed successfully")


@router.get("/services", response_model=ServicesHealthResponse)
async def get_services_health(
    current_user: User = Depends(require_server_admin),
    db: AsyncSession = Depends(get_async_session),
):
    """Return health of infrastructure and explicitly enabled workers."""
    logger.info("Health check requested", user_id=current_user.id)
    enabled, config_error = _enabled_workers()
    workers = enabled or set()
    backup_enabled = os.environ.get("BACKUP_ENABLED", "false").lower() == "true"
    cold_enabled = os.environ.get("COLD_TIER_ENABLED", "false").lower() == "true"

    postgres_task = check_postgres(db)
    frontend_task = check_frontend()
    probe_task = _run_health_probe(workers, backup_enabled, cold_enabled)
    postgres, frontend, probe = await asyncio.gather(postgres_task, frontend_task, probe_task)

    services = [
        postgres,
        _status(
            "redis",
            "healthy" if _probe_ok(probe, "redis_ok") else "unhealthy",
            "Redis connection successful" if _probe_ok(probe, "redis_ok") else "Redis probe failed or timed out",
        ),
        _status(
            "minio",
            "healthy" if _probe_ok(probe, "storage_ok") else "unhealthy",
            "Authenticated raw-images bucket check succeeded" if _probe_ok(probe, "storage_ok") else "Configured object storage check failed or timed out",
        ),
        frontend,
        *(_worker_statuses(enabled, config_error, probe)),
        check_cold_tier_watchdog(probe),
        check_backup(probe),
        _status("api", "healthy", "Service is running"),
    ]
    logger.info(
        "Health check completed",
        healthy_count=sum(1 for service in services if service.status == "healthy"),
        unhealthy_count=sum(1 for service in services if service.status == "unhealthy"),
        disabled_count=sum(1 for service in services if service.status == "disabled"),
    )
    return ServicesHealthResponse(services=services)
