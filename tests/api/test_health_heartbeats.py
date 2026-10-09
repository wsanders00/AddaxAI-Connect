"""Deployment-aware API health and child-probe contracts."""
from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

import pytest
from fastapi import FastAPI
import httpx

_api = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "services", "api"))
if _api not in sys.path:
    sys.path.insert(0, _api)

from auth.permissions import require_server_admin  # noqa: E402
from routers import health as health_router  # noqa: E402
from shared import health_probe, storage as storage_module  # noqa: E402


def _stamp(minutes_ago: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)).isoformat()


def _healthy_cold_stamp(timestamp: str) -> dict:
    return {
        "present": True,
        "valid": True,
        "status": "ok",
        "timestamp": timestamp,
        "hot_gb": 1.0,
        "budget_gb": 80.0,
        "objects_hot": 1,
        "objects_cold": 0,
    }


def _healthy_backup_stamp(timestamp: str) -> dict:
    return {"present": True, "valid": True, "status": "ok", "timestamp": timestamp, "duration_s": 5}


class _FakeRedisClient:
    def __init__(self, values=None):
        self.values = values or {}

    def get(self, key):
        return self.values.get(key)


def test_health_s3_client_uses_configured_endpoint_with_bounded_single_attempt(monkeypatch):
    seen = {}

    def fake_client(service, **kwargs):
        seen.update(service=service, **kwargs)
        return object()

    monkeypatch.setattr(storage_module.boto3, "client", fake_client)
    storage_module.create_health_check_client()
    config = seen["config"]
    assert seen["service"] == "s3"
    assert seen["endpoint_url"] == f"http://{storage_module.settings.minio_endpoint}"
    assert config.connect_timeout == 2
    assert config.read_timeout == 2
    assert config.retries["total_max_attempts"] == 1
    assert config.s3["addressing_style"] == "path"


def test_child_rejects_scalar_null_and_array_redis_status_payloads():
    for raw in ('null', '[]', '7', '"text"', '{broken'):
        snapshot = health_probe._status_snapshot(_FakeRedisClient({"key": raw}), "key", "backup")
        assert snapshot == {"present": True, "valid": False}


def test_child_status_validation_rejects_nonfinite_duration_and_future_is_checked_by_parent(monkeypatch):
    raw = json.dumps({"status": "ok", "timestamp": _stamp(1), "duration_s": float("nan")})
    snapshot = health_probe._status_snapshot(_FakeRedisClient({"key": raw}), "key", "backup")
    assert snapshot["present"] is True
    assert snapshot["valid"] is False

    monkeypatch_probe = {
        "redis_ok": True,
        "backup": _healthy_backup_stamp(_stamp(-5)),
    }
    monkeypatch.setenv("BACKUP_ENABLED", "true")
    assert health_router.check_backup(monkeypatch_probe).status == "unhealthy"


def test_status_numeric_overflow_is_invalid_in_child_and_parent(monkeypatch):
    huge = 10**400
    cold = _healthy_cold_stamp(_stamp(1))
    cold["objects_hot"] = huge
    backup = _healthy_backup_stamp(_stamp(1))
    backup["duration_s"] = huge
    for kind, payload in (("cold_tier", cold), ("backup", backup)):
        snapshot = health_probe._status_snapshot(_FakeRedisClient({"key": json.dumps(payload)}), "key", kind)
        assert snapshot["valid"] is False
    monkeypatch.setenv("BACKUP_ENABLED", "true")
    monkeypatch.setenv("COLD_TIER_ENABLED", "true")
    probe = {"redis_ok": True, "backup": backup, "cold_tier": cold}
    assert health_router.check_backup(probe).status == "unhealthy"
    assert health_router.check_cold_tier_watchdog(probe).status == "unhealthy"


def test_child_collects_only_sanitized_s3_and_redis_snapshot(monkeypatch):
    class S3:
        def __init__(self):
            self.calls = []

        def head_bucket(self, **kwargs):
            self.calls.append(kwargs)

        def close(self):
            pass

    s3 = S3()

    class Redis:
        def ping(self):
            return True

        def get(self, key):
            return {"heartbeat:bulk-upload": _stamp(0.1)}.get(key)

        def llen(self, _key):
            return 0

        def close(self):
            pass

    monkeypatch.setattr(health_probe, "create_health_check_client", lambda: s3)
    monkeypatch.setattr(health_probe.redis.Redis, "from_url", staticmethod(lambda *_a, **_k: Redis()))
    snapshot = health_probe.collect_snapshot({"workers": ["bulk-upload"], "backup": False, "cold_tier": False})
    assert s3.calls == [{"Bucket": "raw-images"}]
    assert snapshot["storage_ok"] is True
    assert snapshot["redis_ok"] is True
    assert snapshot["workers"]["bulk-upload"]["stamp"] is not None
    assert snapshot["workers"]["bulk-upload"]["depth"] is None
    assert "secret" not in json.dumps(snapshot).lower()


def test_worker_configuration_requires_known_explicit_names(monkeypatch):
    for value in (None, "", "bulk-upload,,detection", "detection,detection", "detection,unknown"):
        monkeypatch.delenv("HEALTH_ENABLED_WORKERS", raising=False)
        if value is not None:
            monkeypatch.setenv("HEALTH_ENABLED_WORKERS", value)
        enabled, error = health_router._enabled_workers()
        assert enabled is None
        assert error


def test_required_worker_heartbeat_missing_stale_and_fresh_are_distinct():
    assert health_router._heartbeat_status("bulk-upload", {}, True).status == "unhealthy"
    stale = health_router._heartbeat_status("bulk-upload", {"stamp": _stamp(20)}, True)
    assert stale.status == "unhealthy"
    assert "stale" in stale.message
    fresh = health_router._heartbeat_status("bulk-upload", {"stamp": _stamp(0.5)}, True)
    assert fresh.status == "healthy"
    assert health_router._heartbeat_status("bulk-upload", {"stamp": _stamp(0.5)}, False).status == "unhealthy"


def test_disabled_workers_are_neutral_but_bad_contract_fails_closed():
    rows = health_router._worker_statuses(
        {"bulk-upload", "detection", "classification"}, None,
        {"redis_ok": True, "workers": {"bulk-upload": {"stamp": _stamp(0.1)}}},
    )
    statuses = {row.name: row.status for row in rows}
    assert statuses["bulk-upload"] == "healthy"
    assert statuses["detection"] == "unhealthy"
    assert statuses["notifications-telegram"] == "disabled"
    invalid = health_router._worker_statuses(None, "bad worker configuration", None)
    assert all(row.status == "unhealthy" for row in invalid)


def test_disabled_backup_and_cold_tier_ignore_old_redis_data(monkeypatch):
    monkeypatch.setenv("BACKUP_ENABLED", "false")
    monkeypatch.setenv("COLD_TIER_ENABLED", "false")
    probe = {"redis_ok": False, "backup": {"status": "error"}, "cold_tier": {"status": "error"}}
    assert health_router.check_backup(probe).status == "disabled"
    assert health_router.check_cold_tier_watchdog(probe).status == "disabled"


def test_enabled_backup_and_cold_tier_reject_missing_stale_malformed_and_failed(monkeypatch):
    monkeypatch.setenv("BACKUP_ENABLED", "true")
    monkeypatch.setenv("COLD_TIER_ENABLED", "true")
    assert health_router.check_backup({"redis_ok": True, "backup": None}).status == "unhealthy"
    assert health_router.check_cold_tier_watchdog({"redis_ok": True, "cold_tier": None}).status == "unhealthy"
    for bad in (None, [], 7, "bad"):
        assert health_router.check_backup({"redis_ok": True, "backup": bad}).status == "unhealthy"
        assert health_router.check_cold_tier_watchdog({"redis_ok": True, "cold_tier": bad}).status == "unhealthy"
    assert health_router.check_backup({"redis_ok": True, "backup": _healthy_backup_stamp(_stamp(4 * 24 * 60))}).status == "unhealthy"
    assert health_router.check_cold_tier_watchdog({"redis_ok": True, "cold_tier": _healthy_cold_stamp(_stamp(4 * 24 * 60))}).status == "unhealthy"
    assert health_router.check_backup({"redis_ok": True, "backup": {**_healthy_backup_stamp(_stamp(1)), "status": "error"}}).status == "unhealthy"
    assert health_router.check_cold_tier_watchdog({"redis_ok": True, "cold_tier": {**_healthy_cold_stamp(_stamp(1)), "status": "error"}}).status == "unhealthy"
    assert health_router.check_backup({"redis_ok": True, "backup": _healthy_backup_stamp(_stamp(1))}).status == "healthy"
    assert health_router.check_cold_tier_watchdog({"redis_ok": True, "cold_tier": _healthy_cold_stamp(_stamp(1))}).status == "healthy"


@pytest.mark.asyncio
async def test_health_endpoint_assembles_required_and_disabled_rows(monkeypatch):
    class User:
        id = "test-user"

    class Session:
        pass

    async def healthy(name, *_args):
        return health_router.ServiceStatus(name=name, status="healthy", message="fixture")

    async def fake_probe(*_args):
        stamp = _stamp(0.5)
        return {
            "storage_ok": True,
            "redis_ok": True,
            "workers": {
                name: {"stamp": stamp, "depth": 0, "device": "cuda"}
                for name in ("bulk-upload", "detection", "classification")
            },
            "backup": None,
            "cold_tier": None,
        }

    monkeypatch.setenv("HEALTH_ENABLED_WORKERS", "bulk-upload,detection,classification")
    monkeypatch.setenv("BACKUP_ENABLED", "false")
    monkeypatch.setenv("COLD_TIER_ENABLED", "false")
    monkeypatch.setattr(health_router, "check_postgres", lambda db: healthy("postgres", db))
    monkeypatch.setattr(health_router, "check_frontend", lambda: healthy("frontend"))
    monkeypatch.setattr(health_router, "_run_health_probe", fake_probe)
    response = await health_router.get_services_health(current_user=User(), db=Session())
    rows = {row.name: row for row in response.services}
    assert rows["postgres"].status == "healthy"
    assert rows["redis"].status == "healthy"
    assert rows["minio"].status == "healthy"
    assert rows["bulk-upload"].status == "healthy"
    assert rows["detection"].device == "cuda"
    assert rows["classification"].status == "healthy"
    assert rows["ingestion"].status == "disabled"
    assert rows["backup"].status == "disabled"
    assert rows["cold-tier-watchdog"].status == "disabled"


@pytest.mark.asyncio
async def test_frontend_requires_http_200_and_total_deadline(monkeypatch):
    class Response:
        status_code = 200

    class Client:
        def __init__(self, **kwargs):
            assert kwargs["follow_redirects"] is False

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def get(self, _url):
            return Response()

    monkeypatch.setenv("FRONTEND_HEALTH_URL", "http://frontend:80")
    monkeypatch.setattr(health_router.httpx, "AsyncClient", Client)
    assert (await health_router.check_frontend()).status == "healthy"

    class RedirectClient(Client):
        async def get(self, _url):
            response = Response()
            response.status_code = 301
            return response

    monkeypatch.setattr(health_router.httpx, "AsyncClient", RedirectClient)
    assert (await health_router.check_frontend()).status == "unhealthy"

    class TricklingClient(Client):
        async def get(self, _url):
            await asyncio.sleep(1)

    monkeypatch.setattr(health_router, "HEALTH_FRONTEND_TOTAL_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(health_router.httpx, "AsyncClient", TricklingClient)
    assert (await health_router.check_frontend()).status == "unhealthy"


@pytest.mark.asyncio
async def test_probe_subprocess_timeout_kills_and_reaps_child(tmp_path):
    pid_path = tmp_path / "probe.pid"
    code = "import os,sys,time; open(sys.argv[1], 'w').write(str(os.getpid())); time.sleep(30)"
    task = asyncio.create_task(health_router._run_probe_process([sys.executable, "-c", code, str(pid_path)], b"", 5))
    for _ in range(100):
        if pid_path.exists():
            break
        await asyncio.sleep(0.01)
    assert pid_path.exists()
    pid = int(pid_path.read_text())
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


@pytest.mark.asyncio
async def test_probe_subprocess_deadline_kills_and_reaps_child(tmp_path):
    pid_path = tmp_path / "probe-timeout.pid"
    code = "import os,sys,time; open(sys.argv[1], 'w').write(str(os.getpid())); time.sleep(30)"
    result = await health_router._run_probe_process(
        [sys.executable, "-c", code, str(pid_path)], b"", 0.05
    )
    assert result is None
    assert pid_path.exists()
    with pytest.raises(ProcessLookupError):
        os.kill(int(pid_path.read_text()), 0)


@pytest.mark.asyncio
async def test_probe_process_concurrency_is_capped(monkeypatch):
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = []

    async def slow_process(*_args):
        calls.append(1)
        entered.set()
        await release.wait()
        return b'{"storage_ok":true,"redis_ok":true,"workers":{}}'

    monkeypatch.setattr(health_router, "_health_child_slots", asyncio.Semaphore(1))
    monkeypatch.setattr(health_router, "_run_probe_process", slow_process)
    first = asyncio.create_task(health_router._run_health_probe(set(), False, False))
    await entered.wait()
    second = await health_router._run_health_probe(set(), False, False)
    release.set()
    await first
    assert second is None
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_health_route_deadline_includes_admin_auth_dependency(monkeypatch):
    app = FastAPI()
    # Use the same inclusion path as the application so dependency overrides
    # and the health route class both participate in request handling.
    app.include_router(health_router.router)

    async def hanging_admin_auth():
        await asyncio.sleep(1)

    app.dependency_overrides[require_server_admin] = hanging_admin_auth
    monkeypatch.setattr(health_router, "HEALTH_ROUTE_TIMEOUT_SECONDS", 0.02)
    assert health_router.router.routes[0].dependant.dependencies[0].call is require_server_admin
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/api/health/services")
    assert response.status_code == 503
    assert response.json() == {"detail": "Health request timed out"}


@pytest.mark.asyncio
@pytest.mark.parametrize("stall", [False, True])
async def test_real_asyncpg_down_or_stalled_connect_is_bounded_and_api_stays_responsive(monkeypatch, stall):
    connections = []

    async def stalled_peer(reader, writer):
        connections.append(writer)
        await reader.read()

    peer = await asyncio.start_server(stalled_peer, "127.0.0.1", 0)
    port = peer.sockets[0].getsockname()[1]
    if not stall:
        peer.close()
        await peer.wait_closed()
    engine = create_async_engine(f"postgresql+asyncpg://test:test@127.0.0.1:{port}/test")
    monkeypatch.setattr(health_router, "HEALTH_CHECK_TIMEOUT_SECONDS", 0.1)
    app = FastAPI()

    @app.get("/ping")
    async def ping():
        return {"ok": True}

    try:
        async with AsyncSession(engine) as db:
            fault = asyncio.create_task(health_router.check_postgres(db))
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
                response = await asyncio.wait_for(client.get("/ping"), 0.1)
                assert response.json() == {"ok": True}
            row = await asyncio.wait_for(fault, 0.5)
            assert row.status == "unhealthy"
    finally:
        for writer in connections:
            writer.close()
            await writer.wait_closed()
        peer.close()
        await peer.wait_closed()
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [200, 403, 404])
async def test_real_child_s3_uses_signed_read_only_head_and_handles_server_failures(monkeypatch, status):
    seen = []
    tasks = set()

    async def s3_peer(reader, writer):
        task = asyncio.current_task()
        tasks.add(task)
        try:
            headers = await reader.readuntil(b"\r\n\r\n")
            seen.append((headers.split(b"\r\n", 1)[0], b"Authorization: AWS4-HMAC-SHA256" in headers))
            writer.write(f"HTTP/1.1 {status} Fixture\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".encode())
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            tasks.discard(task)

    server = await asyncio.start_server(s3_peer, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    monkeypatch.setenv("MINIO_ENDPOINT", f"127.0.0.1:{port}")
    monkeypatch.setenv("REDIS_URL", "redis://127.0.0.1:1/0")
    monkeypatch.setenv("PYTHONPATH", str(Path(__file__).resolve().parents[2] / "shared"))
    try:
        snapshot = await health_router._run_health_probe(set(), False, False)
        assert snapshot is not None
        assert snapshot["storage_ok"] is (status == 200)
        assert snapshot["redis_ok"] is False
        assert seen == [(b"HEAD /raw-images HTTP/1.1", True)]
    finally:
        server.close()
        await server.wait_closed()
        if tasks:
            await asyncio.gather(*tasks)


@pytest.mark.asyncio
async def test_real_stalled_redis_peer_is_terminated_without_blocking_the_api(monkeypatch):
    redis_entered = asyncio.Event()
    redis_closed = asyncio.Event()

    async def s3_peer(reader, writer):
        await reader.readuntil(b"\r\n\r\n")
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    async def redis_peer(reader, writer):
        await reader.read(4096)
        redis_entered.set()
        await reader.read()
        writer.close()
        await writer.wait_closed()
        redis_closed.set()

    s3 = await asyncio.start_server(s3_peer, "127.0.0.1", 0)
    redis_server = await asyncio.start_server(redis_peer, "127.0.0.1", 0)
    monkeypatch.setenv("MINIO_ENDPOINT", f"127.0.0.1:{s3.sockets[0].getsockname()[1]}")
    monkeypatch.setenv("REDIS_URL", f"redis://127.0.0.1:{redis_server.sockets[0].getsockname()[1]}/0")
    monkeypatch.setenv("PYTHONPATH", str(Path(__file__).resolve().parents[2] / "shared"))
    monkeypatch.setattr(health_router, "HEALTH_CHILD_TIMEOUT_SECONDS", 1.5)
    task = asyncio.create_task(health_router._run_health_probe(set(), False, False))
    try:
        await asyncio.wait_for(redis_entered.wait(), 1.0)
        await asyncio.wait_for(asyncio.sleep(0), 0.1)
        snapshot = await asyncio.wait_for(task, 2.0)
        assert snapshot is None or snapshot["redis_ok"] is False
        await asyncio.wait_for(redis_closed.wait(), 0.5)
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        s3.close()
        redis_server.close()
        await s3.wait_closed()
        await redis_server.wait_closed()
