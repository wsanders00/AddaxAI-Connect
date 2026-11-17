"""Isolated, read-only S3 and Redis probes used by the API health route.

This module runs in a disposable child process. It intentionally emits only a
small sanitized snapshot: exception text and Redis payload error strings never
cross the process boundary.
"""
from __future__ import annotations

import json
import math
import sys
from collections.abc import Mapping

import redis

from .config import get_settings
from .queue import (
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
    QUEUE_DETECTION_COMPLETE,
    QUEUE_IMAGE_INGESTED,
    QUEUE_NOTIFICATION_EARTHRANGER,
    QUEUE_NOTIFICATION_EMAIL,
    QUEUE_NOTIFICATION_EVENTS,
    QUEUE_NOTIFICATION_SENSINGCLUES,
    QUEUE_NOTIFICATION_TELEGRAM,
)
from .storage import BUCKET_RAW_IMAGES, create_health_check_client

WORKERS = {
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


def _valid_number(value) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def _status_snapshot(client, key: str, kind: str) -> dict:
    raw = client.get(key)
    if raw is None:
        return {"present": False, "valid": False}
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        return {"present": True, "valid": False}
    if not isinstance(payload, Mapping):
        return {"present": True, "valid": False}

    state = payload.get("status")
    timestamp = payload.get("timestamp")
    if (
        not isinstance(state, str)
        or len(state) > 32
        or not isinstance(timestamp, str)
        or len(timestamp) > 64
    ):
        return {"present": True, "valid": False}
    result = {"present": True, "valid": True, "status": state, "timestamp": timestamp}
    if kind == "backup":
        duration = payload.get("duration_s")
        result["valid"] = _valid_number(duration)
        if result["valid"]:
            result["duration_s"] = duration
    else:
        fields = ("hot_gb", "budget_gb")
        counts = ("objects_hot", "objects_cold")
        result["valid"] = (
            all(_valid_number(payload.get(field)) for field in fields)
            and all(
                isinstance(payload.get(field), int)
                and _valid_number(payload[field])
                for field in counts
            )
        )
        if result["valid"]:
            result.update({field: payload[field] for field in (*fields, *counts)})
    return result


def collect_snapshot(request: dict) -> dict:
    enabled = request.get("workers")
    if not isinstance(enabled, list) or any(name not in WORKERS for name in enabled):
        return {"probe_failed": True}

    storage_ok = False
    try:
        s3 = create_health_check_client()
        try:
            s3.head_bucket(Bucket=BUCKET_RAW_IMAGES)
            storage_ok = True
        finally:
            s3.close()
    except Exception:
        pass

    snapshot = {
        "storage_ok": storage_ok,
        "redis_ok": False,
        "workers": {},
        "backup": None,
        "cold_tier": None,
    }
    client = None
    try:
        client = redis.Redis.from_url(
            get_settings().redis_url,
            decode_responses=True,
            socket_connect_timeout=1.0,
            socket_timeout=1.0,
            retry_on_timeout=False,
        )
        client.ping()
        snapshot["redis_ok"] = True
        for name in enabled:
            heartbeat_key, queue_name, device_key = WORKERS[name]
            stamp = client.get(heartbeat_key)
            depth = client.llen(queue_name) if queue_name else None
            device = client.get(device_key) if device_key else None
            snapshot["workers"][name] = {
                "stamp": stamp if isinstance(stamp, str) and len(stamp) <= 128 else None,
                "depth": depth if isinstance(depth, int) and depth >= 0 else None,
                "device": device if device in ("cpu", "cuda") else None,
            }
        if request.get("backup"):
            snapshot["backup"] = _status_snapshot(client, "backup:last_run", "backup")
        if request.get("cold_tier"):
            snapshot["cold_tier"] = _status_snapshot(client, "cold_tier:status", "cold_tier")
    except Exception:
        # A failed Redis round trip means no Redis-derived row can be trusted.
        snapshot["redis_ok"] = False
        snapshot["workers"] = {}
        snapshot["backup"] = None
        snapshot["cold_tier"] = None
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass
    return snapshot


def main() -> None:
    try:
        line = sys.stdin.buffer.readline(4097)
        if len(line) > 4096 or not line:
            result = {"probe_failed": True}
        else:
            request = json.loads(line)
            result = collect_snapshot(request) if isinstance(request, dict) else {"probe_failed": True}
    except BaseException:
        result = {"probe_failed": True}
    sys.stdout.write(json.dumps(result, separators=(",", ":")))
    sys.stdout.write("\n")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
