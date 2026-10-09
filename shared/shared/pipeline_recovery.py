"""Durable, bounded recovery helpers for the detection/classification pipeline."""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import threading
from uuid import uuid4

from sqlalchemy import update

from .database import get_db_session
from .logger import get_logger
from .models import Image
from .queue import (
    RedisQueue,
    QUEUE_IMAGE_INGESTED,
    QUEUE_IMAGE_INGESTED_BULK,
    QUEUE_DETECTION_COMPLETE,
    QUEUE_DETECTION_COMPLETE_BULK,
)

logger = get_logger("pipeline_recovery")
LEASE_SECONDS = 10 * 60
MAX_STAGE_ATTEMPTS = 3
LEASE_HEARTBEAT_SECONDS = 60


def stale_recovery_action(stage: str, status: str, attempts: int) -> tuple[str, str | None]:
    """Return the safe requeue state or bounded failure for a stale row."""
    stage_statuses = {
        "detection": {"pending", "processing"},
        "classification": {"detected", "classifying"},
    }
    if stage not in stage_statuses or status not in stage_statuses[stage]:
        raise ValueError(f"Invalid stale {stage} stage status: {status}")
    if attempts >= MAX_STAGE_ATTEMPTS:
        return "failed", f"{stage.title()} stopped after {attempts} attempts; use Retry to resume this image."
    return ("pending" if stage == "detection" else "detected"), None


def claim_image_stage(image_uuid: str, expected_status: str, active_status: str) -> str | None:
    """Atomically claim a queue delivery and return its fencing token."""
    now = datetime.now(timezone.utc)
    claim_id = str(uuid4())
    with get_db_session() as db:
        result = db.execute(
            update(Image)
            .where(Image.uuid == image_uuid, Image.status == expected_status)
            .values(
                status=active_status,
                pipeline_attempts=Image.pipeline_attempts + 1,
                pipeline_updated_at=now,
                pipeline_error=None,
                pipeline_failed_stage=None,
                pipeline_claim_id=claim_id,
            )
        )
        db.commit()
        return claim_id if result.rowcount == 1 else None


def set_pipeline_status(
    image_uuid: str, status: str, error: str | None = None,
    failed_stage: str | None = None, claim_id: str | None = None,
) -> None:
    """Persist a stage transition and its lease timestamp."""
    with get_db_session() as db:
        where = [Image.uuid == image_uuid]
        if claim_id:
            where.append(Image.pipeline_claim_id == claim_id)
        result = db.execute(
            update(Image)
            .where(*where)
            .values(
                status=status,
                pipeline_updated_at=datetime.now(timezone.utc),
                pipeline_error=error[:1000] if error else None,
                pipeline_failed_stage=failed_stage if status == "failed" else None,
                pipeline_claim_id=None,
                pipeline_attempts=0 if status in ("detected", "classified") else Image.pipeline_attempts,
            )
        )
        if result.rowcount != 1:
            raise ValueError(f"Image not found: {image_uuid}")
        db.commit()


def fail_pipeline_stage(
    image_uuid: str, active_status: str, stage: str, claim_id: str, exc: Exception
) -> bool:
    """Fail only if this worker still owns the active stage.

    If status already advanced to the next durable stage, a queue publish
    failure is repaired by that stage's reconciler instead of hiding its work.
    """
    with get_db_session() as db:
        result = db.execute(
            update(Image)
            .where(
                Image.uuid == image_uuid,
                Image.status == active_status,
                Image.pipeline_claim_id == claim_id,
            )
            .values(
                status="failed",
                pipeline_updated_at=datetime.now(timezone.utc),
                pipeline_error=f"{stage.title()} failed ({type(exc).__name__}); use Retry to run this image again.",
                pipeline_failed_stage=stage,
                pipeline_claim_id=None,
            )
        )
        db.commit()
        return result.rowcount == 1


@contextmanager
def maintain_image_lease(image_uuid: str, active_status: str, claim_id: str):
    """Refresh a claimed stage lease while long inference is running."""
    stopped = threading.Event()

    def beat() -> None:
        while not stopped.wait(LEASE_HEARTBEAT_SECONDS):
            try:
                with get_db_session() as db:
                    db.execute(
                        update(Image)
                        .where(
                            Image.uuid == image_uuid,
                            Image.status == active_status,
                            Image.pipeline_claim_id == claim_id,
                        )
                        .values(pipeline_updated_at=datetime.now(timezone.utc))
                    )
                    db.commit()
            except Exception as exc:
                logger.warning("Could not refresh image pipeline lease", image_uuid=image_uuid, error=str(exc))

    thread = threading.Thread(target=beat, name=f"lease-{image_uuid[:8]}", daemon=True)
    thread.start()
    try:
        yield
    finally:
        stopped.set()
        thread.join(timeout=1)


def reconcile_stale_images(stage: str, limit: int = 100) -> int:
    """Requeue stale image stages; after three executions, fail visibly."""
    if stage == "detection":
        statuses = ("pending", "processing")
        target_status = "pending"
    elif stage == "classification":
        statuses = ("detected", "classifying")
        target_status = "detected"
    else:
        raise ValueError(f"Unknown pipeline stage: {stage}")

    cutoff = datetime.now(timezone.utc) - timedelta(seconds=LEASE_SECONDS)
    with get_db_session() as db:
        rows = (
            db.query(Image)
            .filter(Image.status.in_(statuses), Image.pipeline_updated_at < cutoff)
            .order_by(Image.pipeline_updated_at)
            .with_for_update(skip_locked=True)
            .limit(limit)
            .all()
        )
        queued: list[dict] = []
        for image in rows:
            target, error = stale_recovery_action(stage, image.status, image.pipeline_attempts)
            if target == "failed":
                image.status = target
                image.pipeline_error = error
                image.pipeline_failed_stage = stage
                image.pipeline_claim_id = None
                image.pipeline_updated_at = datetime.now(timezone.utc)
                continue

            image.status = target
            image.pipeline_error = None
            image.pipeline_failed_stage = None
            image.pipeline_claim_id = None
            image.pipeline_updated_at = datetime.now(timezone.utc)
            queued.append({
                "image_uuid": image.uuid,
                "storage_path": image.storage_path,
                "camera_id": image.camera_id,
                "origin": image.origin or "live",
                "num_detections": len(image.detections or []),
                "detection_ids": [d.id for d in (image.detections or [])],
            })
        db.commit()

    for message in queued:
        bulk = message["origin"] == "bulk"
        if stage == "detection":
            queue_name = QUEUE_IMAGE_INGESTED_BULK if bulk else QUEUE_IMAGE_INGESTED
            payload = {k: message[k] for k in ("image_uuid", "storage_path", "camera_id", "origin")}
        else:
            queue_name = QUEUE_DETECTION_COMPLETE_BULK if bulk else QUEUE_DETECTION_COMPLETE
            payload = {k: message[k] for k in ("image_uuid", "num_detections", "detection_ids", "origin")}
        RedisQueue(queue_name).publish(payload)

    if rows:
        logger.info("Reconciled stale pipeline images", stage=stage, requeued=len(queued), examined=len(rows))
    return len(queued)
