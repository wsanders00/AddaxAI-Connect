"""
Bulk-upload worker

One message per job, phase="process", published by the API once the
client has finished uploading every file to the job's staging prefix.
The worker lists the prefix, ingests each object into the live
pipeline via the bulk priority queues, then deletes the staging. Status
moves uploading -> processing -> done (lazily, on API read, once
detection and classification finish).

Bulk-origin images carry origin='bulk' through every queue message.
That gates notification fan-out at the classification stage so an
SD-card import never fires species_detection alerts retroactively.

Legacy path: jobs created before the per-file refactor have a
staged_object_key ending in '.zip' and used a separate "inspect" phase.
That path is retained so any in-flight legacy job can drain after the
refactor lands. New jobs use the prefix layout.
"""
import contextlib
import hashlib
import os
import signal
import sys
import tempfile
import time
import uuid
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Iterator, Optional

import redis
from redis.backoff import NoBackoff
from redis.retry import Retry
# Vendored copy of the live ingestion service. Same Python module path
# as ingestion uses internally so the imports below work unchanged.
sys.path.insert(0, "/ingestion_lib")

from PIL import Image as PILImage
from PIL.ExifTags import TAGS
from sqlalchemy import func, select, text, update

from shared.bulk_jobs import is_bulk_job_cancelled
from shared.camera_profiles import identify_camera_profile
from shared.config import get_settings
from shared.database import get_db_session
from shared.logger import get_logger, set_image_id
from shared.models import BulkUploadJob, Camera, Image
from shared.bulk_outcomes import ledger_from_manifest, terminal_status
from shared.queue import (
    QUEUE_BULK_UPLOAD_JOB,
    QUEUE_BULK_UPLOAD_JOB_PROCESS,
    QUEUE_IMAGE_INGESTED_BULK,
    HEARTBEAT_KEY_BULK_UPLOAD,
    RedisQueue,
)
from shared.storage import BUCKET_BULK_UPLOAD_STAGING, StorageClient

from db_operations import create_image_record, get_or_create_bulk_deployment  # noqa: E402
from exif_parser import extract_exif, get_corrected_datetime  # noqa: E402
from storage_operations import (  # noqa: E402
    generate_and_upload_thumbnail,
    upload_image_to_minio,
)
from validators import validate_image  # noqa: E402
from utils import is_valid_gps  # noqa: E402

PROGRESS_PERSIST_EVERY = 25
IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png")
# Wall-clock cap per file inside the worker. Normal ingestion (EXIF
# read + MinIO write + thumbnail) is 1-2 s; a corrupt JPEG that hangs
# Pillow or a stuck MinIO connection could otherwise wedge the entire
# job. 60 s is two orders of magnitude over the normal case, generous
# enough that legitimate slow disks finish but tight enough that one
# bad frame does not eat the whole batch.
PER_FILE_TIMEOUT_SECONDS = 60
PROGRESS_HEARTBEAT_RETRY_SECONDS = 5
JOB_LEASE_SECONDS = 10 * 60
MAX_JOB_ATTEMPTS = 3


def _new_progress_redis():
    """Create a short-timeout client used only for best-effort progress stamps."""
    return redis.Redis.from_url(
        get_settings().redis_url,
        decode_responses=True,
        socket_connect_timeout=0.25,
        socket_timeout=0.5,
        retry=Retry(NoBackoff(), retries=0),
        retry_on_timeout=False,
    )


def _heartbeat_progress(items):
    """Stamp liveness only after a real unit of bulk work has completed.

    Yield before stamping so a blocked item does not keep the worker looking
    alive. Redis trouble is best-effort: it must not fail otherwise valid
    import work. Warn once per consecutive failure streak without including
    connection details. Failed writes are retried only after a short
    cooldown, and only when another item actually completes.
    """
    client = None
    warned = False
    retry_after = 0.0
    try:
        for item in items:
            yield item
            if time.monotonic() < retry_after:
                continue
            try:
                if client is None:
                    client = _new_progress_redis()
                client.set(
                    HEARTBEAT_KEY_BULK_UPLOAD,
                    datetime.now(timezone.utc).isoformat(),
                )
                warned = False
            except Exception:
                if client is not None:
                    try:
                        client.close()
                    except Exception:
                        pass
                client = None
                retry_after = time.monotonic() + PROGRESS_HEARTBEAT_RETRY_SECONDS
                if not warned:
                    logger.warning("Bulk worker progress heartbeat could not be written")
                    warned = True
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                if not warned:
                    logger.warning("Bulk worker progress heartbeat client could not be closed")


class _FileTimeout(Exception):
    """Raised by the SIGALRM handler when a single file exceeds its budget."""


@contextlib.contextmanager
def _file_timeout(seconds: int) -> Iterator[None]:
    """
    Bound a block of code with SIGALRM. The bulk-upload worker is a
    single-threaded queue consumer, so SIGALRM is safe (signal-based
    timeouts require running in the main thread, which we always are).
    The previous handler is restored on exit.
    """
    def _on_alarm(_signum, _frame):
        raise _FileTimeout()

    prev = signal.signal(signal.SIGALRM, _on_alarm)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, prev)
# Filesystem cruft that ends up inside ZIPs but isn't a real user file.
# macOS adds __MACOSX/._* shadow entries to every zip it creates; macOS
# and Windows both drop .DS_Store / Thumbs.db / desktop.ini turds. We
# drop these silently rather than counting them as "skipped", because
# the skipped count is meant to surface real user issues.
_NOISE_NAMES = {"__MACOSX", ".DS_Store", "Thumbs.db", "desktop.ini"}


def _is_noise_entry(name: str) -> bool:
    for part in name.split("/"):
        if part in _NOISE_NAMES or part.startswith("._"):
            return True
    return False


def _read_exif_fast(path: str) -> dict:
    """
    In-process EXIF read for the inspect phase. Cheap (~5 ms/image)
    compared to spawning exiftool per file, which matters when we are
    inspecting 5,000 images live in a modal. The actual processing
    phase still uses the authoritative exiftool path from
    services/ingestion/exif_parser.py.
    """
    try:
        with PILImage.open(path) as img:
            raw = img._getexif() or {}
    except Exception:
        return {}
    return {TAGS.get(tag_id, str(tag_id)): value for tag_id, value in raw.items()}


def _parse_exif_datetime(value) -> Optional[datetime]:
    """Parse the EXIF DateTimeOriginal 'YYYY:MM:DD HH:MM:SS' format."""
    if not isinstance(value, str):
        return None
    try:
        return datetime.strptime(value, "%Y:%m:%d %H:%M:%S")
    except ValueError:
        return None


def _parse_iso_date(value):
    """Parse an ISO datetime/date string to a date, or None."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).date()
    except (ValueError, TypeError):
        return None


logger = get_logger("bulk-upload")


class _LostBulkClaim(RuntimeError):
    """Raised when a reclaimed bulk pass attempts another durable write."""


def _set_status(job_uuid: str, claim_id: Optional[str] = None, **fields) -> bool:
    """Update one BulkUploadJob row with the given fields."""
    with get_db_session() as session:
        stmt = update(BulkUploadJob).where(BulkUploadJob.uuid == job_uuid)
        if claim_id is not None:
            stmt = stmt.where(
                BulkUploadJob.status == "processing",
                BulkUploadJob.pipeline_claim_id == claim_id,
                BulkUploadJob.staging_complete.is_(False),
            )
        if "pipeline_updated_at" not in fields and claim_id is not None:
            fields["pipeline_updated_at"] = datetime.now(timezone.utc)
        result = session.execute(stmt.values(**fields))
        return result.rowcount == 1


def _persist_file_outcome(
    job_uuid: str, index: Optional[int], entry: dict, claim_id: str
) -> None:
    """Persist one source-file result while holding the authoritative job row."""
    if index is None:
        return
    with get_db_session() as session:
        stmt = select(BulkUploadJob).where(BulkUploadJob.uuid == job_uuid)
        stmt = stmt.where(
            BulkUploadJob.status == "processing",
            BulkUploadJob.pipeline_claim_id == claim_id,
            BulkUploadJob.staging_complete.is_(False),
        ).with_for_update()
        job = session.execute(
            stmt
        ).scalar_one_or_none()
        if job is None:
            raise _LostBulkClaim(f"Bulk job claim lost before outcome write: {job_uuid}")
        manifest = dict(job.manifest or {})
        ledger = dict(manifest.get("upload_ledger") or {})
        key = str(index)
        prior = dict(ledger.get(key) or {})
        # The staging object proves server acceptance even for a job created
        # before the ledger field was introduced.
        accepted = bool(prior.get("accepted", True))
        prior.update({k: v for k, v in entry.items() if v is not None})
        prior["accepted"] = accepted
        ledger[key] = prior
        manifest["upload_ledger"] = ledger
        job.manifest = manifest
        job.pipeline_updated_at = datetime.now(timezone.utc)


def _staged_file_index(object_key: str) -> Optional[int]:
    tail = object_key.rsplit("/", 1)[-1]
    prefix = tail.split("_", 1)[0] if "_" in tail else ""
    return int(prefix) if prefix.isdigit() else None


def _claim_process_job(job_uuid: str) -> Optional[str]:
    """Claim one processing pass with a database compare-and-set."""
    claim_id = str(uuid.uuid4())
    with get_db_session() as session:
        result = session.execute(
            update(BulkUploadJob)
            .where(
                BulkUploadJob.uuid == job_uuid,
                BulkUploadJob.status == "processing",
                BulkUploadJob.staging_complete.is_(False),
                BulkUploadJob.pipeline_claim_id.is_(None),
            )
            .values(
                pipeline_claim_id=claim_id,
                pipeline_attempts=BulkUploadJob.pipeline_attempts + 1,
                pipeline_updated_at=datetime.now(timezone.utc),
                pipeline_error=None,
            )
        )
        return claim_id if result.rowcount == 1 else None


def _fail_missing_staged_files(
    job_uuid: str, present_indexes: set[int], claim_id: str
) -> int:
    """Turn server-accepted uploads with vanished staging objects into failures."""
    with get_db_session() as session:
        stmt = select(BulkUploadJob).where(
            BulkUploadJob.uuid == job_uuid,
            BulkUploadJob.status == "processing",
            BulkUploadJob.pipeline_claim_id == claim_id,
            BulkUploadJob.staging_complete.is_(False),
        ).with_for_update()
        job = session.execute(
            stmt
        ).scalar_one_or_none()
        if job is None:
            raise _LostBulkClaim(f"Bulk job claim lost before missing-file reconciliation: {job_uuid}")
        manifest = dict(job.manifest or {})
        ledger = dict(manifest.get("upload_ledger") or {})
        failed = 0
        for raw_index, raw_entry in ledger.items():
            if not str(raw_index).isdigit() or not isinstance(raw_entry, dict):
                continue
            index = int(raw_index)
            if index in present_indexes:
                continue
            if raw_entry.get("accepted") and raw_entry.get("outcome") in {"uploaded", "processing"}:
                entry = dict(raw_entry)
                entry.update({"outcome": "failed", "reason": "staged_object_missing"})
                ledger[str(raw_index)] = entry
                failed += 1
        if failed:
            manifest["upload_ledger"] = ledger
            job.manifest = manifest
            job.pipeline_updated_at = datetime.now(timezone.utc)
        return failed


def _delete_staged_object(
    job_uuid: str, claim_id: str, storage: StorageClient, object_key: str
) -> None:
    """Delete staging only while the active owner holds the job row lock."""
    with get_db_session() as session:
        job = session.execute(
            select(BulkUploadJob).where(
                BulkUploadJob.uuid == job_uuid,
                BulkUploadJob.status == "processing",
                BulkUploadJob.pipeline_claim_id == claim_id,
                BulkUploadJob.staging_complete.is_(False),
            ).with_for_update()
        ).scalar_one_or_none()
        if job is None:
            raise _LostBulkClaim(f"Bulk job claim lost before staging deletion: {job_uuid}")
        storage.delete_object(BUCKET_BULK_UPLOAD_STAGING, object_key)
        job.pipeline_updated_at = datetime.now(timezone.utc)


def _camera_storage_id(camera: Camera) -> str:
    """
    Storage path uses the camera's `device_id` for live FTPS uploads to
    keep paths human-readable. Bulk uploads target a registered camera
    that may or may not have one; fall back to the DB id so the path
    stays unique either way.
    """
    if camera.device_id:
        return camera.device_id
    return f"camera-{camera.id}"


def _process_zip_entry(
    name: str,
    raw: bytes,
    camera_id: int,
    camera_storage_id: str,
    gps_location,
    bulk_queue: RedisQueue,
    bulk_upload_job_id: int,
    bulk_deployment_id: Optional[int] = None,
    use_profile: bool = False,
    time_offset_seconds: int = 0,
    source_index: Optional[int] = None,
    *,
    claim_id: str,
) -> str:
    """
    Process a single ZIP entry end-to-end.

    Returns a dict with at least `outcome` in
    {'processed','duplicate','skipped'} and, when relevant, the
    `image_uuid` of the new image, the `existing_uuid` of the duplicate
    it matched, and a `reason` for skips. The log-CSV endpoint reads
    these straight back out of manifest.file_log. Per-file issues
    become outcome='skipped' with a warning log so a single bad JPEG
    never sinks the whole batch.

    Two metadata modes:

    - **Mode A (use_profile=True):** the same camera-profile hunt as live
      FTPS. The file's EXIF must match an EXIF profile (path profiles never
      match here, the relative path is empty for a browser upload). GPS is
      read per image and the deployment is resolved from it by
      create_image_record, so a registered camera's SD card builds sites and
      deployments exactly like FTPS. `gps_location` / `bulk_deployment_id` are
      ignored.
    - **Mode B (use_profile=False):** no profile, the batch is pinned to the
      caller-chosen site via `bulk_deployment_id`. DateTimeOriginal is required.
    """
    if not name.lower().endswith(IMAGE_EXTENSIONS):
        return {"outcome": "skipped", "reason": "unsupported_extension"}

    content_hash = hashlib.sha256(raw).hexdigest()

    # Duplicate guard: same camera + same bytes was already imported.
    with get_db_session() as session:
        existing = session.execute(
            select(Image.uuid, Image.bulk_upload_job_id).where(
                Image.camera_id == camera_id,
                Image.content_hash == content_hash,
            ).limit(1)
        ).first()
        if existing:
            existing_uuid, existing_job_id = existing
            if existing_job_id == bulk_upload_job_id and source_index is not None:
                job = session.execute(
                    select(BulkUploadJob).where(BulkUploadJob.id == bulk_upload_job_id)
                ).scalar_one_or_none()
                ledger = (job.manifest or {}).get("upload_ledger", {}) if job else {}
                current = ledger.get(str(source_index), {})
                linked_elsewhere = any(
                    key != str(source_index) and item.get("image_uuid") == existing_uuid
                    for key, item in ledger.items()
                )
                if current.get("image_uuid") == existing_uuid or (
                    current.get("outcome") == "processing" and not linked_elsewhere
                ):
                    return {"outcome": "processed", "image_uuid": existing_uuid}
            logger.info(
                "Skipping duplicate bulk upload entry",
                entry=name,
                existing_uuid=existing_uuid,
            )
            return {"outcome": "duplicate", "existing_uuid": existing_uuid}

    suffix = os.path.splitext(name)[1] or ".jpg"
    tmp_handle, tmp_path = tempfile.mkstemp(suffix=suffix)
    try:
        with os.fdopen(tmp_handle, "wb") as fh:
            fh.write(raw)

        # Validation matches the live FTPS path. Failure is per-file.
        try:
            validate_image(tmp_path)
        except Exception as exc:
            logger.warning(
                "Skipping bulk upload entry, validation failed",
                entry=name,
                error=str(exc),
            )
            return {"outcome": "skipped", "reason": "validation_failed"}

        exif = extract_exif(tmp_path)
        clean_filename = os.path.basename(name)

        if use_profile:
            # Mode A: identify the camera profile and extract metadata the
            # same way live ingestion does. relative_path is empty because a
            # browser upload has no FTPS upload path, so only EXIF profiles
            # can match.
            try:
                profile = identify_camera_profile(
                    exif=exif, filename=clean_filename, relative_path=""
                )
            except ValueError as exc:
                logger.warning(
                    "Skipping bulk upload entry, no camera profile matched",
                    entry=name,
                    error=str(exc),
                )
                return {"outcome": "skipped", "reason": "unsupported_camera"}

            try:
                captured_at = get_corrected_datetime(
                    exif, tmp_path, time_offset_seconds,
                    allow_fallback=not profile.requires_datetime,
                )
            except Exception as exc:
                logger.warning(
                    "Skipping bulk upload entry, no DateTimeOriginal",
                    entry=name,
                    error=str(exc),
                )
                return {"outcome": "skipped", "reason": "missing_datetime"}

            image_gps = exif.get("gps_decimal")
            if image_gps and not is_valid_gps(image_gps):
                logger.warning(
                    "Skipping bulk upload entry, invalid GPS",
                    entry=name,
                    gps=image_gps,
                )
                return {"outcome": "skipped", "reason": "invalid_gps"}
            if not image_gps and profile.requires_gps:
                logger.warning(
                    "Skipping bulk upload entry, no GPS",
                    entry=name,
                    profile=profile.name,
                )
                return {"outcome": "skipped", "reason": "missing_gps"}

            # Per-image GPS, no pinned deployment: create_image_record resolves
            # the site and deployment through the shared FTPS resolver.
            record_gps = image_gps
            record_deployment_id = None
        else:
            # Mode B: chosen-site deployment, DateTimeOriginal required.
            try:
                captured_at = get_corrected_datetime(exif, tmp_path, time_offset_seconds)
            except Exception as exc:
                logger.warning(
                    "Skipping bulk upload entry, no DateTimeOriginal",
                    entry=name,
                    error=str(exc),
                )
                return {"outcome": "skipped", "reason": "missing_datetime"}
            record_gps = gps_location
            record_deployment_id = bulk_deployment_id

        image_uuid = str(uuid.uuid4())

        def upload_assets():
            storage = upload_image_to_minio(
                tmp_path, camera_storage_id, image_uuid, clean_filename
            )
            try:
                thumbnail = generate_and_upload_thumbnail(
                    tmp_path, camera_storage_id, image_uuid, clean_filename
                )
            except Exception as exc:
                logger.warning(
                    "Failed to generate thumbnail for bulk image",
                    entry=name,
                    error=str(exc),
                )
                thumbnail = None
            return storage, thumbnail

        # Serialize claim validation, Image insertion, and its ledger handoff.
        # The lease reconciler cannot steal this job between the ownership
        # check and the durable records.
        with get_db_session() as session:
            job = session.execute(
                select(BulkUploadJob).where(
                    BulkUploadJob.id == bulk_upload_job_id,
                    BulkUploadJob.status == "processing",
                    BulkUploadJob.pipeline_claim_id == claim_id,
                    BulkUploadJob.staging_complete.is_(False),
                ).with_for_update()
            ).scalar_one_or_none()
            if job is None:
                raise _LostBulkClaim(
                    f"Bulk job claim lost before image creation: {bulk_upload_job_id}"
                )
            # Keep the row lock across the bounded per-file MinIO write so
            # stale recovery cannot invalidate this owner's deletion and
            # Image/ledger commit window.
            storage_path, thumbnail_path = upload_assets()
            create_image_record(
                image_uuid=image_uuid, camera_id=camera_id, filename=clean_filename,
                storage_path=storage_path, thumbnail_path=thumbnail_path,
                captured_at=captured_at, gps_location=record_gps,
                exif_metadata=exif, origin="bulk", content_hash=content_hash,
                bulk_upload_job_id=bulk_upload_job_id, deployment_id=record_deployment_id,
                db_session=session,
            )
            if source_index is not None:
                manifest = dict(job.manifest or {})
                ledger = dict(manifest.get("upload_ledger") or {})
                key = str(source_index)
                prior = dict(ledger.get(key) or {})
                prior.update({
                    "outcome": "queued", "image_uuid": image_uuid,
                    "filename": clean_filename,
                    "accepted": bool(prior.get("accepted", True)),
                })
                ledger[key] = prior
                manifest["upload_ledger"] = ledger
                job.manifest = manifest
            job.pipeline_updated_at = datetime.now(timezone.utc)

        set_image_id(image_uuid)
        try:
            bulk_queue.publish({
                "image_uuid": image_uuid,
                "storage_path": storage_path,
                "camera_id": camera_id,
                "origin": "bulk",
            })
        except Exception as exc:
            # The durable Image + ledger transaction is authoritative. The
            # pending-image reconciler republishes it after its stale lease.
            logger.warning(
                "Bulk image saved but queue publish failed; recovery will retry",
                image_uuid=image_uuid,
                error=str(exc),
            )

        return {"outcome": "processed", "image_uuid": image_uuid}
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def _download_staged_zip(storage: StorageClient, staged_object_key: str) -> str:
    """Stream the staged ZIP to a tmp file. Caller deletes the path."""
    tmp_handle, tmp_path = tempfile.mkstemp(suffix=".zip")
    os.close(tmp_handle)
    zip_bytes = storage.download_fileobj(BUCKET_BULK_UPLOAD_STAGING, staged_object_key)
    with open(tmp_path, "wb") as fh:
        fh.write(zip_bytes)
    return tmp_path


def _inspect_job(job_uuid: str) -> None:
    """
    Walk the staged ZIP and build a manifest of per-status counts,
    date range, and the auto-suggested camera. Does not touch MinIO
    object storage and does not create any Image rows. Status moves
    queued -> inspecting -> awaiting_confirmation.
    """
    with get_db_session() as session:
        job = session.execute(
            select(BulkUploadJob).where(BulkUploadJob.uuid == job_uuid)
        ).scalar_one_or_none()
        if not job:
            logger.error("Bulk upload job not found", job_uuid=job_uuid)
            return
        project_id = job.project_id
        staged_object_key = job.staged_object_key
        job.status = "inspecting"
        job.started_at = datetime.now(timezone.utc)

    logger.info(
        "Inspecting bulk upload",
        job_uuid=job_uuid,
        project_id=project_id,
        staged_object_key=staged_object_key,
    )

    storage = StorageClient()
    tmp_zip_path: Optional[str] = None
    try:
        tmp_zip_path = _download_staged_zip(storage, staged_object_key)

        by_status: dict = defaultdict(int)
        serial_counts: Counter = Counter()
        min_dt: Optional[datetime] = None
        max_dt: Optional[datetime] = None
        valid_count = 0
        valid_hashes: list = []

        with zipfile.ZipFile(tmp_zip_path) as zf:
            entries = [
                info for info in zf.infolist()
                if not info.is_dir() and not _is_noise_entry(info.filename)
            ]
            total_entries = len(entries)

            for info in _heartbeat_progress(entries):
                tmp_path: Optional[str] = None
                try:
                    raw = zf.read(info)
                    suffix = os.path.splitext(info.filename)[1] or ".jpg"
                    tmp_handle, tmp_path = tempfile.mkstemp(suffix=suffix)
                    with os.fdopen(tmp_handle, "wb") as fh:
                        fh.write(raw)

                    exif = _read_exif_fast(tmp_path)
                    dt = _parse_exif_datetime(exif.get("DateTimeOriginal"))
                    if dt is None:
                        by_status["missing_exif_datetime"] += 1
                        continue

                    by_status["valid"] += 1
                    valid_count += 1
                    valid_hashes.append(hashlib.sha256(raw).hexdigest())
                    if min_dt is None or dt < min_dt:
                        min_dt = dt
                    if max_dt is None or dt > max_dt:
                        max_dt = dt
                    serial = exif.get("BodySerialNumber") or exif.get("SerialNumber")
                    if serial:
                        serial_counts[str(serial)] += 1
                except Exception as exc:
                    by_status["corrupt"] += 1
                    logger.warning(
                        "Inspect entry failed",
                        entry=info.filename,
                        error=str(exc),
                    )
                finally:
                    if tmp_path:
                        try:
                            os.unlink(tmp_path)
                        except OSError:
                            pass

        # Cross-check valid entries against images already in the
        # project. Project-wide check: identical bytes on two different
        # camera traps is essentially impossible, so a project-scoped
        # match is reliably a duplicate regardless of which camera the
        # user will eventually pick.
        if valid_hashes:
            with get_db_session() as session:
                existing_hashes = {
                    row[0] for row in session.execute(
                        select(Image.content_hash)
                        .join(Camera, Image.camera_id == Camera.id)
                        .where(
                            Camera.project_id == project_id,
                            Image.content_hash.in_(valid_hashes),
                        )
                    ).all()
                }
            if existing_hashes:
                # Note: a single hash could match multiple valid
                # entries (same image twice in the ZIP), but the count
                # we care about is "valid entries that are duplicates"
                # so we recount per entry.
                dup_count = sum(1 for h in valid_hashes if h in existing_hashes)
                by_status["duplicate"] = dup_count
                by_status["valid"] -= dup_count
                valid_count -= dup_count

        # Match every serial that appears against registered cameras
        # in this project. Each match keeps its count; we use the list
        # for both the auto-suggest (when there's a single dominant
        # camera) and the multi-camera refusal (when 2+ are matched).
        matched_cameras: list = []
        if serial_counts:
            with get_db_session() as session:
                # Select tuples instead of full Camera objects so the
                # values are plain Python by the time we use them.
                # Reading attributes off a Camera ORM instance after
                # the session closes triggers a refresh and crashes.
                rows = session.execute(
                    select(Camera.id, Camera.device_id).where(
                        Camera.project_id == project_id,
                        Camera.device_id.in_(list(serial_counts.keys())),
                    )
                ).all()
            for cam_id, device_id in rows:
                match_count = serial_counts.get(device_id, 0)
                if match_count == 0:
                    continue
                matched_cameras.append({
                    "camera_id": cam_id,
                    "camera_name": device_id,
                    "device_id": device_id,
                    "match_count": match_count,
                })
            matched_cameras.sort(key=lambda c: c["match_count"], reverse=True)

        # Cameras with only one stray match are treated as noise (an
        # accidental EXIF serial collision) so they don't trigger a
        # spurious "two cameras detected" refusal.
        significant_cameras = [c for c in matched_cameras if c["match_count"] >= 2]

        # Refuse multi-camera ZIPs: bulk upload attaches every image
        # to one camera_id. Without per-image routing (Slice 3) the
        # only safe option is to make the user split the batch.
        if len(significant_cameras) >= 2:
            names = ", ".join(
                f'{c["camera_name"]} ({c["match_count"]} images)'
                for c in significant_cameras
            )
            err = (
                f"ZIP spans {len(significant_cameras)} registered cameras: "
                f"{names}. Bulk upload handles one camera at a time. "
                "Split the ZIP per camera and retry."
            )
            logger.warning(
                "Refusing multi-camera bulk upload",
                job_uuid=job_uuid,
                matched_cameras=significant_cameras,
            )
            manifest = {
                "total_entries": total_entries,
                "valid_count": valid_count,
                "by_status": dict(by_status),
                "date_range": {
                    "start": min_dt.isoformat() if min_dt else None,
                    "end": max_dt.isoformat() if max_dt else None,
                },
                "suggested_camera": None,
                "matched_cameras": matched_cameras,
            }
            try:
                storage.delete_object(BUCKET_BULK_UPLOAD_STAGING, staged_object_key)
            except Exception as exc:
                logger.warning(
                    "Failed to delete staged zip after multi-camera refusal",
                    error=str(exc),
                )
            _set_status(
                job_uuid,
                status="failed",
                error_message=err,
                manifest=manifest,
                total_files=total_entries,
                finished_at=datetime.now(timezone.utc),
            )
            return

        # Auto-suggest the single matched camera when it covers at
        # least 50% of valid entries. Less than 50% means a lot of
        # serials didn't match anything; safer to let the user pick.
        suggested = None
        if valid_count > 0 and matched_cameras:
            top = matched_cameras[0]
            if top["match_count"] / valid_count >= 0.5:
                suggested = top

        manifest = {
            "total_entries": total_entries,
            "valid_count": valid_count,
            "by_status": dict(by_status),
            "date_range": {
                "start": min_dt.isoformat() if min_dt else None,
                "end": max_dt.isoformat() if max_dt else None,
            },
            "suggested_camera": suggested,
            "matched_cameras": matched_cameras,
        }

        _set_status(
            job_uuid,
            status="awaiting_confirmation",
            total_files=total_entries,
            manifest=manifest,
        )
        logger.info(
            "Inspection complete",
            job_uuid=job_uuid,
            total_entries=total_entries,
            valid_count=valid_count,
            by_status=dict(by_status),
            suggested_camera_id=suggested.get("camera_id") if suggested else None,
        )

    except Exception as exc:
        logger.error(
            "Bulk upload inspection failed",
            job_uuid=job_uuid,
            error=str(exc),
            exc_info=True,
        )
        _set_status(
            job_uuid,
            status="failed",
            error_message=f"Inspection failed: {exc}",
            finished_at=datetime.now(timezone.utc),
        )
    finally:
        if tmp_zip_path:
            try:
                os.unlink(tmp_zip_path)
            except OSError:
                pass


def _list_prefix(storage: StorageClient, prefix: str) -> list:
    """
    List every object under the job's staging prefix.

    Uses paginator-style calls so a 5000-file job is returned in full.
    The wrapper in shared/storage.py caps at a single page, which is
    fine for &lt; 1000 objects but truncates the rest. Drop to the boto3
    paginator for safety.
    """
    keys: list = []
    paginator = storage.client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET_BULK_UPLOAD_STAGING, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            keys.append(obj["Key"])
    return keys


def _process_prefix_job(
    job_uuid: str,
    job_id: int,
    camera_id: int,
    camera_storage_id: str,
    gps_location,
    staged_prefix: str,
    bulk_deployment_id: Optional[int] = None,
    use_profile: bool = False,
    time_offset_seconds: int = 0,
    *,
    claim_id: str,
) -> None:
    """
    Process a new-style per-file bulk-upload job. Lists MinIO under
    the job's staging prefix, downloads each file, runs the usual
    pipeline, deletes the staging.
    """
    storage = StorageClient()
    bulk_queue = RedisQueue(QUEUE_IMAGE_INGESTED_BULK)
    processed = 0
    duplicates = 0
    other_skipped = 0
    failed = 0

    object_keys = sorted(_list_prefix(storage, staged_prefix))
    present_indexes = {
        index for index in (_staged_file_index(key) for key in object_keys)
        if index is not None
    }
    logger.info(
        "Processing bulk upload prefix",
        job_uuid=job_uuid,
        staged_prefix=staged_prefix,
        object_count=len(object_keys),
    )

    for idx, key in enumerate(_heartbeat_progress(object_keys), start=1):
        # Object key shape: "{project_id}/{job_uuid}/{idx:06d}_{name}".
        # Recover the human filename for logs and storage paths.
        tail = key.rsplit("/", 1)[-1]
        filename = tail.split("_", 1)[1] if "_" in tail else tail
        file_index = _staged_file_index(key)
        _persist_file_outcome(
            job_uuid, file_index,
            {"outcome": "processing", "filename": filename, "object_key": key},
            claim_id,
        )
        try:
            raw = storage.download_fileobj(BUCKET_BULK_UPLOAD_STAGING, key)
            with _file_timeout(PER_FILE_TIMEOUT_SECONDS):
                result = _process_zip_entry(
                    filename,
                    raw,
                    camera_id,
                    camera_storage_id,
                    gps_location,
                    bulk_queue,
                    job_id,
                    bulk_deployment_id,
                    use_profile,
                    time_offset_seconds,
                    source_index=file_index,
                    claim_id=claim_id,
                )
        except _FileTimeout:
            logger.warning(
                "Bulk upload object timed out",
                object_key=key,
                timeout_s=PER_FILE_TIMEOUT_SECONDS,
            )
            result = {"outcome": "failed", "reason": "processing_timeout"}
        except Exception as exc:
            logger.warning(
                "Skipping bulk upload object, unexpected error",
                object_key=key,
                error=str(exc),
                exc_info=True,
            )
            result = {"outcome": "failed", "reason": "unexpected_error"}

        if result["outcome"] == "processed":
            processed += 1
            ledger_entry = {"outcome": "queued", "filename": filename,
                            "image_uuid": result.get("image_uuid"), "object_key": key}
        elif result["outcome"] == "duplicate":
            duplicates += 1
            ledger_entry = {"outcome": "duplicate", "filename": filename,
                            "existing_uuid": result.get("existing_uuid"), "object_key": key}
        elif result["outcome"] == "failed":
            failed += 1
            ledger_entry = {"outcome": "failed", "filename": filename,
                            "reason": result.get("reason"), "object_key": key}
        else:
            other_skipped += 1
            ledger_entry = {"outcome": "skipped", "filename": filename,
                            "reason": result.get("reason"), "object_key": key}

        _persist_file_outcome(job_uuid, file_index, ledger_entry, claim_id)

        # Best-effort cleanup as we go so a half-finished job does not
        # leave 5000 stale objects in MinIO.
        try:
            _delete_staged_object(job_uuid, claim_id, storage, key)
        except _LostBulkClaim:
            raise
        except Exception as exc:
            logger.warning(
                "Failed to delete processed staging object",
                object_key=key,
                error=str(exc),
            )

        if idx % PROGRESS_PERSIST_EVERY == 0:
            _set_status(job_uuid, claim_id=claim_id, skipped_files=duplicates + other_skipped)
            # Cooperative stop: if the user cancelled mid-run, stop creating and
            # enqueuing more images. What is already queued is handled by the
            # detection/classification skip checks.
            if is_bulk_job_cancelled(job_uuid):
                logger.info(
                    "Bulk job cancelled, stopping processing loop",
                    job_uuid=job_uuid,
                    processed=processed,
                )
                break

    missing_staged = _fail_missing_staged_files(job_uuid, present_indexes, claim_id)
    if missing_staged:
        logger.error(
            "Accepted bulk uploads were missing from staging",
            job_uuid=job_uuid,
            failed_files=missing_staged,
        )

    # Stash the breakdown so the UI can say "all 30 were duplicates"
    # instead of "30 skipped". Per-file outcomes go alongside so the
    # log-CSV endpoint can stream them back without another scan.
    with get_db_session() as session:
        row = session.execute(
            select(BulkUploadJob).where(
                BulkUploadJob.uuid == job_uuid,
                BulkUploadJob.status == "processing",
                BulkUploadJob.pipeline_claim_id == claim_id,
            ).with_for_update()
        ).scalar_one_or_none()
        if row is None:
            raise _LostBulkClaim("Bulk claim lost before summary commit")
        manifest = dict(row.manifest or {})
        manifest["process_summary"] = {
            "queued_for_pipeline": processed,
            "duplicates": duplicates,
            "other_skipped": other_skipped,
            "failed": failed,
        }
        row.manifest = manifest
        row.skipped_files = duplicates + other_skipped

    logger.info(
        "Finished processing bulk upload prefix",
        job_uuid=job_uuid,
        queued_for_pipeline=processed,
        duplicates=duplicates,
        other_skipped=other_skipped,
        failed=failed,
    )


def _process_legacy_zip_job(
    job_uuid: str,
    job_id: int,
    camera_id: int,
    camera_storage_id: str,
    gps_location,
    staged_object_key: str,
    bulk_deployment_id: Optional[int] = None,
    use_profile: bool = False,
    *,
    claim_id: str,
) -> None:
    """
    Drain a pre-refactor bulk-upload job whose staged_object_key points
    at a single ZIP. Kept so anything created before the per-file
    rollout can still complete. Legacy jobs always carry a chosen site,
    so use_profile is False in practice.
    """
    storage = StorageClient()
    tmp_zip_path: Optional[str] = None
    bulk_queue = RedisQueue(QUEUE_IMAGE_INGESTED_BULK)
    processed = 0
    duplicates = 0
    other_skipped = 0
    file_log: list = []
    try:
        tmp_zip_path = _download_staged_zip(storage, staged_object_key)
        with zipfile.ZipFile(tmp_zip_path) as zf:
            entries = [
                info for info in zf.infolist()
                if not info.is_dir() and not _is_noise_entry(info.filename)
            ]
            for idx, info in enumerate(_heartbeat_progress(entries), start=1):
                try:
                    raw = zf.read(info)
                    with _file_timeout(PER_FILE_TIMEOUT_SECONDS):
                        result = _process_zip_entry(
                            info.filename, raw, camera_id, camera_storage_id,
                            gps_location, bulk_queue, job_id, bulk_deployment_id,
                            use_profile, source_index=idx - 1, claim_id=claim_id,
                        )
                except _FileTimeout:
                    logger.warning(
                        "Legacy zip entry timed out",
                        entry=info.filename,
                        timeout_s=PER_FILE_TIMEOUT_SECONDS,
                    )
                    result = {"outcome": "skipped", "reason": "processing_timeout"}
                except Exception as exc:
                    logger.warning(
                        "Skipping legacy zip entry",
                        entry=info.filename, error=str(exc), exc_info=True,
                    )
                    result = {"outcome": "skipped", "reason": "unexpected_error"}

                source_index = idx - 1
                if result["outcome"] == "processed":
                    ledger_entry = {
                        "outcome": "queued", "image_uuid": result.get("image_uuid"),
                        "filename": info.filename,
                    }
                elif result["outcome"] == "duplicate":
                    ledger_entry = {
                        "outcome": "duplicate", "existing_uuid": result.get("existing_uuid"),
                        "filename": info.filename,
                    }
                elif result["outcome"] == "failed":
                    ledger_entry = {
                        "outcome": "failed", "reason": result.get("reason"),
                        "filename": info.filename,
                    }
                else:
                    ledger_entry = {
                        "outcome": "skipped", "reason": result.get("reason"),
                        "filename": info.filename,
                    }
                _persist_file_outcome(job_uuid, source_index, ledger_entry, claim_id)

                if result["outcome"] == "processed":
                    processed += 1
                elif result["outcome"] == "duplicate":
                    duplicates += 1
                else:
                    other_skipped += 1
                file_log.append({
                    "filename": info.filename,
                    **result,
                })
                if idx % PROGRESS_PERSIST_EVERY == 0:
                    _set_status(job_uuid, claim_id=claim_id, skipped_files=duplicates + other_skipped)
                    if is_bulk_job_cancelled(job_uuid):
                        logger.info(
                            "Bulk job cancelled, stopping legacy processing loop",
                            job_uuid=job_uuid,
                            processed=processed,
                        )
                        break

        with get_db_session() as session:
            row = session.execute(
                select(BulkUploadJob).where(
                    BulkUploadJob.uuid == job_uuid,
                    BulkUploadJob.status == "processing",
                    BulkUploadJob.pipeline_claim_id == claim_id,
                ).with_for_update()
            ).scalar_one_or_none()
            if row is None:
                raise _LostBulkClaim("Bulk claim lost before legacy summary commit")
            manifest = dict(row.manifest or {})
            manifest["process_summary"] = {
                "queued_for_pipeline": processed,
                "duplicates": duplicates,
                "other_skipped": other_skipped,
            }
            manifest["file_log"] = file_log
            row.manifest = manifest
            row.skipped_files = duplicates + other_skipped

        try:
            _delete_staged_object(job_uuid, claim_id, storage, staged_object_key)
        except _LostBulkClaim:
            raise
        except Exception as exc:
            logger.warning(
                "Failed to delete legacy staged zip",
                staged_object_key=staged_object_key, error=str(exc),
            )
    finally:
        if tmp_zip_path:
            try:
                os.unlink(tmp_zip_path)
            except OSError:
                pass


def _process_job(job_uuid: str) -> None:
    """
    Dispatcher for the process phase. Reads the job row, picks the
    per-file or legacy-zip path based on the staging key shape, runs
    the pipeline, marks status. Failure is captured at this level so
    no exception escapes to the queue consumer.
    """
    claim_id = _claim_process_job(job_uuid)
    if claim_id is None:
        logger.info("Bulk upload delivery already claimed or complete", job_uuid=job_uuid)
        return
    with get_db_session() as session:
        job = session.execute(
            select(BulkUploadJob).where(
                BulkUploadJob.uuid == job_uuid,
                BulkUploadJob.pipeline_claim_id == claim_id,
            )
        ).scalar_one_or_none()
        if not job:
            logger.error("Bulk upload job not found", job_uuid=job_uuid)
            return
        camera = session.get(Camera, job.camera_id) if job.camera_id else None
        if not camera:
            job.status = "failed"
            job.error_message = "Target camera no longer exists"
            job.pipeline_error = job.error_message
            job.pipeline_claim_id = None
            job.finished_at = datetime.now(timezone.utc)
            return
        job_id = job.id
        camera_id = camera.id
        camera_storage_id = _camera_storage_id(camera)
        staged_object_key = job.staged_object_key
        manifest = job.manifest or {}
        time_offset_seconds = job.time_offset_seconds

        # Mode A (no pinned site): run the camera-profile hunt per image and
        # resolve site + deployment from each image's GPS, exactly like FTPS.
        # Mode B (pinned site): attach the whole batch to one deployment at the
        # chosen site.
        use_profile = not manifest.get("site_id")

        # In Mode B, default each image's location to where the camera is now
        # (its most recent deployment site). Unused in Mode A, where per-image
        # GPS drives the deployment, so the lookup is skipped there.
        gps_location = None
        if not use_profile:
            loc_row = session.execute(
                text("""
                    SELECT ST_Y(s.location::geometry) AS lat, ST_X(s.location::geometry) AS lon
                    FROM deployments d
                    JOIN sites s ON s.id = d.site_id
                    WHERE d.camera_id = :camera_id
                    ORDER BY d.deployment_number DESC
                    LIMIT 1
                """),
                {"camera_id": camera.id},
            ).fetchone()
            if loc_row:
                gps_location = (loc_row.lat, loc_row.lon)

        # The API flips status to 'processing' at finalize so users see
        # the right state during the brief queue hop; we still own
        # process_started_at since that drives the self-calibrating ETA.
        job.process_started_at = datetime.now(timezone.utc)

    # Mode B pins every image to one deployment at the chosen site. The
    # client already applied the job's clock correction to date_range.
    bulk_deployment_id = None
    if not use_profile:
        site_id = manifest.get("site_id")
        date_range = manifest.get("date_range") or {}
        try:
            bulk_deployment_id = get_or_create_bulk_deployment(
                camera_id,
                int(site_id),
                _parse_iso_date(date_range.get("start")),
                _parse_iso_date(date_range.get("end")),
            )
        except Exception as exc:
            logger.error(
                "Could not resolve pinned site deployment, falling back to camera location",
                job_uuid=job_uuid,
                site_id=site_id,
                error=str(exc),
            )

    is_prefix = staged_object_key.endswith("/")
    logger.info(
        "Processing bulk upload",
        job_uuid=job_uuid,
        camera_id=camera_id,
        staged_object_key=staged_object_key,
        layout="prefix" if is_prefix else "legacy_zip",
        bulk_deployment_id=bulk_deployment_id,
        use_profile=use_profile,
        time_offset_seconds=time_offset_seconds,
    )

    try:
        if is_prefix:
            _process_prefix_job(
                job_uuid, job_id, camera_id, camera_storage_id,
                gps_location, staged_object_key, bulk_deployment_id, use_profile,
                time_offset_seconds, claim_id=claim_id,
            )
        else:
            _process_legacy_zip_job(
                job_uuid, job_id, camera_id, camera_storage_id,
                gps_location, staged_object_key, bulk_deployment_id, use_profile,
                claim_id=claim_id,
            )
    except Exception as exc:
        logger.error(
            "Bulk upload processing failed",
            job_uuid=job_uuid,
            error=str(exc),
            exc_info=True,
        )
        _set_status(
            job_uuid,
            claim_id=claim_id,
            status="failed",
            error_message=str(exc),
            finished_at=datetime.now(timezone.utc),
            pipeline_error=str(exc)[:1000],
            pipeline_claim_id=None,
        )
    else:
        # The stage is complete; image classification continues independently.
        # Keep the original total_files and let periodic maintenance close the
        # job only after every linked image has a terminal pipeline status.
        _set_status(
            job_uuid, claim_id=claim_id, staging_complete=True,
            pipeline_claim_id=None, pipeline_error=None,
        )


def dispatch(message: dict) -> None:
    """Route a queue message to the inspect or process phase."""
    job_uuid = message.get("job_uuid")
    phase = message.get("phase", "inspect")
    if not job_uuid:
        logger.error("Bulk upload message missing job_uuid", payload=message)
        return
    if phase == "inspect":
        _inspect_job(job_uuid)
    elif phase == "process":
        _process_job(job_uuid)
    else:
        logger.error("Unknown bulk upload phase", phase=phase, job_uuid=job_uuid)


def _finalize_classified_jobs() -> int:
    """Close completed jobs from durable image outcomes, without an API poll."""
    finalized = 0
    with get_db_session() as session:
        jobs = session.execute(
            select(BulkUploadJob)
            .where(BulkUploadJob.status == "processing", BulkUploadJob.staging_complete.is_(True))
            .with_for_update(skip_locked=True)
            .limit(100)
        ).scalars().all()
        for job in jobs:
            counts = dict(session.execute(
                select(Image.status, func.count(Image.id))
                .where(Image.bulk_upload_job_id == job.id, Image.status.in_(("classified", "failed")))
                .group_by(Image.status)
            ).all())
            ledger = ledger_from_manifest(job.manifest)
            classified = counts.get("classified", 0)
            failed_pipeline = counts.get("failed", 0)
            queued = sum(bool(entry.get("image_uuid")) for entry in ledger.values())
            duplicates = sum(entry.get("outcome") == "duplicate" for entry in ledger.values())
            skipped = sum(entry.get("outcome") == "skipped" for entry in ledger.values())
            upload_failed = sum(entry.get("outcome") == "failed_upload" for entry in ledger.values())
            worker_failed = sum(
                entry.get("outcome") == "failed" and not entry.get("image_uuid")
                for entry in ledger.values()
            )
            missing = max(0, job.total_files - queued - duplicates - skipped - upload_failed - worker_failed)
            pending = missing + max(0, queued - classified - failed_pipeline)
            terminal = terminal_status({
                "pending_files": pending,
                "classified_files": classified,
                "failed_files": failed_pipeline + upload_failed + worker_failed,
                "duplicate_files": duplicates,
                "skipped_files": skipped,
            })
            if terminal:
                job.status = terminal
                job.finished_at = datetime.now(timezone.utc)
                finalized += 1
        if finalized:
            session.commit()
    return finalized


def _recover_stuck_jobs() -> int:
    """Periodically reclaim stale staging leases and publish one retry."""
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(seconds=JOB_LEASE_SECONDS)
    snapshots = []
    with get_db_session() as session:
        stale = session.execute(
            select(BulkUploadJob)
            .where(
                BulkUploadJob.status == "processing",
                BulkUploadJob.staging_complete.is_(False),
                BulkUploadJob.pipeline_updated_at < cutoff,
            )
            .with_for_update(skip_locked=True)
            .limit(100)
        ).scalars().all()
        for job in stale:
            if job.pipeline_attempts >= MAX_JOB_ATTEMPTS:
                job.status = "failed"
                job.error_message = "Bulk staging stopped after 3 attempts; retry this upload."
                job.pipeline_error = job.error_message
                job.pipeline_claim_id = None
                job.finished_at = now
            else:
                job.pipeline_claim_id = None
                job.pipeline_updated_at = now
                snapshots.append((job.uuid, job.staged_object_key, now))
        if stale:
            session.commit()

    published = 0
    storage = StorageClient() if snapshots else None
    for job_uuid, staged_key, recovery_time in snapshots:
        keys = storage.list_objects(BUCKET_BULK_UPLOAD_STAGING, staged_key) if staged_key else []
        if not keys:
            ledger_complete = False
            with get_db_session() as session:
                job = session.execute(
                    select(BulkUploadJob).where(
                        BulkUploadJob.uuid == job_uuid,
                        BulkUploadJob.status == "processing",
                        BulkUploadJob.staging_complete.is_(False),
                        BulkUploadJob.pipeline_claim_id.is_(None),
                        BulkUploadJob.pipeline_updated_at == recovery_time,
                    ).with_for_update()
                ).scalar_one_or_none()
                if job is None:
                    continue
                ledger = ledger_from_manifest(job.manifest)
                resolved_indexes = {
                    int(index) for index, entry in ledger.items()
                    if index.isdigit() and (
                        entry.get("image_uuid")
                        or entry.get("outcome") in {
                            "duplicate", "skipped", "failed", "failed_upload"
                        }
                    )
                }
                if job.total_files > 0 and len(resolved_indexes) >= job.total_files:
                    # The worker drained staging and persisted every result,
                    # then died before its completion marker. Resume pipeline
                    # finalization from the durable ledger.
                    job.staging_complete = True
                    job.pipeline_error = None
                    ledger_complete = True
                else:
                    missing_error = "Staged upload data is missing; retry this upload."
                    job.status = "failed"
                    job.error_message = missing_error
                    job.pipeline_error = missing_error
                    job.finished_at = datetime.now(timezone.utc)
                session.commit()
            if ledger_complete:
                logger.info("Bulk staging ledger was complete; resuming image finalization", job_uuid=job_uuid)
            else:
                logger.error("Bulk upload staging objects are missing", job_uuid=job_uuid)
            continue
        RedisQueue(QUEUE_BULK_UPLOAD_JOB_PROCESS).publish(
            {"job_uuid": job_uuid, "phase": "process"}
        )
        published += 1
    return published


def _maintain_bulk_jobs() -> None:
    _recover_stuck_jobs()
    _finalize_classified_jobs()


def main() -> None:
    logger.info("Bulk upload worker starting")
    # Pick up any jobs left mid-pass by a previous container restart
    # before we start blocking on the queue. Without this they would
    # sit in 'processing' forever.
    try:
        _recover_stuck_jobs()
    except Exception as exc:
        logger.error(
            "Stuck-job recovery failed, continuing to queue loop",
            error=str(exc),
            exc_info=True,
        )
    # Process-phase messages take priority over inspect-phase so a
    # user clicking Process is never queued behind someone else's
    # pending ZIP inspection. Same pattern as live > bulk for the
    # detection / classification pipeline.
    queue = RedisQueue(QUEUE_BULK_UPLOAD_JOB)
    priority = [QUEUE_BULK_UPLOAD_JOB_PROCESS, QUEUE_BULK_UPLOAD_JOB]
    logger.info("Listening on priority queues", queues=priority)
    queue.consume_forever_priority(
        priority, dispatch, heartbeat_key=HEARTBEAT_KEY_BULK_UPLOAD,
        maintenance_callback=_maintain_bulk_jobs,
    )


if __name__ == "__main__":
    main()
