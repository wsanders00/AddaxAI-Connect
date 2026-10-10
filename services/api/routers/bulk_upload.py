"""
Bulk image upload endpoints

Project-admin-only. The client scans the user's folder locally, picks a
camera, then POSTs one file at a time to a job's staging prefix. Once
every file is in MinIO the client calls /finalize, which flips the job
to 'processing' and publishes to the bulk-upload worker.

Status flow:
    uploading -> processing -> done | failed

Legacy jobs created before the per-file refactor (statuses queued,
inspecting, awaiting_confirmation) are still readable but cannot be
resumed; they expire via the orphan-cleanup pass.
"""
import asyncio
import csv
import hashlib
import io
import json
import math
import os
import re
import shutil
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Literal, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import (
    APIRouter,
    Depends,
    File,
    HTTPException,
    UploadFile,
    status,
)
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from shared.camera_profiles import identify_camera_profile
from shared.bulk_outcomes import ledger_from_manifest, merge_ledger_entry, summarize_ledger, terminal_status
from shared.database import get_async_session
from shared.logger import get_logger
from shared.models import BulkUploadJob, Camera, Deployment, Detection, Image, ServerSettings, Site, User
from shared.queue import (
    QUEUE_BULK_UPLOAD_JOB_PROCESS,
    QUEUE_DETECTION_COMPLETE,
    QUEUE_DETECTION_COMPLETE_BULK,
    QUEUE_IMAGE_INGESTED,
    QUEUE_IMAGE_INGESTED_BULK,
    RedisQueue,
)
from shared.storage import BUCKET_BULK_UPLOAD_STAGING, StorageClient
from auth.permissions import require_project_admin_access
from routers.image_admin import delete_images_by_ids

router = APIRouter(
    prefix="/api/projects/{project_id}/bulk-upload",
    tags=["bulk-upload"],
)
logger = get_logger("api.bulk_upload")

# Per-file and per-job caps. 50 MB covers any realistic single trail-cam
# frame with headroom. 20000 files matches the per-job cap users see in
# the modal, sized for a full-season SD card pull.
MAX_FILE_SIZE_BYTES = 50 * 1024 * 1024
MAX_FILES_PER_JOB = 20000
# A camera clock correction never needs more than this. Thirty years covers
# a clock reset to the firmware's default year.
MAX_TIME_OFFSET_SECONDS = 30 * 366 * 24 * 3600
ALLOWED_CONTENT_TYPES = {"image/jpeg", "image/jpg", "image/png"}
ALLOWED_EXTENSIONS = (".jpg", ".jpeg", ".png")

# Jobs that have been accepting uploads for longer than this without a
# finalize call get auto-failed and their staged objects deleted. Covers
# the case of a user closing the tab mid-upload. 24 h is generous enough
# that a slow upload over a bad connection still has time to finish.
UPLOAD_TTL = timedelta(hours=24)

# Cap on concurrent non-terminal bulk-upload jobs per project. Prevents
# a single user kicking off twenty parallel SD-card uploads and
# starving the worker queue for everyone else. 3 covers a normal
# workflow (one uploading, one or two waiting in processing) with
# headroom.
MAX_CONCURRENT_JOBS_PER_PROJECT = 3
ARCHIVE_FORMAT = "addax-archive-ready-v1"
MAX_ARCHIVE_MANIFEST_BYTES = 8 * 1024 * 1024
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

# A bulk import lands its full size on the local data disk (first in the
# staging bucket, then in raw-images) before the cold tier can drain it.
# Postgres, Redis, and MinIO share that one volume, so an import that does
# not fit would fill the disk and take the database down. Refuse such a job
# up front instead. The API container sees the same volume through its
# project-images mount, so free space there matches the disk MinIO writes to.
DATA_DISK_PROBE_PATH = "/app/project-images"
DISK_RESERVE_BYTES = int(
    float(os.environ.get("BULK_UPLOAD_DISK_RESERVE_GB", "5")) * (1024 ** 3)
)

# Soft cap on in-flight per-file uploads handled by this API process.
# The client uploads with concurrency 4, so a single legitimate user
# never hits the limit; the bound mostly defends against a user who
# opens several tabs or a bug that hammers the endpoint. Module-level
# semaphore so it's process-wide. Multiple uvicorn workers each get
# their own bucket, which is fine: this is a soft DoS guard, not a
# strict admission control.
_UPLOAD_CONCURRENCY = 8
_upload_semaphore = asyncio.Semaphore(_UPLOAD_CONCURRENCY)


class BulkUploadJobResponse(BaseModel):
    """One bulk-upload job row, as returned to the frontend."""
    uuid: str
    project_id: int
    camera_id: Optional[int]
    deployment_id: Optional[int] = None
    client_batch_id: Optional[str] = None
    # Compact response summary; file-level provenance is returned with each
    # Image row through its existing image_metadata API field.
    archive_manifest: Optional[Dict[str, Any]] = None
    camera_name: Optional[str]
    original_filename: str
    status: str
    total_files: int
    uploaded_files: int = 0
    processed_files: int
    classified_files: int = 0
    failed_files: int = 0
    upload_failed_files: int = 0
    duplicate_files: int = 0
    skipped_files: int
    pending_files: int = 0
    outcome_warning: Optional[str] = None
    error_message: Optional[str]
    manifest: Optional[Dict[str, Any]] = None
    time_offset_seconds: int = 0
    queue_position: Optional[int] = None
    started_at: Optional[str]
    process_started_at: Optional[str] = None
    finished_at: Optional[str]
    created_at: str
    created_by_email: Optional[str]


class BulkUploadFileReceipt(BaseModel):
    index: int
    accepted: bool
    outcome: str
    image_uuid: Optional[str] = None
    existing_uuid: Optional[str] = None
    pipeline_status: Optional[str] = None
    reason: Optional[str] = None
    pipeline_error: Optional[str] = None


class BulkUploadReceiptsResponse(BaseModel):
    files: List[BulkUploadFileReceipt]


class CreateBulkUploadRequest(BaseModel):
    """
    Create an empty bulk-upload job. Files are uploaded separately.

    Exactly one target mode is selected:

    - **device_id (Mode A):** the pre-flight matched a camera profile. The
      camera is resolved (and auto-created within this project if new), and
      each image's site + deployment are resolved from its own GPS, like FTPS.
    - **site_id (Mode B):** no profile matched, so the whole batch is pinned to
      one user-chosen site via a synthetic per-site camera.
    - **camera_id + deployment_id (archive):** an explicitly registered
      physical camera and historical deployment are pinned without profile or
      GPS relocation behavior.
    """
    folder_name: str = Field(min_length=1, max_length=255)
    device_id: Optional[str] = Field(default=None, max_length=50)
    site_id: Optional[int] = None
    camera_id: Optional[int] = Field(default=None, gt=0)
    deployment_id: Optional[int] = Field(default=None, gt=0)
    client_batch_id: Optional[str] = Field(default=None, min_length=1, max_length=200)
    archive_manifest: Optional[Dict[str, Any]] = None
    total_files: int = Field(ge=1, le=MAX_FILES_PER_JOB)
    # Sum of the byte sizes of the files about to be uploaded. Used to
    # refuse a job up front when it would not fit on the local data disk.
    total_bytes: int = Field(ge=0)
    # Free-form client-computed scan summary. Stored on the job and shown
    # back in the review UI. See the bulk-upload worker for the shape.
    manifest: Dict[str, Any] = Field(default_factory=dict)
    # Camera clock correction in seconds, added to every capture time. The
    # manifest's date_range must already include it. Bounded so a typo in
    # the year cannot move a batch centuries.
    time_offset_seconds: int = Field(
        default=0, ge=-MAX_TIME_OFFSET_SECONDS, le=MAX_TIME_OFFSET_SECONDS,
    )

    @model_validator(mode="after")
    def _exactly_one_target(self) -> "CreateBulkUploadRequest":
        modes = int(bool(self.device_id)) + int(bool(self.site_id)) + int(bool(self.camera_id) or bool(self.deployment_id))
        if modes != 1 or bool(self.camera_id) != bool(self.deployment_id):
            raise ValueError(
                "Provide exactly one complete target: device_id, site_id, or camera_id and deployment_id"
            )
        if self.archive_manifest is not None and (not self.camera_id or not self.client_batch_id):
            raise ValueError("Archive intake requires camera_id, deployment_id, and client_batch_id")
        if self.camera_id is not None and self.archive_manifest is None:
            raise ValueError("camera_id/deployment_id is reserved for archive intake")
        if self.client_batch_id is not None and self.archive_manifest is None:
            raise ValueError("client_batch_id is reserved for archive intake")
        return self


class ScanProfileEntry(BaseModel):
    """One sampled image's identifying EXIF, as read by the client scan."""
    make: Optional[str] = None
    model: Optional[str] = None
    serial: Optional[str] = None
    filename: str = ""


class ScanProfileRequest(BaseModel):
    """A representative sample of scanned images for the pre-flight profile hunt."""
    entries: List[ScanProfileEntry] = Field(default_factory=list)


class ScanProfileResponse(BaseModel):
    # "profile" = an EXIF profile matched and yielded one device_id (Mode A).
    # "manual" = no profile matched, the user must pick a site (Mode B).
    mode: str
    device_id: Optional[str] = None
    profile_name: Optional[str] = None
    # Whether that device_id is already a camera in this project. False means
    # the job will auto-create it.
    camera_registered: bool = False
    camera_id: Optional[int] = None
    # Set when the sample resolves more than one camera. The batch must be
    # split per camera (bulk attaches one camera per job).
    multiple_cameras: bool = False
    device_ids: List[str] = Field(default_factory=list)


class CheckDuplicatesRequest(BaseModel):
    """
    Fingerprint check: for the picked camera, how many Image rows
    exist at each of these naive EXIF timestamps? Cheap alternative
    to content-hash dedup, one indexed lookup on Image.captured_at.
    """
    camera_id: int
    captured_ats: List[str] = Field(default_factory=list)


class CheckDuplicatesResponse(BaseModel):
    # captured_at iso -> count of matching Image rows on that camera.
    # The client uses the count to decide whether a skip is safe: a
    # single match against a single scan entry is unambiguous, but
    # multi-match (burst mode) is ambiguous and must be sent through
    # so the server's content-hash dedup can sort it out.
    duplicate_counts: Dict[str, int]


def _staging_prefix(project_id: int, job_uuid: str) -> str:
    """MinIO key prefix that holds every file for one bulk-upload job."""
    return f"{project_id}/{job_uuid}/"


def _archive_timezone_fingerprint(timezone_name: str) -> str:
    payload = json.dumps({"timezone": timezone_name}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _archive_local_utc_candidates(local_time: datetime, timezone_name: str) -> Dict[int, datetime]:
    """Return valid fold interpretations; gaps return none, overlaps return two."""
    if local_time.tzinfo is not None:
        raise ValueError("capture_app_local must be naive wall time")
    zone = ZoneInfo(timezone_name)
    candidates: Dict[int, datetime] = {}
    for fold in (0, 1):
        aware = local_time.replace(tzinfo=zone, fold=fold)
        utc_value = aware.astimezone(timezone.utc)
        round_trip = utc_value.astimezone(zone)
        if round_trip.replace(tzinfo=None) == local_time:
            candidates[fold] = utc_value
    # On ordinary timestamps both fold choices represent the same instant.
    if len(candidates) == 2 and candidates[0] == candidates[1]:
        return {0: candidates[0]}
    return candidates


def _archive_request_fingerprint(project_id: int, body: "CreateBulkUploadRequest", archive: Dict[str, Any]) -> str:
    immutable_manifest = dict(archive)
    # Generated wall-clock time is informational and may differ on a retry of
    # the same pinned revision. The source marker/revision hashes identify it.
    immutable_manifest.pop("generated_at_utc", None)
    payload = {
        "project_id": project_id, "camera_id": body.camera_id,
        "deployment_id": body.deployment_id, "total_files": body.total_files,
        "total_bytes": body.total_bytes, "folder_name": body.folder_name,
        "archive_manifest": immutable_manifest,
    }
    return hashlib.sha256(json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode("utf-8")).hexdigest()


def _archive_file_bytes_match(entry: Dict[str, Any], data: bytes) -> bool:
    return len(data) == entry.get("size_bytes") and hashlib.sha256(data).hexdigest() == entry.get("ready_sha256")


def _require_idempotent_payload(existing_fingerprint: Optional[str], incoming_fingerprint: str) -> None:
    if existing_fingerprint != incoming_fingerprint:
        raise HTTPException(status_code=409, detail="client_batch_id was already used for a different archive request")


def _bulk_receipt_rows(job: BulkUploadJob, image_states: Dict[str, Dict[str, Optional[str]]]) -> List[BulkUploadFileReceipt]:
    ledger = ledger_from_manifest(job.manifest)
    receipts = []
    for index in range(job.total_files):
        entry = ledger.get(str(index), {})
        image_uuid = entry.get("image_uuid")
        image_state = image_states.get(image_uuid, {}) if image_uuid else {}
        receipts.append(BulkUploadFileReceipt(
            index=index,
            accepted=bool(entry.get("accepted")),
            outcome=str(entry.get("outcome", "pending"))[:32],
            image_uuid=image_uuid,
            existing_uuid=entry.get("existing_uuid"),
            pipeline_status=image_state.get("pipeline_status"),
            reason=str(entry["reason"])[:160] if entry.get("reason") is not None else None,
            pipeline_error=(str(image_state["pipeline_error"])[:512]
                            if image_state.get("pipeline_error") is not None else None),
        ))
    return receipts


def _validate_archive_manifest(archive: Dict[str, Any], project_id: int, camera_id: int,
                               deployment_id: int, total_files: int) -> Dict[str, Any]:
    """Validate and normalize the bounded, per-target ready-manifest contract."""
    if len(json.dumps(archive, separators=(",", ":")).encode("utf-8")) > MAX_ARCHIVE_MANIFEST_BYTES:
        raise HTTPException(status_code=413, detail="Archive manifest exceeds size limit")
    if archive.get("format") != ARCHIVE_FORMAT:
        raise HTTPException(status_code=422, detail="Unsupported archive manifest format")
    allowed_top = {"format", "batch_id", "revision_sha256", "generated_at_utc",
                   "source_batch_complete_marker_sha256", "app_timezone",
                   "app_timezone_fingerprint", "files"}
    if set(archive) - allowed_top:
        raise HTTPException(status_code=422, detail="Archive manifest has unsupported fields")
    for key in ("batch_id", "revision_sha256", "source_batch_complete_marker_sha256", "app_timezone_fingerprint"):
        value = archive.get(key)
        if (not isinstance(value, str) or len(value) > 128
                or (key.endswith("sha256") and not SHA256_RE.fullmatch(value))):
            raise HTTPException(status_code=422, detail=f"Invalid archive manifest field: {key}")
    files = archive.get("files")
    if not isinstance(files, list) or len(files) != total_files or not files:
        raise HTTPException(status_code=422, detail="Archive manifest file count must match total_files")
    if archive.get("app_timezone") is None or not isinstance(archive["app_timezone"], str) or len(archive["app_timezone"]) > 64:
        raise HTTPException(status_code=422, detail="Invalid archive app_timezone")
    indexes = set()
    forbidden = {"origin", "is_verified", "verified_at", "verified_by_user_id",
                 "image_metadata", "image_uuid", "status", "deployment_id", "camera_id", "project_id"}
    for item in files:
        if not isinstance(item, dict):
            raise HTTPException(status_code=422, detail="Invalid archive file entry")
        if forbidden.intersection(item):
            raise HTTPException(status_code=422, detail="Archive file contains server-owned fields")
        allowed_file = {"index", "ready_relative_path", "ready_sha256", "size_bytes",
                        "media_kind", "target", "provenance", "video_source_relative_path",
                        "video_source_sha256", "selected_frame_pts_seconds", "extraction_policy"}
        if set(item) - allowed_file:
            raise HTTPException(status_code=422, detail="Archive file has unsupported fields")
        index = item.get("index")
        target = item.get("target")
        if isinstance(index, bool) or not isinstance(index, int) or index < 0 or index >= total_files or index in indexes:
            raise HTTPException(status_code=422, detail="Archive file indexes must be unique and in range")
        indexes.add(index)
        if not isinstance(target, dict) or target != {
            "project_id": project_id, "camera_id": camera_id, "deployment_id": deployment_id,
        }:
            raise HTTPException(status_code=422, detail="Archive file target does not match the job target")
        if not SHA256_RE.fullmatch(str(item.get("ready_sha256", ""))):
            raise HTTPException(status_code=422, detail="Invalid ready_sha256")
        if (isinstance(item.get("size_bytes"), bool) or not isinstance(item.get("size_bytes"), int)
                or item["size_bytes"] <= 0 or item["size_bytes"] > MAX_FILE_SIZE_BYTES):
            raise HTTPException(status_code=422, detail="Invalid archive file size")
        path = item.get("ready_relative_path")
        if (not isinstance(path, str) or len(path) > 512 or path.startswith("/")
                or "\\" in path or ".." in path.split("/")
                or not path.lower().endswith(ALLOWED_EXTENSIONS)):
            raise HTTPException(status_code=422, detail="Invalid archive ready_relative_path")
        if item.get("media_kind") not in ("photo", "video_still"):
            raise HTTPException(status_code=422, detail="Invalid archive media_kind")
        if item["media_kind"] == "video_still":
            policy = item.get("extraction_policy")
            pts = item.get("selected_frame_pts_seconds")
            video_path = item.get("video_source_relative_path")
            if (not isinstance(item.get("video_source_relative_path"), str)
                    or not video_path or len(video_path) > 1024 or video_path.startswith("/")
                    or "\\" in video_path or ".." in video_path.split("/")
                    or not SHA256_RE.fullmatch(str(item.get("video_source_sha256", "")))
                    or not isinstance(policy, dict)
                    or not all(isinstance(policy.get(k), str) and len(policy[k]) <= 128
                               for k in ("tool_version", "policy_version"))
                    or not isinstance(policy.get("requested_position_seconds"), (int, float))
                    or isinstance(policy.get("requested_position_seconds"), bool)
                    or not math.isfinite(policy.get("requested_position_seconds", float("nan")))
                    or not isinstance(pts, (int, float)) or isinstance(pts, bool) or not math.isfinite(pts) or pts < 0):
                raise HTTPException(status_code=422, detail="Video still provenance is incomplete")
        elif any(key in item for key in (
            "video_source_relative_path", "video_source_sha256",
            "selected_frame_pts_seconds", "extraction_policy",
        )):
            raise HTTPException(status_code=422, detail="Photo entry cannot include video extraction provenance")
        provenance = item.get("provenance")
        if not isinstance(provenance, dict):
            raise HTTPException(status_code=422, detail="Archive provenance is required")
        allowed_provenance = {"source_relative_path", "source_sha256", "derivative_id",
                              "timestamp_basis", "timestamp_raw", "camera_timezone",
                              "source_utc_offset_seconds", "clock_correction_seconds",
                              "clock_correction_status", "capture_utc", "capture_app_local",
                              "capture_precision_seconds", "source_local_time", "app_local_fold",
                              "manual_override", "intake_index", "source_zone_label",
                              "destination_utc_offset_seconds", "make", "model", "serial",
                              "source_serial"}
        if set(provenance) - allowed_provenance:
            raise HTTPException(status_code=422, detail="Archive provenance has unsupported fields")
        if any(not SHA256_RE.fullmatch(str(provenance.get(k, ""))) for k in ("source_sha256", "derivative_id")):
            raise HTTPException(status_code=422, detail="Invalid source hash or derivative id")
        for key in ("source_utc_offset_seconds", "destination_utc_offset_seconds", "clock_correction_seconds", "intake_index"):
            value = provenance.get(key)
            if value is not None and (isinstance(value, bool) or not isinstance(value, int)
                                      or abs(value) > MAX_TIME_OFFSET_SECONDS):
                raise HTTPException(status_code=422, detail=f"Invalid archive provenance field: {key}")
        for key, value in provenance.items():
            if isinstance(value, str) and len(value) > (1024 if key in ("source_relative_path", "source_local_time") else 255):
                raise HTTPException(status_code=422, detail=f"Archive provenance field is too long: {key}")
        source_path = provenance.get("source_relative_path")
        if (not isinstance(source_path, str) or not source_path or len(source_path) > 1024
                or source_path.startswith("/") or "\\" in source_path or ".." in source_path.split("/")):
            raise HTTPException(status_code=422, detail="Invalid archive source_relative_path")
        if provenance.get("timestamp_basis") not in ("exif_datetime_original", "video_metadata", "manual_override"):
            raise HTTPException(status_code=422, detail="Invalid timestamp basis")
        manual = provenance.get("manual_override")
        if manual is not None and (
            not isinstance(manual, dict)
            or set(manual) - {"reviewer", "reason", "prior_value"}
            or not isinstance(manual.get("reviewer"), str) or len(manual["reviewer"]) > 128
            or not isinstance(manual.get("reason"), str) or len(manual["reason"]) > 512
        ):
            raise HTTPException(status_code=422, detail="Invalid manual timestamp override provenance")
        if provenance.get("clock_correction_status") not in ("verified", "unknown", "not_applicable") or not isinstance(provenance.get("capture_app_local"), str):
            raise HTTPException(status_code=422, detail="Archive capture time is not resolved")
        if (not isinstance(provenance.get("capture_precision_seconds"), (int, float))
                or isinstance(provenance.get("capture_precision_seconds"), bool)
                or not math.isfinite(provenance["capture_precision_seconds"])
                or provenance["capture_precision_seconds"] < 0
                or provenance["capture_precision_seconds"] > 86400):
            raise HTTPException(status_code=422, detail="Invalid capture precision")
        try:
            local_time = datetime.fromisoformat(provenance["capture_app_local"])
            capture_utc = datetime.fromisoformat(provenance["capture_utc"].replace("Z", "+00:00"))
            fold = provenance.get("app_local_fold")
            if fold not in (None, 0, 1):
                raise ValueError("invalid fold")
            candidates = _archive_local_utc_candidates(local_time, archive["app_timezone"])
            if not candidates:
                raise ValueError("nonexistent local wall time")
            if len(candidates) == 2:
                raise ValueError("ambiguous local wall time is not importable")
            expected_utc = candidates.get(fold, candidates[0])
            if capture_utc.tzinfo is None or expected_utc != capture_utc.astimezone(timezone.utc):
                raise ValueError("UTC/local timestamp mismatch")
            expected_offset_seconds = int(local_time.replace(
                tzinfo=ZoneInfo(archive["app_timezone"]), fold=fold or 0
            ).utcoffset().total_seconds())
            destination_offset = provenance.get("destination_utc_offset_seconds")
            if destination_offset is not None and destination_offset != expected_offset_seconds:
                raise ValueError("destination timezone offset mismatch")
        except ZoneInfoNotFoundError:
            raise HTTPException(status_code=422, detail="Unknown archive app_timezone")
        except (AttributeError, KeyError, TypeError, ValueError) as exc:
            raise HTTPException(
                status_code=422,
                detail=f"Archive capture UTC and app-local timestamps do not agree: {exc}",
            )
        # Metadata keys that would assert application trust or bypass validation
        # are never accepted from the client.
        def reject_forged_keys(value):
            if isinstance(value, dict):
                if forbidden.intersection(value):
                    raise HTTPException(status_code=422, detail="Archive file contains server-owned fields")
                for nested in value.values():
                    reject_forged_keys(nested)
            elif isinstance(value, list):
                for nested in value:
                    reject_forged_keys(nested)
        # Target identifiers are the one intentional occurrence of project/camera/deployment IDs.
        reject_forged_keys({k: v for k, v in item.items() if k != "target"})
    if indexes != set(range(total_files)):
        raise HTTPException(status_code=422, detail="Archive file indexes must be contiguous")
    return archive


def _parse_staged_index(object_key: str) -> Optional[int]:
    tail = object_key.rsplit("/", 1)[-1]
    prefix = tail.split("_", 1)[0] if "_" in tail else ""
    return int(prefix) if prefix.isdigit() else None


def _list_staged_uploads(prefix: str) -> List[str]:
    storage = StorageClient()
    paginator = storage.client.get_paginator("list_objects_v2")
    return [
        obj["Key"]
        for page in paginator.paginate(Bucket=BUCKET_BULK_UPLOAD_STAGING, Prefix=prefix)
        for obj in (page.get("Contents", []) or [])
    ]


def _gb(num_bytes: int) -> str:
    """Format a byte count as a one-decimal GB string for user messages."""
    return f"{max(num_bytes, 0) / (1024 ** 3):.1f}"


def _check_disk_headroom(upload_bytes: int) -> None:
    """Refuse a bulk job that would not fit on the local data disk.

    The whole import sits on disk before the cold tier can move it off, so
    a job larger than the free space minus the safety reserve would fill
    the volume that Postgres and MinIO share. Fail early with a message
    that tells the user to split the upload instead of letting the disk
    fill and the server fall over.
    """
    # Probe the data-disk mount, falling back to the container root (same
    # disk on a single-VM deploy) so a missing mount cannot 500 every job.
    probe_path = DATA_DISK_PROBE_PATH if os.path.exists(DATA_DISK_PROBE_PATH) else "/"
    free = shutil.disk_usage(probe_path).free
    usable = free - DISK_RESERVE_BYTES
    if upload_bytes > usable:
        raise HTTPException(
            status_code=status.HTTP_507_INSUFFICIENT_STORAGE,
            detail=(
                f"This upload is {_gb(upload_bytes)} GB but the server has only "
                f"{_gb(usable)} GB free right now "
                f"(a {_gb(DISK_RESERVE_BYTES)} GB safety reserve is kept aside). "
                "Please upload it in parts. Send about half now, wait until it "
                "finishes processing, then send the rest. Each finished part "
                "frees its space again."
            ),
        )


def _safe_basename(name: str) -> str:
    """Strip directory components and replace unsafe chars."""
    base = name.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", base).strip("_")
    return cleaned or "image.jpg"


async def _pipeline_done_counts(
    db: AsyncSession, job_ids: List[int]
) -> Dict[int, int]:
    """Count successfully classified images per bulk-upload job."""
    if not job_ids:
        return {}
    rows = (
        await db.execute(
            select(Image.bulk_upload_job_id, func.count(Image.id))
            .where(
                Image.bulk_upload_job_id.in_(job_ids),
                Image.status == "classified",
            )
            .group_by(Image.bulk_upload_job_id)
        )
    ).all()
    return {row[0]: row[1] for row in rows}


async def _pipeline_failed_counts(db: AsyncSession, job_ids: List[int]) -> Dict[int, int]:
    if not job_ids:
        return {}
    rows = (await db.execute(
        select(Image.bulk_upload_job_id, func.count(Image.id))
        .where(Image.bulk_upload_job_id.in_(job_ids), Image.status == "failed")
        .group_by(Image.bulk_upload_job_id)
    )).all()
    return {row[0]: row[1] for row in rows}


async def _finalise_done_jobs(
    db: AsyncSession, jobs: List[BulkUploadJob], processed_counts: Dict[int, int],
    failed_counts: Optional[Dict[int, int]] = None,
) -> None:
    """Apply a truthful terminal state once each expected file has resolved."""
    finished: Dict[int, str] = {}
    if not jobs:
        return
    current_jobs = (await db.execute(
        select(BulkUploadJob)
        .where(
            BulkUploadJob.id.in_([job.id for job in jobs]),
            BulkUploadJob.status == "processing",
            BulkUploadJob.staging_complete.is_(True),
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )).scalars().all()
    active = [job for job in current_jobs if job.status == "processing" and job.staging_complete]
    if not active:
        return
    job_ids = [job.id for job in active]
    # Re-read pipeline states after locking each job. This serializes terminal
    # finalization with retry-failed, which also locks the job before reset.
    processed_counts = await _pipeline_done_counts(db, job_ids)
    failed_counts = await _pipeline_failed_counts(db, job_ids)
    for job in active:
        manifest = job.manifest or {}
        ledger = ledger_from_manifest(manifest)
        classified = processed_counts.get(job.id, 0)
        pipeline_failed = failed_counts.get(job.id, 0)
        # Count pipeline terminal rows by durable Image FK. The upload ledger
        # records the accepted index and its image UUID at worker handoff.
        pipeline_terminal = classified + pipeline_failed
        queued_indexes = sum(1 for entry in ledger.values() if entry.get("image_uuid"))
        duplicates = sum(1 for entry in ledger.values() if entry.get("outcome") == "duplicate")
        skipped = sum(1 for entry in ledger.values() if entry.get("outcome") == "skipped")
        upload_failed = sum(1 for entry in ledger.values() if entry.get("outcome") == "failed_upload")
        worker_failed = sum(
            1 for entry in ledger.values()
            if entry.get("outcome") == "failed" and not entry.get("image_uuid")
        )
        missing = max(
            0, job.total_files - queued_indexes - duplicates - skipped - upload_failed - worker_failed
        )
        pending = missing + max(0, queued_indexes - pipeline_terminal)
        summary = {
            "pending_files": pending,
            "classified_files": classified,
            "failed_files": pipeline_failed + upload_failed + worker_failed,
            "duplicate_files": duplicates,
            "skipped_files": skipped,
        }
        terminal = terminal_status(summary)
        if terminal:
            finished[job.id] = terminal
    if not finished:
        return
    now = datetime.now(timezone.utc)
    for job_id, terminal in finished.items():
        await db.execute(update(BulkUploadJob).where(BulkUploadJob.id == job_id).values(
            status=terminal, finished_at=now,
        ))
    await db.commit()
    for job in jobs:
        if job.id in finished:
            job.status = finished[job.id]
            job.finished_at = now


def _delete_staging(staged_object_key: str) -> None:
    """
    Delete the MinIO state for a job. Handles both the new per-file
    prefix layout (key ends with '/') and the legacy single-ZIP layout.
    """
    if not staged_object_key:
        return
    storage = StorageClient()
    try:
        if staged_object_key.endswith("/"):
            for key in storage.list_objects(BUCKET_BULK_UPLOAD_STAGING, staged_object_key):
                try:
                    storage.delete_object(BUCKET_BULK_UPLOAD_STAGING, key)
                except Exception as exc:
                    logger.warning(
                        "Failed to delete staged file",
                        key=key,
                        error=str(exc),
                    )
        else:
            storage.delete_object(BUCKET_BULK_UPLOAD_STAGING, staged_object_key)
    except Exception as exc:
        logger.warning(
            "Failed to clean staging",
            staged_object_key=staged_object_key,
            error=str(exc),
        )


async def _expire_orphan_jobs(db: AsyncSession, project_id: int) -> None:
    """
    Fail jobs stuck in a pre-processing state past UPLOAD_TTL and clean
    their staging. Covers both the new 'uploading' state and the legacy
    inspect/awaiting_confirmation states from before the refactor.
    """
    cutoff = datetime.now(timezone.utc) - UPLOAD_TTL
    pre_processing = ("uploading", "queued", "inspecting", "awaiting_confirmation")
    orphans = (
        await db.execute(
            select(BulkUploadJob).where(
                BulkUploadJob.project_id == project_id,
                BulkUploadJob.status.in_(pre_processing),
                BulkUploadJob.created_at < cutoff,
            )
        )
    ).scalars().all()
    if not orphans:
        return
    now = datetime.now(timezone.utc)
    for job in orphans:
        _delete_staging(job.staged_object_key)
        job.status = "failed"
        job.error_message = "Auto-cancelled after 24 hours waiting on upload"
        job.finished_at = now
    await db.commit()


async def _queue_positions(db: AsyncSession, project_id: int) -> Dict[int, int]:
    """
    Map of bulk_upload_job.id -> jobs-ahead-in-the-worker-queue, for
    jobs the worker has not finished yet. Position 0 means next to run.
    Only meaningful for status='processing'.
    """
    rows = (
        await db.execute(
            select(BulkUploadJob.id)
            .where(
                BulkUploadJob.project_id == project_id,
                BulkUploadJob.status == "processing",
            )
            .order_by(BulkUploadJob.id.asc())
        )
    ).all()
    return {row[0]: idx for idx, row in enumerate(rows)}


def _job_to_response(
    job: BulkUploadJob,
    camera_name: Optional[str],
    created_by_email: Optional[str],
    processed_files: int,
    queue_position: Optional[int] = None,
    *,
    pipeline_failed_files: int = 0,
    include_file_log: bool = True,
) -> BulkUploadJobResponse:
    # file_log can run to MB at 20k files; the list endpoint polls
    # every 5 s and only needs the summary counts, never the per-file
    # detail. Strip it there. The CSV download endpoint reads
    # manifest.file_log straight off the row, so it doesn't depend on
    # this response shape.
    manifest = dict(job.manifest) if job.manifest is not None else None
    if manifest is not None:
        manifest.pop("upload_ledger", None)
        if not include_file_log:
            manifest.pop("file_log", None)
    summary = summarize_ledger(job.manifest, job.total_files)
    ledger = ledger_from_manifest(job.manifest)
    worker_duplicates = sum(1 for entry in ledger.values() if entry.get("outcome") == "duplicate")
    worker_skipped = sum(1 for entry in ledger.values() if entry.get("outcome") == "skipped")
    upload_failed = sum(1 for entry in ledger.values() if entry.get("outcome") == "failed_upload")
    worker_failed = sum(
        1 for entry in ledger.values()
        if entry.get("outcome") == "failed" and not entry.get("image_uuid")
    )
    queued_indexes = sum(1 for entry in ledger.values() if entry.get("image_uuid"))
    pipeline_terminal = processed_files + pipeline_failed_files
    summary["classified_files"] = processed_files
    summary["duplicate_files"] = worker_duplicates
    summary["skipped_files"] = worker_skipped
    summary["failed_files"] = upload_failed + pipeline_failed_files + worker_failed
    summary["pending_files"] = max(
        0,
        job.total_files - worker_duplicates - worker_skipped - upload_failed - worker_failed
        - min(queued_indexes, pipeline_terminal),
    )
    outcome_warning = None
    if job.status == "done" and job.total_files == 0 and not ledger and not (manifest or {}).get("file_log"):
        outcome_warning = "Legacy job has no reliable per-file outcome records; historical counts are unknown."
    elif job.status in {"done", "partial", "failed"} and not (job.manifest or {}).get("upload_ledger"):
        if not ledger or len(ledger) != job.total_files:
            outcome_warning = "Legacy expected-file count cannot be reconciled with recorded outcomes; historical totals are unknown."
    return BulkUploadJobResponse(
        uuid=job.uuid,
        project_id=job.project_id,
        camera_id=job.camera_id,
        deployment_id=getattr(job, "deployment_id", None),
        client_batch_id=getattr(job, "client_batch_id", None),
        archive_manifest=(
            {key: value for key, value in job.archive_manifest.items() if key != "files"}
            | {"file_count": len(job.archive_manifest.get("files", []))}
            if getattr(job, "archive_manifest", None) else None
        ),
        camera_name=camera_name,
        original_filename=job.original_filename,
        status=job.status,
        total_files=job.total_files,
        uploaded_files=summary.get("uploaded_files", 0),
        processed_files=processed_files,
        classified_files=processed_files,
        failed_files=summary["failed_files"],
        upload_failed_files=upload_failed,
        duplicate_files=summary["duplicate_files"],
        skipped_files=summary["skipped_files"],
        pending_files=summary["pending_files"],
        outcome_warning=outcome_warning,
        error_message=job.error_message,
        manifest=manifest,
        time_offset_seconds=job.time_offset_seconds,
        queue_position=queue_position if job.status == "processing" else None,
        started_at=job.started_at.isoformat() if job.started_at else None,
        process_started_at=(
            job.process_started_at.isoformat() if job.process_started_at else None
        ),
        finished_at=job.finished_at.isoformat() if job.finished_at else None,
        created_at=job.created_at.isoformat() if job.created_at else "",
        created_by_email=created_by_email,
    )


async def _response_with_live_counts(
    db: AsyncSession, job: BulkUploadJob, camera_name: Optional[str], created_by_email: Optional[str],
) -> BulkUploadJobResponse:
    """Replayed idempotent responses carry the same current counts as polling."""
    done = await _pipeline_done_counts(db, [job.id])
    failed = await _pipeline_failed_counts(db, [job.id])
    await _finalise_done_jobs(db, [job], done, failed)
    await db.refresh(job)
    return _job_to_response(
        job, camera_name, created_by_email,
        processed_files=done.get(job.id, 0),
        pipeline_failed_files=failed.get(job.id, 0),
    )


@router.get("/archive-timezone")
async def get_archive_timezone(
    project_id: int,
    user: User = Depends(require_project_admin_access),
    db: AsyncSession = Depends(get_async_session),
):
    """Current app timezone and stable fingerprint required by archive intake."""
    settings = (await db.execute(select(ServerSettings).limit(1))).scalar_one_or_none()
    timezone_name = settings.timezone if settings and settings.timezone else "UTC"
    return {"timezone": timezone_name,
            "app_timezone_fingerprint": _archive_timezone_fingerprint(timezone_name)}


@router.post("/scan-profile", response_model=ScanProfileResponse)
async def scan_profile(
    project_id: int,
    body: ScanProfileRequest,
    user: User = Depends(require_project_admin_access),
    db: AsyncSession = Depends(get_async_session),
):
    """
    Run the same camera-profile hunt as live ingestion against the EXIF the
    client read locally, to decide the upload mode before any byte is sent.

    Path-based profiles (INSTAR) cannot match here, a browser upload has no
    FTPS upload path, so only EXIF profiles resolve. Returns 'profile' mode
    with the device_id when one camera resolves, 'manual' mode (pick a site)
    when none does, and flags a multi-camera sample so the user splits it.
    """
    device_id_to_profile: Dict[str, str] = {}
    for entry in body.entries:
        exif: Dict[str, Any] = {}
        if entry.make:
            exif["Make"] = entry.make
        if entry.model:
            exif["Model"] = entry.model
        if entry.serial:
            exif["SerialNumber"] = entry.serial
        try:
            profile = identify_camera_profile(
                exif=exif, filename=entry.filename, relative_path=""
            )
        except ValueError:
            continue
        if profile.is_path_based:
            continue
        device_id = profile.get_camera_id(exif, entry.filename)
        if device_id:
            device_id_to_profile[device_id] = profile.name

    if not device_id_to_profile:
        return ScanProfileResponse(mode="manual")

    device_ids = sorted(device_id_to_profile.keys())
    if len(device_ids) > 1:
        return ScanProfileResponse(
            mode="manual", multiple_cameras=True, device_ids=device_ids
        )

    device_id = device_ids[0]
    camera_id = (
        await db.execute(
            select(Camera.id).where(
                Camera.device_id == device_id,
                Camera.project_id == project_id,
            )
        )
    ).scalar_one_or_none()

    return ScanProfileResponse(
        mode="profile",
        device_id=device_id,
        profile_name=device_id_to_profile[device_id],
        camera_registered=camera_id is not None,
        camera_id=camera_id,
    )


@router.post("/check-duplicates", response_model=CheckDuplicatesResponse)
async def check_duplicates(
    project_id: int,
    body: CheckDuplicatesRequest,
    user: User = Depends(require_project_admin_access),
    db: AsyncSession = Depends(get_async_session),
):
    """
    For the picked camera, return the naive EXIF timestamps that
    already exist on an Image row. Used by the pre-flight scan to
    show "N already in the project" before the user pays the upload
    cost. Camera-scoped so two cameras firing at the same second do
    not produce a false positive across cameras.
    """
    if not body.captured_ats:
        return CheckDuplicatesResponse(duplicate_counts={})

    # Verify the camera belongs to this project, otherwise this is a
    # cross-project query attempt.
    cam = (
        await db.execute(
            select(Camera.id).where(
                Camera.id == body.camera_id,
                Camera.project_id == project_id,
            )
        )
    ).scalar_one_or_none()
    if cam is None:
        raise HTTPException(
            status_code=400,
            detail="Camera does not belong to this project",
        )

    # Parse the client's naive ISO strings into Python naive
    # datetimes. captured_at is stored without tzinfo per the
    # camera-clock rule in DEVELOPERS.md, so the comparison must use
    # naive values as well; passing aware datetimes would crash the
    # query with a tz-mismatch error.
    parsed: List[datetime] = []
    for value in body.captured_ats:
        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            continue
        if dt.tzinfo is not None:
            # Drop the tz; everything in this column is camera-clock
            # naive. The client should not send an offset here.
            dt = dt.replace(tzinfo=None)
        parsed.append(dt)
    if not parsed:
        return CheckDuplicatesResponse(duplicate_counts={})

    rows = await db.execute(
        select(Image.captured_at, func.count(Image.id))
        .where(
            Image.camera_id == body.camera_id,
            Image.captured_at.in_(parsed),
        )
        .group_by(Image.captured_at)
    )
    counts: Dict[str, int] = {}
    for dt, n in rows.all():
        if dt is None:
            continue
        # Echo back in the ISO shape the client sent so the frontend
        # can build a Map without timezone gymnastics.
        counts[dt.strftime("%Y-%m-%dT%H:%M:%S")] = int(n)
    return CheckDuplicatesResponse(duplicate_counts=counts)


async def _get_or_create_camera_by_device_id(
    db: AsyncSession, project_id: int, device_id: str
) -> Camera:
    """
    Return the project's camera for this device_id, creating it if needed.

    Unlike live FTPS ingestion (which rejects unknown device_ids because an
    incoming file has no project context), a bulk job already carries its
    project, so it is safe to auto-create the camera here.
    """
    camera = (
        await db.execute(select(Camera).where(Camera.device_id == device_id))
    ).scalar_one_or_none()
    if camera is not None:
        if camera.project_id != project_id:
            raise HTTPException(
                status_code=400,
                detail=f"Camera {device_id} belongs to another project",
            )
        return camera

    camera = Camera(
        device_id=device_id,
        project_id=project_id,
        status="inventory",
        config={},
    )
    db.add(camera)
    await db.flush()
    logger.info(
        "Auto-created camera for bulk upload",
        device_id=device_id,
        project_id=project_id,
        camera_id=camera.id,
    )
    return camera


async def _get_or_create_synthetic_camera(
    db: AsyncSession, project_id: int, site_id: int
) -> Camera:
    """
    Return the synthetic per-site camera used by manual (no-profile) bulk
    uploads. The device_id is `bulk-cam-{site_id}`: it reads as a camera id
    (which it is), and keying it on the site means one camera per site, reused
    on later uploads to the same site so re-imports never spawn phantom cameras.
    """
    return await _get_or_create_camera_by_device_id(
        db, project_id, f"bulk-cam-{site_id}"
    )


@router.post("/jobs", status_code=status.HTTP_201_CREATED, response_model=BulkUploadJobResponse)
async def create_bulk_upload_job(
    project_id: int,
    body: CreateBulkUploadRequest,
    user: User = Depends(require_project_admin_access),
    db: AsyncSession = Depends(get_async_session),
):
    """
    Create an empty bulk-upload job. The frontend has already scanned the
    folder, run the pre-flight profile check, and computed the manifest.
    Exactly one of device_id (Mode A) / site_id (Mode B) is set; see
    CreateBulkUploadRequest. Files are then uploaded one at a time to
    /jobs/{uuid}/files and finished with /jobs/{uuid}/finalize.
    """
    archive = None
    request_fingerprint = None
    archive_camera = None
    archive_deployment = None
    if body.archive_manifest is not None:
        if body.time_offset_seconds != 0:
            raise HTTPException(status_code=422, detail="Archive intake requires time_offset_seconds=0")
        archive = _validate_archive_manifest(
            body.archive_manifest, project_id, body.camera_id, body.deployment_id, body.total_files
        )
        if sum(item["size_bytes"] for item in archive["files"]) != body.total_bytes:
            raise HTTPException(status_code=422, detail="total_bytes does not match archive manifest sizes")
        request_fingerprint = _archive_request_fingerprint(project_id, body, archive)
        prior = (await db.execute(select(BulkUploadJob).where(
            BulkUploadJob.project_id == project_id,
            BulkUploadJob.client_batch_id == body.client_batch_id,
        ))).scalar_one_or_none()
        if prior:
            _require_idempotent_payload(prior.request_fingerprint, request_fingerprint)
            name = await db.scalar(select(Camera.device_id).where(Camera.id == prior.camera_id))
            return await _response_with_live_counts(db, prior, name, user.email)

        settings = (await db.execute(select(ServerSettings).limit(1))).scalar_one_or_none()
        current_tz = settings.timezone if settings and settings.timezone else "UTC"
        if archive["app_timezone"] != current_tz or archive["app_timezone_fingerprint"] != _archive_timezone_fingerprint(current_tz):
            raise HTTPException(status_code=409, detail="Archive timezone settings are stale; rescan the current settings")
        archive_camera = (await db.execute(select(Camera).where(
            Camera.id == body.camera_id, Camera.project_id == project_id,
        ))).scalar_one_or_none()
        archive_deployment = (await db.execute(select(Deployment).join(
            Site, Site.id == Deployment.site_id,
        ).where(
            Deployment.id == body.deployment_id, Deployment.camera_id == body.camera_id,
            Site.project_id == project_id,
        ))).scalar_one_or_none()
        if archive_camera is None or archive_deployment is None:
            raise HTTPException(status_code=400, detail="Archive target camera/deployment is not valid for this project")
        for item in archive["files"]:
            try:
                capture = datetime.fromisoformat(item["provenance"]["capture_app_local"])
                capture_day = capture.date()
            except (TypeError, ValueError, KeyError):
                raise HTTPException(status_code=422, detail="Invalid capture_app_local timestamp")
            if capture_day < archive_deployment.start_date or (archive_deployment.end_date and capture_day > archive_deployment.end_date):
                raise HTTPException(status_code=422, detail="Archive capture time falls outside the selected deployment")

    # Soft cap on in-flight jobs per project so one user cannot
    # starve the bulk worker queue with twenty parallel SD-card
    # uploads. Counts both 'uploading' (client streaming files) and
    # 'processing' (worker pipeline). Terminal jobs do not count.
    in_flight = (
        await db.execute(
            select(func.count(BulkUploadJob.id)).where(
                BulkUploadJob.project_id == project_id,
                BulkUploadJob.status.in_(("uploading", "processing")),
            )
        )
    ).scalar_one()
    if in_flight >= MAX_CONCURRENT_JOBS_PER_PROJECT:
        raise HTTPException(
            status_code=429,
            detail=(
                f"This project already has {in_flight} bulk uploads in "
                f"flight (limit {MAX_CONCURRENT_JOBS_PER_PROJECT}). "
                "Wait for one to finish, or discard a queued one, "
                "before starting another."
            ),
        )

    # Refuse the job if the import would not fit on the local data disk.
    _check_disk_headroom(body.total_bytes)

    # Resolve the target camera. Mode A uses the profile-matched device_id (auto
    # -created if new); Mode B uses a synthetic per-site camera and pins the site.
    manifest = dict(body.manifest or {})
    # These keys are server-owned; the browser scan cannot assert acceptance
    # or claim processing outcomes.
    for reserved in ("upload_ledger", "file_log", "process_summary"):
        manifest.pop(reserved, None)
    if archive is not None:
        camera = archive_camera
    elif body.device_id:
        camera = await _get_or_create_camera_by_device_id(
            db, project_id, body.device_id
        )
    else:
        site_id = (
            await db.execute(
                select(Site.id).where(
                    Site.id == body.site_id,
                    Site.project_id == project_id,
                )
            )
        ).scalar_one_or_none()
        if site_id is None:
            raise HTTPException(
                status_code=400,
                detail="Site does not belong to this project",
            )
        camera = await _get_or_create_synthetic_camera(db, project_id, body.site_id)
        # The chosen site rides along in the manifest so the worker can pin the
        # batch to it after the queue hop, without a dedicated column.
        manifest["site_id"] = body.site_id

    job_uuid = str(uuid.uuid4())
    safe_folder_name = _safe_basename(body.folder_name) or "upload"
    camera_name = camera.device_id
    camera_id = camera.id
    user_email = user.email
    user_id = user.id

    job = BulkUploadJob(
        uuid=job_uuid,
        project_id=project_id,
        created_by_user_id=user_id,
        camera_id=camera_id,
        deployment_id=body.deployment_id if archive is not None else None,
        client_batch_id=body.client_batch_id,
        request_fingerprint=request_fingerprint,
        archive_manifest=archive,
        original_filename=safe_folder_name,
        staged_object_key=_staging_prefix(project_id, job_uuid),
        status="uploading",
        total_files=body.total_files,
        manifest=manifest or None,
        time_offset_seconds=body.time_offset_seconds,
    )
    db.add(job)
    try:
        await db.commit()
    except IntegrityError:
        if body.client_batch_id is None:
            raise
        await db.rollback()
        winner = (await db.execute(select(BulkUploadJob).where(
            BulkUploadJob.project_id == project_id,
            BulkUploadJob.client_batch_id == body.client_batch_id,
        ))).scalar_one_or_none()
        if winner is None:
            raise HTTPException(status_code=409, detail="client_batch_id was concurrently used for a different request")
        _require_idempotent_payload(winner.request_fingerprint, request_fingerprint)
        return await _response_with_live_counts(db, winner, camera_name, user_email)
    await db.refresh(job)

    logger.info(
        "Created bulk upload job",
        job_uuid=job_uuid,
        project_id=project_id,
        camera_id=camera_id,
        deployment_id=body.deployment_id if archive is not None else None,
        total_files=body.total_files,
        time_offset_seconds=body.time_offset_seconds,
        user_id=user_id,
    )

    return _job_to_response(
        job,
        camera_name=camera_name,
        created_by_email=user_email,
        processed_files=0,
    )


@router.post("/jobs/{job_uuid}/files", status_code=status.HTTP_201_CREATED)
async def upload_bulk_file(
    project_id: int,
    job_uuid: str,
    index: int,
    file: UploadFile = File(...),
    user: User = Depends(require_project_admin_access),
    db: AsyncSession = Depends(get_async_session),
):
    """
    Upload one file into a job's staging prefix. The client passes its
    own ordering index so retries write to the same MinIO key
    (idempotent for resume). Module-level semaphore caps the number
    of concurrent uploads handled by this API process so one user
    cannot pin MinIO with hundreds of parallel writes.
    """
    async with _upload_semaphore:
        return await _upload_bulk_file_inner(
            project_id, job_uuid, index, file, user, db,
        )


async def _upload_bulk_file_inner(
    project_id: int,
    job_uuid: str,
    index: int,
    file: UploadFile,
    user: User,
    db: AsyncSession,
):
    if index < 0 or index >= MAX_FILES_PER_JOB:
        raise HTTPException(status_code=400, detail="File index out of range")

    job = (
        await db.execute(
            select(BulkUploadJob).where(
                BulkUploadJob.project_id == project_id,
                BulkUploadJob.uuid == job_uuid,
            ).with_for_update()
        )
    ).scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=404, detail="Bulk upload job not found")
    if job.status != "uploading":
        raise HTTPException(
            status_code=400,
            detail=f"Job is in status '{job.status}', cannot accept more files",
        )
    if index >= job.total_files:
        raise HTTPException(status_code=400, detail="File index exceeds expected file count")

    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename")
    if not file.filename.lower().endswith(ALLOWED_EXTENSIONS):
        raise HTTPException(
            status_code=400,
            detail="Only JPEG and PNG images are supported",
        )
    # Content-type is set by the browser; tolerate octet-stream fallback
    # (Firefox does this for some drag-drop paths) when the extension
    # already vouched for the type.
    if file.content_type and file.content_type not in ALLOWED_CONTENT_TYPES:
        if file.content_type != "application/octet-stream":
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported content type {file.content_type}",
            )

    body = await file.read()
    if len(body) == 0:
        raise HTTPException(status_code=400, detail="Empty file")
    if len(body) > MAX_FILE_SIZE_BYTES:
        raise HTTPException(
            status_code=400,
            detail=f"File exceeds {MAX_FILE_SIZE_BYTES // (1024 * 1024)} MB cap",
        )

    safe_name = _safe_basename(file.filename)
    object_key = f"{_staging_prefix(project_id, job_uuid)}{index:06d}_{safe_name}"
    checksum = hashlib.sha256(body).hexdigest()
    if job.archive_manifest is not None:
        archived = next((entry for entry in job.archive_manifest.get("files", []) if entry.get("index") == index), None)
        if archived is None or not _archive_file_bytes_match(archived, body):
            raise HTTPException(status_code=409, detail="Uploaded file does not match the immutable archive manifest")
    prior = ledger_from_manifest(job.manifest).get(str(index), {})
    if prior.get("accepted"):
        # The job row lock serializes retries across API processes. An index
        # represents one immutable input, even if a resumed folder differs.
        if prior.get("filename") != safe_name or prior.get("sha256") != checksum:
            raise HTTPException(status_code=409, detail="This file index already contains a different image; resume with the original folder")
        return {"object_key": object_key, "size": len(body)}

    storage = StorageClient()
    existing_keys = storage.list_objects(
        BUCKET_BULK_UPLOAD_STAGING,
        prefix=f"{_staging_prefix(project_id, job_uuid)}{index:06d}_",
    )
    if existing_keys:
        # Recover the storage-write/database-commit crash boundary. A staged
        # object also reserves its index; a changed resume cannot add a second.
        if existing_keys != [object_key]:
            raise HTTPException(status_code=409, detail="This file index has a staged image with a different name; resume with the original folder")
        staged = storage.download_fileobj(BUCKET_BULK_UPLOAD_STAGING, object_key)
        if hashlib.sha256(staged).hexdigest() != checksum:
            raise HTTPException(status_code=409, detail="This file index has different staged content; resume with the original folder")
    else:
        storage.upload_fileobj(io.BytesIO(body), BUCKET_BULK_UPLOAD_STAGING, object_key)

    manifest, _ = merge_ledger_entry(
        job.manifest,
        index,
        {"filename": safe_name, "outcome": "uploaded", "accepted": True, "size": len(body), "sha256": checksum},
        protect_accepted=False,
    )
    job.manifest = manifest
    await db.commit()

    return {"object_key": object_key, "size": len(body)}


class ReportedUploadFailure(BaseModel):
    index: int = Field(ge=0, lt=MAX_FILES_PER_JOB)
    filename: str = Field(min_length=1, max_length=255)
    reason: Literal["request_too_large", "upload_failed", "cancelled"]


class ReportUploadOutcomesRequest(BaseModel):
    files: List[ReportedUploadFailure] = Field(min_length=1, max_length=MAX_FILES_PER_JOB)


@router.post("/jobs/{job_uuid}/outcomes", response_model=BulkUploadJobResponse)
async def report_upload_outcomes(
    project_id: int,
    job_uuid: str,
    body: ReportUploadOutcomesRequest,
    user: User = Depends(require_project_admin_access),
    db: AsyncSession = Depends(get_async_session),
):
    """Persist browser-observed upload failures such as a proxy 413."""
    job = (await db.execute(select(BulkUploadJob).where(
        BulkUploadJob.project_id == project_id,
        BulkUploadJob.uuid == job_uuid,
    ).with_for_update())).scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=404, detail="Bulk upload job not found")
    if job.status != "uploading":
        raise HTTPException(status_code=400, detail=f"Job is in status '{job.status}'")

    seen = set()
    manifest = dict(job.manifest or {})
    for item in body.files:
        index = item.index
        filename = item.filename
        reason = item.reason
        if index < 0 or index >= job.total_files:
            raise HTTPException(status_code=422, detail="File index exceeds expected file count")
        if index in seen:
            raise HTTPException(status_code=422, detail="Duplicate index in outcomes request")
        seen.add(index)
        manifest, _ = merge_ledger_entry(
            manifest,
            index,
            {"filename": _safe_basename(filename), "outcome": "failed_upload", "reason": reason},
        )
        # Existing server acceptance is authoritative and cannot be overwritten.
    job.manifest = manifest
    await db.commit()
    await db.refresh(job)
    camera_name = await db.scalar(select(Camera.device_id).where(Camera.id == job.camera_id)) if job.camera_id else None
    return _job_to_response(
        job, camera_name, user.email, processed_files=0,
    )


@router.post("/jobs/{job_uuid}/finalize", response_model=BulkUploadJobResponse)
async def finalize_bulk_upload(
    project_id: int,
    job_uuid: str,
    user: User = Depends(require_project_admin_access),
    db: AsyncSession = Depends(get_async_session),
):
    """
    Mark a job as finished uploading and hand it to the worker. The
    client calls this once every per-file POST has returned 2xx.
    """
    job = (
        await db.execute(
            select(BulkUploadJob).where(
                BulkUploadJob.project_id == project_id,
                BulkUploadJob.uuid == job_uuid,
            ).with_for_update()
        )
    ).scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=404, detail="Bulk upload job not found")
    if job.status != "uploading":
        raise HTTPException(
            status_code=400,
            detail=f"Job is in status '{job.status}', cannot finalize",
        )
    if job.camera_id is None:
        raise HTTPException(
            status_code=400,
            detail="Job has no target camera",
        )

    # Backfill acceptance for already-staged objects (including an upload
    # started before a process restart). Refuse ambiguous storage state: the
    # worker would otherwise process two objects under one expected index.
    manifest = dict(job.manifest or {})
    staged_by_index: Dict[int, List[str]] = {}
    for key in _list_staged_uploads(job.staged_object_key):
        index = _parse_staged_index(key)
        if index is None or index >= job.total_files:
            continue
        staged_by_index.setdefault(index, []).append(key)

    duplicate_indexes = sorted(index for index, keys in staged_by_index.items() if len(keys) > 1)
    if duplicate_indexes:
        shown = ", ".join(str(index) for index in duplicate_indexes[:20])
        suffix = " …" if len(duplicate_indexes) > 20 else ""
        raise HTTPException(
            status_code=409,
            detail=(
                f"Multiple staged objects share file index(es) {shown}{suffix}. "
                "No staged files were removed and the job was not finalized. "
                "Preserve these objects and have an administrator resolve the upload before retrying."
            ),
        )

    for index, keys in staged_by_index.items():
        key = keys[0]
        tail = key.rsplit("/", 1)[-1]
        filename = tail.split("_", 1)[1] if "_" in tail else tail
        manifest, _ = merge_ledger_entry(
            manifest, index,
            {"filename": _safe_basename(filename), "outcome": "uploaded", "accepted": True},
        )
    job.manifest = manifest
    ledger_summary = summarize_ledger(job.manifest, job.total_files)
    unresolved = [
        index for index in range(job.total_files)
        if str(index) not in ledger_from_manifest(job.manifest)
    ]
    if unresolved:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Upload is incomplete; report every failed file before finalizing.",
                "pending_files": ledger_summary.get("pending_files", len(unresolved)),
                "missing_index_count": len(unresolved),
                "missing_indexes": unresolved[:100],
            },
        )
    accepted = ledger_summary.get("uploaded_files", 0)
    if accepted == 0:
        job.status = "failed"
        job.error_message = "No files were accepted; retry the failed uploads."
        job.finished_at = datetime.now(timezone.utc)
        await db.commit()
        await db.refresh(job)
        return _job_to_response(job, None, user.email, processed_files=0)

    job.status = "processing"
    job.started_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(job)

    queue = RedisQueue(QUEUE_BULK_UPLOAD_JOB_PROCESS)
    queue.publish({"job_uuid": job_uuid, "phase": "process"})

    camera_name = (
        await db.execute(select(Camera.device_id).where(Camera.id == job.camera_id))
    ).scalar_one_or_none()

    logger.info(
        "Bulk upload finalized",
        job_uuid=job_uuid,
        camera_id=job.camera_id,
        total_files=job.total_files,
        user_id=user.id,
    )
    return _job_to_response(
        job,
        camera_name=camera_name,
        created_by_email=user.email,
        processed_files=0,
    )


@router.post("/jobs/{job_uuid}/cancel", response_model=BulkUploadJobResponse)
async def cancel_bulk_upload(
    project_id: int,
    job_uuid: str,
    user: User = Depends(require_project_admin_access),
    db: AsyncSession = Depends(get_async_session),
):
    """
    Stop a bulk upload. Allowed while uploading (and the legacy pre-processing
    states) and while processing. Marks the job 'cancelled' and clears staging;
    the bulk worker stops its loop and the detection/classification workers skip
    the job's remaining images. Images already imported are kept (use the
    delete-images endpoint to remove them).
    """
    job = (
        await db.execute(
            select(BulkUploadJob).where(
                BulkUploadJob.project_id == project_id,
                BulkUploadJob.uuid == job_uuid,
            )
        )
    ).scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=404, detail="Bulk upload job not found")
    cancellable = ("uploading", "queued", "inspecting", "awaiting_confirmation", "processing")
    if job.status not in cancellable:
        raise HTTPException(
            status_code=400,
            detail=f"Job is in status '{job.status}', cannot stop",
        )

    # Safe in both phases: pre-processing has no worker touching staging, and a
    # processing job's worker tolerates a missing staged object (per-file skip).
    _delete_staging(job.staged_object_key)

    job.status = "cancelled"
    job.error_message = None
    job.finished_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(job)

    camera_name = None
    if job.camera_id:
        camera_name = (
            await db.execute(select(Camera.device_id).where(Camera.id == job.camera_id))
        ).scalar_one_or_none()

    return _job_to_response(
        job,
        camera_name=camera_name,
        created_by_email=user.email,
        processed_files=0,
    )


@router.delete("/jobs/{job_uuid}/images")
async def delete_bulk_upload_images(
    project_id: int,
    job_uuid: str,
    user: User = Depends(require_project_admin_access),
    db: AsyncSession = Depends(get_async_session),
):
    """
    Delete every image imported by this job, with its detections,
    classifications, and stored files. The cleanup action offered on a stopped
    job. The job row itself stays; discard it separately. Only meaningful while
    the job exists, since discarding the job unlinks its images.
    """
    job = (
        await db.execute(
            select(BulkUploadJob).where(
                BulkUploadJob.project_id == project_id,
                BulkUploadJob.uuid == job_uuid,
            )
        )
    ).scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=404, detail="Bulk upload job not found")

    image_ids = [
        row[0]
        for row in (
            await db.execute(
                select(Image.id).where(Image.bulk_upload_job_id == job.id)
            )
        ).all()
    ]
    deleted, errors, _emptied_sites = await delete_images_by_ids(db, image_ids)
    logger.info(
        "Deleted bulk upload images",
        job_uuid=job_uuid,
        deleted=deleted,
        failed=len(errors),
        user_id=user.id,
    )
    return {"deleted": deleted, "failed": len(errors), "errors": errors}


@router.delete("/jobs/{job_uuid}", status_code=status.HTTP_204_NO_CONTENT)
async def discard_bulk_upload_job(
    project_id: int,
    job_uuid: str,
    user: User = Depends(require_project_admin_access),
    db: AsyncSession = Depends(get_async_session),
):
    """
    Remove a bulk-upload job from the list. Cleans staging for any
    pre-processing state. Refuses while the worker is mid-pipeline so
    we don't leak half-deleted state.
    """
    job = (
        await db.execute(
            select(BulkUploadJob).where(
                BulkUploadJob.project_id == project_id,
                BulkUploadJob.uuid == job_uuid,
            )
        )
    ).scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=404, detail="Bulk upload job not found")
    if job.status == "processing":
        raise HTTPException(
            status_code=400,
            detail="Cannot discard a job that is currently processing",
        )

    pre_processing = ("uploading", "queued", "inspecting", "awaiting_confirmation")
    if job.status in pre_processing:
        _delete_staging(job.staged_object_key)

    await db.delete(job)
    await db.commit()
    logger.info(
        "Discarded bulk upload job",
        job_uuid=job_uuid,
        prior_status=job.status,
        user_id=user.id,
    )


@router.get("/jobs", response_model=List[BulkUploadJobResponse])
async def list_bulk_upload_jobs(
    project_id: int,
    user: User = Depends(require_project_admin_access),
    db: AsyncSession = Depends(get_async_session),
):
    """List bulk-upload jobs for this project, most recent first."""
    await _expire_orphan_jobs(db, project_id)
    rows = (
        await db.execute(
            select(BulkUploadJob, Camera.device_id, User.email)
            .outerjoin(Camera, BulkUploadJob.camera_id == Camera.id)
            .join(User, BulkUploadJob.created_by_user_id == User.id)
            .where(BulkUploadJob.project_id == project_id)
            .order_by(BulkUploadJob.created_at.desc())
            .limit(100)
        )
    ).all()
    jobs = [job for job, _, _ in rows]
    processed_counts = await _pipeline_done_counts(db, [j.id for j in jobs])
    failed_counts = await _pipeline_failed_counts(db, [j.id for j in jobs])
    await _finalise_done_jobs(db, jobs, processed_counts, failed_counts)
    positions = await _queue_positions(db, project_id)
    return [
        _job_to_response(
            job,
            camera_name=cam_name,
            created_by_email=email,
            processed_files=processed_counts.get(job.id, 0),
            pipeline_failed_files=failed_counts.get(job.id, 0),
            queue_position=positions.get(job.id),
            # The list endpoint polls every 5 s and only needs the
            # summary. Per-file detail can run to MB at 20k-image
            # jobs and is only used by the CSV download.
            include_file_log=False,
        )
        for job, cam_name, email in rows
    ]


@router.get("/jobs/{job_uuid}/uploaded-indexes")
async def get_uploaded_indexes(
    project_id: int,
    job_uuid: str,
    user: User = Depends(require_project_admin_access),
    db: AsyncSession = Depends(get_async_session),
):
    """
    Return the list of file indexes already in the job's staging
    prefix. The client uses this on resume to skip files that landed
    before the previous tab closed.
    """
    job = (
        await db.execute(
            select(BulkUploadJob).where(
                BulkUploadJob.project_id == project_id,
                BulkUploadJob.uuid == job_uuid,
            )
        )
    ).scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=404, detail="Bulk upload job not found")
    if job.status != "uploading":
        raise HTTPException(
            status_code=400,
            detail=f"Job is in status '{job.status}', cannot list staged uploads",
        )
    if not job.staged_object_key or not job.staged_object_key.endswith("/"):
        # Legacy single-zip job, has no per-file indexes by design.
        return {"indexes": []}

    storage = StorageClient()
    indexes: List[int] = []
    for key in storage.list_objects(BUCKET_BULK_UPLOAD_STAGING, job.staged_object_key):
        tail = key.rsplit("/", 1)[-1]
        prefix = tail.split("_", 1)[0] if "_" in tail else tail
        try:
            indexes.append(int(prefix))
        except ValueError:
            continue
    indexes.sort()
    return {"indexes": indexes}


@router.get("/jobs/{job_uuid}/log.csv")
async def get_bulk_upload_log(
    project_id: int,
    job_uuid: str,
    user: User = Depends(require_project_admin_access),
    db: AsyncSession = Depends(get_async_session),
):
    """
    Stream a per-file CSV log of what happened to every file in this
    job. One row per source file, columns filename, outcome, reason,
    image_uuid, existing_uuid. Built from the manifest.file_log the
    worker writes at the end of processing; empty rows result when
    the job is still running or never finished.
    """
    job = (
        await db.execute(
            select(BulkUploadJob).where(
                BulkUploadJob.project_id == project_id,
                BulkUploadJob.uuid == job_uuid,
            )
        )
    ).scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=404, detail="Bulk upload job not found")

    rows: List[Dict[str, Any]] = []
    manifest = job.manifest or {}
    file_log = manifest.get("file_log") or []
    for entry in file_log:
        rows.append({
            "filename": entry.get("filename", ""),
            "outcome": entry.get("outcome", ""),
            "reason": entry.get("reason", ""),
            "image_uuid": entry.get("image_uuid", ""),
            "existing_uuid": entry.get("existing_uuid", ""),
        })
    if not rows:
        for index, entry in sorted(
            ledger_from_manifest(manifest).items(), key=lambda item: int(item[0])
        ):
            rows.append({
                "filename": entry.get("filename", ""),
                "outcome": entry.get("outcome", ""),
                "reason": entry.get("reason", ""),
                "image_uuid": entry.get("image_uuid", ""),
                "existing_uuid": entry.get("existing_uuid", ""),
            })
    image_uuids = {row["image_uuid"] for row in rows if row["image_uuid"]}
    pipeline_errors = {}
    if image_uuids:
        pipeline_errors = {
            image_uuid: (state or "", error or "", stage or "")
            for image_uuid, state, error, stage in (await db.execute(
                select(
                    Image.uuid, Image.status, Image.pipeline_error,
                    Image.pipeline_failed_stage,
                ).where(
                    Image.bulk_upload_job_id == job.id,
                    Image.uuid.in_(image_uuids),
                )
            )).all()
        }
    for row in rows:
        state, row["pipeline_error"], row["pipeline_failed_stage"] = pipeline_errors.get(
            row["image_uuid"], ("", "", "")
        )
        if state:
            row["outcome"] = state

    def stream() -> Any:
        buf = io.StringIO()
        writer = csv.DictWriter(
            buf,
            fieldnames=[
                "filename", "outcome", "reason", "image_uuid", "existing_uuid",
                "pipeline_error", "pipeline_failed_stage",
            ],
        )
        writer.writeheader()
        yield buf.getvalue()
        buf.seek(0)
        buf.truncate(0)
        for row in rows:
            writer.writerow(row)
            yield buf.getvalue()
            buf.seek(0)
            buf.truncate(0)

    download_name = f"bulk-upload-{job_uuid[:8]}.csv"
    return StreamingResponse(
        stream(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{download_name}"'},
    )


@router.post("/jobs/{job_uuid}/retry-failed")
async def retry_failed_bulk_images(
    project_id: int,
    job_uuid: str,
    user: User = Depends(require_project_admin_access),
    db: AsyncSession = Depends(get_async_session),
):
    """Retry failed pipeline stages without changing image annotations."""
    job = (await db.execute(select(BulkUploadJob).where(
        BulkUploadJob.project_id == project_id,
        BulkUploadJob.uuid == job_uuid,
    ).with_for_update())).scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=404, detail="Bulk upload job not found")
    if job.status not in {"failed", "partial", "done"}:
        raise HTTPException(status_code=409, detail=f"Job is in status '{job.status}'")

    failed_images = (await db.execute(select(Image).where(
        Image.bulk_upload_job_id == job.id,
        Image.status == "failed",
    ).with_for_update())).scalars().all()
    retryable = []
    for image in failed_images:
        stage = image.pipeline_failed_stage
        if stage not in {"detection", "classification"}:
            continue
        image.status = "pending" if stage == "detection" else "detected"
        image.pipeline_updated_at = datetime.now(timezone.utc)
        image.pipeline_attempts = 0
        image.pipeline_error = None
        image.pipeline_failed_stage = None
        image.pipeline_claim_id = None
        detection_ids = [row[0] for row in (await db.execute(
            select(Detection.id).where(Detection.image_id == image.id).order_by(Detection.id)
        )).all()]
        retryable.append((
            image.uuid, image.origin, stage, image.storage_path, image.camera_id, detection_ids,
        ))

    if retryable:
        job.status = "processing"
        job.finished_at = None
        job.error_message = None
    await db.commit()

    for image_uuid, origin, stage, storage_path, camera_id, detection_ids in retryable:
        if stage == "detection":
            queue_name = QUEUE_IMAGE_INGESTED_BULK if origin == "bulk" else QUEUE_IMAGE_INGESTED
            message = {
                "image_uuid": image_uuid,
                "storage_path": storage_path,
                "camera_id": camera_id,
                "origin": origin,
            }
        else:
            queue_name = QUEUE_DETECTION_COMPLETE_BULK if origin == "bulk" else QUEUE_DETECTION_COMPLETE
            message = {
                "image_uuid": image_uuid,
                "num_detections": len(detection_ids),
                "detection_ids": detection_ids,
                "origin": origin,
            }
        RedisQueue(queue_name).publish(message)

    return {
        "job_uuid": job_uuid,
        "status": "processing" if retryable else job.status,
        "retried_files": len(retryable),
        "unretryable_files": len(failed_images) - len(retryable),
    }


@router.get("/jobs/by-client-batch/{client_batch_id}", response_model=BulkUploadJobResponse)
async def get_bulk_upload_job_by_client_batch(
    project_id: int,
    client_batch_id: str,
    user: User = Depends(require_project_admin_access),
    db: AsyncSession = Depends(get_async_session),
):
    """Authorized idempotency recovery lookup, scoped to the route project."""
    job = (await db.execute(select(BulkUploadJob).where(
        BulkUploadJob.project_id == project_id,
        BulkUploadJob.client_batch_id == client_batch_id,
    ))).scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=404, detail="Bulk upload job not found")
    name = await db.scalar(select(Camera.device_id).where(Camera.id == job.camera_id)) if job.camera_id else None
    return await _response_with_live_counts(db, job, name, user.email)


@router.get("/jobs/{job_uuid}/receipts", response_model=BulkUploadReceiptsResponse)
async def get_bulk_upload_receipts(
    project_id: int,
    job_uuid: str,
    user: User = Depends(require_project_admin_access),
    db: AsyncSession = Depends(get_async_session),
):
    """Per-index upload and processing receipt for archive/retry reconciliation."""
    job = (await db.execute(select(BulkUploadJob).where(
        BulkUploadJob.project_id == project_id,
        BulkUploadJob.uuid == job_uuid,
    ))).scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=404, detail="Bulk upload job not found")
    rows = (await db.execute(select(
        Image.uuid, Image.status, Image.pipeline_error,
    ).where(Image.bulk_upload_job_id == job.id))).all()
    states = {
        row.uuid: {"pipeline_status": row.status, "pipeline_error": row.pipeline_error}
        for row in rows
    }
    return BulkUploadReceiptsResponse(files=_bulk_receipt_rows(job, states))


@router.get("/jobs/{job_uuid}", response_model=BulkUploadJobResponse)
async def get_bulk_upload_job(
    project_id: int,
    job_uuid: str,
    user: User = Depends(require_project_admin_access),
    db: AsyncSession = Depends(get_async_session),
):
    """Single job, used by the frontend for live progress polling."""
    row = (
        await db.execute(
            select(BulkUploadJob, Camera.device_id, User.email)
            .outerjoin(Camera, BulkUploadJob.camera_id == Camera.id)
            .join(User, BulkUploadJob.created_by_user_id == User.id)
            .where(
                BulkUploadJob.project_id == project_id,
                BulkUploadJob.uuid == job_uuid,
            )
        )
    ).first()
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Bulk upload job not found",
        )
    job, cam_name, email = row
    processed_counts = await _pipeline_done_counts(db, [job.id])
    failed_counts = await _pipeline_failed_counts(db, [job.id])
    await _finalise_done_jobs(db, [job], processed_counts, failed_counts)
    positions = await _queue_positions(db, project_id)
    return _job_to_response(
        job,
        camera_name=cam_name,
        created_by_email=email,
        processed_files=processed_counts.get(job.id, 0),
        pipeline_failed_files=failed_counts.get(job.id, 0),
        queue_position=positions.get(job.id),
    )
