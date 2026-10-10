"""Pure contract tests for the versioned archive bulk-upload intake."""
import sys
from pathlib import Path
from datetime import datetime
from datetime import timezone

import pytest
from pydantic import ValidationError
from types import SimpleNamespace

API_PATH = Path(__file__).resolve().parents[2] / "services" / "api"
if str(API_PATH) not in sys.path:
    sys.path.insert(0, str(API_PATH))

from routers import bulk_upload as bulk_upload_router  # noqa: E402
from routers.bulk_upload import (  # noqa: E402
    CreateBulkUploadRequest,
    _archive_file_bytes_match,
    _archive_local_utc_candidates,
    _archive_request_fingerprint,
    _bulk_receipt_rows,
    _require_idempotent_payload,
    _validate_archive_manifest,
)
from auth.permissions import require_project_admin_access  # noqa: E402


def archive_manifest():
    return {
        "format": "addax-archive-ready-v1", "batch_id": "batch-1",
        "revision_sha256": "a" * 64, "generated_at_utc": "2026-10-10T12:00:00Z",
        "source_batch_complete_marker_sha256": "b" * 64,
        "app_timezone": "UTC", "app_timezone_fingerprint": "c" * 64,
        "files": [{
            "index": 0, "ready_relative_path": "READY/frame.jpg",
            "ready_sha256": "d" * 64, "size_bytes": 123,
            "media_kind": "photo",
            "target": {"project_id": 7, "camera_id": 11, "deployment_id": 19},
            "provenance": {
                "source_relative_path": "DCIM/frame.jpg", "source_sha256": "e" * 64,
                "derivative_id": "f" * 64, "timestamp_basis": "manual_override",
                "timestamp_raw": None, "camera_timezone": None,
                "source_utc_offset_seconds": None, "clock_correction_seconds": 0,
                "clock_correction_status": "verified", "capture_utc": "2026-10-10T11:00:00Z",
                "capture_app_local": "2026-10-10T11:00:00", "capture_precision_seconds": 1,
            },
        }],
    }


def test_archive_request_is_a_mutually_exclusive_physical_target():
    request = CreateBulkUploadRequest(
        folder_name="archive", camera_id=11, deployment_id=19,
        client_batch_id="archive-batch-rev-target", archive_manifest=archive_manifest(),
        total_files=1, total_bytes=123, time_offset_seconds=0,
    )
    assert request.camera_id == 11 and request.deployment_id == 19
    with pytest.raises(ValidationError):
        CreateBulkUploadRequest(
            folder_name="archive", camera_id=11, deployment_id=19, site_id=4,
            client_batch_id="x", archive_manifest=archive_manifest(),
            total_files=1, total_bytes=123,
        )
    with pytest.raises(ValidationError, match="reserved"):
        CreateBulkUploadRequest(
            folder_name="legacy", camera_id=11, deployment_id=19,
            total_files=1, total_bytes=123,
        )
    assert CreateBulkUploadRequest(
        folder_name="profile", device_id="camera-device", total_files=1, total_bytes=0,
    ).device_id == "camera-device"
    assert CreateBulkUploadRequest(
        folder_name="manual", site_id=5, total_files=1, total_bytes=0,
    ).site_id == 5


def test_manifest_pins_project_camera_deployment_and_contiguous_indexes():
    valid = archive_manifest()
    assert _validate_archive_manifest(valid, 7, 11, 19, 1) == valid
    wrong_target = archive_manifest()
    wrong_target["files"][0]["target"]["project_id"] = 8
    with pytest.raises(Exception, match="target"):
        _validate_archive_manifest(wrong_target, 7, 11, 19, 1)
    bad_index = archive_manifest()
    bad_index["files"][0]["index"] = 1
    with pytest.raises(Exception, match="indexes"):
        _validate_archive_manifest(bad_index, 7, 11, 19, 1)


def test_manifest_rejects_server_owned_verification_and_unresolved_time():
    forged = archive_manifest()
    forged["files"][0]["is_verified"] = True
    with pytest.raises(Exception, match="server-owned"):
        _validate_archive_manifest(forged, 7, 11, 19, 1)
    unresolved = archive_manifest()
    unresolved["files"][0]["provenance"]["clock_correction_status"] = "unresolved"
    with pytest.raises(Exception, match="capture time"):
        _validate_archive_manifest(unresolved, 7, 11, 19, 1)


def test_archive_timezone_rejects_dst_gaps_and_all_overlaps():
    gap = _archive_local_utc_candidates(datetime(2025, 3, 9, 2, 30), "America/New_York")
    assert gap == {}
    overlap = _archive_local_utc_candidates(datetime(2025, 11, 2, 1, 30), "America/New_York")
    assert overlap[0].isoformat() == "2025-11-02T05:30:00+00:00"
    assert overlap[1].isoformat() == "2025-11-02T06:30:00+00:00"

    for fold, utc in ((0, "2025-11-02T05:30:00Z"), (1, "2025-11-02T06:30:00Z")):
        manifest = archive_manifest()
        manifest["app_timezone"] = "America/New_York"
        file = manifest["files"][0]
        file["provenance"]["capture_app_local"] = "2025-11-02T01:30:00"
        file["provenance"]["capture_utc"] = utc
        file["provenance"]["app_local_fold"] = fold
        with pytest.raises(Exception, match="ambiguous"):
            _validate_archive_manifest(manifest, 7, 11, 19, 1)

    manifest = archive_manifest()
    manifest["files"][0]["provenance"]["capture_precision_seconds"] = 86401
    with pytest.raises(Exception, match="precision"):
        _validate_archive_manifest(manifest, 7, 11, 19, 1)


def test_archive_upload_hash_and_size_are_bound_to_manifest():
    entry = {"ready_sha256": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", "size_bytes": 5}
    assert _archive_file_bytes_match(entry, b"hello")
    assert not _archive_file_bytes_match(entry, b"other")
    assert not _archive_file_bytes_match({**entry, "size_bytes": 6}, b"hello")


def test_archive_request_fingerprint_is_stable_and_pins_target():
    request = CreateBulkUploadRequest(
        folder_name="archive", camera_id=11, deployment_id=19,
        client_batch_id="archive-batch-rev-target", archive_manifest=archive_manifest(),
        total_files=1, total_bytes=123, time_offset_seconds=0,
    )
    fingerprint = _archive_request_fingerprint(7, request, request.archive_manifest)
    assert fingerprint == _archive_request_fingerprint(7, request, request.archive_manifest)
    changed_generated_at = {**request.archive_manifest, "generated_at_utc": "2026-10-11T00:00:00Z"}
    assert fingerprint == _archive_request_fingerprint(7, request, changed_generated_at)
    assert fingerprint != _archive_request_fingerprint(8, request, request.archive_manifest)
    _require_idempotent_payload(fingerprint, fingerprint)
    with pytest.raises(Exception, match="different archive request"):
        _require_idempotent_payload(fingerprint, "0" * 64)


def test_archive_routes_require_project_admin_dependency():
    paths = {
        "/archive-timezone", "/jobs", "/jobs/by-client-batch/{client_batch_id}",
        "/jobs/{job_uuid}/receipts",
    }
    routes = [route for route in bulk_upload_router.router.routes if route.path.endswith(tuple(paths))]
    assert {route.path.rsplit("/bulk-upload", 1)[-1] for route in routes} == paths
    for route in routes:
        assert any(dep.call is require_project_admin_access for dep in route.dependant.dependencies)


def test_receipts_join_upload_ledger_to_current_pipeline_state():
    job = SimpleNamespace(
        total_files=2,
        manifest={"upload_ledger": {
            "0": {"accepted": True, "outcome": "queued", "image_uuid": "image-a"},
            "1": {"accepted": True, "outcome": "duplicate", "existing_uuid": "image-b"},
        }},
    )
    receipts = _bulk_receipt_rows(job, {
        "image-a": {"pipeline_status": "classified", "pipeline_error": None},
    })
    assert receipts[0].model_dump() == {
        "index": 0, "accepted": True, "outcome": "queued", "image_uuid": "image-a",
        "existing_uuid": None, "pipeline_status": "classified", "reason": None,
        "pipeline_error": None,
    }
    assert receipts[1].index == 1 and receipts[1].existing_uuid == "image-b"
    assert receipts[1].pipeline_status is None


@pytest.mark.asyncio
async def test_idempotent_replay_response_uses_current_pipeline_counts(monkeypatch):
    job = SimpleNamespace(
        id=42, uuid="job-42", project_id=7, camera_id=11, deployment_id=19,
        client_batch_id="batch", archive_manifest=None, original_filename="folder",
        status="processing", total_files=2, manifest={"upload_ledger": {
            "0": {"outcome": "queued", "accepted": True, "image_uuid": "img-a"},
            "1": {"outcome": "queued", "accepted": True, "image_uuid": "img-b"},
        }}, error_message=None, time_offset_seconds=0, started_at=None,
        process_started_at=None, finished_at=None, created_at=datetime.now(timezone.utc),
    )

    async def done(_db, ids):
        assert ids == [42]
        return {42: 1}

    async def failed(_db, ids):
        assert ids == [42]
        return {42: 1}

    async def finalize(_db, jobs, _done, _failed):
        assert jobs == [job]

    class FakeDb:
        async def refresh(self, refreshed):
            assert refreshed is job

    monkeypatch.setattr(bulk_upload_router, "_pipeline_done_counts", done)
    monkeypatch.setattr(bulk_upload_router, "_pipeline_failed_counts", failed)
    monkeypatch.setattr(bulk_upload_router, "_finalise_done_jobs", finalize)
    result = await bulk_upload_router._response_with_live_counts(FakeDb(), job, "cam", "admin")
    assert result.classified_files == 1
    assert result.failed_files == 1
