"""Contract tests for production bulk upload outcome and API helpers."""

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

API_PATH = Path(__file__).resolve().parents[2] / "services" / "api"
if str(API_PATH) not in sys.path:
    sys.path.insert(0, str(API_PATH))

from routers import bulk_upload as bulk_upload_router  # noqa: E402
from routers.bulk_upload import _finalise_done_jobs  # noqa: E402

from shared.bulk_outcomes import merge_ledger_entry, summarize_ledger, terminal_status


class _FakeSession:
    def __init__(self, job, classified=None, failed=None):
        self.updates = []
        self.commits = 0
        self.job = job
        self.count_results = [classified or [], failed or []]

    async def execute(self, statement):
        self.updates.append(statement)
        if len(self.updates) == 1:
            return _FakeResult([self.job])
        if len(self.updates) <= 3:
            return _FakeResult(self.count_results.pop(0))
        return _FakeResult([])

    async def commit(self):
        self.commits += 1


class _FakeResult:
    def __init__(self, rows):
        self.rows = rows

    def scalars(self):
        return self

    def all(self):
        return self.rows

    def scalar_one_or_none(self):
        return self.rows[0] if self.rows else None


class _RetrySession:
    def __init__(self, job, images, detections):
        self.job = job
        self.images = images
        self.detections = detections
        self.select_count = 0
        self.commits = 0

    async def execute(self, statement):
        self.select_count += 1
        if self.select_count == 1:
            return _FakeResult([self.job])
        if self.select_count == 2:
            return _FakeResult(self.images)
        return _FakeResult(self.detections)

    async def commit(self):
        self.commits += 1


def _job(total, ledger):
    return SimpleNamespace(
        id=1, status="processing", total_files=total,
        manifest={"upload_ledger": ledger}, finished_at=None,
    )


@pytest.mark.asyncio
async def test_api_finalizer_keeps_missing_expected_files_pending():
    job = _job(2, {"0": {"outcome": "duplicate", "accepted": True}})
    db = _FakeSession(job)

    await _finalise_done_jobs(db, [job], {}, {})

    assert job.status == "processing"
    assert db.commits == 0


@pytest.mark.asyncio
async def test_api_finalizer_accepts_all_duplicates_as_done():
    job = _job(2, {
        "0": {"outcome": "duplicate", "accepted": True},
        "1": {"outcome": "duplicate", "accepted": True},
    })
    db = _FakeSession(job)

    await _finalise_done_jobs(db, [job], {}, {})

    assert job.status == "done"
    assert db.commits == 1


@pytest.mark.asyncio
async def test_api_finalizer_marks_mixed_classified_and_upload_failure_partial():
    job = _job(2, {
        "0": {"outcome": "queued", "accepted": True, "image_uuid": "image-1"},
        "1": {"outcome": "failed_upload", "reason": "request_too_large"},
    })
    db = _FakeSession(job, classified=[(1, 1)])

    await _finalise_done_jobs(db, [job], {1: 1}, {})

    assert job.status == "partial"
    assert db.commits == 1


@pytest.mark.asyncio
async def test_api_finalizer_marks_zero_accepted_failure_failed():
    job = _job(2, {
        "0": {"outcome": "failed_upload"},
        "1": {"outcome": "skipped", "accepted": True},
    })
    db = _FakeSession(job)

    await _finalise_done_jobs(db, [job], {}, {})

    assert job.status == "failed"


@pytest.mark.asyncio
async def test_retry_endpoint_publishes_stage_specific_durable_payloads(monkeypatch):
    job = SimpleNamespace(id=7, status="partial", finished_at=object(), error_message="old")
    detection_retry = SimpleNamespace(
        id=11, uuid="image-detect", origin="bulk", pipeline_failed_stage="detection",
        status="failed", pipeline_updated_at=None, pipeline_attempts=3,
        pipeline_error="failed", pipeline_claim_id="old-claim", storage_path="raw/a.jpg",
        camera_id=4, is_verified=True,
    )
    classification_retry = SimpleNamespace(
        id=12, uuid="image-classify", origin="live", pipeline_failed_stage="classification",
        status="failed", pipeline_updated_at=None, pipeline_attempts=2,
        pipeline_error="failed", pipeline_claim_id="old-claim", storage_path="raw/b.jpg",
        camera_id=5, is_verified=False,
    )
    untyped_failure = SimpleNamespace(
        id=13, uuid="image-untyped", origin="bulk", pipeline_failed_stage=None,
        status="failed", pipeline_updated_at=None, pipeline_attempts=1,
        pipeline_error="failed", pipeline_claim_id=None, storage_path="raw/c.jpg",
        camera_id=6, is_verified=False,
    )
    db = _RetrySession(job, [detection_retry, classification_retry, untyped_failure], [(21,), (22,)])
    published = []

    class Queue:
        def __init__(self, name):
            self.name = name

        def publish(self, message):
            published.append((self.name, message))

    monkeypatch.setattr(bulk_upload_router, "RedisQueue", Queue)
    result = await bulk_upload_router.retry_failed_bulk_images(
        project_id=2,
        job_uuid="job-uuid",
        user=SimpleNamespace(email="admin@example.invalid"),
        db=db,
    )

    assert result == {
        "job_uuid": "job-uuid", "status": "processing",
        "retried_files": 2, "unretryable_files": 1,
    }
    assert detection_retry.status == "pending"
    assert classification_retry.status == "detected"
    assert detection_retry.pipeline_attempts == classification_retry.pipeline_attempts == 0
    assert detection_retry.pipeline_claim_id is None
    assert detection_retry.is_verified is True
    assert job.status == "processing"
    assert published == [
        ("image-ingested-bulk", {
            "image_uuid": "image-detect", "storage_path": "raw/a.jpg",
            "camera_id": 4, "origin": "bulk",
        }),
        ("detection-complete", {
            "image_uuid": "image-classify", "num_detections": 2,
            "detection_ids": [21, 22], "origin": "live",
        }),
    ]


def test_expected_count_stays_the_denominator_when_some_uploads_fail():
    manifest = {}
    manifest, _ = merge_ledger_entry(
        manifest, 0,
        {"filename": "a.jpg", "outcome": "uploaded", "accepted": True},
        protect_accepted=False,
    )
    manifest, _ = merge_ledger_entry(
        manifest, 1,
        {"filename": "b.jpg", "outcome": "failed_upload", "reason": "request_too_large"},
    )

    counts = summarize_ledger(manifest, total_files=3)

    assert counts["uploaded_files"] == 1
    assert counts["failed_files"] == 1
    assert counts["pending_files"] == 2
    assert counts["missing_indexes"] == [2]
    assert terminal_status(counts) is None


def test_client_failure_cannot_overwrite_a_server_accepted_file():
    manifest, _ = merge_ledger_entry(
        {}, 4,
        {"filename": "frame.jpg", "outcome": "uploaded", "accepted": True, "size": 99},
        protect_accepted=False,
    )
    updated, changed = merge_ledger_entry(
        manifest, 4,
        {"filename": "frame.jpg", "outcome": "failed_upload", "reason": "request_too_large"},
    )

    assert changed is False
    assert updated["upload_ledger"]["4"]["outcome"] == "uploaded"
    assert updated["upload_ledger"]["4"]["size"] == 99


def test_all_duplicate_batch_is_a_legitimate_success():
    manifest = {"upload_ledger": {
        "0": {"outcome": "duplicate", "accepted": True},
        "1": {"outcome": "duplicate", "accepted": True},
    }}

    counts = summarize_ledger(manifest, total_files=2)

    assert counts["duplicate_files"] == 2
    assert counts["pending_files"] == 0
    assert terminal_status(counts) == "done"


def test_failed_or_skipped_only_batch_is_not_green():
    failed = {"upload_ledger": {"0": {"outcome": "failed_upload"}}}
    skipped = {"upload_ledger": {"0": {"outcome": "skipped"}}}

    assert terminal_status(summarize_ledger(failed, 1)) == "failed"
    assert terminal_status(summarize_ledger(skipped, 1)) == "failed"


def test_mixed_classified_and_failed_batch_is_partial():
    manifest = {"upload_ledger": {
        "0": {"outcome": "classified", "accepted": True},
        "1": {"outcome": "failed", "accepted": True},
    }}

    counts = summarize_ledger(manifest, 2)

    assert counts["classified_files"] == 1
    assert counts["failed_files"] == 1
    assert terminal_status(counts) == "partial"


@pytest.mark.asyncio
@pytest.mark.parametrize("image_state,error,stage", [
    ("pending", None, None),
    ("classified", None, None),
    ("failed", "Detection failed; use Retry.", "detection"),
])
async def test_csv_stream_reports_actual_pipeline_state(image_state, error, stage):
    import csv
    import io
    job = SimpleNamespace(id=7, manifest={"upload_ledger": {"0": {
        "filename": "fixture.jpg", "outcome": "queued", "image_uuid": "fixture-image",
    }}})

    class Session:
        def __init__(self):
            self.selects = 0

        async def execute(self, statement):
            self.selects += 1
            return _FakeResult([job] if self.selects == 1 else [
                ("fixture-image", image_state, error, stage),
            ])

    response = await bulk_upload_router.get_bulk_upload_log(
        project_id=2, job_uuid="fixture-job", user=SimpleNamespace(), db=Session(),
    )
    chunks = [chunk async for chunk in response.body_iterator]
    rows = list(csv.DictReader(io.StringIO("".join(chunks))))
    assert len(rows) == 1
    assert rows[0]["outcome"] == image_state
    assert rows[0]["pipeline_error"] == (error or "")
    assert rows[0]["pipeline_failed_stage"] == (stage or "")


@pytest.mark.asyncio
@pytest.mark.parametrize("accepted,staged_name,same_content,should_succeed", [
    (True, "fixture.jpg", True, True),
    (True, "changed.jpg", True, False),
    (True, "fixture.jpg", False, False),
    (False, "fixture.jpg", True, True),
    (False, "changed.jpg", True, False),
    (False, "fixture.jpg", False, False),
])
async def test_upload_index_is_immutable_across_acceptance_and_storage_commit_gap(
    monkeypatch, accepted, staged_name, same_content, should_succeed,
):
    import hashlib
    import io
    from fastapi import HTTPException, UploadFile
    from starlette.datastructures import Headers
    body = b"fixture-image-content"
    prior = {"accepted": True, "filename": staged_name,
             "sha256": hashlib.sha256(body if same_content else b"old").hexdigest()}
    job = SimpleNamespace(status="uploading", total_files=1,
                          manifest={"upload_ledger": {"0": prior}} if accepted else {})
    writes = []

    class Session:
        async def execute(self, statement):
            return _FakeResult([job])

        async def commit(self):
            pass

    class Storage:
        def list_objects(self, bucket, prefix):
            return [bulk_upload_router._staging_prefix(2, "fixture-job") + "000000_" + staged_name]

        def download_fileobj(self, bucket, key):
            return body if same_content else b"old"

        def upload_fileobj(self, *args):
            writes.append(args)

    monkeypatch.setattr(bulk_upload_router, "StorageClient", Storage)
    file = UploadFile(io.BytesIO(body), filename="fixture.jpg", headers=Headers({"content-type": "image/jpeg"}))
    if should_succeed:
        result = await bulk_upload_router._upload_bulk_file_inner(2, "fixture-job", 0, file, SimpleNamespace(), Session())
        assert result["size"] == len(body)
    else:
        with pytest.raises(HTTPException) as exc:
            await bulk_upload_router._upload_bulk_file_inner(2, "fixture-job", 0, file, SimpleNamespace(), Session())
        assert exc.value.status_code == 409
    assert not writes


@pytest.mark.asyncio
async def test_finalize_rejects_duplicate_staged_objects_for_one_expected_index(monkeypatch):
    from fastapi import HTTPException

    prefix = bulk_upload_router._staging_prefix(2, "fixture-job")
    original_manifest = {"folder_name": "original-card"}
    job = SimpleNamespace(
        id=7, uuid="fixture-job", status="uploading", camera_id=4,
        total_files=1, manifest=original_manifest, staged_object_key=prefix,
    )

    class Session:
        def __init__(self):
            self.commits = 0

        async def execute(self, statement):
            return _FakeResult([job])

        async def commit(self):
            self.commits += 1

    class Paginator:
        def paginate(self, **kwargs):
            return [{"Contents": [
                {"Key": f"{prefix}000000_a.jpg"},
                {"Key": f"{prefix}000000_b.jpg"},
            ]}]

    class Storage:
        def __init__(self):
            self.client = SimpleNamespace(get_paginator=lambda name: Paginator())

    db = Session()
    monkeypatch.setattr(bulk_upload_router, "StorageClient", Storage)
    with pytest.raises(HTTPException) as exc:
        await bulk_upload_router.finalize_bulk_upload(
            project_id=2, job_uuid="fixture-job",
            user=SimpleNamespace(email="admin@example.invalid"), db=db,
        )

    assert exc.value.status_code == 409
    assert "index(es) 0" in exc.value.detail
    assert "No staged files were removed" in exc.value.detail
    assert job.status == "uploading"
    assert job.manifest == original_manifest
    assert db.commits == 0


@pytest.mark.asyncio
async def test_finalize_backfill_preserves_known_accepted_ledger_entry(monkeypatch):
    from fastapi import HTTPException

    prefix = bulk_upload_router._staging_prefix(2, "fixture-job")
    accepted = {
        "filename": "original.jpg", "outcome": "duplicate", "accepted": True,
        "reason": "content_hash_duplicate", "sha256": "original-hash",
    }
    job = SimpleNamespace(
        id=7, uuid="fixture-job", status="uploading", camera_id=4,
        total_files=2, manifest={"upload_ledger": {"0": accepted}},
        staged_object_key=prefix,
    )

    class Session:
        async def execute(self, statement):
            return _FakeResult([job])

        async def commit(self):
            pytest.fail("incomplete jobs must not commit during finalize backfill")

    class Paginator:
        def paginate(self, **kwargs):
            return [{"Contents": [{"Key": f"{prefix}000000_original.jpg"}]}]

    class Storage:
        def __init__(self):
            self.client = SimpleNamespace(get_paginator=lambda name: Paginator())

    monkeypatch.setattr(bulk_upload_router, "StorageClient", Storage)
    with pytest.raises(HTTPException) as exc:
        await bulk_upload_router.finalize_bulk_upload(
            project_id=2, job_uuid="fixture-job",
            user=SimpleNamespace(email="admin@example.invalid"), db=Session(),
        )

    assert exc.value.status_code == 409
    assert exc.value.detail["missing_indexes"] == [1]
    assert job.manifest["upload_ledger"]["0"] == accepted
