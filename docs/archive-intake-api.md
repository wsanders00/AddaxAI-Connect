# Archive intake API contract

Archive clients use the existing project bulk-upload endpoints. The new mode
is selected by `camera_id` and `deployment_id` together; `device_id` and
`site_id` must be absent. Existing profile and manual-site modes are unchanged.

Before preparing timestamps, a project administrator reads
`GET /api/projects/{project_id}/bulk-upload/archive-timezone`. It returns the
current `timezone` and `app_timezone_fingerprint`, which is SHA-256 over the
UTF-8 encoding of compact, key-sorted JSON `{"timezone":"<name>"}`. A timezone
change between preparation and job creation or processing rejects the job.

Create one job per target group with `POST
/api/projects/{project_id}/bulk-upload/jobs`. Include the ordinary
`folder_name`, `total_files`, and `total_bytes` fields, plus
`time_offset_seconds: 0`, `camera_id`, `deployment_id`, a deterministic
`client_batch_id` of at most 200 characters, and the target subset as
`archive_manifest`. The manifest is the `addax-archive-ready-v1` object
produced by the local archive preparer. Subset `files[].index` values must be
reindexed contiguously from zero; retain the original source index separately
as `provenance.intake_index` when needed.

Every entry must pin the same `{project_id, camera_id, deployment_id}` target,
contain bounded provenance and SHA-256 values, and place its ready-file byte
count and hash in `size_bytes` and `ready_sha256`. The API checks target
ownership, deployment date bounds, timezone fingerprint, count, total bytes,
and server-owned field exclusions. The existing
`POST /jobs/{job_uuid}/files?index={index}` endpoint checks each uploaded file
against the ready hash and size. The worker rechecks staged bytes, target,
deployment dates, and timezone immediately before ingestion. Archive capture
wall time is `provenance.capture_app_local`; `capture_utc` must agree with it
under the pinned application timezone. Ambiguous and nonexistent destination
wall times are held, including overlaps with an explicitly supplied fold.
Nonzero job clock offsets are rejected.

Use the existing `/jobs/{job_uuid}/finalize` endpoint after upload. If the
create response is lost, repeat the same create request with the same
`client_batch_id`; an identical request returns the existing job, while a
different request with that key returns HTTP 409. Authorized recovery lookup
is `GET /api/projects/{project_id}/bulk-upload/jobs/by-client-batch/{client_batch_id}`.
The standard job response includes a compact archive summary and the existing
`/jobs/{job_uuid}/uploaded-indexes` supports staging reconciliation, and
`GET /jobs/{job_uuid}/receipts` returns bounded per-index `accepted`, `outcome`,
`image_uuid`, `existing_uuid`, `pipeline_status`, `reason`, and `pipeline_error`
fields for durable reconciliation.
Processed image detail responses expose the complete per-image archive record
at `image_metadata.archive_import`. Images retain `origin: bulk` and the
application's ordinary unverified default.
