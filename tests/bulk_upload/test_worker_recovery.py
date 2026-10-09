"""Focused contracts for durable bulk job claims and background finalization."""
from contextlib import contextmanager
import ast
import importlib.util
from pathlib import Path
import sys
import types
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
SERVICE_DIR = ROOT / "services" / "bulk-upload"
_original_path = list(sys.path)
sys.path.insert(0, str(SERVICE_DIR))

# The production image copies these ingestion modules into /ingestion_lib;
# the unit-test checkout keeps them under services/ingestion.
_saved_modules = {}
for name, symbols in {
    "db_operations": ("create_image_record", "get_or_create_bulk_deployment"),
    "exif_parser": ("extract_exif", "get_corrected_datetime"),
    "storage_operations": ("generate_and_upload_thumbnail", "upload_image_to_minio"),
    "validators": ("validate_image",),
    "utils": ("is_valid_gps",),
}.items():
    _saved_modules[name] = sys.modules.get(name)
    module = types.ModuleType(name)
    for symbol in symbols:
        setattr(module, symbol, lambda *args, **kwargs: None)
    sys.modules[name] = module

_spec = importlib.util.spec_from_file_location(
    "_addax_bulk_upload_worker_under_test", SERVICE_DIR / "worker.py"
)
bulk_worker = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = bulk_worker
_spec.loader.exec_module(bulk_worker)
for _name, _module in _saved_modules.items():
    if _module is None:
        sys.modules.pop(_name, None)
    else:
        sys.modules[_name] = _module
sys.path[:] = _original_path


@contextmanager
def fake_session(session):
    yield session


def test_bulk_process_claim_is_a_compare_and_set(monkeypatch):
    seen = []

    class Session:
        def execute(self, statement):
            seen.append(str(statement))
            return SimpleNamespace(rowcount=1)

    session = Session()
    monkeypatch.setattr(bulk_worker, "get_db_session", lambda: fake_session(session))
    claim_id = bulk_worker._claim_process_job("job-uuid")

    assert claim_id and len(claim_id) == 36
    sql = seen[0]
    assert "bulk_upload_jobs.status = :status_1" in sql
    assert "bulk_upload_jobs.pipeline_claim_id IS NULL" in sql
    assert "bulk_upload_jobs.staging_complete IS false" in sql


def test_all_production_entry_calls_forward_the_claim_token():
    tree = ast.parse((SERVICE_DIR / "worker.py").read_text())
    calls = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_process_zip_entry"
    ]
    assert len(calls) == 2
    for call in calls:
        keywords = {keyword.arg: keyword.value for keyword in call.keywords}
        assert isinstance(keywords.get("claim_id"), ast.Name)
        assert keywords["claim_id"].id == "claim_id"
        assert "source_index" in keywords


def test_periodic_finalizer_closes_classified_job_without_api_poll(monkeypatch):
    job = SimpleNamespace(
        id=9, uuid="job-uuid", status="processing", staging_complete=True,
        total_files=1, manifest={"upload_ledger": {"0": {
            "accepted": True, "outcome": "queued", "image_uuid": "image-uuid",
        }}}, finished_at=None,
    )

    class Result:
        def scalars(self):
            return self

        def all(self):
            return [job]

    class Session:
        committed = False
        calls = 0

        def execute(self, statement):
            self.calls += 1
            if self.calls == 1:
                return Result()
            return SimpleNamespace(all=lambda: [("classified", 1)])

        def commit(self):
            self.committed = True

    session = Session()
    monkeypatch.setattr(bulk_worker, "get_db_session", lambda: fake_session(session))

    assert bulk_worker._finalize_classified_jobs() == 1
    assert job.status == "done"
    assert job.finished_at is not None
    assert session.committed


def test_lost_claim_stops_prefix_before_download_or_staging_delete(monkeypatch):
    class Session:
        def execute(self, _statement):
            return SimpleNamespace(scalar_one_or_none=lambda: None)

    class Storage:
        downloaded = False
        deleted = False

        def download_fileobj(self, *_args):
            self.downloaded = True

        def delete_object(self, *_args):
            self.deleted = True

    storage = Storage()
    monkeypatch.setattr(bulk_worker, "get_db_session", lambda: fake_session(Session()))
    monkeypatch.setattr(bulk_worker, "StorageClient", lambda: storage)
    monkeypatch.setattr(bulk_worker, "RedisQueue", lambda *_args: object())
    monkeypatch.setattr(bulk_worker, "_list_prefix", lambda *_args: ["prefix/000001_frame.jpg"])

    try:
        bulk_worker._process_prefix_job(
            "job-uuid", 1, 2, "camera", None, "prefix/", claim_id="old-claim"
        )
    except bulk_worker._LostBulkClaim:
        pass
    else:
        raise AssertionError("lost claim must stop the process pass")

    assert not storage.downloaded
    assert not storage.deleted


def test_lost_claim_is_checked_before_image_asset_upload(monkeypatch):
    class Session:
        calls = 0

        def execute(self, _statement):
            self.calls += 1
            if self.calls == 1:
                return SimpleNamespace(first=lambda: None)
            return SimpleNamespace(scalar_one_or_none=lambda: None)

    uploaded = []
    session = Session()
    monkeypatch.setattr(bulk_worker, "get_db_session", lambda: fake_session(session))
    monkeypatch.setattr(bulk_worker, "validate_image", lambda _path: None)
    monkeypatch.setattr(bulk_worker, "extract_exif", lambda _path: {})
    monkeypatch.setattr(
        bulk_worker, "get_corrected_datetime", lambda *_args, **_kwargs: __import__("datetime").datetime.now()
    )
    monkeypatch.setattr(bulk_worker, "upload_image_to_minio", lambda *args: uploaded.append(args))
    monkeypatch.setattr(bulk_worker, "create_image_record", lambda **_kwargs: uploaded.append("image"))

    try:
        bulk_worker._process_zip_entry(
            "frame.jpg", b"image", 2, "camera", None, object(), 1,
            use_profile=False, source_index=0, claim_id="old-claim",
        )
    except bulk_worker._LostBulkClaim:
        pass
    else:
        raise AssertionError("image creation must require the active job claim")

    assert uploaded == []
