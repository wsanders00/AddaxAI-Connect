"""Regression coverage for the bounded image recovery policy."""
import pytest
from contextlib import contextmanager
from types import SimpleNamespace

from shared import pipeline_recovery
from shared.pipeline_recovery import MAX_STAGE_ATTEMPTS, stale_recovery_action


@pytest.mark.parametrize(
    ("stage", "active_status", "retry_status"),
    [
        ("detection", "pending", "pending"),
        ("detection", "processing", "pending"),
        ("classification", "detected", "detected"),
        ("classification", "classifying", "detected"),
    ],
)
def test_stale_lease_recovery_returns_to_safe_stage(stage, active_status, retry_status):
    assert stale_recovery_action(stage, active_status, MAX_STAGE_ATTEMPTS - 1) == (retry_status, None)


@pytest.mark.parametrize(
    ("stage", "active_status"),
    [("detection", "processing"), ("classification", "classifying")],
)
def test_stale_lease_at_attempt_cap_becomes_actionable_failure(stage, active_status):
    status, reason = stale_recovery_action(stage, active_status, MAX_STAGE_ATTEMPTS)
    assert status == "failed"
    assert stage.title() in reason
    assert "Retry" in reason


def test_recovery_refuses_status_from_another_pipeline_stage():
    with pytest.raises(ValueError, match="Invalid stale detection stage status"):
        stale_recovery_action("detection", "classified", 0)


@pytest.mark.parametrize(("rowcount", "claimed"), [(1, True), (0, False)])
def test_stage_claim_only_returns_token_when_atomic_status_update_wins(monkeypatch, rowcount, claimed):
    class FakeDB:
        def execute(self, statement):
            # The generated UPDATE carries the compare-and-set predicates.
            assert "images.status" in str(statement)
            assert "images.uuid" in str(statement)
            return SimpleNamespace(rowcount=rowcount)

        def commit(self):
            pass

    @contextmanager
    def fake_session():
        yield FakeDB()

    monkeypatch.setattr(pipeline_recovery, "get_db_session", fake_session)
    token = pipeline_recovery.claim_image_stage("image-uuid", "pending", "processing")
    assert bool(token) is claimed
    if token:
        assert len(token) == 36


class RecoveryQuery:
    def __init__(self, rows):
        self.rows = rows

    def filter(self, *_args):
        return self

    def order_by(self, *_args):
        return self

    def with_for_update(self, **_kwargs):
        return self

    def limit(self, _count):
        return self

    def all(self):
        return self.rows


def test_reconcile_stale_classifying_lease_commits_before_republish(monkeypatch):
    row = SimpleNamespace(
        status="classifying", pipeline_attempts=1, pipeline_error=None,
        pipeline_failed_stage=None, pipeline_claim_id="old-claim",
        pipeline_updated_at=None, uuid="image-uuid", storage_path="raw/path.jpg",
        camera_id=4, origin="bulk", detections=[SimpleNamespace(id=99)],
    )

    class FakeDB:
        committed = False

        def query(self, _model):
            return RecoveryQuery([row])

        def commit(self):
            self.committed = True

    db = FakeDB()

    @contextmanager
    def fake_session():
        yield db

    published = []

    class FakeQueue:
        def __init__(self, queue_name):
            self.queue_name = queue_name

        def publish(self, message):
            assert db.committed, "durable recovery state must commit before queue publish"
            published.append((self.queue_name, message))

    monkeypatch.setattr(pipeline_recovery, "get_db_session", fake_session)
    monkeypatch.setattr(pipeline_recovery, "RedisQueue", FakeQueue)

    assert pipeline_recovery.reconcile_stale_images("classification") == 1
    assert row.status == "detected"
    assert row.pipeline_claim_id is None
    assert row.pipeline_failed_stage is None
    assert published == [
        ("detection-complete-bulk", {
            "image_uuid": "image-uuid", "num_detections": 1,
            "detection_ids": [99], "origin": "bulk",
        })
    ]


def test_reconcile_at_attempt_cap_marks_failed_without_requeue(monkeypatch):
    row = SimpleNamespace(
        status="processing", pipeline_attempts=MAX_STAGE_ATTEMPTS,
        pipeline_error=None, pipeline_failed_stage=None, pipeline_claim_id="old-claim",
        pipeline_updated_at=None, uuid="image-uuid", storage_path="raw/path.jpg",
        camera_id=4, origin="live", detections=[],
    )

    class FakeDB:
        def query(self, _model):
            return RecoveryQuery([row])

        def commit(self):
            pass

    @contextmanager
    def fake_session():
        yield FakeDB()

    monkeypatch.setattr(pipeline_recovery, "get_db_session", fake_session)
    monkeypatch.setattr(pipeline_recovery, "RedisQueue", lambda _name: pytest.fail("must not requeue poison work"))

    assert pipeline_recovery.reconcile_stale_images("detection") == 0
    assert row.status == "failed"
    assert row.pipeline_failed_stage == "detection"
    assert "Retry" in row.pipeline_error
