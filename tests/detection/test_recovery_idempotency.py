"""A redelivered detection stage reuses committed rows after a publish gap."""
from contextlib import contextmanager
from types import SimpleNamespace

from shared.models import Detection as DetectionModel, Image


class Query:
    def __init__(self, rows):
        self.rows = rows

    def filter(self, *_args):
        return self

    def with_for_update(self):
        return self

    def first(self):
        return self.rows[0] if self.rows else None

    def all(self):
        return self.rows


class Session:
    def __init__(self, image, detections):
        self.image = image
        self.detections = detections
        self.added = []
        self.commits = 0

    def query(self, model):
        return Query([self.image] if model is Image else self.detections)

    def add(self, row):
        self.added.append(row)

    def flush(self):
        raise AssertionError("idempotent recovery must not insert duplicate detections")

    def commit(self):
        self.commits += 1


def test_detection_db_commit_before_queue_publish_reuses_existing_rows(monkeypatch):
    from services.detection import db_operations

    image = SimpleNamespace(id=7, uuid="image-uuid", pipeline_claim_id="claim-1", status="processing")
    rows = [SimpleNamespace(id=41), SimpleNamespace(id=42)]
    session = Session(image, rows)

    @contextmanager
    def fake_session():
        yield session

    monkeypatch.setattr(db_operations, "get_db_session", fake_session)
    output = db_operations.insert_detections("image-uuid", [object()], "claim-1")

    assert output == [41, 42]
    assert session.added == []
    assert session.commits == 0
