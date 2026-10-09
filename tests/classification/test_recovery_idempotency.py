"""A classifier redelivery after commit must not add the same result twice."""
from contextlib import contextmanager
from types import SimpleNamespace

from shared.models import Classification as ClassificationModel, Image


class Query:
    def __init__(self, rows):
        self.rows = rows

    def filter(self, *_args):
        return self

    def with_for_update(self):
        return self

    def all(self):
        return self.rows

    def first(self):
        return self.rows[0] if self.rows else None


class Session:
    def __init__(self, image, existing_detection_ids):
        self.image = image
        self.existing_detection_ids = existing_detection_ids
        self.added = []
        self.commits = 0

    def query(self, entity):
        if entity is Image:
            return Query([self.image])
        if entity is ClassificationModel.detection_id:
            return Query([(id_,) for id_ in self.existing_detection_ids])
        raise AssertionError(f"unexpected query: {entity}")

    def add(self, row):
        self.added.append(row)

    def flush(self):
        raise AssertionError("idempotent recovery must not insert a duplicate classification")

    def commit(self):
        self.commits += 1


def test_classification_db_commit_before_publish_reuses_existing_row(monkeypatch):
    import db_operations

    image = SimpleNamespace(uuid="image-uuid", pipeline_claim_id="claim-2", status="classifying")
    session = Session(image, [88])

    @contextmanager
    def fake_session():
        yield session

    monkeypatch.setattr(db_operations, "get_db_session", fake_session)
    result = db_operations.insert_classifications(
        [SimpleNamespace(detection_id=88, species="Panthera leo", confidence=0.9)],
        "image-uuid",
        "claim-2",
    )

    assert result == []
    assert session.added == []
    assert session.commits == 1
