"""The real bulk-worker heartbeat helper stays tied to completed work."""
import ast
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace


WORKER_PATH = Path(__file__).resolve().parents[2] / "services" / "bulk-upload" / "worker.py"


class _Logger:
    def __init__(self):
        self.warnings = []

    def warning(self, message):
        self.warnings.append(message)


def _load_progress_helpers(*, client_factory=None, monotonic=None):
    """Compile the production helper alone to avoid importing GPU/service deps."""
    tree = ast.parse(WORKER_PATH.read_text())
    functions = {
        node.name: node for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name in ("_new_progress_redis", "_heartbeat_progress")
    }
    logger = _Logger()
    calls = {}

    def factory(url, **kwargs):
        calls["url"] = url
        calls.update(kwargs)
        return client_factory() if client_factory else _Queue()

    fake_redis = SimpleNamespace(Redis=SimpleNamespace(from_url=factory))
    fake_time = SimpleNamespace(monotonic=monotonic or __import__("time").monotonic)
    namespace = {
        "redis": fake_redis,
        "Retry": lambda backoff, retries: (backoff, retries),
        "NoBackoff": lambda: "no-backoff",
        "get_settings": lambda: SimpleNamespace(redis_url="redis://fixture:6379/0"),
        "time": fake_time,
        "datetime": datetime,
        "timezone": timezone,
        "PROGRESS_HEARTBEAT_RETRY_SECONDS": 5,
        "HEARTBEAT_KEY_BULK_UPLOAD": "heartbeat:bulk-upload",
        "logger": logger,
    }
    isolated = ast.Module(body=list(functions.values()), type_ignores=[])
    exec(compile(isolated, str(WORKER_PATH), "exec"), namespace)
    return namespace["_new_progress_redis"], namespace["_heartbeat_progress"], logger, calls


class _Queue:
    def __init__(self, fail=False):
        self.calls = []
        self.fail = fail

    def stamp_heartbeat(self, key):
        self.calls.append(key)
        if self.fail:
            raise TimeoutError("private redis connection detail")

    def set(self, key, value):
        self.stamp_heartbeat(key)
        self.values = getattr(self, "values", [])
        self.values.append((key, value))

    def close(self):
        self.closed = True


def test_heartbeat_tracks_completed_files_only():
    client = _Queue()
    _factory, heartbeat_progress, _logger, _calls = _load_progress_helpers(client_factory=lambda: client)
    progress = heartbeat_progress(["first", "second"])

    assert next(progress) == "first"
    assert client.calls == []  # A stalled current file must become stale.
    assert next(progress) == "second"
    assert client.calls == ["heartbeat:bulk-upload"]
    try:
        next(progress)
    except StopIteration:
        pass
    assert client.calls == ["heartbeat:bulk-upload", "heartbeat:bulk-upload"]
    assert client.closed is True


def test_stalled_current_item_does_not_get_a_timer_heartbeat():
    created = []
    _factory, heartbeat_progress, _logger, _calls = _load_progress_helpers(
        client_factory=lambda: created.append(_Queue()) or created[-1]
    )
    progress = heartbeat_progress(["slow-file"])
    assert next(progress) == "slow-file"
    # The caller is doing the work while the generator is suspended here.
    assert created == []
    progress.close()
    assert created == []


def test_redis_heartbeat_failure_does_not_fail_bulk_processing_or_leak_error():
    client = _Queue(fail=True)
    _factory, heartbeat_progress, logger, _calls = _load_progress_helpers(client_factory=lambda: client)
    completed = list(heartbeat_progress(["one", "two", "three"]))
    assert completed == ["one", "two", "three"]
    assert client.calls == ["heartbeat:bulk-upload"]
    assert client.closed is True
    assert logger.warnings == ["Bulk worker progress heartbeat could not be written"]
    assert "private" not in " ".join(logger.warnings)


def test_progress_redis_client_has_short_timeouts_and_zero_retries():
    from_url, _progress, _logger, calls = _load_progress_helpers()
    client = from_url()
    assert client is not None
    assert calls["url"] == "redis://fixture:6379/0"
    assert calls["decode_responses"] is True
    assert calls["socket_connect_timeout"] == 0.25
    assert calls["socket_timeout"] == 0.5
    assert calls["retry"] == ("no-backoff", 0)
    assert calls["retry_on_timeout"] is False
