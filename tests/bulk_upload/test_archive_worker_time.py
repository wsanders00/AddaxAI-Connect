"""Exercise the production archive/EXIF gate without importing worker services."""
import ast
from datetime import datetime, timezone
import math
from pathlib import Path
import re
from zoneinfo import ZoneInfo


WORKER_PATH = Path(__file__).resolve().parents[2] / "services" / "bulk-upload" / "worker.py"


def _production_validator():
    tree = ast.parse(WORKER_PATH.read_text())
    function = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_validate_archive_exif_time"
    )

    def parse_original(exif, _path, _offset):
        # This is the value returned by the real parser after parsing the
        # staged file's DateTimeOriginal. The test isolates the archive gate.
        return datetime.fromisoformat(exif["DateTimeOriginal"])

    namespace = {
        "datetime": datetime,
        "timezone": timezone,
        "math": math,
        "re": re,
        "ZoneInfo": ZoneInfo,
        "get_corrected_datetime": parse_original,
    }
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(WORKER_PATH), "exec"), namespace)
    return namespace["_validate_archive_exif_time"]


def test_worker_rejects_mismatched_exif_wall_time_and_offset():
    validate = _production_validator()
    entry = {
        "app_timezone": "America/Los_Angeles",
        "provenance": {
            "capture_app_local": "2026-07-10T12:00:00",
            "capture_precision_seconds": 1,
            "app_local_fold": None,
        },
    }
    good_exif = {"DateTimeOriginal": "2026-07-10T12:00:00", "OffsetTimeOriginal": "-07:00"}
    assert validate(good_exif, "unused.jpg", entry) == datetime(2026, 7, 10, 12, 0)
    try:
        validate({**good_exif, "DateTimeOriginal": "2026-07-10T12:01:00"}, "unused.jpg", entry)
    except ValueError as exc:
        assert "DateTimeOriginal" in str(exc)
    else:
        raise AssertionError("mismatched DateTimeOriginal was accepted")
    try:
        validate({**good_exif, "OffsetTimeOriginal": "-08:00"}, "unused.jpg", entry)
    except ValueError as exc:
        assert "OffsetTimeOriginal" in str(exc)
    else:
        raise AssertionError("mismatched EXIF offset was accepted")


def test_worker_rejects_all_ambiguous_wall_times_even_with_fold():
    validate = _production_validator()
    for fold, offset in ((0, "-04:00"), (1, "-05:00")):
        entry = {
            "app_timezone": "America/New_York",
            "provenance": {
                "capture_app_local": "2025-11-02T01:30:00",
                "capture_precision_seconds": 1,
                "app_local_fold": fold,
            },
        }
        try:
            validate({"DateTimeOriginal": "2025-11-02T01:30:00", "OffsetTimeOriginal": offset}, "unused.jpg", entry)
        except ValueError as exc:
            assert "ambiguous" in str(exc)
        else:
            raise AssertionError("ambiguous wall time was accepted")


def test_worker_precision_cannot_widen_exif_timestamp_check():
    validate = _production_validator()
    entry = {"app_timezone": "UTC", "provenance": {
        "capture_app_local": "2026-07-10T12:00:00", "capture_precision_seconds": 86401,
        "app_local_fold": None}}
    try:
        validate({"DateTimeOriginal": "2026-07-10T12:00:00"}, "unused.jpg", entry)
    except ValueError as exc:
        assert "precision" in str(exc)
    else:
        raise AssertionError("unbounded precision was accepted")
    entry["provenance"]["capture_precision_seconds"] = 86400
    try:
        validate({"DateTimeOriginal": "2026-07-10T12:00:02"}, "unused.jpg", entry)
    except ValueError as exc:
        assert "DateTimeOriginal" in str(exc)
    else:
        raise AssertionError("precision widened EXIF timestamp tolerance")
