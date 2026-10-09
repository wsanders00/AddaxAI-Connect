"""Tests for ingestion validators (MIME type and file size checks)."""
import os
import pytest
import tempfile
from utils import ValidationError
from validators import validate_mime_type, validate_file_size, validate_image


class TestValidateMimeType:
    def test_valid_jpeg(self, tmp_path):
        f = tmp_path / "test.jpg"
        f.write_bytes(b"\xff\xd8\xff\xe0" + b"\x00" * 100)
        validate_mime_type(str(f))  # should not raise

    def test_invalid_magic_bytes(self, tmp_path):
        f = tmp_path / "test.png"
        f.write_bytes(b"\x89PNG" + b"\x00" * 100)
        with pytest.raises(ValidationError, match="Invalid JPEG magic bytes"):
            validate_mime_type(str(f))

    def test_empty_file(self, tmp_path):
        f = tmp_path / "empty.jpg"
        f.write_bytes(b"")
        with pytest.raises(ValidationError):
            validate_mime_type(str(f))


class TestValidateFileSize:
    def test_within_limit(self, tmp_path):
        f = tmp_path / "small.jpg"
        f.write_bytes(b"\x00" * 1000)
        validate_file_size(str(f), max_mb=1)  # should not raise

    def test_exceeds_limit(self, tmp_path):
        f = tmp_path / "big.jpg"
        f.write_bytes(b"\x00" * (2 * 1024 * 1024))  # 2 MB
        with pytest.raises(ValidationError, match="File too large"):
            validate_file_size(str(f), max_mb=1)


def test_bulk_image_limit_matches_accepted_upload_sizes(tmp_path):
    f = tmp_path / "large.jpg"
    with f.open("wb") as stream:
        stream.write(b"\xff\xd8\xff")
        stream.truncate(50 * 1024 * 1024)
    validate_image(str(f), max_mb=50)
    with pytest.raises(ValidationError, match="10MB limit"):
        validate_image(str(f))
    with f.open("ab") as stream:
        stream.write(b"x")
    with pytest.raises(ValidationError, match="50MB limit"):
        validate_image(str(f), max_mb=50)
