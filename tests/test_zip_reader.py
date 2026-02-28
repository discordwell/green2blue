"""Tests for ZIP reader, including path traversal defense."""

from __future__ import annotations

import zipfile

import pytest

from green2blue.exceptions import InvalidZipError, MissingNDJSONError
from green2blue.parser.zip_reader import open_export_zip


class TestZipReader:
    def test_valid_zip(self, sample_export_zip):
        with open_export_zip(sample_export_zip) as export:
            assert export.ndjson_path.exists()
            assert export.data_dir is not None

    def test_missing_ndjson(self, tmp_dir):
        zip_path = tmp_dir / "bad.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("other.txt", "hello")
        with pytest.raises(MissingNDJSONError), open_export_zip(zip_path):
            pass

    def test_not_a_zip(self, tmp_dir):
        bad = tmp_dir / "notazip.zip"
        bad.write_text("this is not a zip file")
        with pytest.raises(InvalidZipError), open_export_zip(bad):
            pass

    def test_file_not_found(self, tmp_dir):
        with pytest.raises(InvalidZipError), open_export_zip(tmp_dir / "nonexistent.zip"):
            pass


class TestPathTraversalDefense:
    def test_rejects_traversal_entry(self, tmp_dir):
        """ZIP with ../../etc/passwd entry should be rejected."""
        zip_path = tmp_dir / "malicious.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("messages.ndjson", '{"address":"+1","body":"hi","date":"0","type":"1"}\n')
            zf.writestr("../../etc/evil.txt", "pwned")
        with (
            pytest.raises(InvalidZipError, match="path traversal"),
            open_export_zip(zip_path),
        ):
            pass

    def test_allows_normal_nested_paths(self, tmp_dir):
        """Normal nested paths like data/parts/file.jpg should work fine."""
        zip_path = tmp_dir / "normal.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("messages.ndjson", '{"address":"+1","body":"hi","date":"0","type":"1"}\n')
            zf.writestr("data/parts/photo.jpg", b"\xff\xd8fake")
        with open_export_zip(zip_path) as export:
            assert export.ndjson_path.exists()
            assert export.data_dir is not None
