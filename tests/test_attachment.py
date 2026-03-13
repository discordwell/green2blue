"""Tests for attachment file handling."""

from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

import pytest

from green2blue.ios.attachment import copy_attachment_to_backup, resolve_attachment_paths
from green2blue.ios.manifest import ManifestDB, compute_file_id
from green2blue.ios.plist_utils import extract_mbfile_digest


@pytest.fixture
def manifest(tmp_dir: Path):
    """Create a ManifestDB for attachment testing."""
    db_path = tmp_dir / "Manifest.db"
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE Files (
            fileID TEXT PRIMARY KEY,
            domain TEXT,
            relativePath TEXT,
            flags INTEGER,
            file BLOB
        )
    """)
    conn.commit()
    conn.close()

    m = ManifestDB(db_path)
    m.open()
    yield m
    m.close()


class TestCopyAttachmentToBackup:
    def test_copy_file(self, tmp_dir, manifest):
        # Create a source file
        source = tmp_dir / "source" / "photo.jpg"
        source.parent.mkdir()
        source.write_bytes(b"\xff\xd8\xff\xe0" + b"x" * 100)

        backup_dir = tmp_dir / "backup"
        backup_dir.mkdir()

        ios_path = "Library/SMS/Attachments/ab/test-uuid/photo.jpg"
        size = copy_attachment_to_backup(
            source, ios_path, backup_dir, manifest, domain="HomeDomain"
        )

        assert size == 104

        # Verify file exists in backup
        file_id = compute_file_id("HomeDomain", ios_path)
        dest = backup_dir / file_id[:2] / file_id
        assert dest.exists()
        assert dest.stat().st_size == 104

        # Verify manifest entry
        entry = manifest.get_entry(file_id)
        assert entry is not None
        assert entry["relativePath"] == ios_path

    def test_missing_source(self, tmp_dir, manifest):
        backup_dir = tmp_dir / "backup"
        backup_dir.mkdir()

        size = copy_attachment_to_backup(
            tmp_dir / "nonexistent.jpg",
            "Library/SMS/Attachments/xx/uuid/file.jpg",
            backup_dir,
            manifest,
        )
        assert size == 0

    def test_empty_source(self, tmp_dir, manifest):
        source = tmp_dir / "empty.jpg"
        source.write_bytes(b"")

        backup_dir = tmp_dir / "backup"
        backup_dir.mkdir()

        size = copy_attachment_to_backup(
            source,
            "Library/SMS/Attachments/xx/uuid/empty.jpg",
            backup_dir,
            manifest,
        )
        assert size == 0

    def test_attachment_digest_stored(self, tmp_dir, manifest):
        """Attachment SHA-1 digest should be stored in the Manifest.db MBFile blob."""
        source = tmp_dir / "source" / "photo.jpg"
        source.parent.mkdir(exist_ok=True)
        file_content = b"\xff\xd8\xff\xe0" + b"digest_test_data"
        source.write_bytes(file_content)

        backup_dir = tmp_dir / "backup"
        backup_dir.mkdir()

        ios_path = "Library/SMS/Attachments/cd/test-uuid/photo.jpg"
        copy_attachment_to_backup(source, ios_path, backup_dir, manifest, domain="HomeDomain")

        # Read the MBFile blob and verify digest matches
        file_id = compute_file_id("HomeDomain", ios_path)
        entry = manifest.get_entry(file_id)
        assert entry is not None

        stored_digest = extract_mbfile_digest(entry["file"])
        expected_digest = hashlib.sha1(file_content).digest()
        assert stored_digest == expected_digest

    def test_copy_file_streams_unencrypted_attachment(self, tmp_dir, manifest, monkeypatch):
        source = tmp_dir / "source" / "large.jpg"
        source.parent.mkdir()
        source.write_bytes(b"\xff\xd8\xff\xe0" + b"x" * 8192)

        original_read_bytes = Path.read_bytes

        def _guarded_read_bytes(path_self):
            if path_self == source:
                raise AssertionError("copy_attachment_to_backup should stream unencrypted files")
            return original_read_bytes(path_self)

        monkeypatch.setattr(Path, "read_bytes", _guarded_read_bytes)

        backup_dir = tmp_dir / "backup"
        backup_dir.mkdir()
        ios_path = "Library/SMS/Attachments/ee/test-uuid/large.jpg"

        size = copy_attachment_to_backup(
            source, ios_path, backup_dir, manifest, domain="HomeDomain"
        )

        assert size == source.stat().st_size


class TestResolveAttachmentPaths:
    def test_resolve_existing(self, tmp_dir):
        data_dir = tmp_dir / "data"
        parts_dir = data_dir / "parts"
        parts_dir.mkdir(parents=True)
        (parts_dir / "image.jpg").write_bytes(b"jpeg_data")

        attachments = [("data/parts/image.jpg", "Library/SMS/Attachments/ab/uuid/image.jpg")]
        resolved = resolve_attachment_paths(attachments, data_dir)
        assert len(resolved) == 1
        assert resolved[0][0] is not None
        assert resolved[0][0].exists()

    def test_resolve_missing(self, tmp_dir):
        data_dir = tmp_dir / "data"
        data_dir.mkdir()

        attachments = [("data/parts/missing.jpg", "Library/SMS/Attachments/ab/uuid/missing.jpg")]
        resolved = resolve_attachment_paths(attachments, data_dir)
        assert resolved[0][0] is None

    def test_resolve_no_data_dir(self):
        attachments = [("data/parts/image.jpg", "Library/SMS/Attachments/ab/uuid/image.jpg")]
        resolved = resolve_attachment_paths(attachments, None)
        assert resolved[0][0] is None
