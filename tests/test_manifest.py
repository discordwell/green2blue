"""Tests for Manifest.db management."""

from __future__ import annotations

import hashlib
import plistlib
import sqlite3
from pathlib import Path

import pytest

from green2blue.ios.manifest import ManifestDB, compute_file_id
from green2blue.ios.plist_utils import (
    build_mbfile_blob,
    extract_mbfile_digest,
    patch_mbfile_blob,
)


@pytest.fixture
def manifest_db(tmp_dir: Path) -> Path:
    """Create a minimal Manifest.db for testing."""
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
    # Insert a sms.db entry with a minimal blob
    sms_file_id = compute_file_id("HomeDomain", "Library/SMS/sms.db")
    conn.execute(
        "INSERT INTO Files (fileID, domain, relativePath, flags, file) VALUES (?, ?, ?, ?, ?)",
        (sms_file_id, "HomeDomain", "Library/SMS/sms.db", 1, b""),
    )
    conn.commit()
    conn.close()
    return db_path


class TestComputeFileId:
    def test_sms_db_hash(self):
        file_id = compute_file_id("HomeDomain", "Library/SMS/sms.db")
        assert file_id == "3d0d7e5fb2ce288813306e4d4636395e047a3d28"

    def test_deterministic(self):
        a = compute_file_id("HomeDomain", "Library/SMS/Attachments/test.jpg")
        b = compute_file_id("HomeDomain", "Library/SMS/Attachments/test.jpg")
        assert a == b

    def test_different_domain(self):
        a = compute_file_id("HomeDomain", "Library/SMS/sms.db")
        b = compute_file_id("MediaDomain", "Library/SMS/sms.db")
        assert a != b


class TestManifestDB:
    def test_open_close(self, manifest_db):
        m = ManifestDB(manifest_db)
        m.open()
        assert m.conn is not None
        m.close()
        assert m.conn is None

    def test_context_manager(self, manifest_db):
        with ManifestDB(manifest_db) as m:
            assert m.conn is not None

    def test_update_sms_db_entry(self, manifest_db):
        with ManifestDB(manifest_db) as m:
            file_id = m.update_sms_db_entry(new_size=1024000)
            assert file_id == compute_file_id("HomeDomain", "Library/SMS/sms.db")

            entry = m.get_entry(file_id)
            assert entry is not None
            assert entry["domain"] == "HomeDomain"
            # The file blob should be non-empty after update
            assert entry["file"] is not None
            assert len(entry["file"]) > 0

    def test_add_attachment_entry(self, manifest_db):
        rel_path = "Library/SMS/Attachments/ab/uuid123/photo.jpg"
        with ManifestDB(manifest_db) as m:
            file_id = m.add_attachment_entry(rel_path, file_size=2048)
            assert file_id == compute_file_id("HomeDomain", rel_path)

            entry = m.get_entry(file_id)
            assert entry is not None
            assert entry["domain"] == "HomeDomain"
            assert entry["relativePath"] == rel_path
            assert entry["flags"] == 1

    def test_detect_attachment_domain_default(self, manifest_db):
        with ManifestDB(manifest_db) as m:
            assert m.detect_attachment_domain() == "MediaDomain"

    def test_detect_attachment_domain_from_existing(self, manifest_db):
        # Insert a MediaDomain attachment entry
        conn = sqlite3.connect(manifest_db)
        conn.execute(
            "INSERT INTO Files VALUES (?, ?, ?, ?, ?)",
            ("abcdef", "MediaDomain", "Library/SMS/Attachments/test.jpg", 1, b""),
        )
        conn.commit()
        conn.close()

        with ManifestDB(manifest_db) as m:
            assert m.detect_attachment_domain() == "MediaDomain"

    def test_get_entry_not_found(self, manifest_db):
        with ManifestDB(manifest_db) as m:
            assert m.get_entry("nonexistent") is None

    def test_get_file_encryption_info_nsdata_wrapper(self, tmp_dir):
        """EncryptionKey stored as NSKeyedArchiver NSData wrapper (real iOS format)."""
        import plistlib

        db_path = tmp_dir / "Manifest_nsdata.db"
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE Files (
                fileID TEXT PRIMARY KEY, domain TEXT, relativePath TEXT,
                flags INTEGER, file BLOB
            )
        """)

        # Build an NSKeyedArchiver-style blob with NSData wrapper for EncryptionKey
        enc_key_bytes = b"\x03\x00\x00\x00" + b"\xaa" * 40  # 4-byte prefix + 40-byte wrapped key
        blob = plistlib.dumps({
            "$archiver": "NSKeyedArchiver",
            "$version": 100000,
            "$top": {"root": plistlib.UID(1)},
            "$objects": [
                "$null",
                {
                    "RelativePath": plistlib.UID(2),
                    "EncryptionKey": plistlib.UID(3),
                    "ProtectionClass": 3,
                    "$class": plistlib.UID(5),
                },
                "Library/SMS/sms.db",
                {"NS.data": enc_key_bytes, "$class": plistlib.UID(4)},
                {
                    "$classname": "NSMutableData",
                    "$classes": ["NSMutableData", "NSData", "NSObject"],
                },
                {"$classname": "MBFile", "$classes": ["MBFile", "NSObject"]},
            ],
        }, fmt=plistlib.FMT_BINARY)

        file_id = compute_file_id("HomeDomain", "Library/SMS/sms.db")
        conn.execute(
            "INSERT INTO Files VALUES (?, ?, ?, ?, ?)",
            (file_id, "HomeDomain", "Library/SMS/sms.db", 1, blob),
        )
        conn.commit()
        conn.close()

        with ManifestDB(db_path) as m:
            enc_key, prot_class = m.get_file_encryption_info(file_id)
            assert enc_key == enc_key_bytes
            assert prot_class == 3

    def test_get_file_encryption_info_raw_bytes(self, tmp_dir):
        """EncryptionKey stored as raw bytes (synthetic/test format)."""
        import plistlib

        db_path = tmp_dir / "Manifest_raw.db"
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE Files (
                fileID TEXT PRIMARY KEY, domain TEXT, relativePath TEXT,
                flags INTEGER, file BLOB
            )
        """)

        enc_key_bytes = b"\x03\x00\x00\x00" + b"\xbb" * 40
        blob = plistlib.dumps({
            "$archiver": "NSKeyedArchiver",
            "$version": 100000,
            "$top": {"root": plistlib.UID(1)},
            "$objects": [
                "$null",
                {
                    "EncryptionKey": plistlib.UID(2),
                    "ProtectionClass": 3,
                    "$class": plistlib.UID(3),
                },
                enc_key_bytes,  # raw bytes, not wrapped in NSData
                {"$classname": "MBFile", "$classes": ["MBFile", "NSObject"]},
            ],
        }, fmt=plistlib.FMT_BINARY)

        file_id = compute_file_id("HomeDomain", "Library/SMS/sms.db")
        conn.execute(
            "INSERT INTO Files VALUES (?, ?, ?, ?, ?)",
            (file_id, "HomeDomain", "Library/SMS/sms.db", 1, blob),
        )
        conn.commit()
        conn.close()

        with ManifestDB(db_path) as m:
            enc_key, prot_class = m.get_file_encryption_info(file_id)
            assert enc_key == enc_key_bytes
            assert prot_class == 3

    def test_add_attachment_creates_directory_entries(self, manifest_db):
        """add_attachment_entry creates flags=2 entries for parent directories."""
        rel_path = "Library/SMS/Attachments/2a/98/UUID-HERE/photo.jpg"
        with ManifestDB(manifest_db) as m:
            m.add_attachment_entry(rel_path, file_size=4096)

            # Check that directory entries were created for each parent
            cursor = m.conn.cursor()
            expected_dirs = [
                "Library/SMS/Attachments/2a/98/UUID-HERE",
                "Library/SMS/Attachments/2a/98",
                "Library/SMS/Attachments/2a",
                "Library/SMS/Attachments",
                "Library/SMS",
                "Library",
            ]
            for dir_path in expected_dirs:
                dir_id = compute_file_id("HomeDomain", dir_path)
                cursor.execute(
                    "SELECT flags FROM Files WHERE fileID = ?", (dir_id,),
                )
                row = cursor.fetchone()
                assert row is not None, f"Missing directory entry: {dir_path}"
                assert row[0] == 2, f"Wrong flags for {dir_path}: {row[0]}"

    def test_directory_entries_no_duplicates(self, manifest_db):
        """Calling add_attachment_entry twice doesn't create duplicate dirs."""
        path1 = "Library/SMS/Attachments/ab/UUID1/img1.jpg"
        path2 = "Library/SMS/Attachments/ab/UUID2/img2.jpg"
        with ManifestDB(manifest_db) as m:
            m.add_attachment_entry(path1, file_size=1024)
            m.add_attachment_entry(path2, file_size=2048)

            # Count entries for shared parent "Library/SMS/Attachments/ab"
            dir_id = compute_file_id("HomeDomain", "Library/SMS/Attachments/ab")
            cursor = m.conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM Files WHERE fileID = ?", (dir_id,),
            )
            assert cursor.fetchone()[0] == 1

    def test_directory_entries_use_correct_domain(self, manifest_db):
        """Directory entries match the domain of the file entry."""
        rel_path = "Library/SMS/Attachments/cd/UUID/vid.mp4"
        with ManifestDB(manifest_db) as m:
            m.add_attachment_entry(rel_path, file_size=8192, domain="MediaDomain")

            dir_id = compute_file_id("MediaDomain", "Library/SMS/Attachments/cd")
            cursor = m.conn.cursor()
            cursor.execute(
                "SELECT domain FROM Files WHERE fileID = ?", (dir_id,),
            )
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == "MediaDomain"


class TestPatchMBFileDigest:
    """Tests for digest patching in MBFile blobs."""

    def _make_blob_with_digest(self, size: int, digest: bytes) -> bytes:
        """Build an MBFile blob that has a Digest field (simulating real iOS)."""
        return plistlib.dumps({
            "$archiver": "NSKeyedArchiver",
            "$version": 100000,
            "$top": {"root": plistlib.UID(1)},
            "$objects": [
                "$null",
                {
                    "Size": size,
                    "Mode": 0o100644,
                    "LastModified": 1700000000,
                    "Birth": 1700000000,
                    "UserID": 501,
                    "GroupID": 501,
                    "Digest": plistlib.UID(2),
                    "ProtectionClass": 3,
                    "$class": plistlib.UID(3),
                },
                digest,
                {"$classname": "MBFile", "$classes": ["MBFile", "NSObject"]},
            ],
        }, fmt=plistlib.FMT_BINARY)

    def _extract_size(self, blob: bytes) -> int | None:
        """Extract Size from an MBFile blob."""
        plist = plistlib.loads(blob)
        for obj in plist["$objects"]:
            if isinstance(obj, dict) and "Size" in obj:
                return obj["Size"]
        return None

    def test_digest_correctly_updated(self):
        """patch_mbfile_blob correctly updates existing digest."""
        old_digest = hashlib.sha1(b"old file content").digest()
        new_digest = hashlib.sha1(b"new file content").digest()
        blob = self._make_blob_with_digest(1000, old_digest)

        patched = patch_mbfile_blob(blob, 2000, new_digest=new_digest)

        stored = extract_mbfile_digest(patched)
        assert stored == new_digest
        assert len(stored) == 20

    def test_digest_added_to_blob_without_one(self):
        """patch_mbfile_blob adds digest to a blob that had none."""
        blob = build_mbfile_blob(1000)
        assert extract_mbfile_digest(blob) is None  # no digest initially

        new_digest = hashlib.sha1(b"content").digest()
        patched = patch_mbfile_blob(blob, 2000, new_digest=new_digest)

        stored = extract_mbfile_digest(patched)
        assert stored == new_digest
        assert len(stored) == 20

    def test_size_updated_with_digest(self):
        """Size is correctly updated alongside digest."""
        old_digest = hashlib.sha1(b"old").digest()
        new_digest = hashlib.sha1(b"new").digest()
        blob = self._make_blob_with_digest(1000, old_digest)

        patched = patch_mbfile_blob(blob, 5000, new_digest=new_digest)

        assert self._extract_size(patched) == 5000
        assert extract_mbfile_digest(patched) == new_digest

    def test_digest_not_corrupted_by_colliding_size(self):
        """Digest survives even when old Size bytes appear inside it.

        This was the original bug: _replace_int_value blind search matched
        Size bytes inside the digest data, corrupting it.
        """
        # Craft a digest whose bytes contain the old size pattern
        old_size = 42  # 0x2A — encoded as 0x10 0x2A in bplist
        # Create a digest that contains these bytes
        crafted_digest = b"\x10\x2a" + b"\x00" * 18
        new_digest = hashlib.sha1(b"new content").digest()

        blob = self._make_blob_with_digest(old_size, crafted_digest)
        patched = patch_mbfile_blob(blob, 999, new_digest=new_digest)

        stored = extract_mbfile_digest(patched)
        assert stored == new_digest
        assert len(stored) == 20

    def test_build_mbfile_blob_with_digest(self):
        """build_mbfile_blob includes digest when provided."""
        digest = hashlib.sha1(b"test content").digest()
        blob = build_mbfile_blob(4096, digest=digest)

        stored = extract_mbfile_digest(blob)
        assert stored == digest

    def test_build_mbfile_blob_without_digest(self):
        """build_mbfile_blob omits digest when not provided."""
        blob = build_mbfile_blob(4096)
        assert extract_mbfile_digest(blob) is None

    def test_update_sms_db_entry_with_digest(self, manifest_db):
        """update_sms_db_entry stores correct digest in Manifest.db."""
        new_digest = hashlib.sha1(b"modified sms.db content").digest()

        with ManifestDB(manifest_db) as m:
            file_id = m.update_sms_db_entry(
                new_size=2048000, new_digest=new_digest,
            )
            entry = m.get_entry(file_id)
            assert entry is not None

            stored = extract_mbfile_digest(entry["file"])
            assert stored == new_digest
            assert len(stored) == 20

    def test_roundtrip_digest_matches_verify(self, manifest_db):
        """Digest stored by pipeline matches what verify.py would check."""
        file_content = b"simulated sms.db bytes"
        expected_digest = hashlib.sha1(file_content).digest()

        with ManifestDB(manifest_db) as m:
            m.update_sms_db_entry(
                new_size=len(file_content), new_digest=expected_digest,
            )
            file_id = compute_file_id("HomeDomain", "Library/SMS/sms.db")
            entry = m.get_entry(file_id)

        # Use the shared extraction function (same one verify.py uses)
        stored = extract_mbfile_digest(entry["file"])
        assert stored == expected_digest
