"""Tests for Manifest.db management."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from green2blue.ios.manifest import ManifestDB, compute_file_id


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
            assert m.detect_attachment_domain() == "HomeDomain"

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
                {"$classname": "NSMutableData", "$classes": ["NSMutableData", "NSData", "NSObject"]},
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
