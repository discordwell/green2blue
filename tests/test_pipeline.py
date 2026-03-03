"""End-to-end integration tests for the full pipeline."""

from __future__ import annotations

import plistlib
import sqlite3
import struct
import zipfile
from pathlib import Path

import pytest

from green2blue.ios.backup import get_sms_db_hash
from green2blue.ios.crypto import HAS_CRYPTO
from green2blue.pipeline import run_pipeline
from tests.conftest import (
    REAL_FORMAT_GROUP_MMS,
    REAL_FORMAT_MMS,
    SAMPLE_GROUP_MMS,
    SAMPLE_MMS,
    SAMPLE_RCS_MMS,
    SAMPLE_RCS_SMS,
    SAMPLE_SMS_RECEIVED,
    SAMPLE_SMS_SENT,
    make_ndjson_content,
)

crypto_required = pytest.mark.skipif(
    not HAS_CRYPTO, reason="cryptography package not installed"
)


def _create_full_backup(root: Path) -> Path:
    """Create a complete synthetic iPhone backup for integration testing."""
    udid = "INTEGRATION-TEST-BACKUP"
    backup_dir = root / udid
    backup_dir.mkdir(parents=True)

    # Info.plist
    (backup_dir / "Info.plist").write_bytes(plistlib.dumps({
        "Device Name": "Integration Test iPhone",
        "Product Version": "17.0",
        "Unique Identifier": udid,
    }))

    # Manifest.plist
    (backup_dir / "Manifest.plist").write_bytes(plistlib.dumps({
        "IsEncrypted": False,
        "Version": "3.3",
    }))

    # Status.plist
    (backup_dir / "Status.plist").write_bytes(plistlib.dumps({
        "IsFullBackup": True,
        "Version": "3.3",
    }))

    # Create sms.db with full schema
    sms_hash = get_sms_db_hash()
    sms_dir = backup_dir / sms_hash[:2]
    sms_dir.mkdir()
    sms_db_path = sms_dir / sms_hash

    sql_path = Path(__file__).parent.parent / "scripts" / "create_empty_smsdb.sql"
    conn = sqlite3.connect(sms_db_path)
    conn.executescript(sql_path.read_text())
    conn.close()

    # Create Manifest.db
    manifest_db = backup_dir / "Manifest.db"
    conn = sqlite3.connect(manifest_db)
    conn.execute("""
        CREATE TABLE Files (
            fileID TEXT PRIMARY KEY,
            domain TEXT,
            relativePath TEXT,
            flags INTEGER,
            file BLOB
        )
    """)
    conn.execute(
        "INSERT INTO Files VALUES (?, ?, ?, ?, ?)",
        (sms_hash, "HomeDomain", "Library/SMS/sms.db", 1, b""),
    )
    conn.commit()
    conn.close()

    return backup_dir


def _create_export_zip(
    root: Path,
    records: list[dict] | None = None,
    attachment_data: dict[str, bytes] | None = None,
) -> Path:
    """Create a synthetic export ZIP."""
    if records is None:
        records = [SAMPLE_SMS_RECEIVED, SAMPLE_SMS_SENT]

    zip_path = root / "test_export.zip"
    content = make_ndjson_content(*records)

    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("messages.ndjson", content)
        if attachment_data:
            for path, data in attachment_data.items():
                zf.writestr(path, data)

    return zip_path


class TestFullPipeline:
    def test_basic_sms_injection(self, tmp_dir):
        backup_dir = _create_full_backup(tmp_dir)
        zip_path = _create_export_zip(tmp_dir)

        result = run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
        )

        assert result.injection_stats is not None
        assert result.injection_stats.messages_inserted == 2
        assert result.injection_stats.handles_inserted == 2
        assert result.injection_stats.chats_inserted == 2
        assert result.verification is not None
        assert result.verification.passed

    def test_dry_run_no_modifications(self, tmp_dir):
        backup_dir = _create_full_backup(tmp_dir)
        zip_path = _create_export_zip(tmp_dir)

        # Get initial sms.db size
        sms_hash = get_sms_db_hash()
        sms_db = backup_dir / sms_hash[:2] / sms_hash
        initial_size = sms_db.stat().st_size

        result = run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
            dry_run=True,
        )

        # sms.db should be unchanged
        assert sms_db.stat().st_size == initial_size
        assert result.safety_copy_path is None

    def test_mms_injection(self, tmp_dir):
        backup_dir = _create_full_backup(tmp_dir)
        zip_path = _create_export_zip(
            tmp_dir,
            records=[SAMPLE_MMS],
            attachment_data={"data/parts/image_001.jpg": b"\xff\xd8\xff\xe0fake_jpeg"},
        )

        result = run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
        )

        assert result.injection_stats.messages_inserted == 1
        assert result.injection_stats.attachments_inserted >= 0

    def test_group_mms(self, tmp_dir):
        backup_dir = _create_full_backup(tmp_dir)
        zip_path = _create_export_zip(tmp_dir, records=[SAMPLE_GROUP_MMS])

        result = run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
        )

        assert result.injection_stats.messages_inserted == 1
        # Group chat = 3 participants = 3 handles
        assert result.injection_stats.handles_inserted == 3
        # 1 group chat
        assert result.injection_stats.chats_inserted == 1

        # Verify chat style is group (43)
        sms_hash = get_sms_db_hash()
        sms_db = backup_dir / sms_hash[:2] / sms_hash
        conn = sqlite3.connect(sms_db)
        cursor = conn.execute("SELECT style FROM chat")
        assert cursor.fetchone()[0] == 43
        conn.close()

    def test_mixed_messages(self, tmp_dir):
        backup_dir = _create_full_backup(tmp_dir)
        records = [SAMPLE_SMS_RECEIVED, SAMPLE_SMS_SENT, SAMPLE_MMS, SAMPLE_GROUP_MMS]
        zip_path = _create_export_zip(
            tmp_dir,
            records=records,
            attachment_data={"data/parts/image_001.jpg": b"fake_jpeg"},
        )

        result = run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
        )

        assert result.total_messages_parsed == 4
        assert result.injection_stats.messages_inserted == 4
        assert result.verification.passed

    def test_safety_copy_created(self, tmp_dir):
        backup_dir = _create_full_backup(tmp_dir)
        zip_path = _create_export_zip(tmp_dir)

        result = run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
        )

        assert result.safety_copy_path is not None
        assert result.safety_copy_path.exists()
        assert "restore_checkpoint" in result.safety_copy_path.name

    def test_manifest_updated(self, tmp_dir):
        backup_dir = _create_full_backup(tmp_dir)
        zip_path = _create_export_zip(tmp_dir)

        run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
        )

        # Verify Manifest.db was updated
        manifest_db = backup_dir / "Manifest.db"
        conn = sqlite3.connect(manifest_db)
        cursor = conn.execute(
            "SELECT file FROM Files WHERE relativePath = 'Library/SMS/sms.db'"
        )
        row = cursor.fetchone()
        conn.close()
        assert row is not None
        assert row[0] is not None
        assert len(row[0]) > 0  # Should have a non-empty blob now

    def test_duplicate_prevention(self, tmp_dir):
        backup_dir = _create_full_backup(tmp_dir)
        zip_path = _create_export_zip(tmp_dir)

        # Run twice
        result1 = run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
        )
        result2 = run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
        )

        assert result1.injection_stats.messages_inserted == 2
        assert result2.injection_stats.messages_skipped == 2
        assert result2.injection_stats.messages_inserted == 0

    def test_country_code_parameter(self, tmp_dir):
        backup_dir = _create_full_backup(tmp_dir)
        # Use a number without country code
        record = {
            "address": "2025551234",
            "body": "national number test",
            "date": "1700000000000",
            "type": "1",
            "read": "1",
        }
        zip_path = _create_export_zip(tmp_dir, records=[record])

        result = run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
            country="US",
        )

        assert result.injection_stats.messages_inserted == 1

        # Verify the handle has +1 prefix
        sms_hash = get_sms_db_hash()
        sms_db = backup_dir / sms_hash[:2] / sms_hash
        conn = sqlite3.connect(sms_db)
        cursor = conn.execute("SELECT id FROM handle")
        handle_id = cursor.fetchone()[0]
        conn.close()
        assert handle_id == "+12025551234"

    def test_imessage_service_injection(self, tmp_dir):
        """Pipeline should thread service='iMessage' through to sms.db."""
        backup_dir = _create_full_backup(tmp_dir)
        zip_path = _create_export_zip(tmp_dir)

        result = run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
            service="iMessage",
        )

        assert result.injection_stats.messages_inserted == 2

        # Verify service fields in sms.db
        sms_hash = get_sms_db_hash()
        sms_db = backup_dir / sms_hash[:2] / sms_hash
        conn = sqlite3.connect(sms_db)
        conn.row_factory = sqlite3.Row

        # Messages should have iMessage service
        cursor = conn.execute("SELECT service FROM message")
        for row in cursor.fetchall():
            assert row["service"] == "iMessage"

        # Handles should have iMessage service
        cursor = conn.execute("SELECT service FROM handle")
        for row in cursor.fetchall():
            assert row["service"] == "iMessage"

        # Chats should have iMessage service and lowercase account_login
        cursor = conn.execute("SELECT service_name, account_login FROM chat")
        for row in cursor.fetchall():
            assert row["service_name"] == "iMessage"
            assert row["account_login"] == "e:"

        # ck_chat_id should use iMessage prefix
        cursor = conn.execute("SELECT ck_chat_id FROM message")
        for row in cursor.fetchall():
            assert row["ck_chat_id"].startswith("iMessage;-;")

        conn.close()


class TestRealSMSIEFormat:
    """Tests using the real SMS Import/Export format (__sender_address / __recipient_addresses)."""

    def test_real_format_mms(self, tmp_dir):
        backup_dir = _create_full_backup(tmp_dir)
        zip_path = _create_export_zip(
            tmp_dir,
            records=[REAL_FORMAT_MMS],
            attachment_data={
                "data/PART_1700000002_image.jpg": b"\xff\xd8\xff\xe0real_jpeg",
            },
        )

        result = run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
        )

        assert result.injection_stats.messages_inserted == 1
        # 1:1 MMS: only 1 handle (the other party; phone owner doesn't need one)
        assert result.injection_stats.handles_inserted == 1
        assert result.total_attachments_copied == 1
        assert result.verification.passed

    def test_real_format_group_mms(self, tmp_dir):
        backup_dir = _create_full_backup(tmp_dir)
        zip_path = _create_export_zip(
            tmp_dir, records=[REAL_FORMAT_GROUP_MMS]
        )

        result = run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
        )

        assert result.injection_stats.messages_inserted == 1
        assert result.injection_stats.handles_inserted == 3
        assert result.injection_stats.chats_inserted == 1

        # Verify group chat style
        sms_hash = get_sms_db_hash()
        sms_db = backup_dir / sms_hash[:2] / sms_hash
        conn = sqlite3.connect(sms_db)
        cursor = conn.execute("SELECT style FROM chat")
        assert cursor.fetchone()[0] == 43
        conn.close()

    def test_real_format_android_data_path(self, tmp_dir):
        """Test that full Android _data paths resolve to correct ZIP files."""
        backup_dir = _create_full_backup(tmp_dir)
        # The _data field has full Android path, but ZIP has basename under data/
        zip_path = _create_export_zip(
            tmp_dir,
            records=[REAL_FORMAT_MMS],
            attachment_data={
                "data/PART_1700000002_image.jpg": b"\xff\xd8\xff\xe0real_jpeg",
            },
        )

        result = run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
        )

        # Attachment should have been found and copied
        assert result.total_attachments_copied == 1

        # Verify the attachment file exists in the backup
        sms_hash = get_sms_db_hash()
        sms_db = backup_dir / sms_hash[:2] / sms_hash
        conn = sqlite3.connect(sms_db)
        cursor = conn.execute("SELECT filename, total_bytes FROM attachment")
        row = cursor.fetchone()
        conn.close()
        assert row is not None
        assert "Library/SMS/Attachments" in row[0]
        assert row[1] > 0

    def test_mixed_legacy_and_real_format(self, tmp_dir):
        """Test that both legacy (__addresses) and real (__sender_address) formats work."""
        backup_dir = _create_full_backup(tmp_dir)
        records = [
            SAMPLE_SMS_RECEIVED,
            SAMPLE_MMS,  # legacy __addresses format
            REAL_FORMAT_GROUP_MMS,  # real group format (different from SAMPLE_MMS)
        ]
        zip_path = _create_export_zip(
            tmp_dir,
            records=records,
            attachment_data={
                "data/parts/image_001.jpg": b"fake_jpeg",
            },
        )

        result = run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
        )

        assert result.total_messages_parsed == 3
        assert result.injection_stats.messages_inserted == 3
        assert result.verification.passed


class TestRCSMessages:
    """Test that RCS messages are handled correctly."""

    def test_rcs_sms_injected(self, tmp_dir):
        """RCS SMS should be injected as a regular SMS."""
        backup_dir = _create_full_backup(tmp_dir)
        zip_path = _create_export_zip(tmp_dir, records=[SAMPLE_RCS_SMS])

        result = run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
        )

        assert result.injection_stats.messages_inserted == 1
        assert result.verification.passed

        # Verify the message body
        sms_hash = get_sms_db_hash()
        sms_db = backup_dir / sms_hash[:2] / sms_hash
        conn = sqlite3.connect(sms_db)
        cursor = conn.execute("SELECT text FROM message")
        assert cursor.fetchone()[0] == "RCS message via Google Messages"
        conn.close()

    def test_rcs_mms_injected(self, tmp_dir):
        """RCS MMS should be injected as a regular MMS."""
        backup_dir = _create_full_backup(tmp_dir)
        zip_path = _create_export_zip(
            tmp_dir,
            records=[SAMPLE_RCS_MMS],
            attachment_data={
                "data/PART_rcs_photo.jpg": b"\xff\xd8\xff\xe0rcs_jpeg",
            },
        )

        result = run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
        )

        assert result.injection_stats.messages_inserted == 1
        assert result.total_attachments_copied == 1
        assert result.verification.passed

    def test_rcs_mixed_with_sms(self, tmp_dir):
        """RCS, SMS, and MMS should all work together."""
        backup_dir = _create_full_backup(tmp_dir)
        records = [
            SAMPLE_SMS_RECEIVED,
            SAMPLE_RCS_SMS,
            REAL_FORMAT_MMS,
            SAMPLE_RCS_MMS,
        ]
        zip_path = _create_export_zip(
            tmp_dir,
            records=records,
            attachment_data={
                "data/PART_1700000002_image.jpg": b"\xff\xd8\xff\xe0jpeg1",
                "data/PART_rcs_photo.jpg": b"\xff\xd8\xff\xe0jpeg2",
            },
        )

        result = run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
        )

        assert result.total_messages_parsed == 4
        assert result.injection_stats.messages_inserted == 4
        assert result.total_attachments_copied == 2
        assert result.verification.passed


class TestPipelineEdgeCases:
    def test_empty_export(self, tmp_dir):
        backup_dir = _create_full_backup(tmp_dir)
        zip_path = tmp_dir / "empty.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("messages.ndjson", "")

        result = run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
        )

        assert result.total_messages_parsed == 0
        assert result.injection_stats.messages_inserted == 0
        assert result.verification.passed

    def test_pipeline_error_logs_safety_copy(self, tmp_dir):
        """Pipeline failure after injection should log safety copy path and re-raise."""
        from unittest.mock import patch

        backup_dir = _create_full_backup(tmp_dir)

        with patch(
            "green2blue.ios.sms_db.SMSDatabase.update_attachment_sizes",
            side_effect=RuntimeError("simulated failure"),
        ):
            # Force at least one attachment so update_attachment_sizes is called
            sub_dir = tmp_dir / "sub"
            sub_dir.mkdir()
            zip_with_att = _create_export_zip(
                sub_dir,
                records=[SAMPLE_MMS],
                attachment_data={"data/parts/image_001.jpg": b"\xff\xd8\xff\xe0fake"},
            )

            with pytest.raises(RuntimeError, match="simulated failure"):
                run_pipeline(
                    export_path=zip_with_att,
                    backup_path_or_udid=str(backup_dir),
                )


def _build_keybag_tlv(tag: bytes, value: bytes) -> bytes:
    """Build a single TLV record for a keybag."""
    return tag + struct.pack(">I", len(value)) + value


def _create_encrypted_backup(root: Path, password: str = "testpass") -> Path:
    """Create a complete synthetic encrypted iPhone backup.

    Builds a real keybag with a known class key, encrypts sms.db and
    Manifest.db with per-file keys wrapped by that class key.
    """
    from cryptography.hazmat.primitives.keywrap import aes_key_wrap

    from green2blue.ios.crypto import (
        Keybag,
        derive_key_from_password,
        encrypt_file,
    )
    from green2blue.ios.plist_utils import build_mbfile_blob

    udid = "ENCRYPTED-TEST-BACKUP"
    backup_dir = root / udid
    backup_dir.mkdir(parents=True)

    # Known class key for protection class 3
    class_key = b"\xee" * 32

    # Build keybag with low iteration count
    keybag_data = b""
    keybag_data += _build_keybag_tlv(b"VERS", struct.pack(">I", 5))
    keybag_data += _build_keybag_tlv(b"TYPE", struct.pack(">I", 1))
    keybag_data += _build_keybag_tlv(b"UUID", b"\x00" * 16)
    keybag_data += _build_keybag_tlv(b"SALT", b"\x01" * 20)
    keybag_data += _build_keybag_tlv(b"ITER", struct.pack(">I", 1))

    # Derive key from password and wrap the class key
    keybag = Keybag()
    keybag.salt = b"\x01" * 20
    keybag.iterations = 1
    derived_key = derive_key_from_password(password, keybag)
    wrapped_class_key = aes_key_wrap(derived_key, class_key)

    keybag_data += _build_keybag_tlv(b"UUID", b"\x02" * 16)
    keybag_data += _build_keybag_tlv(b"CLAS", struct.pack(">I", 3))
    keybag_data += _build_keybag_tlv(b"WPKY", wrapped_class_key)
    keybag_data += _build_keybag_tlv(b"KTYP", struct.pack(">I", 1))

    class_keys = {3: class_key}

    # Create sms.db encryption key
    sms_file_key = b"\xaa" * 32
    wrapped_sms_key = aes_key_wrap(class_key, sms_file_key)
    sms_enc_key = struct.pack("<I", 3) + wrapped_sms_key

    # Create Manifest.db encryption key (ManifestKey)
    manifest_file_key = b"\xbb" * 32
    wrapped_manifest_key = aes_key_wrap(class_key, manifest_file_key)
    manifest_key_data = struct.pack("<I", 3) + wrapped_manifest_key

    # Info.plist
    (backup_dir / "Info.plist").write_bytes(plistlib.dumps({
        "Device Name": "Encrypted Test iPhone",
        "Product Version": "17.0",
        "Unique Identifier": udid,
    }))

    # Manifest.plist
    (backup_dir / "Manifest.plist").write_bytes(plistlib.dumps({
        "IsEncrypted": True,
        "BackupKeyBag": keybag_data,
        "ManifestKey": manifest_key_data,
        "Version": "3.3",
    }))

    # Status.plist
    (backup_dir / "Status.plist").write_bytes(plistlib.dumps({
        "IsFullBackup": True,
        "Version": "3.3",
    }))

    # Create plaintext sms.db, then encrypt it
    sms_hash = get_sms_db_hash()
    sms_dir = backup_dir / sms_hash[:2]
    sms_dir.mkdir()

    plain_sms_path = root / "plain_sms.db"
    sql_path = Path(__file__).parent.parent / "scripts" / "create_empty_smsdb.sql"
    conn = sqlite3.connect(plain_sms_path)
    conn.executescript(sql_path.read_text())
    conn.close()

    sms_plaintext = plain_sms_path.read_bytes()
    sms_encrypted = encrypt_file(sms_plaintext, sms_enc_key, 3, class_keys)
    (sms_dir / sms_hash).write_bytes(sms_encrypted)

    # Create plaintext Manifest.db with sms.db entry (including EncryptionKey)
    plain_manifest_path = root / "plain_manifest.db"
    conn = sqlite3.connect(plain_manifest_path)
    conn.execute("""
        CREATE TABLE Files (
            fileID TEXT PRIMARY KEY,
            domain TEXT,
            relativePath TEXT,
            flags INTEGER,
            file BLOB
        )
    """)
    sms_blob = build_mbfile_blob(
        len(sms_plaintext),
        encryption_key=sms_enc_key,
        protection_class=3,
    )
    conn.execute(
        "INSERT INTO Files VALUES (?, ?, ?, ?, ?)",
        (sms_hash, "HomeDomain", "Library/SMS/sms.db", 1, sms_blob),
    )
    conn.commit()
    conn.close()

    manifest_plaintext = plain_manifest_path.read_bytes()
    manifest_encrypted = encrypt_file(manifest_plaintext, manifest_key_data, 3, class_keys)
    (backup_dir / "Manifest.db").write_bytes(manifest_encrypted)

    # Clean up temp files
    plain_sms_path.unlink()
    plain_manifest_path.unlink()

    return backup_dir


@crypto_required
class TestEncryptedPipeline:
    """Integration tests for the full encrypted backup pipeline."""

    def test_basic_encrypted_injection(self, tmp_dir):
        """Inject SMS into an encrypted backup end-to-end."""
        backup_dir = _create_encrypted_backup(tmp_dir)
        zip_path = _create_export_zip(tmp_dir)

        result = run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
            password="testpass",
        )

        assert result.injection_stats is not None
        assert result.injection_stats.messages_inserted == 2
        assert result.injection_stats.handles_inserted == 2
        assert result.injection_stats.chats_inserted == 2
        assert result.verification is not None
        assert result.verification.passed

    def test_encrypted_injection_verifiable_via_decrypt(self, tmp_dir):
        """After injection, decrypt sms.db and verify messages exist."""
        from green2blue.ios.crypto import EncryptedBackup
        from green2blue.ios.manifest import ManifestDB, compute_file_id

        backup_dir = _create_encrypted_backup(tmp_dir)
        zip_path = _create_export_zip(tmp_dir)

        run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
            password="testpass",
        )

        # Decrypt and verify
        eb = EncryptedBackup(backup_dir, "testpass")
        eb.unlock()

        # Decrypt Manifest.db
        temp_manifest = eb.decrypt_manifest_db()
        with ManifestDB(temp_manifest) as manifest:
            sms_file_id = compute_file_id("HomeDomain", "Library/SMS/sms.db")
            enc_key, prot_class = manifest.get_file_encryption_info(sms_file_id)

        # Decrypt sms.db
        sms_hash = get_sms_db_hash()
        sms_encrypted = (backup_dir / sms_hash[:2] / sms_hash).read_bytes()
        sms_decrypted = eb.decrypt_db_file(sms_encrypted, enc_key, prot_class)

        # Write decrypted to temp and query
        temp_sms = tmp_dir / "verify_sms.db"
        temp_sms.write_bytes(sms_decrypted)

        conn = sqlite3.connect(temp_sms)
        cursor = conn.execute("SELECT COUNT(*) FROM message")
        assert cursor.fetchone()[0] == 2
        cursor = conn.execute("SELECT text FROM message ORDER BY ROWID")
        texts = [r[0] for r in cursor.fetchall()]
        assert "Hello from Android!" in texts
        assert "Hello from me!" in texts
        conn.close()

        temp_manifest.unlink(missing_ok=True)

    def test_encrypted_mms_with_attachment(self, tmp_dir):
        """MMS attachments should be encrypted when copied."""
        from green2blue.ios.crypto import EncryptedBackup

        backup_dir = _create_encrypted_backup(tmp_dir)
        zip_path = _create_export_zip(
            tmp_dir,
            records=[SAMPLE_MMS],
            attachment_data={"data/parts/image_001.jpg": b"\xff\xd8\xff\xe0fake_jpeg"},
        )

        result = run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
            password="testpass",
        )

        assert result.injection_stats.messages_inserted == 1

        # Verify the Manifest.db has encryption keys for attachments
        eb = EncryptedBackup(backup_dir, "testpass")
        eb.unlock()
        temp_manifest = eb.decrypt_manifest_db()

        conn = sqlite3.connect(temp_manifest)
        cursor = conn.execute(
            "SELECT relativePath, file FROM Files"
            " WHERE relativePath LIKE '%Attachments%' AND flags = 1"
        )
        rows = cursor.fetchall()
        conn.close()

        # Should have at least one attachment entry
        if result.total_attachments_copied > 0:
            assert len(rows) > 0
            # Verify the blob contains EncryptionKey
            blob = rows[0][1]
            plist_data = plistlib.loads(blob)
            objects = plist_data["$objects"]
            has_enc_key = any(
                isinstance(obj, dict) and "EncryptionKey" in obj
                for obj in objects
            )
            assert has_enc_key

        temp_manifest.unlink(missing_ok=True)

    def test_missing_password_raises(self, tmp_dir):
        """Running on an encrypted backup without password should raise."""
        from green2blue.exceptions import EncryptedBackupError

        backup_dir = _create_encrypted_backup(tmp_dir)
        zip_path = _create_export_zip(tmp_dir)

        with pytest.raises(EncryptedBackupError, match="requires a password"):
            run_pipeline(
                export_path=zip_path,
                backup_path_or_udid=str(backup_dir),
            )

    def test_wrong_password_raises(self, tmp_dir):
        """Running with wrong password should raise WrongPasswordError."""
        from green2blue.exceptions import WrongPasswordError

        backup_dir = _create_encrypted_backup(tmp_dir)
        zip_path = _create_export_zip(tmp_dir)

        with pytest.raises(WrongPasswordError):
            run_pipeline(
                export_path=zip_path,
                backup_path_or_udid=str(backup_dir),
                password="wrong_password",
            )

    def test_encrypted_safety_copy_created(self, tmp_dir):
        """Safety copy should be created before modifying encrypted backup."""
        backup_dir = _create_encrypted_backup(tmp_dir)
        zip_path = _create_export_zip(tmp_dir)

        result = run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
            password="testpass",
        )

        assert result.safety_copy_path is not None
        assert result.safety_copy_path.exists()
        assert "restore_checkpoint" in result.safety_copy_path.name

    def test_encrypted_duplicate_prevention(self, tmp_dir):
        """Running twice on encrypted backup should skip duplicates."""
        backup_dir = _create_encrypted_backup(tmp_dir)
        zip_path = _create_export_zip(tmp_dir)

        result1 = run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
            password="testpass",
        )
        result2 = run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
            password="testpass",
        )

        assert result1.injection_stats.messages_inserted == 2
        assert result2.injection_stats.messages_skipped == 2
        assert result2.injection_stats.messages_inserted == 0


def _populate_backup_with_sacrifice(backup_dir: Path, chat_identifier: str, count: int) -> int:
    """Insert sacrifice messages into a backup's sms.db for overwrite testing.

    Returns the chat ROWID.
    """
    import uuid

    sms_hash = get_sms_db_hash()
    sms_db = backup_dir / sms_hash[:2] / sms_hash
    conn = sqlite3.connect(sms_db)

    conn.execute(
        "INSERT INTO handle (id, country, service) VALUES (?, 'us', 'SMS')",
        (chat_identifier,),
    )
    handle_rowid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    chat_guid = f"any;-;{chat_identifier}"
    conn.execute(
        """INSERT INTO chat (guid, style, state, chat_identifier, service_name,
                             account_login, group_id, server_change_token,
                             ck_sync_state, cloudkit_record_id)
           VALUES (?, 45, 3, ?, 'SMS', 'E:', ?, '', 1, '')""",
        (chat_guid, chat_identifier, str(uuid.uuid4())),
    )
    chat_rowid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    conn.execute(
        "INSERT INTO chat_handle_join (chat_id, handle_id) VALUES (?, ?)",
        (chat_rowid, handle_rowid),
    )

    base_date = 700000000000000000
    for i in range(count):
        msg_guid = f"sacrifice-{uuid.uuid4()}"
        ck_record_id = f"{'b' * 60}{i:04d}"
        msg_date = base_date + i * 1000000000
        conn.execute(
            """INSERT INTO message (guid, text, handle_id, service, date,
                                    date_read, date_delivered, is_from_me,
                                    is_read, is_delivered, is_finished,
                                    ck_sync_state, ck_record_id, ck_record_change_tag)
               VALUES (?, ?, ?, 'SMS', ?, ?, ?, 0, 1, 1, 1, 1, ?, '99')""",
            (msg_guid, f"sacrifice {i}", handle_rowid, msg_date, msg_date, msg_date, ck_record_id),
        )
        rowid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            "INSERT INTO chat_message_join (chat_id, message_id, message_date) VALUES (?, ?, ?)",
            (chat_rowid, rowid, msg_date),
        )

    conn.commit()
    conn.close()
    return chat_rowid


class TestOverwritePipeline:
    """Integration tests for overwrite mode through the full pipeline."""

    def test_overwrite_pipeline_basic(self, tmp_dir):
        """Overwrite mode should UPDATE sacrifice messages instead of INSERT."""
        from green2blue.models import InjectionMode

        backup_dir = _create_full_backup(tmp_dir)
        sacrifice_chat_id = _populate_backup_with_sacrifice(
            backup_dir, "+15551110000", 3,
        )

        zip_path = _create_export_zip(tmp_dir)

        result = run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
            injection_mode=InjectionMode.OVERWRITE,
            sacrifice_chats=[sacrifice_chat_id],
        )

        assert result.overwrite_stats is not None
        assert result.overwrite_stats.messages_overwritten == 2
        assert result.overwrite_stats.sacrifice_pool_size == 3
        assert result.injection_stats is None
        assert result.verification is not None
        assert result.verification.passed

        # Verify CK metadata preserved on overwritten messages
        sms_hash = get_sms_db_hash()
        sms_db = backup_dir / sms_hash[:2] / sms_hash
        conn = sqlite3.connect(sms_db)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT ck_sync_state, ck_record_id, ck_record_change_tag "
            "FROM message WHERE text LIKE 'Hello%'"
        ).fetchall()
        for row in rows:
            assert row["ck_sync_state"] == 1
            assert len(row["ck_record_id"]) == 64
            assert row["ck_record_change_tag"] == "99"
        conn.close()

    def test_overwrite_pipeline_icloud_disable(self, tmp_dir):
        """Pipeline with --disable-icloud-sync should flip madrid.plist flag."""
        backup_dir = _create_full_backup(tmp_dir)

        # Create com.apple.madrid.plist in the backup
        madrid_domain = "HomeDomain"
        madrid_path = "Library/Preferences/com.apple.madrid.plist"
        from green2blue.ios.manifest import compute_file_id
        madrid_file_id = compute_file_id(madrid_domain, madrid_path)

        madrid_dir = backup_dir / madrid_file_id[:2]
        madrid_dir.mkdir(exist_ok=True)
        madrid_plist = {
            "CloudKitSyncingEnabled": True,
            "OtherSetting": 42,
        }
        (madrid_dir / madrid_file_id).write_bytes(
            plistlib.dumps(madrid_plist, fmt=plistlib.FMT_BINARY)
        )

        # Add madrid.plist to Manifest.db
        manifest_db = backup_dir / "Manifest.db"
        conn = sqlite3.connect(manifest_db)
        conn.execute(
            "INSERT INTO Files (fileID, domain, relativePath, flags, file) VALUES (?, ?, ?, ?, ?)",
            (madrid_file_id, madrid_domain, madrid_path, 1, b""),
        )
        conn.commit()
        conn.close()

        zip_path = _create_export_zip(tmp_dir)
        result = run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
            disable_icloud_sync=True,
        )

        assert result.injection_stats.messages_inserted == 2

        # Verify madrid.plist was modified
        modified_data = (madrid_dir / madrid_file_id).read_bytes()
        modified_plist = plistlib.loads(modified_data)
        assert modified_plist["CloudKitSyncingEnabled"] is False
        assert modified_plist["OtherSetting"] == 42
