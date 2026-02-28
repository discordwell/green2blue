"""End-to-end integration tests for the full pipeline."""

from __future__ import annotations

import plistlib
import sqlite3
import zipfile
from pathlib import Path

from green2blue.ios.backup import get_sms_db_hash
from green2blue.pipeline import run_pipeline
from tests.conftest import (
    SAMPLE_GROUP_MMS,
    SAMPLE_MMS,
    SAMPLE_SMS_RECEIVED,
    SAMPLE_SMS_SENT,
    make_ndjson_content,
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
        assert "g2b_backup" in result.safety_copy_path.name

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
