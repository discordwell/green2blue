"""Tests for the canonical archive import path."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from green2blue.archive import (
    CanonicalArchive,
    build_archive_report,
    import_android_export,
    import_ios_backup,
)
from green2blue.converter.timestamp import unix_ms_to_ios_ns
from green2blue.ios.manifest import compute_file_id
from green2blue.models import ATTACHMENT_PLACEHOLDER


class TestAndroidArchiveImport:
    def test_import_android_export_creates_archive(self, sample_export_zip, tmp_dir):
        archive_path = tmp_dir / "sample.g2b.sqlite"

        result = import_android_export(sample_export_zip, archive_path)

        assert archive_path.exists()
        assert result.messages_imported == 3
        assert result.messages_deduped == 0
        assert result.attachments_imported == 1
        assert result.blobs_imported == 1

        with CanonicalArchive(archive_path) as archive:
            summary = archive.summary()

        assert summary.import_runs == 1
        assert summary.messages == 3
        assert summary.attachment_parts >= 2
        assert summary.blobs == 1
        assert summary.blob_bytes > 0

    def test_reimport_dedupes_messages(self, sample_export_zip, tmp_dir):
        archive_path = tmp_dir / "sample.g2b.sqlite"

        first = import_android_export(sample_export_zip, archive_path)
        second = import_android_export(sample_export_zip, archive_path)

        assert first.messages_imported == 3
        assert second.messages_imported == 0
        assert second.messages_deduped == 3

        conn = sqlite3.connect(archive_path)
        assert conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 3
        assert conn.execute("SELECT COUNT(*) FROM import_runs").fetchone()[0] == 2
        conn.close()


def _populate_ios_backup(sample_backup_dir: Path) -> None:
    sms_hash = compute_file_id("HomeDomain", "Library/SMS/sms.db")
    sms_db = sample_backup_dir / sms_hash[:2] / sms_hash
    conn = sqlite3.connect(sms_db)
    conn.execute(
        "INSERT INTO handle (ROWID, id, service, uncanonicalized_id) VALUES (1, ?, 'SMS', ?)",
        ("+12025550101", "+12025550101"),
    )
    conn.execute(
        """
        INSERT INTO chat (
            ROWID, guid, chat_identifier, service_name, display_name, style
        ) VALUES (1, ?, ?, 'SMS', ?, 45)
        """,
        ("any;+;+12025550101", "+12025550101", "+12025550101"),
    )
    conn.execute("INSERT INTO chat_handle_join (chat_id, handle_id) VALUES (1, 1)")

    date_one = unix_ms_to_ios_ns(1_700_000_000_000)
    date_two = unix_ms_to_ios_ns(1_700_000_001_000)
    conn.execute(
        """
        INSERT INTO message (
            ROWID, guid, text, handle_id, service, date, date_read, is_from_me,
            is_read, is_sent, is_finished, is_delivered, cache_has_attachments,
            part_count
        ) VALUES (?, ?, ?, ?, 'SMS', ?, ?, 0, 1, 0, 1, 0, 0, 1)
        """,
        (1, "msg-1", "Hello from iPhone", 1, date_one, date_one),
    )
    conn.execute(
        """
        INSERT INTO message (
            ROWID, guid, text, handle_id, service, date, date_read, is_from_me,
            is_read, is_sent, is_finished, is_delivered, cache_has_attachments,
            part_count
        ) VALUES (?, ?, ?, ?, 'SMS', ?, ?, 0, 0, 0, 1, 0, 1, 2)
        """,
        (
            2,
            "msg-2",
            ATTACHMENT_PLACEHOLDER + "Photo caption",
            1,
            date_two,
            0,
        ),
    )
    conn.execute(
        "INSERT INTO chat_message_join (chat_id, message_id, message_date) VALUES (1, 1, ?)",
        (date_one,),
    )
    conn.execute(
        "INSERT INTO chat_message_join (chat_id, message_id, message_date) VALUES (1, 2, ?)",
        (date_two,),
    )
    conn.execute(
        """
        INSERT INTO attachment (
            ROWID, guid, filename, mime_type, transfer_name, total_bytes
        ) VALUES (1, ?, ?, 'image/jpeg', ?, ?)
        """,
        (
            "att-1",
            "~/Library/SMS/Attachments/ab/cd/ATT-1/image000000.jpg",
            "image000000.jpg",
            15,
        ),
    )
    conn.execute(
        "INSERT INTO message_attachment_join (message_id, attachment_id) VALUES (2, 1)",
    )
    conn.commit()
    conn.close()

    attachment_rel = "Library/SMS/Attachments/ab/cd/ATT-1/image000000.jpg"
    attachment_id = compute_file_id("HomeDomain", attachment_rel)
    attachment_dir = sample_backup_dir / attachment_id[:2]
    attachment_dir.mkdir(exist_ok=True)
    (attachment_dir / attachment_id).write_bytes(b"real-jpeg-bytes")


class TestIOSArchiveImport:
    def test_import_ios_backup_creates_archive(self, sample_backup_dir, tmp_dir):
        _populate_ios_backup(sample_backup_dir)
        archive_path = tmp_dir / "ios.g2b.sqlite"

        result = import_ios_backup(sample_backup_dir, archive_path)

        assert archive_path.exists()
        assert result.messages_imported == 2
        assert result.messages_deduped == 0
        assert result.attachments_imported == 1
        assert result.blobs_imported == 1

        conn = sqlite3.connect(archive_path)
        conn.row_factory = sqlite3.Row
        summary = CanonicalArchive(archive_path)
        with summary as archive:
            archive_summary = archive.summary()
        assert archive_summary.import_runs == 1
        assert archive_summary.messages == 2
        assert archive_summary.blobs == 1

        photo_row = conn.execute(
            "SELECT body_text, has_attachments FROM messages WHERE source_uid = 'ios:msg-2'",
        ).fetchone()
        assert photo_row["body_text"] == "Photo caption"
        assert photo_row["has_attachments"] == 1
        attachment_row = conn.execute(
            "SELECT filename FROM message_attachments WHERE message_id = 2",
        ).fetchone()
        assert attachment_row["filename"] == "image000000.jpg"
        conn.close()

    def test_reimport_ios_backup_dedupes_messages(self, sample_backup_dir, tmp_dir):
        _populate_ios_backup(sample_backup_dir)
        archive_path = tmp_dir / "ios.g2b.sqlite"

        first = import_ios_backup(sample_backup_dir, archive_path)
        second = import_ios_backup(sample_backup_dir, archive_path)

        assert first.messages_imported == 2
        assert second.messages_imported == 0
        assert second.messages_deduped == 2

        conn = sqlite3.connect(archive_path)
        assert conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM import_runs").fetchone()[0] == 2
        conn.close()


class TestArchiveReport:
    def test_report_includes_cross_source_warning(
        self,
        sample_export_zip,
        sample_backup_dir,
        tmp_dir,
    ):
        _populate_ios_backup(sample_backup_dir)
        archive_path = tmp_dir / "merged.g2b.sqlite"

        import_android_export(sample_export_zip, archive_path)
        import_ios_backup(sample_backup_dir, archive_path)

        report = build_archive_report(archive_path)

        assert report.summary.import_runs == 2
        assert report.source_type_counts["android.sms"] >= 1
        assert report.source_type_counts["ios.message"] == 2
        assert report.messages_with_attachments >= 2
        assert report.messages_with_url == 0
        assert any("cross-source merge" in warning for warning in report.warnings)
