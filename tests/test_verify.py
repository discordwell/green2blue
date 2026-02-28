"""Tests for post-injection verification."""

from __future__ import annotations

import sqlite3

from green2blue.ios.manifest import compute_file_id
from green2blue.verify import verify_backup


class TestVerifyBackup:
    def test_empty_backup_passes(self, sample_backup_dir):
        sms_hash = compute_file_id("HomeDomain", "Library/SMS/sms.db")
        sms_db = sample_backup_dir / sms_hash[:2] / sms_hash

        result = verify_backup(
            sample_backup_dir, sms_db, sample_backup_dir / "Manifest.db"
        )
        assert result.passed
        assert result.checks_passed > 0

    def test_integrity_check(self, sample_backup_dir):
        sms_hash = compute_file_id("HomeDomain", "Library/SMS/sms.db")
        sms_db = sample_backup_dir / sms_hash[:2] / sms_hash

        result = verify_backup(sample_backup_dir, sms_db)
        # Integrity check should pass
        assert result.checks_passed >= 1

    def test_corrupted_db_fails(self, tmp_dir):
        db_path = tmp_dir / "corrupt.db"
        db_path.write_bytes(b"not a database")

        result = verify_backup(tmp_dir, db_path)
        assert not result.passed
        assert len(result.errors) > 0

    def test_orphaned_handle_detected(self, sample_backup_dir):
        sms_hash = compute_file_id("HomeDomain", "Library/SMS/sms.db")
        sms_db = sample_backup_dir / sms_hash[:2] / sms_hash

        # Insert a message with a non-existent handle
        conn = sqlite3.connect(sms_db)
        conn.execute(
            """INSERT INTO message (guid, text, handle_id, service, account, account_guid,
               date, is_from_me, is_finished, is_read, is_empty)
               VALUES ('test-orphan', 'orphan msg', 999, 'SMS', 'p:0', 'p:0',
               0, 0, 1, 1, 0)"""
        )
        conn.commit()
        conn.close()

        result = verify_backup(sample_backup_dir, sms_db)
        assert not result.passed


class TestJoinTableConsistency:
    def test_valid_joins_pass(self, sample_backup_dir):
        sms_hash = compute_file_id("HomeDomain", "Library/SMS/sms.db")
        sms_db = sample_backup_dir / sms_hash[:2] / sms_hash

        conn = sqlite3.connect(sms_db)
        conn.execute("INSERT INTO handle (id, country, service) VALUES ('+1234', 'us', 'SMS')")
        handle_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            "INSERT INTO chat (guid, style, chat_identifier, service_name) "
            "VALUES ('SMS;-;+1234', 45, '+1234', 'SMS')"
        )
        chat_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            """INSERT INTO message (guid, text, handle_id, service, account, account_guid,
               date, is_from_me, is_finished, is_read, is_empty)
               VALUES ('test-valid', 'hi', ?, 'SMS', 'p:0', 'p:0', 0, 0, 1, 1, 0)""",
            (handle_id,),
        )
        msg_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute("INSERT INTO chat_handle_join VALUES (?, ?)", (chat_id, handle_id))
        conn.execute("INSERT INTO chat_message_join VALUES (?, ?, 0)", (chat_id, msg_id))
        conn.commit()
        conn.close()

        result = verify_backup(sample_backup_dir, sms_db)
        assert result.passed
