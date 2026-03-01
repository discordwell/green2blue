"""Tests for post-injection verification."""

from __future__ import annotations

import hashlib
import plistlib
import sqlite3

from green2blue.ios.manifest import ManifestDB, compute_file_id
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


    def test_digest_mismatch_detected(self, sample_backup_dir):
        """Verification should fail when Manifest.db digest doesn't match sms.db."""
        sms_hash = compute_file_id("HomeDomain", "Library/SMS/sms.db")
        sms_db = sample_backup_dir / sms_hash[:2] / sms_hash

        # Build an MBFile blob with a WRONG digest
        wrong_digest = b"\x00" * 20
        actual_size = sms_db.stat().st_size
        blob = plistlib.dumps({
            "$archiver": "NSKeyedArchiver",
            "$version": 100000,
            "$top": {"root": plistlib.UID(1)},
            "$objects": [
                "$null",
                {
                    "Size": actual_size,
                    "LastModified": 1700000000,
                    "Digest": plistlib.UID(2),
                    "$class": plistlib.UID(3),
                },
                wrong_digest,
                {"$classname": "MBFile", "$classes": ["MBFile", "NSObject"]},
            ],
        }, fmt=plistlib.FMT_BINARY)

        # Update Manifest.db with the wrong-digest blob
        manifest_path = sample_backup_dir / "Manifest.db"
        conn = sqlite3.connect(manifest_path)
        conn.execute(
            "UPDATE Files SET file = ? WHERE fileID = ?",
            (blob, sms_hash),
        )
        conn.commit()
        conn.close()

        result = verify_backup(sample_backup_dir, sms_db, manifest_path)
        assert not result.passed
        assert any("digest mismatch" in e for e in result.errors)

    def test_correct_digest_passes(self, sample_backup_dir):
        """Verification should pass when Manifest.db digest matches sms.db."""
        sms_hash = compute_file_id("HomeDomain", "Library/SMS/sms.db")
        sms_db = sample_backup_dir / sms_hash[:2] / sms_hash

        # Build an MBFile blob with the CORRECT digest
        actual_digest = hashlib.sha1(sms_db.read_bytes()).digest()
        actual_size = sms_db.stat().st_size
        blob = plistlib.dumps({
            "$archiver": "NSKeyedArchiver",
            "$version": 100000,
            "$top": {"root": plistlib.UID(1)},
            "$objects": [
                "$null",
                {
                    "Size": actual_size,
                    "LastModified": 1700000000,
                    "Digest": plistlib.UID(2),
                    "$class": plistlib.UID(3),
                },
                actual_digest,
                {"$classname": "MBFile", "$classes": ["MBFile", "NSObject"]},
            ],
        }, fmt=plistlib.FMT_BINARY)

        manifest_path = sample_backup_dir / "Manifest.db"
        conn = sqlite3.connect(manifest_path)
        conn.execute(
            "UPDATE Files SET file = ? WHERE fileID = ?",
            (blob, sms_hash),
        )
        conn.commit()
        conn.close()

        result = verify_backup(sample_backup_dir, sms_db, manifest_path)
        assert result.passed

    def test_update_sms_db_entry_fixes_digest(self, sample_backup_dir):
        """update_sms_db_entry with new_digest should fix a stale digest."""
        sms_hash = compute_file_id("HomeDomain", "Library/SMS/sms.db")
        sms_db = sample_backup_dir / sms_hash[:2] / sms_hash

        # Build an MBFile blob with a WRONG digest
        wrong_digest = b"\x00" * 20
        actual_size = sms_db.stat().st_size
        blob = plistlib.dumps({
            "$archiver": "NSKeyedArchiver",
            "$version": 100000,
            "$top": {"root": plistlib.UID(1)},
            "$objects": [
                "$null",
                {
                    "Size": actual_size,
                    "LastModified": 1700000000,
                    "Digest": plistlib.UID(2),
                    "$class": plistlib.UID(3),
                },
                wrong_digest,
                {"$classname": "MBFile", "$classes": ["MBFile", "NSObject"]},
            ],
        }, fmt=plistlib.FMT_BINARY)

        # Put the wrong-digest blob in Manifest.db
        manifest_path = sample_backup_dir / "Manifest.db"
        conn = sqlite3.connect(manifest_path)
        conn.execute(
            "UPDATE Files SET file = ? WHERE fileID = ?",
            (blob, sms_hash),
        )
        conn.commit()
        conn.close()

        # Now call update_sms_db_entry with the correct digest
        correct_digest = hashlib.sha1(sms_db.read_bytes()).digest()
        with ManifestDB(manifest_path) as manifest:
            manifest.update_sms_db_entry(actual_size, new_digest=correct_digest)

        # Verification should now pass
        result = verify_backup(sample_backup_dir, sms_db, manifest_path)
        assert result.passed


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
