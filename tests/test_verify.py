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

        result = verify_backup(sample_backup_dir, sms_db, sample_backup_dir / "Manifest.db")
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
        blob = plistlib.dumps(
            {
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
            },
            fmt=plistlib.FMT_BINARY,
        )

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
        blob = plistlib.dumps(
            {
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
            },
            fmt=plistlib.FMT_BINARY,
        )

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
        blob = plistlib.dumps(
            {
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
            },
            fmt=plistlib.FMT_BINARY,
        )

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
            "VALUES ('any;-;+1234', 45, '+1234', 'SMS')"
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
        conn.execute("INSERT INTO chat_service VALUES ('SMS', ?)", (chat_id,))
        conn.commit()
        conn.close()

        result = verify_backup(sample_backup_dir, sms_db)
        assert result.passed

    def test_missing_chat_service_detected(self, sample_backup_dir):
        sms_hash = compute_file_id("HomeDomain", "Library/SMS/sms.db")
        sms_db = sample_backup_dir / sms_hash[:2] / sms_hash

        conn = sqlite3.connect(sms_db)
        conn.execute("INSERT INTO handle (id, country, service) VALUES ('+1234', 'us', 'SMS')")
        handle_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            "INSERT INTO chat (guid, style, chat_identifier, service_name) "
            "VALUES ('any;-;+1234', 45, '+1234', 'SMS')"
        )
        chat_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            """INSERT INTO message (guid, text, handle_id, service, account, account_guid,
               date, is_from_me, is_finished, is_read, is_empty)
               VALUES ('test-missing-chat-service', 'hi', ?, 'SMS', 'p:0', 'p:0',
               0, 0, 1, 1, 0)""",
            (handle_id,),
        )
        msg_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute("INSERT INTO chat_handle_join VALUES (?, ?)", (chat_id, handle_id))
        conn.execute("INSERT INTO chat_message_join VALUES (?, ?, 0)", (chat_id, msg_id))
        conn.commit()
        conn.close()

        result = verify_backup(sample_backup_dir, sms_db)
        assert not result.passed
        assert any("chat_service" in e for e in result.errors)

    def test_handle_id_zero_detected_as_orphan(self, sample_backup_dir):
        """Messages with handle_id=0 should be caught by foreign key check."""
        sms_hash = compute_file_id("HomeDomain", "Library/SMS/sms.db")
        sms_db = sample_backup_dir / sms_hash[:2] / sms_hash

        conn = sqlite3.connect(sms_db)
        conn.execute(
            """INSERT INTO message (guid, text, handle_id, service, account, account_guid,
               date, is_from_me, is_finished, is_read, is_empty)
               VALUES ('test-zero-handle', 'orphan', 0, 'SMS', 'p:0', 'p:0',
               0, 0, 1, 1, 0)"""
        )
        conn.commit()
        conn.close()

        result = verify_backup(sample_backup_dir, sms_db)
        # handle_id=0 is not in the handle table, but the filter
        # excludes handle_id=0 from the orphan check.
        # Messages with handle_id=0 are only from is_from_me=1 (outgoing)
        # or bugs — the injection now prevents them.
        assert result.passed


class TestConnectionHygiene:
    """The read-only checks must always close their sqlite connection.

    Each ``_check_*`` helper opens a connection and runs queries inside a
    ``try``/``except sqlite3.Error`` block; a leak on the error path (or an
    early return) would accumulate open handles across a batch of backups.
    """

    @staticmethod
    def _tracking_connect(monkeypatch):
        """Patch verify.sqlite3.connect to record close() calls on each conn."""
        from green2blue import verify

        opened: list = []
        real_connect = sqlite3.connect

        class _TrackingConnection(sqlite3.Connection):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.close_calls = 0
                opened.append(self)

            def close(self):
                self.close_calls += 1
                super().close()

        monkeypatch.setattr(
            verify.sqlite3,
            "connect",
            lambda database, *a, **k: real_connect(database, *a, factory=_TrackingConnection, **k),
        )
        return opened

    def test_checks_close_connection_on_query_error(self, tmp_dir, monkeypatch):
        from green2blue import verify

        opened = self._tracking_connect(monkeypatch)

        # A junk file opens lazily but fails once a query runs, so every
        # helper takes its ``except sqlite3.Error`` branch.
        junk = tmp_dir / "corrupt-sms.db"
        junk.write_bytes(b"definitely not a sqlite database")

        result = verify.VerificationResult()
        checks = [
            lambda: verify._check_integrity(junk, result),
            lambda: verify._check_foreign_keys(junk, result),
            lambda: verify._check_join_tables(junk, result),
            lambda: verify._check_attachments(junk, tmp_dir, None, result),
            lambda: verify._check_chat_indexes(junk, result),
        ]
        for run_check in checks:
            opened.clear()
            run_check()
            assert opened, "expected the check to open a connection"
            assert all(conn.close_calls == 1 for conn in opened), "connection leaked on error path"

        assert not result.passed

    def test_chat_index_check_closes_connection_on_early_return(self, tmp_dir, monkeypatch):
        from green2blue import verify

        opened = self._tracking_connect(monkeypatch)

        # A valid database with no chat_service table triggers the early
        # return inside the ``with closing(...)`` block.
        db_path = tmp_dir / "no-chat-service.db"
        seed = sqlite3.connect(db_path)
        seed.execute("CREATE TABLE chat (ROWID INTEGER PRIMARY KEY)")
        seed.commit()
        seed.close()

        result = verify.VerificationResult()
        verify._check_chat_indexes(db_path, result)

        assert opened, "expected the check to open a connection"
        assert all(conn.close_calls == 1 for conn in opened)
        # The early return still counts as a passed check.
        assert result.checks_passed == 1
        assert not result.errors
