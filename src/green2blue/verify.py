"""Post-injection integrity verification.

Checks performed after injecting messages into a backup:
1. SQLite integrity check on sms.db
2. Foreign key consistency in join tables
3. Manifest.db file hash matches (if applicable)
4. Attachment files exist on disk
"""

from __future__ import annotations

import hashlib
import logging
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

from green2blue.ios.manifest import ManifestDB, compute_file_id
from green2blue.ios.plist_utils import extract_mbfile_digest

logger = logging.getLogger(__name__)


@dataclass
class VerificationResult:
    """Results of post-injection verification."""

    passed: bool = True
    checks_run: int = 0
    checks_passed: int = 0
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def add_error(self, msg: str) -> None:
        self.errors.append(msg)
        self.passed = False

    def add_warning(self, msg: str) -> None:
        self.warnings.append(msg)


def verify_backup(
    backup_path: Path,
    sms_db_path: Path,
    manifest_db_path: Path | None = None,
) -> VerificationResult:
    """Run all post-injection verification checks.

    Args:
        backup_path: Root of the iPhone backup directory.
        sms_db_path: Path to the sms.db file within the backup.
        manifest_db_path: Path to Manifest.db (optional).

    Returns:
        VerificationResult with pass/fail status and details.
    """
    result = VerificationResult()

    # 1. SQLite integrity check
    _check_integrity(sms_db_path, result)

    # 2. Foreign key consistency
    _check_foreign_keys(sms_db_path, result)

    # 3. Join table consistency
    _check_join_tables(sms_db_path, result)

    # 4. Attachment files exist
    _check_attachments(sms_db_path, backup_path, manifest_db_path, result)

    # 5. Messages UI chat index consistency
    _check_chat_indexes(sms_db_path, result)

    # 6. Manifest.db consistency
    if manifest_db_path:
        _check_manifest(manifest_db_path, sms_db_path, result)

    return result


def _check_integrity(db_path: Path, result: VerificationResult) -> None:
    """Run PRAGMA integrity_check on sms.db."""
    result.checks_run += 1
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.execute("PRAGMA integrity_check")
        check_result = cursor.fetchone()[0]
        conn.close()

        if check_result == "ok":
            result.checks_passed += 1
            logger.debug("SQLite integrity check: PASSED")
        else:
            result.add_error(f"SQLite integrity check failed: {check_result}")
    except sqlite3.Error as e:
        result.add_error(f"SQLite integrity check error: {e}")


def _check_foreign_keys(db_path: Path, result: VerificationResult) -> None:
    """Check that handle_id references in messages are valid."""
    result.checks_run += 1
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.execute("""
            SELECT COUNT(*) FROM message m
            WHERE m.handle_id != 0
            AND m.handle_id NOT IN (SELECT ROWID FROM handle)
        """)
        orphaned = cursor.fetchone()[0]
        conn.close()

        if orphaned == 0:
            result.checks_passed += 1
            logger.debug("Foreign key check: PASSED")
        else:
            result.add_error(
                f"{orphaned} messages reference non-existent handles"
            )
    except sqlite3.Error as e:
        result.add_error(f"Foreign key check error: {e}")


def _check_join_tables(db_path: Path, result: VerificationResult) -> None:
    """Check consistency of join tables."""
    result.checks_run += 1
    try:
        conn = sqlite3.connect(db_path)

        # Check chat_message_join references valid messages
        cursor = conn.execute("""
            SELECT COUNT(*) FROM chat_message_join cmj
            WHERE cmj.message_id NOT IN (SELECT ROWID FROM message)
        """)
        orphaned_msgs = cursor.fetchone()[0]

        # Check chat_message_join references valid chats
        cursor = conn.execute("""
            SELECT COUNT(*) FROM chat_message_join cmj
            WHERE cmj.chat_id NOT IN (SELECT ROWID FROM chat)
        """)
        orphaned_chats = cursor.fetchone()[0]

        # Check chat_handle_join
        cursor = conn.execute("""
            SELECT COUNT(*) FROM chat_handle_join chj
            WHERE chj.handle_id NOT IN (SELECT ROWID FROM handle)
        """)
        orphaned_handles = cursor.fetchone()[0]

        # Check message_attachment_join
        cursor = conn.execute("""
            SELECT COUNT(*) FROM message_attachment_join maj
            WHERE maj.message_id NOT IN (SELECT ROWID FROM message)
        """)
        orphaned_att_msgs = cursor.fetchone()[0]

        cursor = conn.execute("""
            SELECT COUNT(*) FROM message_attachment_join maj
            WHERE maj.attachment_id NOT IN (SELECT ROWID FROM attachment)
        """)
        orphaned_att = cursor.fetchone()[0]

        conn.close()

        errors = []
        if orphaned_msgs:
            errors.append(f"{orphaned_msgs} chat_message_join entries reference missing messages")
        if orphaned_chats:
            errors.append(f"{orphaned_chats} chat_message_join entries reference missing chats")
        if orphaned_handles:
            errors.append(f"{orphaned_handles} chat_handle_join entries reference missing handles")
        if orphaned_att_msgs:
            errors.append(
                f"{orphaned_att_msgs} message_attachment_join refs missing messages"
            )
        if orphaned_att:
            errors.append(
                f"{orphaned_att} message_attachment_join refs missing attachments"
            )

        if errors:
            for e in errors:
                result.add_error(e)
        else:
            result.checks_passed += 1
            logger.debug("Join table consistency: PASSED")

    except sqlite3.Error as e:
        result.add_error(f"Join table check error: {e}")


def _check_attachments(
    db_path: Path,
    backup_path: Path,
    manifest_db_path: Path | None,
    result: VerificationResult,
) -> None:
    """Check that attachment files referenced in sms.db exist in the backup."""
    result.checks_run += 1
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.execute(
            "SELECT filename FROM attachment WHERE filename IS NOT NULL AND filename != ''"
        )
        filenames = [row[0] for row in cursor.fetchall()]
        conn.close()

        if not filenames:
            result.checks_passed += 1
            return

        # For each attachment, check that the file exists in the backup
        missing = 0
        checked = 0
        for filename in filenames:
            # Strip the ~/ prefix for iOS relative path
            relative = filename.removeprefix("~/")
            file_id = compute_file_id("HomeDomain", relative)
            backup_file = backup_path / file_id[:2] / file_id
            if not backup_file.exists():
                # Also try MediaDomain
                file_id_media = compute_file_id("MediaDomain", relative)
                backup_file_media = backup_path / file_id_media[:2] / file_id_media
                if not backup_file_media.exists():
                    missing += 1
                    logger.debug("Missing attachment file: %s", filename)
            checked += 1

        if missing == 0:
            result.checks_passed += 1
            logger.debug("Attachment file check: PASSED (%d files)", checked)
        else:
            result.add_warning(
                f"{missing}/{checked} attachment files not found in backup"
            )
            result.checks_passed += 1  # Warning, not error

    except sqlite3.Error as e:
        result.add_error(f"Attachment check error: {e}")


def _check_chat_indexes(db_path: Path, result: VerificationResult) -> None:
    """Check that chats are indexed in auxiliary tables used by Messages UI."""
    result.checks_run += 1
    try:
        conn = sqlite3.connect(db_path)
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }

        if "chat_service" not in tables:
            conn.close()
            result.checks_passed += 1
            return

        missing_chat_service = conn.execute("""
            SELECT COUNT(*) FROM chat c
            WHERE c.service_name IS NOT NULL
              AND c.service_name != ''
              AND NOT EXISTS (
                  SELECT 1 FROM chat_service cs
                  WHERE cs.chat = c.ROWID
                    AND cs.service = c.service_name
              )
        """).fetchone()[0]
        conn.close()

        if missing_chat_service == 0:
            result.checks_passed += 1
            logger.debug("Chat index consistency: PASSED")
        else:
            result.add_error(
                f"{missing_chat_service} chats are missing chat_service index rows"
            )
    except sqlite3.Error as e:
        result.add_error(f"Chat index check error: {e}")


def _check_manifest(
    manifest_db_path: Path,
    sms_db_path: Path,
    result: VerificationResult,
) -> None:
    """Check that Manifest.db has a valid entry for sms.db with correct digest."""
    result.checks_run += 1
    try:
        with ManifestDB(manifest_db_path) as manifest:
            file_id = compute_file_id("HomeDomain", "Library/SMS/sms.db")
            entry = manifest.get_entry(file_id)
            if not entry:
                result.add_error("sms.db entry missing from Manifest.db")
                return

            result.checks_passed += 1
            logger.debug("Manifest.db sms.db entry: PRESENT")

            # Check digest if the MBFile blob has one
            blob = entry.get("file")
            if blob:
                stored_digest = extract_mbfile_digest(blob)
                if stored_digest is not None:
                    result.checks_run += 1
                    actual_digest = hashlib.sha1(sms_db_path.read_bytes()).digest()
                    if stored_digest == actual_digest:
                        result.checks_passed += 1
                        logger.debug("Manifest.db sms.db digest: MATCH")
                    else:
                        result.add_error(
                            f"sms.db digest mismatch: manifest={stored_digest.hex()}, "
                            f"actual={actual_digest.hex()}"
                        )
    except Exception as e:
        result.add_error(f"Manifest check error: {e}")

