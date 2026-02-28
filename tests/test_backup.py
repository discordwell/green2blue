"""Tests for iPhone backup discovery and validation."""

from __future__ import annotations

import plistlib
import sqlite3
from pathlib import Path

import pytest

from green2blue.exceptions import BackupNotFoundError, InvalidBackupError, MultipleBackupsError
from green2blue.ios.backup import (
    create_safety_copy,
    find_backup,
    get_sms_db_hash,
    list_backups,
    validate_backup,
)


def _create_backup(
    root: Path, udid: str, device_name: str = "Test iPhone", encrypted: bool = False,
) -> Path:
    """Create a synthetic backup directory."""
    backup_dir = root / udid
    backup_dir.mkdir(parents=True)

    # Info.plist
    info = {
        "Device Name": device_name,
        "Product Version": "17.0",
        "Unique Identifier": udid,
    }
    (backup_dir / "Info.plist").write_bytes(plistlib.dumps(info))

    # Manifest.plist
    manifest = {"IsEncrypted": encrypted, "Version": "3.3"}
    (backup_dir / "Manifest.plist").write_bytes(plistlib.dumps(manifest))

    # Status.plist
    status = {"IsFullBackup": True, "Version": "3.3", "Date": "2024-01-01T00:00:00Z"}
    (backup_dir / "Status.plist").write_bytes(plistlib.dumps(status))

    # Manifest.db
    manifest_db = backup_dir / "Manifest.db"
    conn = sqlite3.connect(manifest_db)
    conn.execute(
        "CREATE TABLE Files (fileID TEXT PRIMARY KEY, domain TEXT, "
        "relativePath TEXT, flags INTEGER, file BLOB)"
    )
    sms_hash = get_sms_db_hash()
    conn.execute(
        "INSERT INTO Files VALUES (?, ?, ?, ?, ?)",
        (sms_hash, "HomeDomain", "Library/SMS/sms.db", 1, b""),
    )
    conn.commit()
    conn.close()

    # sms.db
    sms_dir = backup_dir / sms_hash[:2]
    sms_dir.mkdir()
    sms_db = sms_dir / sms_hash
    conn = sqlite3.connect(sms_db)
    conn.execute("CREATE TABLE message (ROWID INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()

    return backup_dir


class TestListBackups:
    def test_empty_directory(self, tmp_dir):
        root = tmp_dir / "backups"
        root.mkdir()
        assert list_backups(root) == []

    def test_nonexistent_directory(self, tmp_dir):
        assert list_backups(tmp_dir / "nonexistent") == []

    def test_single_backup(self, tmp_dir):
        root = tmp_dir / "backups"
        root.mkdir()
        _create_backup(root, "AAAA-BBBB-CCCC")
        backups = list_backups(root)
        assert len(backups) == 1
        assert backups[0].udid == "AAAA-BBBB-CCCC"
        assert backups[0].device_name == "Test iPhone"

    def test_multiple_backups(self, tmp_dir):
        root = tmp_dir / "backups"
        root.mkdir()
        _create_backup(root, "AAAA-1111", "iPhone 1")
        _create_backup(root, "BBBB-2222", "iPhone 2")
        backups = list_backups(root)
        assert len(backups) == 2

    def test_encrypted_detection(self, tmp_dir):
        root = tmp_dir / "backups"
        root.mkdir()
        _create_backup(root, "ENC-BACKUP", encrypted=True)
        backups = list_backups(root)
        assert backups[0].is_encrypted


class TestFindBackup:
    def test_auto_select_single(self, tmp_dir):
        root = tmp_dir / "backups"
        root.mkdir()
        _create_backup(root, "ONLY-ONE")
        backup = find_backup(backup_root=root)
        assert backup.udid == "ONLY-ONE"

    def test_auto_select_fails_with_multiple(self, tmp_dir):
        root = tmp_dir / "backups"
        root.mkdir()
        _create_backup(root, "AAAA")
        _create_backup(root, "BBBB")
        with pytest.raises(MultipleBackupsError):
            find_backup(backup_root=root)

    def test_find_by_udid(self, tmp_dir):
        root = tmp_dir / "backups"
        root.mkdir()
        _create_backup(root, "AAAA")
        _create_backup(root, "BBBB")
        backup = find_backup("BBBB", backup_root=root)
        assert backup.udid == "BBBB"

    def test_find_by_path(self, tmp_dir):
        root = tmp_dir / "backups"
        root.mkdir()
        path = _create_backup(root, "DIRECT")
        backup = find_backup(str(path))
        assert backup.udid == "DIRECT"

    def test_not_found(self, tmp_dir):
        root = tmp_dir / "backups"
        root.mkdir()
        with pytest.raises(BackupNotFoundError):
            find_backup(backup_root=root)

    def test_not_found_with_udid(self, tmp_dir):
        root = tmp_dir / "backups"
        root.mkdir()
        _create_backup(root, "EXISTING")
        with pytest.raises(BackupNotFoundError):
            find_backup("NONEXISTENT", backup_root=root)


class TestValidateBackup:
    def test_valid_backup(self, tmp_dir):
        root = tmp_dir / "backups"
        root.mkdir()
        path = _create_backup(root, "VALID")
        validate_backup(path)  # Should not raise

    def test_missing_info_plist(self, tmp_dir):
        root = tmp_dir / "backups"
        root.mkdir()
        path = _create_backup(root, "BAD")
        (path / "Info.plist").unlink()
        with pytest.raises(InvalidBackupError):
            validate_backup(path)

    def test_missing_sms_db(self, tmp_dir):
        root = tmp_dir / "backups"
        root.mkdir()
        path = _create_backup(root, "NOSMS")
        sms_hash = get_sms_db_hash()
        (path / sms_hash[:2] / sms_hash).unlink()
        with pytest.raises(InvalidBackupError):
            validate_backup(path)


class TestSafetyCopy:
    def test_creates_copy(self, tmp_dir):
        root = tmp_dir / "backups"
        root.mkdir()
        path = _create_backup(root, "ORIGINAL")
        safety = create_safety_copy(path)
        assert safety.exists()
        assert safety != path
        assert "g2b_backup" in safety.name

        # Verify the copy has the same files
        assert (safety / "Info.plist").exists()
        assert (safety / "Manifest.db").exists()
