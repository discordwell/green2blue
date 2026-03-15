"""Tests for iPhone backup discovery and validation."""

from __future__ import annotations

import plistlib
import sqlite3
from pathlib import Path

import pytest

from green2blue.exceptions import BackupNotFoundError, InvalidBackupError
from green2blue.ios.backup import (
    create_safety_copy,
    find_backup,
    get_sms_db_hash,
    has_restore_checkpoint,
    list_backups,
    stash_safety_copy,
    validate_backup,
)


def _create_backup(
    root: Path,
    udid: str,
    device_name: str = "Test iPhone",
    encrypted: bool = False,
    date: str = "2024-01-01T00:00:00Z",
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
    status = {"IsFullBackup": True, "Version": "3.3", "Date": date}
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

    def test_auto_select_picks_from_multiple(self, tmp_dir):
        """With multiple backups, auto-select picks the most recent."""
        root = tmp_dir / "backups"
        root.mkdir()
        _create_backup(root, "AAAA", date="2024-01-01T00:00:00Z")
        _create_backup(root, "BBBB", date="2024-06-01T00:00:00Z")
        backup = find_backup(backup_root=root)
        assert backup.udid == "BBBB"

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
        assert "restore_checkpoint" in safety.name

        # Verify the copy has the same files
        assert (safety / "Info.plist").exists()
        assert (safety / "Manifest.db").exists()

    def test_safety_copy_named_restore_checkpoint(self, tmp_dir):
        """Safety copy uses the .restore_checkpoint_ naming pattern."""
        root = tmp_dir / "backups"
        root.mkdir()
        path = _create_backup(root, "MY-BACKUP")
        safety = create_safety_copy(path)
        assert safety.name.startswith("MY-BACKUP.restore_checkpoint_")


class TestListBackupsCheckpointFiltering:
    def test_list_backups_skips_restore_checkpoints(self, tmp_dir):
        """list_backups() should not return restore checkpoint directories."""
        root = tmp_dir / "backups"
        root.mkdir()
        _create_backup(root, "REAL-BACKUP")
        # Create a restore checkpoint with valid backup files — should still be skipped
        checkpoint_name = "REAL-BACKUP.restore_checkpoint_20260228_120000_000000"
        _create_backup(root, checkpoint_name)
        backups = list_backups(root)
        assert len(backups) == 1
        assert backups[0].udid == "REAL-BACKUP"


class TestHasRestoreCheckpoint:
    def test_has_restore_checkpoint(self, tmp_dir):
        """has_restore_checkpoint() detects sibling checkpoint dirs."""
        root = tmp_dir / "backups"
        root.mkdir()
        path = _create_backup(root, "INJECTED")
        # No checkpoint yet
        assert not has_restore_checkpoint(path)
        # Create a checkpoint sibling
        (root / "INJECTED.restore_checkpoint_20260101_000000_000000").mkdir()
        assert has_restore_checkpoint(path)

    def test_has_restore_checkpoint_ignores_unrelated(self, tmp_dir):
        """Checkpoint dirs for other backups don't count."""
        root = tmp_dir / "backups"
        root.mkdir()
        path = _create_backup(root, "MY-PHONE")
        (root / "OTHER-PHONE.restore_checkpoint_20260101_000000_000000").mkdir()
        assert not has_restore_checkpoint(path)


class TestStashSafetyCopy:
    def test_stash_moves_to_hidden_dir(self, tmp_dir, monkeypatch):
        """stash_safety_copy moves the checkpoint out of the backup dir."""
        import green2blue.ios.backup as backup_mod

        stash_dir = tmp_dir / "stash"
        monkeypatch.setattr(backup_mod, "_STASH_DIR", stash_dir)

        root = tmp_dir / "backups"
        root.mkdir()
        safety = root / "PHONE.restore_checkpoint_20260314_000000_000000"
        safety.mkdir()
        (safety / "Info.plist").write_bytes(b"test")

        stashed = stash_safety_copy(safety)
        assert not safety.exists()
        assert stashed.parent == stash_dir
        assert (stashed / "Info.plist").read_bytes() == b"test"

    def test_stash_handles_name_collision(self, tmp_dir, monkeypatch):
        """Duplicate stash names get a counter suffix."""
        import green2blue.ios.backup as backup_mod

        stash_dir = tmp_dir / "stash"
        monkeypatch.setattr(backup_mod, "_STASH_DIR", stash_dir)

        stash_dir.mkdir()
        name = "PHONE.restore_checkpoint_20260314_000000_000000"
        (stash_dir / name).mkdir()  # pre-existing collision

        root = tmp_dir / "backups"
        root.mkdir()
        safety = root / name
        safety.mkdir()

        stashed = stash_safety_copy(safety)
        assert stashed.name == f"{name}_1"


class TestSmartAutoSelect:
    def test_find_backup_picks_most_recent(self, tmp_dir):
        """Auto-select chooses the most recent backup by date."""
        root = tmp_dir / "backups"
        root.mkdir()
        _create_backup(root, "OLD", device_name="Old iPhone", date="2024-01-01T00:00:00Z")
        _create_backup(root, "NEW", device_name="New iPhone", date="2026-02-28T00:00:00Z")
        _create_backup(root, "MID", device_name="Mid iPhone", date="2025-06-15T00:00:00Z")
        backup = find_backup(backup_root=root)
        assert backup.udid == "NEW"

    def test_find_backup_prefers_uninjected(self, tmp_dir):
        """Auto-select prefers uninjected backups over already-injected ones."""
        root = tmp_dir / "backups"
        root.mkdir()
        _create_backup(root, "INJECTED", device_name="Injected iPhone", date="2026-02-28T00:00:00Z")
        _create_backup(root, "FRESH", device_name="Fresh iPhone", date="2024-01-01T00:00:00Z")
        # Mark INJECTED as already having a checkpoint
        (root / "INJECTED.restore_checkpoint_20260228_000000_000000").mkdir()
        backup = find_backup(backup_root=root)
        # Should pick FRESH despite being older, because INJECTED already has a checkpoint
        assert backup.udid == "FRESH"

    def test_find_backup_all_injected_picks_most_recent(self, tmp_dir):
        """When all backups are injected, pick the most recent."""
        root = tmp_dir / "backups"
        root.mkdir()
        _create_backup(root, "OLD-INJ", date="2024-01-01T00:00:00Z")
        _create_backup(root, "NEW-INJ", date="2026-02-28T00:00:00Z")
        (root / "OLD-INJ.restore_checkpoint_20240101_000000_000000").mkdir()
        (root / "NEW-INJ.restore_checkpoint_20260228_000000_000000").mkdir()
        backup = find_backup(backup_root=root)
        assert backup.udid == "NEW-INJ"
