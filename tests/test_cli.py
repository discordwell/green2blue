"""Tests for CLI interactive confirmation and --yes flag."""

from __future__ import annotations

import argparse
import plistlib
import sqlite3
import zipfile
from pathlib import Path
from unittest.mock import patch

from green2blue.cli import _cmd_device_restore, _confirm_backup, _show_backup_list, main
from green2blue.ios.backup import BackupInfo, get_sms_db_hash
from green2blue.ios.device import DeviceInfo


def _create_backup(root: Path, udid: str, device_name: str = "Test iPhone") -> Path:
    """Create a minimal synthetic backup for CLI testing."""
    backup_dir = root / udid
    backup_dir.mkdir(parents=True, exist_ok=True)

    (backup_dir / "Info.plist").write_bytes(plistlib.dumps({
        "Device Name": device_name,
        "Product Version": "17.4",
        "Unique Identifier": udid,
    }))
    (backup_dir / "Manifest.plist").write_bytes(plistlib.dumps({
        "IsEncrypted": False,
        "Version": "3.3",
    }))
    (backup_dir / "Status.plist").write_bytes(plistlib.dumps({
        "IsFullBackup": True,
        "Version": "3.3",
        "Date": "2026-02-28T00:00:00Z",
    }))

    sms_hash = get_sms_db_hash()
    sms_dir = backup_dir / sms_hash[:2]
    sms_dir.mkdir(exist_ok=True)

    # Create Manifest.db
    manifest_db = backup_dir / "Manifest.db"
    conn = sqlite3.connect(manifest_db)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS Files (fileID TEXT PRIMARY KEY, domain TEXT, "
        "relativePath TEXT, flags INTEGER, file BLOB)"
    )
    conn.execute(
        "INSERT OR IGNORE INTO Files VALUES (?, ?, ?, ?, ?)",
        (sms_hash, "HomeDomain", "Library/SMS/sms.db", 1, b""),
    )
    conn.commit()
    conn.close()

    # Create sms.db with full schema
    sms_db_path = sms_dir / sms_hash
    sql_path = Path(__file__).parent.parent / "scripts" / "create_empty_smsdb.sql"
    conn = sqlite3.connect(sms_db_path)
    conn.executescript(sql_path.read_text())
    conn.close()

    return backup_dir


def _create_export_zip(root: Path) -> Path:
    """Create a minimal export ZIP for CLI testing."""
    import json

    zip_path = root / "export.zip"
    record = {
        "address": "+12025551234",
        "body": "CLI test",
        "date": "1700000000000",
        "type": "1",
        "read": "1",
    }
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("messages.ndjson", json.dumps(record) + "\n")
    return zip_path


def _create_synthetic_backup(root: Path, udid: str) -> Path:
    """Create a minimal synthetic backup directory with Manifest.mbdb."""
    backup_dir = root / udid
    backup_dir.mkdir(parents=True, exist_ok=True)
    (backup_dir / "Manifest.mbdb").write_bytes(b"synthetic-backup")
    return backup_dir


class TestConfirmBackup:
    def test_confirm_yes(self, tmp_dir):
        """User enters 'y' to confirm."""
        root = tmp_dir / "backups"
        root.mkdir()
        path = _create_backup(root, "TEST-UDID")
        info = BackupInfo(
            path=path, udid="TEST-UDID", device_name="Test iPhone",
            product_version="17.4", is_encrypted=False, date="2026-02-28",
        )
        with patch("builtins.input", return_value="y"):
            result = _confirm_backup(info)
        assert result == path

    def test_confirm_empty_enters_yes(self, tmp_dir):
        """Pressing Enter (empty input) confirms."""
        root = tmp_dir / "backups"
        root.mkdir()
        path = _create_backup(root, "TEST-UDID")
        info = BackupInfo(
            path=path, udid="TEST-UDID", device_name="Test iPhone",
            product_version="17.4", is_encrypted=False,
        )
        with patch("builtins.input", return_value=""):
            result = _confirm_backup(info)
        assert result == path

    def test_confirm_no(self, tmp_dir):
        """User enters 'n' to abort."""
        root = tmp_dir / "backups"
        root.mkdir()
        path = _create_backup(root, "TEST-UDID")
        info = BackupInfo(
            path=path, udid="TEST-UDID", device_name="Test iPhone",
            product_version="17.4", is_encrypted=False,
        )
        with patch("builtins.input", return_value="n"):
            result = _confirm_backup(info)
        assert result is None

    def test_confirm_eof(self, tmp_dir):
        """EOFError (piped stdin) returns None."""
        root = tmp_dir / "backups"
        root.mkdir()
        path = _create_backup(root, "TEST-UDID")
        info = BackupInfo(
            path=path, udid="TEST-UDID", device_name="Test iPhone",
            product_version="17.4", is_encrypted=False,
        )
        with patch("builtins.input", side_effect=EOFError):
            result = _confirm_backup(info)
        assert result is None


class TestShowBackupList:
    def test_pick_by_number(self, tmp_dir):
        """User picks a backup by number."""
        root = tmp_dir / "backups"
        root.mkdir()
        _create_backup(root, "AAAA", "iPhone A")
        path_b = _create_backup(root, "BBBB", "iPhone B")

        with patch("builtins.input", return_value="2"):
            result = _show_backup_list(root)
        assert result == path_b

    def test_quit(self, tmp_dir):
        """User enters 'q' to quit."""
        root = tmp_dir / "backups"
        root.mkdir()
        _create_backup(root, "AAAA")

        with patch("builtins.input", return_value="q"):
            result = _show_backup_list(root)
        assert result is None

    def test_empty_list(self, tmp_dir):
        """No backups returns None."""
        root = tmp_dir / "backups"
        root.mkdir()
        result = _show_backup_list(root)
        assert result is None


class TestYesFlag:
    def test_yes_flag_skips_prompt(self, tmp_dir):
        """--yes flag should skip confirmation and complete injection."""
        root = tmp_dir / "backups"
        root.mkdir()
        _create_backup(root, "SINGLE")
        zip_path = _create_export_zip(tmp_dir)

        ret = main([
            "inject", str(zip_path),
            "--backup-root", str(root),
            "--yes",
        ])
        assert ret == 0

    def test_explicit_backup_skips_prompt(self, tmp_dir):
        """--backup flag should skip confirmation."""
        root = tmp_dir / "backups"
        root.mkdir()
        backup_path = _create_backup(root, "EXPLICIT")
        zip_path = _create_export_zip(tmp_dir)

        ret = main([
            "inject", str(zip_path),
            "--backup", str(backup_path),
        ])
        assert ret == 0


class TestDeviceRestoreRouting:
    def test_device_restore_uses_synthetic_push(self, tmp_dir):
        root = tmp_dir / "synthetic_backups"
        root.mkdir()
        _create_synthetic_backup(root, "SYNTH-UDID")
        args = argparse.Namespace(
            backup_path=root,
            udid="SYNTH-UDID",
            yes=True,
            password=None,
        )
        device = DeviceInfo(
            udid="SYNTH-UDID",
            name="Test iPhone",
            ios_version="18.0",
            is_paired=True,
        )

        with (
            patch("green2blue.ios.device.list_devices", return_value=[device]),
            patch("green2blue.ios.device.push_synthetic_backup") as push_mock,
            patch("green2blue.ios.device.restore_backup") as restore_mock,
            patch("green2blue.cli._print_post_restore_instructions"),
        ):
            ret = _cmd_device_restore(args)

        assert ret == 0
        push_mock.assert_called_once()
        restore_mock.assert_not_called()
        assert push_mock.call_args.kwargs["backup_dir"] == root
        assert push_mock.call_args.kwargs["udid"] == "SYNTH-UDID"

    def test_device_restore_uses_full_restore_for_manifest_db(self, tmp_dir):
        root = tmp_dir / "full_backups"
        root.mkdir()
        _create_backup(root, "FULL-UDID")
        args = argparse.Namespace(
            backup_path=root,
            udid="FULL-UDID",
            yes=True,
            password="secret",
        )
        device = DeviceInfo(
            udid="FULL-UDID",
            name="Test iPhone",
            ios_version="18.0",
            is_paired=True,
        )

        with (
            patch("green2blue.ios.device.list_devices", return_value=[device]),
            patch("green2blue.ios.device.push_synthetic_backup") as push_mock,
            patch("green2blue.ios.device.restore_backup") as restore_mock,
            patch("green2blue.cli._print_post_restore_instructions"),
        ):
            ret = _cmd_device_restore(args)

        assert ret == 0
        push_mock.assert_not_called()
        restore_mock.assert_called_once()
        assert restore_mock.call_args.kwargs["backup_dir"] == root
        assert restore_mock.call_args.kwargs["udid"] == "FULL-UDID"
        assert restore_mock.call_args.kwargs["password"] == "secret"
