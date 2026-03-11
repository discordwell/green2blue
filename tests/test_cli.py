"""Tests for CLI interactive confirmation and --yes flag."""

from __future__ import annotations

import argparse
import logging
import plistlib
import sqlite3
import zipfile
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from green2blue.cli import (
    _capture_mobiledevice_logs,
    _cmd_device_doctor,
    _cmd_device_restore,
    _confirm_backup,
    _device_run_session,
    _format_progress_heartbeat,
    _show_backup_list,
    main,
)
from green2blue.ios.backup import BackupInfo, get_sms_db_hash
from green2blue.ios.device import DeviceCheckResult, DeviceHealthReport, DeviceInfo


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


def _ready_report(udid: str = "TEST-UDID") -> DeviceHealthReport:
    return DeviceHealthReport(
        udid=udid,
        name="Test iPhone",
        ios_version="18.0",
        product_type="iPhone13,2",
        state="ready",
        ready_for_backup_restore=True,
        hint="ready",
        checks=(DeviceCheckResult("USBMux detection", True, "ok"),),
    )


def _blocked_report(udid: str = "TEST-UDID") -> DeviceHealthReport:
    return DeviceHealthReport(
        udid=udid,
        name="Test iPhone",
        ios_version="18.0",
        product_type="iPhone13,2",
        state="password_protected",
        ready_for_backup_restore=False,
        hint="Unlock device",
        checks=(DeviceCheckResult("MobileBackup2 service", False, "PasswordProtected"),),
    )


@contextmanager
def _fake_device_run_session(*_args, **_kwargs):
    yield MagicMock()


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
            patch("green2blue.ios.device.doctor_device", return_value=_ready_report("SYNTH-UDID")),
            patch("green2blue.ios.device.push_synthetic_backup") as push_mock,
            patch("green2blue.ios.device.restore_backup") as restore_mock,
            patch("green2blue.cli._device_run_session", _fake_device_run_session),
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
            patch("green2blue.ios.device.doctor_device", return_value=_ready_report("FULL-UDID")),
            patch("green2blue.ios.device.push_synthetic_backup") as push_mock,
            patch("green2blue.ios.device.restore_backup") as restore_mock,
            patch("green2blue.cli._device_run_session", _fake_device_run_session),
            patch("green2blue.cli._print_post_restore_instructions"),
        ):
            ret = _cmd_device_restore(args)

        assert ret == 0
        push_mock.assert_not_called()
        restore_mock.assert_called_once()
        assert restore_mock.call_args.kwargs["backup_dir"] == root
        assert restore_mock.call_args.kwargs["udid"] == "FULL-UDID"
        assert restore_mock.call_args.kwargs["password"] == "secret"

    def test_device_restore_refuses_when_doctor_blocks(self, tmp_dir):
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
            patch("green2blue.ios.device.doctor_device", return_value=_blocked_report("FULL-UDID")),
            patch("green2blue.ios.device.push_synthetic_backup") as push_mock,
            patch("green2blue.ios.device.restore_backup") as restore_mock,
            patch("green2blue.cli._device_run_session", _fake_device_run_session),
            patch("green2blue.cli._print_post_restore_instructions"),
        ):
            ret = _cmd_device_restore(args)

        assert ret == 1
        push_mock.assert_not_called()
        restore_mock.assert_not_called()


class TestDeviceDoctorCommand:
    def test_device_doctor_returns_nonzero_when_not_ready(self):
        args = argparse.Namespace(udid="TEST-UDID")

        with patch("green2blue.ios.device.doctor_device", return_value=_blocked_report()):
            ret = _cmd_device_doctor(args)

        assert ret == 1


class TestDeviceRunArtifacts:
    def test_device_run_session_writes_metadata_and_logs(self, tmp_dir):
        run_root = tmp_dir / "runs"

        def fake_capture(log_path, _started_at):
            log_path.write_text("host logs")

        with (
            patch("green2blue.cli._default_device_run_root", return_value=run_root),
            patch("green2blue.cli._capture_mobiledevice_logs", side_effect=fake_capture),
        ):
            with _device_run_session("restore", {"device_udid": "abc123"}) as artifacts:
                logging.getLogger("green2blue.tests").warning("session works")

        assert artifacts.run_dir.exists()
        assert artifacts.metadata_path.exists()
        assert artifacts.mobiledevice_log_path.read_text() == "host logs"
        assert "session works" in artifacts.log_path.read_text()

    def test_capture_mobiledevice_logs_writes_stdout(self, tmp_dir):
        output_path = tmp_dir / "mobiledevice.log"
        completed = MagicMock(returncode=0, stdout="usb log", stderr="")

        with patch("green2blue.cli.subprocess.run", return_value=completed) as run_mock:
            _capture_mobiledevice_logs(output_path, started_at=datetime(2026, 3, 10, 12, 0, 0))

        assert output_path.read_text() == "usb log"
        assert run_mock.called


class TestProgressFormatting:
    def test_format_progress_heartbeat_waiting(self):
        message = _format_progress_heartbeat("Restore", None, None, 30.0)
        assert "waiting for progress callbacks" in message

    def test_format_progress_heartbeat_stalled(self):
        message = _format_progress_heartbeat("Restore", 44.9, 18.0, 30.0)
        assert "44.9%" in message
        assert "18s ago" in message


class TestSmartNoArgs:
    def test_no_args_tty_launches_wizard(self):
        """No args with TTY stdin should launch wizard."""
        with (
            patch("sys.stdin") as mock_stdin,
            patch("green2blue.wizard.run_wizard", return_value=0) as mock_wizard,
        ):
            mock_stdin.isatty.return_value = True
            ret = main([])
        assert ret == 0
        mock_wizard.assert_called_once()

    def test_no_args_non_tty_prints_help(self, capsys):
        """No args without TTY should print help."""
        with patch("sys.stdin") as mock_stdin:
            mock_stdin.isatty.return_value = False
            ret = main([])
        assert ret == 1

    def test_zip_arg_suggests_inject(self, capsys):
        """A bare .zip arg should suggest 'green2blue inject'."""
        ret = main(["export.zip"])
        captured = capsys.readouterr()
        assert "Did you mean" in captured.err
        assert "green2blue inject export.zip" in captured.err
        assert ret == 1


class TestQuickstartCommand:
    def test_quickstart_prints_guide(self, capsys):
        """quickstart subcommand prints the walkthrough."""
        ret = main(["quickstart"])
        captured = capsys.readouterr()
        assert "Quick Start Guide" in captured.out
        assert "SMS Import/Export" in captured.out
        assert "Restore Backup" in captured.out
        assert ret == 0


class TestWizardSubcommand:
    def test_wizard_subcommand_launches_wizard(self):
        """'green2blue wizard' should launch the wizard."""
        with patch("green2blue.wizard.run_wizard", return_value=0) as mock_wizard:
            ret = main(["wizard"])
        assert ret == 0
        mock_wizard.assert_called_once()


class TestInjectHelpGroups:
    def test_inject_help_has_common_and_advanced(self, capsys):
        """inject --help should show 'Common options' and 'Advanced options'."""
        with pytest.raises(SystemExit):
            main(["inject", "--help"])
        captured = capsys.readouterr()
        assert "Common options" in captured.out
        assert "Advanced options" in captured.out
