"""Tests for the interactive wizard module."""

from __future__ import annotations

from contextlib import contextmanager
import json
import plistlib
import sqlite3
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from green2blue.ios.backup import BackupInfo, get_sms_db_hash
from green2blue.ios.device import DeviceCheckResult, DeviceHealthReport
from green2blue.wizard import (
    _clean_path,
    _detect_country,
    _pick_backup,
    _print_no_backups_help,
    _step_results,
    _step_workflow_choice,
    _step_welcome,
    _us_numbers_pass,
    run_wizard,
)

# -- Helpers --

def _create_backup(root: Path, udid: str, device_name: str = "Test iPhone",
                   encrypted: bool = False) -> Path:
    """Create a minimal synthetic backup for wizard testing."""
    backup_dir = root / udid
    backup_dir.mkdir(parents=True, exist_ok=True)

    (backup_dir / "Info.plist").write_bytes(plistlib.dumps({
        "Device Name": device_name,
        "Product Version": "17.4",
        "Unique Identifier": udid,
    }))
    (backup_dir / "Manifest.plist").write_bytes(plistlib.dumps({
        "IsEncrypted": encrypted,
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


def _create_export_zip(root: Path, num_messages: int = 5) -> Path:
    """Create a minimal export ZIP for wizard testing."""
    zip_path = root / "export.zip"
    records = []
    for i in range(num_messages):
        records.append({
            "address": f"+1202555{1000 + i}",
            "body": f"Test message {i}",
            "date": str(1700000000000 + i * 1000),
            "type": "1",
            "read": "1",
        })
    content = "\n".join(json.dumps(r) for r in records) + "\n"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("messages.ndjson", content)
    return zip_path


def _create_non_us_export_zip(root: Path) -> Path:
    """Create an export ZIP with non-US phone numbers."""
    zip_path = root / "non_us_export.zip"
    records = []
    # UK numbers without + prefix — these should fail US normalization
    for i in range(20):
        records.append({
            "address": f"0778800{1000 + i}",
            "body": f"UK message {i}",
            "date": str(1700000000000 + i * 1000),
            "type": "1",
            "read": "1",
        })
    content = "\n".join(json.dumps(r) for r in records) + "\n"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("messages.ndjson", content)
    return zip_path


# -- Tests --

class TestCleanPath:
    def test_strips_whitespace(self):
        assert _clean_path("  /path/to/file.zip  ") == "/path/to/file.zip"

    def test_strips_single_quotes(self):
        assert _clean_path("'/path/to/file.zip'") == "/path/to/file.zip"

    def test_strips_double_quotes(self):
        assert _clean_path('"/path/to/file.zip"') == "/path/to/file.zip"

    def test_strips_backslash_escapes(self):
        assert _clean_path("/path/to/my\\ file.zip") == "/path/to/my file.zip"

    def test_empty_string(self):
        assert _clean_path("") == ""

    def test_preserves_normal_path(self):
        assert _clean_path("/Users/test/export.zip") == "/Users/test/export.zip"


class TestDetectCountry:
    def test_us_numbers_detected(self, tmp_dir):
        zip_path = _create_export_zip(tmp_dir)
        country = _detect_country(zip_path)
        assert country == "US"

    def test_plus_prefixed_numbers_default_us(self, tmp_dir):
        """Numbers with + prefix should default to US (country code already present)."""
        zip_path = _create_export_zip(tmp_dir)  # Uses +1... numbers
        country = _detect_country(zip_path)
        assert country == "US"


class TestUsNumbersPass:
    def test_us_numbers_pass(self, tmp_dir):
        zip_path = _create_export_zip(tmp_dir)
        assert _us_numbers_pass(zip_path) is True

    def test_non_us_numbers_fail(self, tmp_dir):
        zip_path = _create_non_us_export_zip(tmp_dir)
        assert _us_numbers_pass(zip_path) is False


class TestWelcome:
    def test_prints_version(self, capsys):
        _step_welcome()
        captured = capsys.readouterr()
        assert "green2blue" in captured.out
        assert "Ctrl+C" in captured.out


class TestWorkflowChoice:
    def test_zip_path_shortcuts_to_classic_flow(self, tmp_dir):
        zip_path = _create_export_zip(tmp_dir)

        with patch("builtins.input", return_value=str(zip_path)):
            workflow, initial = _step_workflow_choice()

        assert workflow == "classic"
        assert initial == str(zip_path)


class TestNoBackupsHelp:
    def test_macos_instructions(self, capsys):
        with patch("green2blue.wizard.platform.system", return_value="Darwin"):
            _print_no_backups_help()
        captured = capsys.readouterr()
        assert "Finder" in captured.out
        assert "No iPhone backups found" in captured.out

    def test_windows_instructions(self, capsys):
        with patch("green2blue.wizard.platform.system", return_value="Windows"):
            _print_no_backups_help()
        captured = capsys.readouterr()
        assert "iTunes" in captured.out


class TestPickBackup:
    def test_pick_by_number(self, tmp_dir):
        root = tmp_dir / "backups"
        root.mkdir()
        _create_backup(root, "AAAA", "iPhone A")
        path_b = _create_backup(root, "BBBB", "iPhone B")

        backups = [
            BackupInfo(path=root / "AAAA", udid="AAAA", device_name="iPhone A",
                       product_version="17.4", is_encrypted=False),
            BackupInfo(path=path_b, udid="BBBB", device_name="iPhone B",
                       product_version="17.4", is_encrypted=False),
        ]

        with patch("builtins.input", return_value="2"):
            result = _pick_backup(backups)
        assert result.path == path_b


class TestWizardHappyPath:
    def test_full_wizard_flow(self, tmp_dir):
        """Test the full wizard flow with mocked inputs."""
        root = tmp_dir / "backups"
        root.mkdir()
        backup_path = _create_backup(root, "WIZARD-TEST")
        zip_path = _create_export_zip(tmp_dir, num_messages=3)

        backup_info = BackupInfo(
            path=backup_path, udid="WIZARD-TEST", device_name="Test iPhone",
            product_version="17.4", is_encrypted=False,
        )

        # Mock the input sequence:
        # 1. Export ZIP path
        # 2. Confirm backup (Y)
        # 3. Confirm inject (Y)
        # 4. Decline automatic device restore
        inputs = iter([str(zip_path), "y", "y", "n"])

        with (
            patch("builtins.input", side_effect=lambda _: next(inputs)),
            patch("green2blue.ios.backup.list_backups", return_value=[backup_info]),
            patch("green2blue.pipeline.run_pipeline") as mock_pipeline,
        ):
            mock_result = MagicMock()
            mock_result.injection_stats = MagicMock(
                messages_inserted=3, messages_skipped=0,
            )
            mock_result.clone_stats = None
            mock_result.overwrite_stats = None
            mock_result.total_attachments_copied = 0
            mock_result.verification = MagicMock(passed=True)
            mock_result.safety_copy_path = tmp_dir / "safety"
            mock_pipeline.return_value = mock_result

            ret = run_wizard()

        assert ret == 0
        mock_pipeline.assert_called_once()

    def test_merge_wizard_flow(self, tmp_dir):
        """Wizard can build/archive/merge and inject via the merged path."""
        root = tmp_dir / "backups"
        root.mkdir()
        backup_path = _create_backup(root, "MERGE-WIZARD")
        zip_path = _create_export_zip(tmp_dir, num_messages=4)

        backup_info = BackupInfo(
            path=backup_path, udid="MERGE-WIZARD", device_name="Merge iPhone",
            product_version="17.4", is_encrypted=False,
        )

        inputs = iter([
            "2",            # choose merge workflow
            str(zip_path),  # export zip
            "y",            # confirm backup
            "y",            # build merged archive
            "y",            # proceed with merged injection
            "n",            # decline automatic device restore
        ])

        with (
            patch("builtins.input", side_effect=lambda _: next(inputs)),
            patch("green2blue.ios.backup.list_backups", return_value=[backup_info]),
            patch("green2blue.archive.import_android_export") as mock_import_android,
            patch("green2blue.archive.import_ios_backup") as mock_import_ios,
            patch("green2blue.archive.merge_archive") as mock_merge,
            patch("green2blue.archive.build_archive_report") as mock_report,
            patch("green2blue.archive.verify_archive") as mock_verify,
            patch("green2blue.archive.stage_ios_export") as mock_stage,
            patch("green2blue.archive.verify_ios_render_target") as mock_render_verify,
            patch("green2blue.pipeline.run_pipeline") as mock_pipeline,
        ):
            mock_import_android.return_value = MagicMock(messages_imported=4)
            mock_import_ios.return_value = MagicMock(messages_imported=2)
            mock_merge.return_value = MagicMock(
                merge_run_id=7,
                merged_conversations=3,
                merged_messages=5,
                duplicate_messages=1,
            )
            mock_report.return_value = MagicMock(warnings=("reply warning",))
            mock_verify.return_value = MagicMock(
                passed=True,
                checks_passed=6,
                checks_run=6,
                warnings=(),
                errors=(),
            )
            mock_stage.return_value = MagicMock(
                records_written=3,
                stage_dir=tmp_dir / "stage",
                output_zip=tmp_dir / "stage" / "merged_export.zip",
                reused_existing=False,
                verification_passed=True,
                verification_errors=(),
            )

            mock_result = MagicMock()
            mock_result.injection_stats = MagicMock(
                messages_inserted=3, messages_skipped=0,
            )
            mock_result.clone_stats = None
            mock_result.overwrite_stats = None
            mock_result.total_attachments_copied = 0
            mock_result.verification = MagicMock(passed=True)
            mock_result.safety_copy_path = tmp_dir / "safety"
            mock_pipeline.return_value = mock_result
            mock_render_verify.return_value = MagicMock(
                passed=True,
                checks_passed=6,
                checks_run=6,
                warnings=(),
                errors=(),
            )

            ret = run_wizard()

        assert ret == 0
        mock_import_android.assert_called_once()
        mock_import_ios.assert_called_once()
        mock_merge.assert_called_once()
        mock_report.assert_called_once()
        mock_verify.assert_called_once()
        mock_stage.assert_called_once()
        mock_render_verify.assert_called_once()
        mock_pipeline.assert_called_once()

    def test_wizard_live_device_restore_flow(self, tmp_dir):
        """Wizard can doctor, create rollback backup, and restore live device."""
        root = tmp_dir / "backups"
        root.mkdir()
        backup_path = _create_backup(root, "LIVE-TEST")
        zip_path = _create_export_zip(tmp_dir, num_messages=2)

        backup_info = BackupInfo(
            path=backup_path, udid="LIVE-TEST", device_name="Live iPhone",
            product_version="17.4", is_encrypted=False,
        )

        report = DeviceHealthReport(
            udid="LIVE-TEST",
            name="Live iPhone",
            ios_version="17.4",
            product_type="iPhone15,3",
            state="ready",
            ready_for_backup_restore=True,
            hint="ready",
            checks=(
                DeviceCheckResult("Lockdown", True, "ok"),
                DeviceCheckResult("MobileBackup2 service", True, "ok"),
            ),
        )

        @contextmanager
        def _fake_device_run_session(command, metadata):
            yield MagicMock(run_dir=tmp_dir / "live-run")

        class _FakeProgress:
            def __init__(self, label):
                self.label = label

            def start(self):
                return None

            def finish(self):
                return None

            def callback(self, pct):
                return None

        inputs = iter([
            str(zip_path),  # export zip
            "y",            # use backup
            "y",            # proceed with injection
            "y",            # use live device restore
            "y",            # create rollback backup
            "y",            # restore modified backup now
        ])

        with (
            patch("builtins.input", side_effect=lambda _: next(inputs)),
            patch("green2blue.ios.backup.list_backups", return_value=[backup_info]),
            patch("green2blue.pipeline.run_pipeline") as mock_pipeline,
            patch("green2blue.ios.device.doctor_device", return_value=report) as mock_doctor,
            patch(
                "green2blue.ios.device.create_backup",
                return_value=tmp_dir / "rollback" / "LIVE-TEST",
            ) as mock_create_backup,
            patch("green2blue.ios.device.restore_backup") as mock_restore_backup,
            patch("green2blue.cli._device_run_session", side_effect=_fake_device_run_session),
            patch("green2blue.cli._ProgressReporter", _FakeProgress),
            patch("green2blue.cli._print_post_restore_instructions"),
        ):
            mock_result = MagicMock()
            mock_result.injection_stats = MagicMock(
                messages_inserted=2, messages_skipped=0,
            )
            mock_result.clone_stats = None
            mock_result.overwrite_stats = None
            mock_result.total_attachments_copied = 0
            mock_result.verification = MagicMock(passed=True)
            mock_result.safety_copy_path = tmp_dir / "safety"
            mock_pipeline.return_value = mock_result

            ret = run_wizard()

        assert ret == 0
        mock_doctor.assert_called_once_with()
        mock_create_backup.assert_called_once()
        assert mock_create_backup.call_args.kwargs["udid"] == "LIVE-TEST"
        mock_restore_backup.assert_called_once()
        assert mock_restore_backup.call_args.kwargs["backup_dir"] == backup_path.parent
        assert mock_restore_backup.call_args.kwargs["udid"] == "LIVE-TEST"

    def test_step_results_blocks_live_restore_when_rendered_target_verification_fails(self, tmp_dir):
        backup_info = BackupInfo(
            path=tmp_dir / "backup",
            udid="RENDER-FAIL",
            device_name="Render Fail iPhone",
            product_version="17.4",
            is_encrypted=False,
        )
        mock_result = MagicMock()
        mock_result.injection_stats = MagicMock(messages_inserted=2, messages_skipped=0)
        mock_result.clone_stats = None
        mock_result.overwrite_stats = None
        mock_result.total_attachments_copied = 0
        mock_result.verification = MagicMock(passed=True, errors=())
        mock_result.safety_copy_path = None

        with (
            patch("green2blue.wizard._step_offer_device_restore") as mock_offer_restore,
            patch("green2blue.wizard._print_manual_restore_instructions") as mock_manual,
        ):
            _step_results(
                mock_result,
                False,
                backup_info,
                None,
                render_target_passed=False,
                render_target_errors=("render mismatch",),
            )

        mock_offer_restore.assert_not_called()
        mock_manual.assert_called_once()

    def test_wizard_with_no_backups(self, tmp_dir):
        """Wizard exits gracefully when no backups found."""
        zip_path = _create_export_zip(tmp_dir)

        inputs = iter([str(zip_path)])

        with (
            patch("builtins.input", side_effect=lambda _: next(inputs)),
            patch("green2blue.ios.backup.list_backups", return_value=[]),
            pytest.raises(SystemExit) as exc_info,
        ):
            run_wizard()

        assert exc_info.value.code == 1

    def test_wizard_with_bad_zip(self, tmp_dir):
        """Wizard rejects a non-export ZIP, then accepts a good one."""
        bad_zip = tmp_dir / "bad.zip"
        bad_zip.write_bytes(b"not a zip")
        good_zip = _create_export_zip(tmp_dir)

        backup_info = BackupInfo(
            path=tmp_dir / "backup", udid="TEST", device_name="iPhone",
            product_version="17.4", is_encrypted=False,
        )

        # First try bad path (not a ZIP), then good path
        inputs = iter([
            str(bad_zip),      # bad file (not a valid ZIP)
            str(good_zip),     # good file
            "y",               # confirm backup
            "y",               # confirm inject
            "n",               # decline automatic device restore
        ])

        with (
            patch("builtins.input", side_effect=lambda _: next(inputs)),
            patch("green2blue.ios.backup.list_backups", return_value=[backup_info]),
            patch("green2blue.pipeline.run_pipeline") as mock_pipeline,
        ):
            mock_result = MagicMock()
            mock_result.injection_stats = MagicMock(
                messages_inserted=1, messages_skipped=0,
            )
            mock_result.clone_stats = None
            mock_result.overwrite_stats = None
            mock_result.total_attachments_copied = 0
            mock_result.verification = MagicMock(passed=True)
            mock_result.safety_copy_path = None
            mock_pipeline.return_value = mock_result

            ret = run_wizard()

        assert ret == 0

    def test_wizard_ctrl_c_aborts(self, tmp_dir):
        """Ctrl+C during wizard returns 130."""
        with patch("builtins.input", side_effect=KeyboardInterrupt):
            ret = run_wizard()
        assert ret == 130


class TestWizardEncryptedBackup:
    def test_encrypted_backup_prompts_password(self, tmp_dir):
        """Wizard prompts for password when backup is encrypted."""
        root = tmp_dir / "backups"
        root.mkdir()
        zip_path = _create_export_zip(tmp_dir)

        backup_info = BackupInfo(
            path=root / "ENC-TEST", udid="ENC-TEST",
            device_name="Encrypted iPhone", product_version="17.4",
            is_encrypted=True,
        )

        # Inputs: zip path, confirm backup, password, confirm inject, decline device restore
        inputs = iter([str(zip_path), "y", "y", "n"])

        with (
            patch("builtins.input", side_effect=lambda _: next(inputs)),
            patch("green2blue.ios.backup.list_backups", return_value=[backup_info]),
            patch("green2blue.wizard._validate_password", return_value=True),
            patch("green2blue.wizard.getpass.getpass", return_value="secret123"),
            patch("green2blue.pipeline.run_pipeline") as mock_pipeline,
        ):
            mock_result = MagicMock()
            mock_result.injection_stats = MagicMock(
                messages_inserted=5, messages_skipped=0,
            )
            mock_result.clone_stats = None
            mock_result.overwrite_stats = None
            mock_result.total_attachments_copied = 0
            mock_result.verification = MagicMock(passed=True)
            mock_result.safety_copy_path = None
            mock_pipeline.return_value = mock_result

            ret = run_wizard()

        assert ret == 0
        # Verify pipeline was called with the password
        call_kwargs = mock_pipeline.call_args
        assert call_kwargs.kwargs.get("password") == "secret123" or \
               call_kwargs[1].get("password") == "secret123"

    def test_encrypted_backup_wrong_password_retries(self, tmp_dir):
        """Wizard retries on wrong password up to 3 times then exits."""
        zip_path = _create_export_zip(tmp_dir)

        backup_info = BackupInfo(
            path=tmp_dir / "ENC-TEST", udid="ENC-TEST",
            device_name="Encrypted iPhone", product_version="17.4",
            is_encrypted=True,
        )

        inputs = iter([str(zip_path), "y"])

        with (
            patch("builtins.input", side_effect=lambda _: next(inputs)),
            patch("green2blue.ios.backup.list_backups", return_value=[backup_info]),
            patch("green2blue.wizard._validate_password", return_value=False),
            patch("green2blue.wizard.getpass.getpass", return_value="wrong"),
            pytest.raises(SystemExit) as exc_info,
        ):
            run_wizard()

        assert exc_info.value.code == 1


class TestCountryDetection:
    def test_us_numbers_detect_correctly(self, tmp_dir):
        zip_path = _create_export_zip(tmp_dir)
        # US numbers with + prefix should pass
        assert _us_numbers_pass(zip_path) is True

    def test_non_us_numbers_prompt_user(self, tmp_dir):
        """Non-US numbers should trigger a country prompt in the wizard."""
        zip_path = _create_non_us_export_zip(tmp_dir)
        assert _us_numbers_pass(zip_path) is False
