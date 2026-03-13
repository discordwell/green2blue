"""Tests for device module (mocked pymobiledevice3, no real device needed)."""

from __future__ import annotations

import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from green2blue.ios.device import (
    DeviceDependencyError,
    DeviceError,
    DeviceInfo,
    DeviceNotFoundError,
    DevicePairingError,
    build_device_recovery_plan,
    check_pymobiledevice3,
    doctor_device,
    extract_sms_db,
)


def _make_pmd3_mocks():
    """Create a set of mock pymobiledevice3 modules for sys.modules patching."""
    mock_pmd3 = MagicMock()
    mock_lockdown_mod = MagicMock()
    mock_usbmux_mod = MagicMock()
    mock_services = MagicMock()
    mock_mb2_mod = MagicMock()

    return {
        "pymobiledevice3": mock_pmd3,
        "pymobiledevice3.lockdown": mock_lockdown_mod,
        "pymobiledevice3.usbmux": mock_usbmux_mod,
        "pymobiledevice3.services": mock_services,
        "pymobiledevice3.services.mobilebackup2": mock_mb2_mod,
    }


# --- check_pymobiledevice3 tests ---


class TestCheckPymobiledevice3:
    def test_raises_when_not_installed(self):
        with (
            patch.dict(sys.modules, {"pymobiledevice3": None}),
            pytest.raises(DeviceDependencyError, match="not installed"),
        ):
            check_pymobiledevice3()

    def test_hint_includes_install_command(self):
        with (
            patch.dict(sys.modules, {"pymobiledevice3": None}),
            pytest.raises(DeviceDependencyError) as exc_info,
        ):
            check_pymobiledevice3()
        assert "pip install green2blue[device]" in exc_info.value.hint

    def test_succeeds_when_installed(self):
        mock_module = MagicMock()
        with patch.dict(sys.modules, {"pymobiledevice3": mock_module}):
            check_pymobiledevice3()  # Should not raise


# --- list_devices tests ---


class TestListDevices:
    def test_returns_device_info(self):
        from green2blue.ios.device import list_devices

        mocks = _make_pmd3_mocks()
        mock_lockdown = MagicMock()
        mock_lockdown.udid = "abc123"
        mock_lockdown.display_name = "Test iPhone"
        mock_lockdown.product_version = "18.0"

        mock_mux_device = MagicMock()
        mock_mux_device.serial = "abc123"

        mocks["pymobiledevice3.usbmux"].list_devices.return_value = [mock_mux_device]
        mocks["pymobiledevice3.lockdown"].create_using_usbmux.return_value = mock_lockdown

        with patch.dict(sys.modules, mocks):
            devices = list_devices()

        assert len(devices) == 1
        assert devices[0].udid == "abc123"
        assert devices[0].name == "Test iPhone"
        assert devices[0].ios_version == "18.0"
        assert devices[0].is_paired is True

    def test_handles_unpaired_device(self):
        from green2blue.ios.device import list_devices

        mocks = _make_pmd3_mocks()
        mock_mux_device = MagicMock()
        mock_mux_device.serial = "unpaired123"

        mocks["pymobiledevice3.usbmux"].list_devices.return_value = [mock_mux_device]
        mocks["pymobiledevice3.lockdown"].create_using_usbmux.side_effect = Exception(
            "Not paired"
        )

        with patch.dict(sys.modules, mocks):
            devices = list_devices()

        assert len(devices) == 1
        assert devices[0].udid == "unpaired123"
        assert devices[0].is_paired is False
        assert devices[0].name == "(not paired)"

    def test_returns_empty_when_no_devices(self):
        from green2blue.ios.device import list_devices

        mocks = _make_pmd3_mocks()
        mocks["pymobiledevice3.usbmux"].list_devices.return_value = []

        with patch.dict(sys.modules, mocks):
            devices = list_devices()
        assert devices == []

    def test_multiple_devices(self):
        from green2blue.ios.device import list_devices

        mocks = _make_pmd3_mocks()
        mock_devices = []
        mock_lockdowns = {}
        for i, (udid, name) in enumerate([("aaa", "iPhone A"), ("bbb", "iPhone B")]):
            dev = MagicMock()
            dev.serial = udid
            mock_devices.append(dev)

            ld = MagicMock()
            ld.udid = udid
            ld.display_name = name
            ld.product_version = f"18.{i}"
            mock_lockdowns[udid] = ld

        def make_lockdown(serial):
            return mock_lockdowns[serial]

        mocks["pymobiledevice3.usbmux"].list_devices.return_value = mock_devices
        mocks["pymobiledevice3.lockdown"].create_using_usbmux.side_effect = make_lockdown

        with patch.dict(sys.modules, mocks):
            devices = list_devices()

        assert len(devices) == 2
        assert devices[0].name == "iPhone A"
        assert devices[1].name == "iPhone B"


# --- create_backup tests ---


class TestCreateBackup:
    def test_calls_mobilebackup2_service(self, tmp_path):
        from green2blue.ios.device import create_backup

        mocks = _make_pmd3_mocks()
        mock_lockdown = MagicMock()
        mock_lockdown.udid = "test-udid"
        mock_lockdown.display_name = "Test iPhone"

        mock_service = MagicMock()
        mock_mb2_cls = mocks["pymobiledevice3.services.mobilebackup2"].Mobilebackup2Service
        mock_mb2_cls.return_value = mock_service

        # Mock _get_lockdown to return our mock lockdown
        with (
            patch.dict(sys.modules, mocks),
            patch("green2blue.ios.device._get_lockdown_async", new=AsyncMock(return_value=mock_lockdown)),
        ):
            result = create_backup(backup_dir=tmp_path, udid="test-udid")

        mock_mb2_cls.assert_called_once_with(mock_lockdown)
        mock_service.connect.assert_called_once()
        mock_service.backup.assert_called_once()
        mock_service.close.assert_called_once()

        call_kwargs = mock_service.backup.call_args
        assert call_kwargs.kwargs["full"] is True
        assert call_kwargs.kwargs["backup_directory"] == str(tmp_path)

        assert result == tmp_path / "test-udid"

    def test_passes_progress_callback(self, tmp_path):
        from green2blue.ios.device import create_backup

        mocks = _make_pmd3_mocks()
        mock_lockdown = MagicMock()
        mock_lockdown.udid = "test-udid"
        mock_lockdown.display_name = "Test iPhone"

        mock_service = MagicMock()
        mocks["pymobiledevice3.services.mobilebackup2"].Mobilebackup2Service.return_value = (
            mock_service
        )
        progress_fn = MagicMock()

        with (
            patch.dict(sys.modules, mocks),
            patch("green2blue.ios.device._get_lockdown_async", new=AsyncMock(return_value=mock_lockdown)),
        ):
            create_backup(backup_dir=tmp_path, udid="test-udid", progress_cb=progress_fn)

        call_kwargs = mock_service.backup.call_args
        wrapped_progress = call_kwargs.kwargs["progress_callback"]
        wrapped_progress(42.0)
        progress_fn.assert_called_once_with(42.0)

    def test_wraps_password_protected_backup_errors(self, tmp_path):
        from green2blue.ios.device import create_backup

        mocks = _make_pmd3_mocks()
        mock_lockdown = MagicMock()
        mock_lockdown.udid = "test-udid"
        mock_lockdown.display_name = "Test iPhone"

        mock_service = MagicMock()
        mock_service.backup.side_effect = Exception("PasswordProtected")
        mocks["pymobiledevice3.services.mobilebackup2"].Mobilebackup2Service.return_value = (
            mock_service
        )

        with (
            patch.dict(sys.modules, mocks),
            patch("green2blue.ios.device._get_lockdown_async", new=AsyncMock(return_value=mock_lockdown)),
            pytest.raises(DevicePairingError, match="Backup failed"),
        ):
            create_backup(backup_dir=tmp_path, udid="test-udid")

    def test_wraps_device_locked_backup_errors(self, tmp_path):
        from green2blue.ios.device import create_backup

        mocks = _make_pmd3_mocks()
        mock_lockdown = MagicMock()
        mock_lockdown.udid = "test-udid"
        mock_lockdown.display_name = "Test iPhone"

        mock_service = MagicMock()
        mock_service.backup.side_effect = Exception("Device locked (MBErrorDomain/208)")
        mocks["pymobiledevice3.services.mobilebackup2"].Mobilebackup2Service.return_value = (
            mock_service
        )

        with (
            patch.dict(sys.modules, mocks),
            patch("green2blue.ios.device._get_lockdown_async", new=AsyncMock(return_value=mock_lockdown)),
            pytest.raises(DevicePairingError) as exc_info,
        ):
            create_backup(backup_dir=tmp_path, udid="test-udid")

        assert "passcode" in exc_info.value.hint.lower()

    def test_wraps_connection_terminated_as_backup_authorization(self, tmp_path):
        from green2blue.ios.device import create_backup

        mocks = _make_pmd3_mocks()
        mock_lockdown = MagicMock()
        mock_lockdown.udid = "test-udid"
        mock_lockdown.display_name = "Test iPhone"

        mock_service = MagicMock()
        mock_service.backup.side_effect = Exception(
            "SSL handshake is taking longer than 10 seconds: aborting the connection"
        )
        mocks["pymobiledevice3.services.mobilebackup2"].Mobilebackup2Service.return_value = (
            mock_service
        )

        with (
            patch.dict(sys.modules, mocks),
            patch("green2blue.ios.device._get_lockdown_async", new=AsyncMock(return_value=mock_lockdown)),
            pytest.raises(DevicePairingError) as exc_info,
        ):
            create_backup(backup_dir=tmp_path, udid="test-udid")

        assert "authorizing local backup" in exc_info.value.hint.lower()

    def test_retries_protocol_exchange_failure_once_before_progress(self, tmp_path):
        from green2blue.ios.device import create_backup

        mocks = _make_pmd3_mocks()
        mock_lockdown = MagicMock()
        mock_lockdown.udid = "test-udid"
        mock_lockdown.display_name = "Test iPhone"

        mock_service = MagicMock()
        mock_service.backup.side_effect = [
            Exception("Could not perform backup protocol version exchange, error code -1"),
            None,
        ]
        mocks["pymobiledevice3.services.mobilebackup2"].Mobilebackup2Service.return_value = (
            mock_service
        )

        with (
            patch.dict(sys.modules, mocks),
            patch("green2blue.ios.device._get_lockdown_async", new=AsyncMock(return_value=mock_lockdown)),
        ):
            result = create_backup(backup_dir=tmp_path, udid="test-udid")

        assert result == tmp_path / "test-udid"
        assert mock_service.backup.call_count == 2

    def test_does_not_retry_protocol_exchange_failure_after_progress(self, tmp_path):
        from green2blue.ios.device import create_backup

        mocks = _make_pmd3_mocks()
        mock_lockdown = MagicMock()
        mock_lockdown.udid = "test-udid"
        mock_lockdown.display_name = "Test iPhone"

        mock_service = MagicMock()

        def _fail_after_progress(*, progress_callback=None, **_kwargs):
            assert progress_callback is not None
            progress_callback(1.0)
            raise Exception("Could not perform backup protocol version exchange, error code -1")

        mock_service.backup.side_effect = _fail_after_progress
        mocks["pymobiledevice3.services.mobilebackup2"].Mobilebackup2Service.return_value = (
            mock_service
        )

        with (
            patch.dict(sys.modules, mocks),
            patch("green2blue.ios.device._get_lockdown_async", new=AsyncMock(return_value=mock_lockdown)),
            pytest.raises(DeviceError, match="Backup failed"),
        ):
            create_backup(backup_dir=tmp_path, udid="test-udid")

        assert mock_service.backup.call_count == 1


# --- restore_backup tests ---


class TestRestoreBackup:
    def test_restore_uses_correct_flags(self, tmp_path):
        """Critical test: verify system=True, settings=True, remove=True, reboot=True."""
        from green2blue.ios.device import restore_backup

        mocks = _make_pmd3_mocks()
        mock_lockdown = MagicMock()
        mock_lockdown.display_name = "Test iPhone"

        mock_service = MagicMock()
        mocks["pymobiledevice3.services.mobilebackup2"].Mobilebackup2Service.return_value = (
            mock_service
        )

        with (
            patch.dict(sys.modules, mocks),
            patch("green2blue.ios.device._get_lockdown_async", new=AsyncMock(return_value=mock_lockdown)),
        ):
            restore_backup(backup_dir=tmp_path)

        mock_service.connect.assert_called_once()
        mock_service.restore.assert_called_once()
        mock_service.close.assert_called_once()
        call_kwargs = mock_service.restore.call_args.kwargs
        assert call_kwargs["system"] is True
        assert call_kwargs["settings"] is True
        assert call_kwargs["remove"] is True
        assert call_kwargs["reboot"] is True

    def test_restore_passes_backup_directory(self, tmp_path):
        from green2blue.ios.device import restore_backup

        mocks = _make_pmd3_mocks()
        mock_lockdown = MagicMock()
        mock_lockdown.display_name = "Test iPhone"

        mock_service = MagicMock()
        mocks["pymobiledevice3.services.mobilebackup2"].Mobilebackup2Service.return_value = (
            mock_service
        )

        with (
            patch.dict(sys.modules, mocks),
            patch("green2blue.ios.device._get_lockdown_async", new=AsyncMock(return_value=mock_lockdown)),
        ):
            restore_backup(backup_dir=tmp_path)

        call_kwargs = mock_service.restore.call_args.kwargs
        assert call_kwargs["backup_directory"] == str(tmp_path)

    def test_restore_passes_password(self, tmp_path):
        from green2blue.ios.device import restore_backup

        mocks = _make_pmd3_mocks()
        mock_lockdown = MagicMock()
        mock_lockdown.display_name = "Test iPhone"

        mock_service = MagicMock()
        mocks["pymobiledevice3.services.mobilebackup2"].Mobilebackup2Service.return_value = (
            mock_service
        )

        with (
            patch.dict(sys.modules, mocks),
            patch("green2blue.ios.device._get_lockdown_async", new=AsyncMock(return_value=mock_lockdown)),
        ):
            restore_backup(backup_dir=tmp_path, password="secret")

        call_kwargs = mock_service.restore.call_args.kwargs
        assert call_kwargs["password"] == "secret"

    def test_restore_retries_protocol_exchange_failure_once_before_progress(self, tmp_path):
        from green2blue.ios.device import restore_backup

        mocks = _make_pmd3_mocks()
        mock_lockdown = MagicMock()
        mock_lockdown.display_name = "Test iPhone"

        mock_service = MagicMock()
        mock_service.restore.side_effect = [
            Exception("Could not perform backup protocol version exchange, error code -1"),
            None,
        ]
        mocks["pymobiledevice3.services.mobilebackup2"].Mobilebackup2Service.return_value = (
            mock_service
        )

        with (
            patch.dict(sys.modules, mocks),
            patch("green2blue.ios.device._get_lockdown_async", new=AsyncMock(return_value=mock_lockdown)),
        ):
            restore_backup(backup_dir=tmp_path)

        assert mock_service.restore.call_count == 2


class TestDeviceRecoveryPlan:
    def test_restore_after_progress_requires_fresh_device_state(self):
        plan = build_device_recovery_plan(
            "restore",
            "DeviceError: Restore failed: Could not receive from mobilebackup2 (-4)",
            progress_seen=True,
            last_progress=44.9,
        )

        assert plan.classification == "partial_restore_state"
        assert plan.safe_to_retry is False
        assert "44.9%" in plan.summary
        assert any("Erase the test phone" in step for step in plan.next_steps)

    def test_find_my_restore_failure_is_retryable_after_setting_change(self):
        plan = build_device_recovery_plan(
            "restore",
            "DeviceError: Restore failed: MBErrorDomain/211",
            progress_seen=False,
        )

        assert plan.classification == "find_my_enabled"
        assert plan.safe_to_retry is True
        assert "Find My iPhone" in plan.summary

    def test_backup_authorization_failure_is_retryable(self):
        plan = build_device_recovery_plan(
            "backup",
            "DevicePairingError: Backup failed: SSL handshake is taking longer than 10 seconds: aborting the connection",
            progress_seen=False,
        )

        assert plan.classification == "backup_authorization_pending"
        assert plan.safe_to_retry is True
        assert any("Unlock the iPhone" in step for step in plan.next_steps)


# --- push_synthetic_backup tests ---


class TestPushSyntheticBackup:
    def test_synthetic_uses_remove_false(self, tmp_path):
        """Synthetic/partial backup must use remove=False to avoid wiping."""
        from green2blue.ios.device import push_synthetic_backup

        mocks = _make_pmd3_mocks()
        mock_lockdown = MagicMock()
        mock_lockdown.display_name = "Test iPhone"

        mock_service = MagicMock()
        mocks["pymobiledevice3.services.mobilebackup2"].Mobilebackup2Service.return_value = (
            mock_service
        )

        with (
            patch.dict(sys.modules, mocks),
            patch("green2blue.ios.device._get_lockdown_async", new=AsyncMock(return_value=mock_lockdown)),
        ):
            push_synthetic_backup(backup_dir=tmp_path)

        mock_service.connect.assert_called_once()
        mock_service.restore.assert_called_once()
        mock_service.close.assert_called_once()
        call_kwargs = mock_service.restore.call_args.kwargs
        assert call_kwargs["system"] is True
        assert call_kwargs["remove"] is False
        assert call_kwargs["reboot"] is True


class TestDoctorDevice:
    def test_reports_ready_device(self):
        mocks = _make_pmd3_mocks()
        mock_mux_device = MagicMock()
        mock_mux_device.serial = "abc123"

        mock_lockdown = MagicMock()
        mock_lockdown.display_name = "Test iPhone"
        mock_lockdown.product_version = "18.0"

        async def get_value(*, key=None, domain=None):
            values = {
                "ProductType": "iPhone13,2",
                "DevicePublicKey": b"pubkey",
            }
            return values[key]

        mock_lockdown.get_value = AsyncMock(side_effect=get_value)
        mocks["pymobiledevice3.usbmux"].list_devices.return_value = [mock_mux_device]
        mocks["pymobiledevice3.lockdown"].create_using_usbmux = AsyncMock(return_value=mock_lockdown)

        with patch.dict(sys.modules, mocks):
            report = doctor_device("abc123")

        assert report.ready_for_backup_restore is True
        assert report.state == "ready"
        assert report.product_type == "iPhone13,2"
        assert any(check.name == "MobileBackup2 service" and check.ok for check in report.checks)

    def test_reports_password_protected_device(self):
        mocks = _make_pmd3_mocks()
        mock_mux_device = MagicMock()
        mock_mux_device.serial = "abc123"

        mock_lockdown = MagicMock()
        mock_lockdown.display_name = "Test iPhone"
        mock_lockdown.product_version = "18.0"

        async def get_value(*, key=None, domain=None):
            values = {
                "ProductType": "iPhone13,2",
                "DevicePublicKey": b"pubkey",
            }
            return values[key]

        mock_lockdown.get_value = AsyncMock(side_effect=get_value)
        mocks["pymobiledevice3.usbmux"].list_devices.return_value = [mock_mux_device]
        mocks["pymobiledevice3.lockdown"].create_using_usbmux = AsyncMock(return_value=mock_lockdown)
        mocks["pymobiledevice3.services.mobilebackup2"].Mobilebackup2Service.side_effect = Exception(
            "PasswordProtected"
        )

        with patch.dict(sys.modules, mocks):
            report = doctor_device("abc123")

        assert report.ready_for_backup_restore is False
        assert report.state == "password_protected"
        assert "Unlock the iPhone" in report.hint

    def test_reports_requires_tunnel_when_backup_service_is_invalid(self):
        mocks = _make_pmd3_mocks()
        mock_mux_device = MagicMock()
        mock_mux_device.serial = "abc123"

        mock_lockdown = MagicMock()
        mock_lockdown.display_name = "Test iPhone"
        mock_lockdown.product_version = "18.0"

        async def get_value(*, key=None, domain=None):
            values = {
                "ProductType": "iPhone13,2",
                "DevicePublicKey": b"pubkey",
            }
            return values[key]

        mock_lockdown.get_value = AsyncMock(side_effect=get_value)
        mock_service = MagicMock()
        mock_service.connect.side_effect = Exception("InvalidService")

        mocks["pymobiledevice3.usbmux"].list_devices.return_value = [mock_mux_device]
        mocks["pymobiledevice3.lockdown"].create_using_usbmux = AsyncMock(return_value=mock_lockdown)
        mocks["pymobiledevice3.services.mobilebackup2"].Mobilebackup2Service.return_value = (
            mock_service
        )

        with patch.dict(sys.modules, mocks):
            report = doctor_device("abc123")

        assert report.ready_for_backup_restore is False
        assert report.state == "requires_tunnel"
        assert "tunnel" in report.hint.lower()


# --- extract_sms_db tests ---


class TestExtractSmsDb:
    def test_finds_sms_db_at_hash_path(self, tmp_path):
        import hashlib

        file_id = hashlib.sha1(b"HomeDomain-Library/SMS/sms.db").hexdigest()
        sms_path = tmp_path / file_id[:2] / file_id
        sms_path.parent.mkdir(parents=True)
        sms_path.write_bytes(b"fake sms data")

        result = extract_sms_db(tmp_path)
        assert result == sms_path

    def test_raises_when_not_found(self, tmp_path):
        with pytest.raises(DeviceError, match="Could not find sms.db"):
            extract_sms_db(tmp_path)


# --- _get_lockdown tests ---


class TestGetLockdown:
    def test_raises_when_no_devices(self):
        from green2blue.ios.device import _get_lockdown

        mocks = _make_pmd3_mocks()
        mocks["pymobiledevice3.usbmux"].list_devices.return_value = []

        with (
            patch.dict(sys.modules, mocks),
            pytest.raises(DeviceNotFoundError, match="No iOS device"),
        ):
            _get_lockdown()

    def test_raises_when_multiple_and_no_udid(self):
        from green2blue.ios.device import _get_lockdown

        mocks = _make_pmd3_mocks()
        mock_devices = [MagicMock(), MagicMock()]
        mock_devices[0].serial = "aaa"
        mock_devices[1].serial = "bbb"
        mocks["pymobiledevice3.usbmux"].list_devices.return_value = mock_devices

        with (
            patch.dict(sys.modules, mocks),
            pytest.raises(DeviceError, match="Multiple devices"),
        ):
            _get_lockdown()

    def test_raises_when_udid_not_found(self):
        from green2blue.ios.device import _get_lockdown

        mocks = _make_pmd3_mocks()
        mock_device = MagicMock()
        mock_device.serial = "aaa"
        mocks["pymobiledevice3.usbmux"].list_devices.return_value = [mock_device]

        with (
            patch.dict(sys.modules, mocks),
            pytest.raises(DeviceNotFoundError, match="not found"),
        ):
            _get_lockdown(udid="bbb")

    def test_auto_selects_single_device(self):
        from green2blue.ios.device import _get_lockdown

        mocks = _make_pmd3_mocks()
        mock_device = MagicMock()
        mock_device.serial = "single-udid"
        mock_lockdown = MagicMock()
        mocks["pymobiledevice3.usbmux"].list_devices.return_value = [mock_device]
        mocks["pymobiledevice3.lockdown"].create_using_usbmux.return_value = mock_lockdown

        with patch.dict(sys.modules, mocks):
            result = _get_lockdown()

        mocks["pymobiledevice3.lockdown"].create_using_usbmux.assert_called_once_with(
            serial="single-udid"
        )
        assert result is mock_lockdown


# --- DeviceInfo tests ---


class TestDeviceInfo:
    def test_frozen(self):
        info = DeviceInfo(udid="abc", name="iPhone", ios_version="18.0", is_paired=True)
        with pytest.raises(AttributeError):
            info.udid = "xyz"

    def test_fields(self):
        info = DeviceInfo(
            udid="test-udid",
            name="Test iPhone",
            ios_version="17.5.1",
            is_paired=False,
        )
        assert info.udid == "test-udid"
        assert info.name == "Test iPhone"
        assert info.ios_version == "17.5.1"
        assert info.is_paired is False
