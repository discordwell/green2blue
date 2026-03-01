"""Device communication via pymobiledevice3.

Wraps pymobiledevice3 for direct USB backup/restore operations. This module
uses lazy imports so the core green2blue tool works without pymobiledevice3
installed.

Install with: pip install green2blue[device]
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from green2blue.exceptions import Green2BlueError

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)


class DeviceError(Green2BlueError):
    """An error in device communication."""

    hint = "Make sure your device is connected via USB and trusted."


class DeviceDependencyError(DeviceError):
    """pymobiledevice3 is not installed."""

    hint = "Install device support: pip install green2blue[device]"


class DeviceNotFoundError(DeviceError):
    """No iOS device found."""

    hint = "Connect your iPhone via USB and trust the computer when prompted."


class DevicePairingError(DeviceError):
    """Device is not paired/trusted."""

    hint = "Unlock your iPhone and tap 'Trust' when the trust dialog appears."


@dataclass(frozen=True)
class DeviceInfo:
    """Information about a connected iOS device."""

    udid: str
    name: str
    ios_version: str
    is_paired: bool


def check_pymobiledevice3() -> None:
    """Check that pymobiledevice3 is available. Raises DeviceDependencyError if not."""
    try:
        import pymobiledevice3  # noqa: F401
    except ImportError as e:
        raise DeviceDependencyError(
            "pymobiledevice3 is not installed.",
            hint="Install device support: pip install green2blue[device]",
        ) from e


def list_devices() -> list[DeviceInfo]:
    """List connected iOS devices.

    Returns:
        List of DeviceInfo for each connected device.
    """
    check_pymobiledevice3()

    from pymobiledevice3.lockdown import create_using_usbmux
    from pymobiledevice3.usbmux import list_devices as usbmux_list

    devices = []
    for mux_device in usbmux_list():
        try:
            lockdown = create_using_usbmux(serial=mux_device.serial)
            info = DeviceInfo(
                udid=lockdown.udid,
                name=lockdown.display_name,
                ios_version=lockdown.product_version,
                is_paired=True,
            )
            devices.append(info)
        except Exception as e:
            logger.debug("Could not connect to device %s: %s", mux_device.serial, e)
            devices.append(
                DeviceInfo(
                    udid=mux_device.serial,
                    name="(not paired)",
                    ios_version="unknown",
                    is_paired=False,
                )
            )

    return devices


def _get_lockdown(udid: str | None = None):
    """Get a lockdown client for a device. Auto-selects if only one device connected."""
    check_pymobiledevice3()

    from pymobiledevice3.lockdown import create_using_usbmux
    from pymobiledevice3.usbmux import list_devices as usbmux_list

    mux_devices = usbmux_list()
    if not mux_devices:
        raise DeviceNotFoundError("No iOS device connected.")

    if udid:
        for d in mux_devices:
            if d.serial == udid:
                return create_using_usbmux(serial=udid)
        raise DeviceNotFoundError(
            f"Device {udid} not found.",
            hint=f"Connected devices: {', '.join(d.serial for d in mux_devices)}",
        )

    if len(mux_devices) > 1:
        udids = ", ".join(d.serial for d in mux_devices)
        raise DeviceError(
            f"Multiple devices connected ({len(mux_devices)}). Specify a UDID.",
            hint=f"Available devices: {udids}",
        )

    return create_using_usbmux(serial=mux_devices[0].serial)


def create_backup(
    backup_dir: Path,
    udid: str | None = None,
    password: str | None = None,
    progress_cb: Callable[[float], None] | None = None,
) -> Path:
    """Create a full backup of a connected iOS device.

    Args:
        backup_dir: Directory to store the backup. A subdirectory named after
            the device UDID will be created.
        udid: Target device UDID (auto-select if None and only one device).
        password: Backup encryption password (None = unencrypted).
        progress_cb: Optional callback receiving progress percentage (0.0-100.0).

    Returns:
        Path to the created backup directory (backup_dir / udid).
    """
    from pymobiledevice3.services.mobilebackup2 import Mobilebackup2Service

    lockdown = _get_lockdown(udid)
    device_udid = lockdown.udid

    backup_dir.mkdir(parents=True, exist_ok=True)
    device_backup_dir = backup_dir / device_udid
    device_backup_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Starting backup of %s to %s...", lockdown.display_name, device_backup_dir)

    service = Mobilebackup2Service(lockdown)

    service.backup(
        full=True,
        backup_directory=str(backup_dir),
        progress_callback=progress_cb,
    )

    logger.info("Backup complete: %s", device_backup_dir)
    return device_backup_dir


def restore_backup(
    backup_dir: Path,
    udid: str | None = None,
    password: str | None = None,
    progress_cb: Callable[[float], None] | None = None,
) -> None:
    """Restore a backup to a connected iOS device.

    Uses system=True, settings=True, remove=True, reboot=True — the critical
    flags discovered from idevicebackup2 issue #1504 that trigger iOS post-restore
    data migration, making sms.db usable.

    Args:
        backup_dir: Backup directory (the parent containing the UDID subdir).
        udid: Target device UDID (auto-select if None and only one device).
        password: Backup encryption password (None = unencrypted).
        progress_cb: Optional callback receiving progress percentage (0.0-100.0).
    """
    from pymobiledevice3.services.mobilebackup2 import Mobilebackup2Service

    lockdown = _get_lockdown(udid)

    logger.info("Starting restore to %s...", lockdown.display_name)

    service = Mobilebackup2Service(lockdown)

    # Critical flags: system + settings + remove + reboot
    # remove=True sets RemoveItemsNotRestored which triggers iOS data migration
    service.restore(
        backup_directory=str(backup_dir),
        system=True,
        settings=True,
        remove=True,
        reboot=True,
        progress_callback=progress_cb,
    )

    logger.info("Restore complete. Device will reboot.")


def push_synthetic_backup(
    backup_dir: Path,
    udid: str | None = None,
    progress_cb: Callable[[float], None] | None = None,
) -> None:
    """Restore a synthetic/partial backup (experimental).

    Uses remove=False to overlay files without deleting existing data.
    Note: SMS may NOT work with this mode — iOS data migration requires remove=True.

    Args:
        backup_dir: Backup directory (the parent containing the UDID subdir).
        udid: Target device UDID (auto-select if None and only one device).
        progress_cb: Optional callback receiving progress percentage (0.0-100.0).
    """
    from pymobiledevice3.services.mobilebackup2 import Mobilebackup2Service

    lockdown = _get_lockdown(udid)

    logger.info("Pushing synthetic backup to %s (experimental)...", lockdown.display_name)

    service = Mobilebackup2Service(lockdown)

    # Partial restore: system=True, remove=False (overlay, no delete)
    service.restore(
        backup_directory=str(backup_dir),
        system=True,
        remove=False,
        reboot=True,
        progress_callback=progress_cb,
    )

    logger.info("Synthetic restore complete. Device will reboot.")


def extract_sms_db(backup_dir: Path) -> Path:
    """Locate sms.db within a pymobiledevice3-created backup.

    Args:
        backup_dir: The backup directory (UDID-named subdirectory).

    Returns:
        Path to the sms.db file within the backup.

    Raises:
        DeviceError: If sms.db cannot be found.
    """
    from green2blue.ios.manifest import compute_file_id

    # sms.db is stored at the standard hash path
    file_id = compute_file_id("HomeDomain", "Library/SMS/sms.db")
    sms_path = backup_dir / file_id[:2] / file_id

    if sms_path.exists():
        return sms_path

    # Fallback: search by hash name (files in iOS backups have no extension)
    for candidate in backup_dir.rglob(file_id):
        if candidate.is_file():
            return candidate

    raise DeviceError(
        "Could not find sms.db in backup.",
        hint=f"Expected at {sms_path}. Is this a valid iOS backup?",
    )
