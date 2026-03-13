"""Device communication via pymobiledevice3.

Wraps pymobiledevice3 for direct USB backup/restore operations. This module
uses lazy imports so the core green2blue tool works without pymobiledevice3
installed.

Install with: pip install green2blue[device]
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from green2blue.exceptions import Green2BlueError

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

_MOBILEBACKUP_HANDSHAKE_RETRY_ATTEMPTS = 2


async def _maybe_await(value):
    """Await async pymobiledevice3 results, pass through sync values."""
    if inspect.isawaitable(value):
        return await value
    return value


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


@dataclass(frozen=True)
class DeviceCheckResult:
    """One preflight diagnostic check."""

    name: str
    ok: bool
    detail: str


@dataclass(frozen=True)
class DeviceHealthReport:
    """Aggregate preflight report for a connected device."""

    udid: str
    name: str
    ios_version: str
    product_type: str
    state: str
    ready_for_backup_restore: bool
    hint: str
    checks: tuple[DeviceCheckResult, ...]


def _classify_device_exception(exc: Exception) -> tuple[str, str]:
    """Map raw device-layer exceptions to a stable state + user hint."""
    message = f"{type(exc).__name__}: {exc}"
    lowered = message.lower()

    if "mberrordomain/208" in lowered or "device locked" in lowered:
        return (
            "device_locked",
            "Unlock the iPhone, leave it on the home screen, and enter the device "
            "passcode on the phone if it asks to authorize local backup or restore.",
        )
    if "passwordprotected" in lowered or "password protected" in lowered:
        return (
            "password_protected",
            "Unlock the iPhone with its passcode, leave it on the home screen, and retry.",
        )
    if "connectionterminatederror" in lowered or "ssl handshake is taking longer than 10 seconds" in lowered:
        return (
            "backup_authorization_pending",
            "The backup session ended before MobileBackup2 fully came up. On a freshly "
            "restored device this usually means the iPhone is waiting for an on-device "
            "passcode prompt authorizing local backup. Unlock it, watch the phone, and retry.",
        )
    if "invalidservice" in lowered:
        return (
            "requires_tunnel",
            "The backup service is not available over direct USB on this iOS build. "
            "Use a pymobiledevice3 tunnel/rsd session before retrying backup or restore.",
        )
    if "invalidhostid" in lowered:
        return (
            "invalid_host_id",
            "The Mac pairing record is stale or the restore session was interrupted. "
            "Unlock the device, reconnect it, and re-trust this Mac.",
        )
    if "missingvalue" in lowered:
        return (
            "pairing_blocked",
            "The iPhone is connected but not exposing its pairing key. "
            "Keep it unlocked on the home screen; if this follows a bad restore, use recovery mode.",
        )
    if "getprohibited" in lowered:
        return (
            "lockdown_blocked",
            "The device is blocking lockdown queries. Unlock it fully and dismiss any setup or recovery screens.",
        )
    if "not paired" in lowered or "userdeniedpairing" in lowered:
        return (
            "not_paired",
            "Unlock the iPhone and accept the 'Trust This Computer' prompt.",
        )
    if "no such device" in lowered or "devicenotfound" in lowered:
        return (
            "not_connected",
            "Reconnect the iPhone over USB and verify the cable carries data.",
        )
    return ("unknown_error", message)


def _wrap_device_exception(action: str, exc: Exception) -> DeviceError:
    """Translate third-party device exceptions into green2blue errors."""
    state, hint = _classify_device_exception(exc)
    if state in {
        "device_locked",
        "backup_authorization_pending",
        "password_protected",
        "invalid_host_id",
        "pairing_blocked",
        "lockdown_blocked",
        "not_paired",
    }:
        return DevicePairingError(f"{action}: {exc}", hint=hint)
    if state == "not_connected":
        return DeviceNotFoundError(f"{action}: {exc}", hint=hint)
    return DeviceError(f"{action}: {exc}", hint=hint)


def _is_retryable_mobilebackup_handshake_error(exc: Exception, *, progress_seen: bool) -> bool:
    """Return whether a MobileBackup2 failure is safe to retry immediately once.

    We only retry the known first-exchange protocol failure, and only if the
    operation never emitted progress. If progress has already started, retrying
    would hide a real mid-operation failure.
    """
    if progress_seen:
        return False

    lowered = f"{type(exc).__name__}: {exc}".lower()
    return (
        "protocol version exchange" in lowered
        and "error code -1" in lowered
    )


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

    async def _list_devices_async() -> list[DeviceInfo]:
        from pymobiledevice3.lockdown import create_using_usbmux
        from pymobiledevice3.usbmux import list_devices as usbmux_list

        devices = []
        for mux_device in await _maybe_await(usbmux_list()):
            try:
                lockdown = await _maybe_await(create_using_usbmux(serial=mux_device.serial))
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

    return asyncio.run(_list_devices_async())


def _get_lockdown(udid: str | None = None):
    """Get a lockdown client for a device. Auto-selects if only one device connected."""
    check_pymobiledevice3()

    return asyncio.run(_get_lockdown_async(udid))


async def _get_lockdown_async(udid: str | None = None):
    """Async lockdown lookup for use within a single event loop."""
    from pymobiledevice3.lockdown import create_using_usbmux
    from pymobiledevice3.usbmux import list_devices as usbmux_list

    mux_devices = await _maybe_await(usbmux_list())
    if not mux_devices:
        raise DeviceNotFoundError("No iOS device connected.")

    if udid:
        for d in mux_devices:
            if d.serial == udid:
                try:
                    return await _maybe_await(create_using_usbmux(serial=udid))
                except Green2BlueError:
                    raise
                except Exception as exc:
                    raise _wrap_device_exception(
                        f"Could not connect to device {udid}",
                        exc,
                    ) from exc
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

    try:
        return await _maybe_await(create_using_usbmux(serial=mux_devices[0].serial))
    except Green2BlueError:
        raise
    except Exception as exc:
        raise _wrap_device_exception(
            f"Could not connect to device {mux_devices[0].serial}",
            exc,
        ) from exc


def doctor_device(udid: str | None = None) -> DeviceHealthReport:
    """Probe the current device restore state without mutating it."""
    check_pymobiledevice3()

    async def _doctor_device_async() -> DeviceHealthReport:
        from pymobiledevice3.lockdown import create_using_usbmux
        from pymobiledevice3.services.mobilebackup2 import Mobilebackup2Service
        from pymobiledevice3.usbmux import list_devices as usbmux_list

        mux_devices = await _maybe_await(usbmux_list())
        if not mux_devices:
            raise DeviceNotFoundError("No iOS device connected.")

        selected = None
        if udid:
            for mux_device in mux_devices:
                if mux_device.serial == udid:
                    selected = mux_device
                    break
            if selected is None:
                raise DeviceNotFoundError(
                    f"Device {udid} not found.",
                    hint=f"Connected devices: {', '.join(d.serial for d in mux_devices)}",
                )
        elif len(mux_devices) == 1:
            selected = mux_devices[0]
        else:
            udids = ", ".join(d.serial for d in mux_devices)
            raise DeviceError(
                f"Multiple devices connected ({len(mux_devices)}). Specify a UDID.",
                hint=f"Available devices: {udids}",
            )

        checks: list[DeviceCheckResult] = [
            DeviceCheckResult("USBMux detection", True, f"Device visible as {selected.serial}.")
        ]

        try:
            lockdown = await _maybe_await(
                create_using_usbmux(serial=selected.serial, autopair=False)
            )
        except Exception as exc:
            state, hint = _classify_device_exception(exc)
            checks.append(DeviceCheckResult("Lockdown session", False, str(exc)))
            return DeviceHealthReport(
                udid=selected.serial,
                name="(unknown)",
                ios_version="unknown",
                product_type="unknown",
                state=state,
                ready_for_backup_restore=False,
                hint=hint,
                checks=tuple(checks),
            )

        name = getattr(lockdown, "display_name", "(unknown)")
        ios_version = getattr(lockdown, "product_version", "unknown")
        product_type = "unknown"

        checks.append(DeviceCheckResult("Lockdown session", True, "Connected without auto-pairing."))

        try:
            product_type = await _maybe_await(lockdown.get_value(key="ProductType"))
            checks.append(
                DeviceCheckResult(
                    "Identity query",
                    True,
                    f"{name} ({product_type}, iOS {ios_version}) responded to lockdown queries.",
                )
            )
        except Exception as exc:
            state, hint = _classify_device_exception(exc)
            checks.append(DeviceCheckResult("Identity query", False, str(exc)))
            return DeviceHealthReport(
                udid=selected.serial,
                name=name,
                ios_version=ios_version,
                product_type=product_type,
                state=state,
                ready_for_backup_restore=False,
                hint=hint,
                checks=tuple(checks),
            )

        state = "ready"
        ready = True
        hint = "Device is ready for backup and restore operations."

        try:
            await _maybe_await(lockdown.get_value(key="DevicePublicKey"))
            checks.append(
                DeviceCheckResult(
                    "Pairing key",
                    True,
                    "DevicePublicKey is accessible; pairing should be stable.",
                )
            )
        except Exception as exc:
            state, hint = _classify_device_exception(exc)
            ready = False
            checks.append(DeviceCheckResult("Pairing key", False, str(exc)))

        try:
            service = Mobilebackup2Service(lockdown)
            connect = getattr(service, "connect", None)
            if callable(connect):
                await _maybe_await(connect())
            close = getattr(service, "close", None)
            if callable(close):
                await _maybe_await(close())
            checks.append(
                DeviceCheckResult(
                    "MobileBackup2 service",
                    True,
                    "Backup/restore service started successfully.",
                )
            )
        except Exception as exc:
            service_state, service_hint = _classify_device_exception(exc)
            if ready:
                state = service_state
                hint = service_hint
            ready = False
            checks.append(DeviceCheckResult("MobileBackup2 service", False, str(exc)))

        return DeviceHealthReport(
            udid=selected.serial,
            name=name,
            ios_version=ios_version,
            product_type=product_type,
            state=state,
            ready_for_backup_restore=ready,
            hint=hint,
            checks=tuple(checks),
        )

    return asyncio.run(_doctor_device_async())


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
    async def _create_backup_async() -> Path:
        from pymobiledevice3.services.mobilebackup2 import Mobilebackup2Service

        try:
            lockdown = await _get_lockdown_async(udid)
            device_udid = lockdown.udid

            backup_dir.mkdir(parents=True, exist_ok=True)
            device_backup_dir = backup_dir / device_udid
            device_backup_dir.mkdir(parents=True, exist_ok=True)

            logger.info("Starting backup of %s to %s...", lockdown.display_name, device_backup_dir)

            for attempt in range(1, _MOBILEBACKUP_HANDSHAKE_RETRY_ATTEMPTS + 1):
                service = Mobilebackup2Service(lockdown)
                connect = getattr(service, "connect", None)
                close = getattr(service, "close", None)
                attempt_progress_seen = False

                def _progress_wrapper(pct: float) -> None:
                    nonlocal attempt_progress_seen
                    attempt_progress_seen = True
                    if progress_cb is not None:
                        progress_cb(pct)

                if callable(connect):
                    await _maybe_await(connect())
                try:
                    await _maybe_await(service.backup(
                        full=True,
                        backup_directory=str(backup_dir),
                        progress_callback=_progress_wrapper,
                    ))
                    break
                except Exception as exc:
                    if (
                        attempt < _MOBILEBACKUP_HANDSHAKE_RETRY_ATTEMPTS
                        and _is_retryable_mobilebackup_handshake_error(
                            exc,
                            progress_seen=attempt_progress_seen,
                        )
                    ):
                        logger.warning(
                            "Backup handshake failed before progress on attempt %d/%d; retrying once: %s",
                            attempt,
                            _MOBILEBACKUP_HANDSHAKE_RETRY_ATTEMPTS,
                            exc,
                        )
                        continue
                    raise
                finally:
                    if callable(close):
                        await _maybe_await(close())

            logger.info("Backup complete: %s", device_backup_dir)
            return device_backup_dir
        except Green2BlueError:
            raise
        except Exception as exc:
            raise _wrap_device_exception("Backup failed", exc) from exc

    return asyncio.run(_create_backup_async())


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
    async def _restore_backup_async() -> None:
        from pymobiledevice3.services.mobilebackup2 import Mobilebackup2Service
        try:
            lockdown = await _get_lockdown_async(udid)

            logger.info("Starting restore to %s...", lockdown.display_name)

            for attempt in range(1, _MOBILEBACKUP_HANDSHAKE_RETRY_ATTEMPTS + 1):
                service = Mobilebackup2Service(lockdown)
                connect = getattr(service, "connect", None)
                close = getattr(service, "close", None)
                attempt_progress_seen = False

                def _progress_wrapper(pct: float) -> None:
                    nonlocal attempt_progress_seen
                    attempt_progress_seen = True
                    if progress_cb is not None:
                        progress_cb(pct)

                # Critical flags: system + settings + remove + reboot
                # remove=True sets RemoveItemsNotRestored which triggers iOS data migration
                if callable(connect):
                    await _maybe_await(connect())
                try:
                    await _maybe_await(service.restore(
                        backup_directory=str(backup_dir),
                        system=True,
                        settings=True,
                        remove=True,
                        reboot=True,
                        password=password or "",
                        progress_callback=_progress_wrapper,
                    ))
                    break
                except Exception as exc:
                    if (
                        attempt < _MOBILEBACKUP_HANDSHAKE_RETRY_ATTEMPTS
                        and _is_retryable_mobilebackup_handshake_error(
                            exc,
                            progress_seen=attempt_progress_seen,
                        )
                    ):
                        logger.warning(
                            "Restore handshake failed before progress on attempt %d/%d; retrying once: %s",
                            attempt,
                            _MOBILEBACKUP_HANDSHAKE_RETRY_ATTEMPTS,
                            exc,
                        )
                        continue
                    raise
                finally:
                    if callable(close):
                        await _maybe_await(close())

            logger.info("Restore complete. Device will reboot.")
        except Green2BlueError:
            raise
        except Exception as exc:
            raise _wrap_device_exception("Restore failed", exc) from exc

    asyncio.run(_restore_backup_async())


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
    async def _push_synthetic_backup_async() -> None:
        from pymobiledevice3.services.mobilebackup2 import Mobilebackup2Service
        try:
            lockdown = await _get_lockdown_async(udid)

            logger.info("Pushing synthetic backup to %s (experimental)...", lockdown.display_name)

            for attempt in range(1, _MOBILEBACKUP_HANDSHAKE_RETRY_ATTEMPTS + 1):
                service = Mobilebackup2Service(lockdown)
                connect = getattr(service, "connect", None)
                close = getattr(service, "close", None)
                attempt_progress_seen = False

                def _progress_wrapper(pct: float) -> None:
                    nonlocal attempt_progress_seen
                    attempt_progress_seen = True
                    if progress_cb is not None:
                        progress_cb(pct)

                # Partial restore: system=True, remove=False (overlay, no delete)
                if callable(connect):
                    await _maybe_await(connect())
                try:
                    await _maybe_await(service.restore(
                        backup_directory=str(backup_dir),
                        system=True,
                        remove=False,
                        reboot=True,
                        progress_callback=_progress_wrapper,
                    ))
                    break
                except Exception as exc:
                    if (
                        attempt < _MOBILEBACKUP_HANDSHAKE_RETRY_ATTEMPTS
                        and _is_retryable_mobilebackup_handshake_error(
                            exc,
                            progress_seen=attempt_progress_seen,
                        )
                    ):
                        logger.warning(
                            "Synthetic restore handshake failed before progress on attempt %d/%d; retrying once: %s",
                            attempt,
                            _MOBILEBACKUP_HANDSHAKE_RETRY_ATTEMPTS,
                            exc,
                        )
                        continue
                    raise
                finally:
                    if callable(close):
                        await _maybe_await(close())

            logger.info("Synthetic restore complete. Device will reboot.")
        except Green2BlueError:
            raise
        except Exception as exc:
            raise _wrap_device_exception("Synthetic restore failed", exc) from exc

    asyncio.run(_push_synthetic_backup_async())


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
