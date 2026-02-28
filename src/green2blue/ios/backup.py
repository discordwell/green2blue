"""Find, validate, and copy iPhone backups."""

from __future__ import annotations

import logging
import platform
import plistlib
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from green2blue.exceptions import (
    BackupNotFoundError,
    InvalidBackupError,
    MultipleBackupsError,
)

logger = logging.getLogger(__name__)


@dataclass
class BackupInfo:
    """Metadata about an iPhone backup."""

    path: Path
    udid: str
    device_name: str
    product_version: str
    is_encrypted: bool
    date: str | None = None


def get_backup_dir() -> Path:
    """Return the platform-specific iPhone backup directory."""
    system = platform.system()
    if system == "Darwin":
        return Path.home() / "Library" / "Application Support" / "MobileSync" / "Backup"
    elif system == "Windows":
        # Try both locations
        appdata = Path.home() / "AppData" / "Roaming" / "Apple Computer" / "MobileSync" / "Backup"
        if appdata.exists():
            return appdata
        return (
            Path.home() / "Apple" / "MobileSync" / "Backup"
        )
    else:
        raise BackupNotFoundError(
            f"Unsupported platform: {system}",
            hint="green2blue supports macOS and Windows.",
        )


def list_backups(backup_root: Path | None = None) -> list[BackupInfo]:
    """List all available iPhone backups.

    Args:
        backup_root: Override the default backup directory.

    Returns:
        List of BackupInfo for each valid backup found.
    """
    root = backup_root or get_backup_dir()

    if not root.exists():
        return []

    backups = []
    for entry in sorted(root.iterdir()):
        if not entry.is_dir():
            continue
        try:
            info = _read_backup_info(entry)
            backups.append(info)
        except (InvalidBackupError, Exception) as e:
            logger.debug("Skipping %s: %s", entry.name, e)
            continue

    return backups


def find_backup(
    backup_path_or_udid: str | None = None,
    backup_root: Path | None = None,
) -> BackupInfo:
    """Find a specific backup or auto-select if only one exists.

    Args:
        backup_path_or_udid: Explicit path or UDID to match.
        backup_root: Override the default backup directory.

    Returns:
        BackupInfo for the matched backup.

    Raises:
        BackupNotFoundError: No matching backup found.
        MultipleBackupsError: Multiple backups and none specified.
    """
    if backup_path_or_udid:
        # Check if it's a direct path
        p = Path(backup_path_or_udid)
        if p.is_dir():
            return _read_backup_info(p)

        # Try as UDID
        root = backup_root or get_backup_dir()
        candidate = root / backup_path_or_udid
        if candidate.is_dir():
            return _read_backup_info(candidate)

        # Search by partial UDID match
        backups = list_backups(backup_root)
        matches = [b for b in backups if backup_path_or_udid in b.udid]
        if len(matches) == 1:
            return matches[0]
        elif len(matches) > 1:
            raise MultipleBackupsError(
                f"Multiple backups match '{backup_path_or_udid}': "
                + ", ".join(b.udid for b in matches)
            )
        raise BackupNotFoundError(
            f"No backup found matching '{backup_path_or_udid}'",
        )

    # Auto-select
    backups = list_backups(backup_root)
    if not backups:
        raise BackupNotFoundError("No iPhone backups found.")
    if len(backups) > 1:
        raise MultipleBackupsError(
            f"Found {len(backups)} backups. Specify one with --backup.",
        )
    return backups[0]


def create_safety_copy(backup_path: Path) -> Path:
    """Create a safety copy of the backup directory.

    Args:
        backup_path: Path to the original backup.

    Returns:
        Path to the safety copy.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    safety_path = backup_path.parent / f"{backup_path.name}.g2b_backup_{timestamp}"
    logger.info("Creating safety copy: %s", safety_path)
    shutil.copytree(backup_path, safety_path, symlinks=True)
    return safety_path


def validate_backup(backup_path: Path) -> None:
    """Validate that a backup directory has the required structure.

    Raises:
        InvalidBackupError: If critical files are missing.
    """
    required_files = ["Manifest.db", "Info.plist", "Manifest.plist"]
    missing = [f for f in required_files if not (backup_path / f).exists()]
    if missing:
        raise InvalidBackupError(
            f"Missing required files in backup: {', '.join(missing)}",
        )

    # Check that sms.db exists in the backup
    sms_hash = get_sms_db_hash()
    sms_path = backup_path / sms_hash[:2] / sms_hash
    if not sms_path.exists():
        raise InvalidBackupError(
            "sms.db not found in backup. The backup may be incomplete.",
            hint="Make sure the iPhone has Messages data and create a fresh backup.",
        )


def get_sms_db_hash() -> str:
    """Return the SHA1 hash used for sms.db in iPhone backups.

    The hash is SHA1('HomeDomain-Library/SMS/sms.db').
    """
    import hashlib

    return hashlib.sha1(b"HomeDomain-Library/SMS/sms.db").hexdigest()


def get_sms_db_path(backup_path: Path) -> Path:
    """Return the path to sms.db within a backup."""
    h = get_sms_db_hash()
    return backup_path / h[:2] / h


def _read_backup_info(path: Path) -> BackupInfo:
    """Read backup metadata from plist files."""
    info_path = path / "Info.plist"
    manifest_path = path / "Manifest.plist"

    if not info_path.exists():
        raise InvalidBackupError(f"No Info.plist in {path}")

    with open(info_path, "rb") as f:
        info = plistlib.load(f)

    is_encrypted = False
    if manifest_path.exists():
        with open(manifest_path, "rb") as f:
            manifest = plistlib.load(f)
        is_encrypted = manifest.get("IsEncrypted", False)

    # Read date from Status.plist if available
    date_str = None
    status_path = path / "Status.plist"
    if status_path.exists():
        with open(status_path, "rb") as f:
            status = plistlib.load(f)
        date_val = status.get("Date")
        if date_val:
            date_str = str(date_val)

    return BackupInfo(
        path=path,
        udid=info.get("Unique Identifier", path.name),
        device_name=info.get("Device Name", "Unknown"),
        product_version=info.get("Product Version", "Unknown"),
        is_encrypted=is_encrypted,
        date=date_str,
    )
