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
from green2blue.user_paths import looks_like_path_text, normalize_user_path

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


@dataclass(frozen=True)
class BackupScanIssue:
    """Diagnostic about a discovered but unusable backup directory."""

    path: Path
    error_type: str
    message: str


@dataclass(frozen=True)
class BackupScanResult:
    """Structured result of scanning a backup root."""

    root: Path
    backups: tuple[BackupInfo, ...]
    skipped: tuple[BackupScanIssue, ...]


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
        return Path.home() / "Apple" / "MobileSync" / "Backup"
    else:
        raise BackupNotFoundError(
            f"Unsupported platform: {system}",
            hint="green2blue supports macOS and Windows.",
        )


def scan_backups(backup_root: Path | None = None) -> BackupScanResult:
    """Scan for iPhone backups and preserve diagnostics for skipped entries.

    Args:
        backup_root: Override the default backup directory.

    Returns:
        BackupScanResult with valid backups and skipped-entry diagnostics.
    """
    root = backup_root or get_backup_dir()

    if not root.exists():
        return BackupScanResult(root=root, backups=(), skipped=())

    backups: list[BackupInfo] = []
    skipped: list[BackupScanIssue] = []
    for entry in root.iterdir():
        if not entry.is_dir():
            continue
        # Skip restore checkpoint directories (green2blue safety copies)
        if ".restore_checkpoint_" in entry.name:
            continue
        try:
            info = _read_backup_info(entry)
            backups.append(info)
        except Exception as e:
            logger.debug("Skipping %s: %s", entry.name, e)
            skipped.append(
                BackupScanIssue(
                    path=entry,
                    error_type=type(e).__name__,
                    message=str(e),
                )
            )

    backups.sort(key=_backup_selection_key, reverse=True)
    return BackupScanResult(
        root=root,
        backups=tuple(backups),
        skipped=tuple(sorted(skipped, key=lambda issue: issue.path.name.lower())),
    )


def list_backups(backup_root: Path | None = None) -> list[BackupInfo]:
    """List all usable iPhone backups in recommendation order."""
    return list(scan_backups(backup_root).backups)


def find_backup(
    backup_path_or_udid: str | None = None,
    backup_root: Path | None = None,
) -> BackupInfo:
    """Find a specific backup or auto-select the best candidate.

    When no backup is specified, selects the most recent backup that has
    not yet been injected (no ``.restore_checkpoint_`` sibling). If all
    backups have been injected, falls back to the most recent overall.

    Args:
        backup_path_or_udid: Explicit path or UDID to match.
        backup_root: Override the default backup directory.

    Returns:
        BackupInfo for the matched backup.

    Raises:
        BackupNotFoundError: No matching backup found.
        MultipleBackupsError: Multiple backups match a partial UDID.
    """
    if backup_path_or_udid:
        raw_value = str(backup_path_or_udid)
        explicit_path = normalize_user_path(raw_value)
        if explicit_path.is_dir():
            return _read_backup_info(explicit_path)
        if looks_like_path_text(raw_value):
            raise BackupNotFoundError(f"No backup found at '{explicit_path}'")

        # Try as UDID
        root = backup_root or get_backup_dir()
        candidate = root / raw_value
        if candidate.is_dir():
            return _read_backup_info(candidate)

        # Search by partial UDID match
        backups = scan_backups(backup_root).backups
        matches = [b for b in backups if raw_value in b.udid]
        if len(matches) == 1:
            return matches[0]
        elif len(matches) > 1:
            raise MultipleBackupsError(
                f"Multiple backups match '{raw_value}': "
                + ", ".join(b.udid for b in matches)
            )
        raise BackupNotFoundError(
            f"No backup found matching '{raw_value}'",
        )

    # Auto-select: pick the most recent backup, preferring uninjected ones
    backups = list_backups(backup_root)
    if not backups:
        raise BackupNotFoundError("No iPhone backups found.")
    return backups[0]


def has_restore_checkpoint(backup_path: Path) -> bool:
    """Check if a backup has a restore checkpoint (previous green2blue safety copy).

    Args:
        backup_path: Path to the backup directory.

    Returns:
        True if a sibling directory matches ``{name}.restore_checkpoint_*``.
    """
    prefix = f"{backup_path.name}.restore_checkpoint_"
    parent = backup_path.parent
    if not parent.exists():
        return False
    return any(entry.is_dir() and entry.name.startswith(prefix) for entry in parent.iterdir())


def create_safety_copy(backup_path: Path) -> Path:
    """Create a safety copy of the backup directory.

    Args:
        backup_path: Path to the original backup.

    Returns:
        Path to the safety copy.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    safety_path = backup_path.parent / f"{backup_path.name}.restore_checkpoint_{timestamp}"
    logger.info("Creating safety copy: %s", safety_path)
    shutil.copytree(backup_path, safety_path, symlinks=True)
    return safety_path


_STASH_DIR = Path.home() / ".green2blue_safety_copies"


def stash_safety_copy(safety_copy_path: Path) -> Path:
    """Move a safety copy out of the MobileSync/Backup directory.

    Finder shows all backup directories as separate restore options, which
    confuses users. This moves the safety copy to a hidden stash directory
    so only the modified backup appears in Finder.

    Returns:
        New path of the stashed safety copy.
    """
    _STASH_DIR.mkdir(parents=True, exist_ok=True)
    dest = _STASH_DIR / safety_copy_path.name
    if dest.exists():
        # Append a counter to avoid collisions
        i = 1
        while dest.exists():
            dest = _STASH_DIR / f"{safety_copy_path.name}_{i}"
            i += 1
    logger.info("Stashing safety copy: %s -> %s", safety_copy_path, dest)
    shutil.move(str(safety_copy_path), str(dest))
    return dest


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


def _backup_sort_key(info: BackupInfo) -> str:
    """Return a sort key for ordering backups by date (most recent = highest).

    Uses the date string from Status.plist, falling back to Manifest.db mtime.
    """
    if info.date:
        return info.date

    # Fallback: Manifest.db modification time as ISO string
    manifest_path = info.path / "Manifest.db"
    if manifest_path.exists():
        mtime = manifest_path.stat().st_mtime
        return datetime.fromtimestamp(mtime).isoformat()

    return ""


def _backup_selection_key(info: BackupInfo) -> tuple[int, str, str]:
    """Sort usable backups the same way auto-selection prefers them."""
    return (0 if has_restore_checkpoint(info.path) else 1, _backup_sort_key(info), info.udid)


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
