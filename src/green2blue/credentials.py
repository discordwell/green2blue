"""Interactive helpers for encrypted iPhone backup passwords."""

from __future__ import annotations

import getpass
import sys
from typing import TYPE_CHECKING

from green2blue.exceptions import CryptoDependencyError, WrongPasswordError

if TYPE_CHECKING:
    from green2blue.ios.backup import BackupInfo


def is_interactive_stdin() -> bool:
    """Return True when stdin looks interactive."""
    return bool(getattr(sys.stdin, "isatty", lambda: False)())


def validate_backup_password(backup_info: BackupInfo, password: str) -> bool:
    """Validate an iPhone backup password against the backup keybag."""
    try:
        from green2blue.ios.crypto import EncryptedBackup

        encrypted = EncryptedBackup(backup_info.path, password)
        encrypted.unlock()
        return True
    except Exception:
        return False


def prompt_for_backup_password(
    backup_info: BackupInfo,
    *,
    prompt_label: str = "backup",
) -> str:
    """Prompt for an encrypted backup password and validate it."""
    if not backup_info.is_encrypted:
        return ""

    try:
        import cryptography  # noqa: F401
    except ImportError as exc:
        raise CryptoDependencyError(
            "This backup is encrypted, but cryptography is not installed."
        ) from exc

    label_text = prompt_label.strip() or "backup"
    print(f"\nThis {label_text} is encrypted. Enter the local backup password.")
    print("(This is the Finder/iTunes backup password, not your Apple ID.)\n")

    for attempt in range(3):
        password = getpass.getpass("Backup password: ").strip()
        if not password:
            print("Password cannot be empty.\n")
            continue
        if validate_backup_password(backup_info, password):
            print("Password accepted.\n")
            return password

        remaining = 2 - attempt
        if remaining > 0:
            print(f"Wrong password. {remaining} attempt(s) remaining.\n")

    raise WrongPasswordError("Wrong password. Too many attempts.")


def resolve_backup_password(
    backup_info: BackupInfo,
    password: str | None,
    *,
    prompt_label: str = "backup",
    interactive: bool | None = None,
) -> str | None:
    """Return a usable backup password, prompting when safe to do so."""
    if not backup_info.is_encrypted:
        return password
    if password:
        return password
    if interactive is None:
        interactive = is_interactive_stdin()
    if interactive:
        return prompt_for_backup_password(backup_info, prompt_label=prompt_label)
    return None
