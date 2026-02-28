"""Exception hierarchy for green2blue.

All exceptions inherit from Green2BlueError so callers can catch broadly.
Each exception carries a `hint` attribute with a user-friendly suggestion.
"""


class Green2BlueError(Exception):
    """Base exception for all green2blue errors."""

    hint: str = ""

    def __init__(self, message: str, hint: str = ""):
        super().__init__(message)
        if hint:
            self.hint = hint


# --- Export / Parser errors ---


class ExportError(Green2BlueError):
    """The Android export ZIP is invalid or unreadable."""

    hint = "Make sure the file was exported with the 'SMS Import/Export' Android app."


class InvalidZipError(ExportError):
    """The file is not a valid ZIP archive."""

    hint = "The file does not appear to be a ZIP archive. Re-export from the Android app."


class MissingNDJSONError(ExportError):
    """The ZIP does not contain the expected messages.ndjson file."""

    hint = (
        "The ZIP must contain a 'messages.ndjson' file. "
        "Make sure you exported using the NDJSON format in SMS Import/Export."
    )


class ParseError(Green2BlueError):
    """A line in the NDJSON file could not be parsed."""

    hint = "The export file may be corrupted. Try re-exporting from the Android app."

    def __init__(self, message: str, line_number: int | None = None, hint: str = ""):
        super().__init__(message, hint=hint)
        self.line_number = line_number


# --- Backup errors ---


class BackupError(Green2BlueError):
    """An iPhone backup could not be found or is invalid."""

    hint = (
        "Create a local backup in Finder (macOS) or iTunes (Windows) first. "
        "Make sure 'Encrypt local backup' matches your intended usage."
    )


class BackupNotFoundError(BackupError):
    """No iPhone backup found at the expected location."""

    hint = (
        "No backups found. Connect your iPhone and create a local backup via "
        "Finder (macOS) or iTunes (Windows)."
    )


class MultipleBackupsError(BackupError):
    """Multiple backups found and none was specified."""

    hint = "Multiple backups found. Use --backup <path-or-udid> to choose one."


class InvalidBackupError(BackupError):
    """The backup directory is missing critical files."""

    hint = (
        "The backup appears incomplete or corrupted. "
        "Try creating a fresh backup from your iPhone."
    )


class EncryptedBackupError(BackupError):
    """The backup is encrypted but no password was provided or decryption failed."""

    hint = (
        "This backup is encrypted. Install green2blue[encrypted] and provide "
        "the backup password with --password."
    )


# --- Database errors ---


class DatabaseError(Green2BlueError):
    """An error occurred while reading or writing sms.db."""

    hint = "The sms.db file may be corrupted. Try restoring from a fresh backup."


class ManifestError(Green2BlueError):
    """An error occurred while updating Manifest.db."""

    hint = "The Manifest.db file may be corrupted. Try restoring from a fresh backup."


# --- Conversion errors ---


class ConversionError(Green2BlueError):
    """A message could not be converted from Android to iOS format."""


class PhoneNormalizationError(ConversionError):
    """A phone number could not be normalized."""

    hint = "Try specifying --country with your country code (e.g., --country US)."


# --- Verification errors ---


class VerificationError(Green2BlueError):
    """Post-injection verification failed."""

    hint = (
        "The backup may be in an inconsistent state. "
        "Restore from the safety copy (the .bak directory) and try again."
    )


# --- Crypto errors ---


class CryptoError(Green2BlueError):
    """An error in encrypted backup handling."""

    hint = "Make sure you have the correct backup password."


class CryptoDependencyError(CryptoError):
    """The cryptography package is not installed."""

    hint = "Install encrypted backup support: pip install green2blue[encrypted]"


class WrongPasswordError(CryptoError):
    """The backup password is incorrect."""

    hint = "The password you entered is incorrect. Please try again."
