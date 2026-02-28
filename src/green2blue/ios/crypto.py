"""Encrypted iPhone backup support.

Handles decryption and re-encryption of encrypted iPhone backups:
- Parse keybag from Manifest.plist (BackupKeyBag)
- Derive encryption key from user password via PBKDF2
- Unwrap class keys using AES key unwrap (RFC3394)
- Decrypt/re-encrypt Manifest.db and individual files (sms.db)

Requires the `cryptography` package (optional dependency).
Install via: pip install green2blue[encrypted]
"""

from __future__ import annotations

import logging
import plistlib
import struct
import tempfile
from pathlib import Path

from green2blue.exceptions import (
    CryptoDependencyError,
    CryptoError,
    WrongPasswordError,
)

logger = logging.getLogger(__name__)

try:
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    from cryptography.hazmat.primitives.keywrap import aes_key_unwrap

    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False


def check_crypto_available() -> None:
    """Raise if the cryptography package is not installed."""
    if not HAS_CRYPTO:
        raise CryptoDependencyError(
            "The 'cryptography' package is required for encrypted backups."
        )


# --- Keybag parsing ---

# Keybag tag identifiers (4-byte big-endian tags)
KEYBAG_TAGS = {
    b"VERS": "version",
    b"TYPE": "type",
    b"UUID": "uuid",
    b"HMCK": "hmck",
    b"WRAP": "wrap",
    b"SALT": "salt",
    b"ITER": "iter",
    b"DPWT": "dpwt",
    b"DPIC": "dpic",
    b"DPSL": "dpsl",
    b"CLAS": "class",
    b"WPKY": "wpky",
    b"KTYP": "ktyp",
}


class KeybagKey:
    """A single class key from the keybag."""

    def __init__(self):
        self.protection_class: int = 0
        self.wrapped_key: bytes = b""
        self.key_type: int = 0
        self.unwrapped_key: bytes = b""


class Keybag:
    """Parsed iOS keybag containing class keys."""

    def __init__(self):
        self.version: int = 0
        self.type: int = 0
        self.uuid: bytes = b""
        self.salt: bytes = b""
        self.iterations: int = 0
        self.dpwt: int = 0  # Double-protection wrap type
        self.dpic: int = 0  # Double-protection iteration count
        self.dpsl: bytes = b""  # Double-protection salt
        self.keys: list[KeybagKey] = []


def parse_keybag(data: bytes) -> Keybag:
    """Parse an iOS BackupKeyBag binary blob.

    The keybag is a sequence of TLV (tag-length-value) records.
    Each record: 4-byte tag + 4-byte big-endian length + value bytes.
    """
    keybag = Keybag()
    current_key: KeybagKey | None = None
    offset = 0

    while offset + 8 <= len(data):
        tag = data[offset : offset + 4]
        length = struct.unpack(">I", data[offset + 4 : offset + 8])[0]
        value = data[offset + 8 : offset + 8 + length]
        offset += 8 + length

        if tag == b"VERS":
            keybag.version = struct.unpack(">I", value)[0] if len(value) == 4 else 0
        elif tag == b"TYPE":
            keybag.type = struct.unpack(">I", value)[0] if len(value) == 4 else 0
        elif tag == b"UUID":
            if not keybag.uuid:
                # First UUID is the keybag-level UUID, not a key
                keybag.uuid = value
            else:
                # Subsequent UUIDs signal start of a new class key
                current_key = KeybagKey()
                keybag.keys.append(current_key)
        elif tag == b"SALT":
            keybag.salt = value
        elif tag == b"ITER":
            keybag.iterations = struct.unpack(">I", value)[0] if len(value) == 4 else 0
        elif tag == b"DPWT":
            keybag.dpwt = struct.unpack(">I", value)[0] if len(value) == 4 else 0
        elif tag == b"DPIC":
            keybag.dpic = struct.unpack(">I", value)[0] if len(value) == 4 else 0
        elif tag == b"DPSL":
            keybag.dpsl = value
        elif tag == b"CLAS" and current_key:
            pclass = struct.unpack(">I", value)[0] if len(value) == 4 else 0
            current_key.protection_class = pclass
        elif tag == b"WPKY" and current_key:
            current_key.wrapped_key = value
        elif tag == b"KTYP" and current_key:
            ktype = struct.unpack(">I", value)[0] if len(value) == 4 else 0
            current_key.key_type = ktype

    return keybag


def derive_key_from_password(password: str, keybag: Keybag) -> bytes:
    """Derive the backup encryption key from the user's password.

    iOS uses a two-round key derivation:
    1. PBKDF2-SHA256 with dpsl/dpic (if present, iOS 10.2+)
    2. PBKDF2-SHA1 with salt/iterations

    Args:
        password: The backup password.
        keybag: Parsed keybag with salt and iteration counts.

    Returns:
        32-byte derived key.
    """
    check_crypto_available()

    password_bytes = password.encode("utf-8")

    # Round 1: PBKDF2-SHA256 with double-protection parameters (iOS 10.2+)
    if keybag.dpsl and keybag.dpic > 0:
        kdf1 = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=keybag.dpsl,
            iterations=keybag.dpic,
        )
        password_bytes = kdf1.derive(password_bytes)

    # Round 2: PBKDF2-SHA1 with keybag salt/iterations
    kdf2 = PBKDF2HMAC(
        algorithm=hashes.SHA1(),
        length=32,
        salt=keybag.salt,
        iterations=keybag.iterations,
    )
    return kdf2.derive(password_bytes)


def unwrap_class_keys(keybag: Keybag, derived_key: bytes) -> dict[int, bytes]:
    """Unwrap all class keys in the keybag using the derived key.

    Uses AES key unwrap (RFC 3394).

    Returns:
        Dict of {protection_class: unwrapped_key}.

    Raises:
        WrongPasswordError: If key unwrapping fails (wrong password).
    """
    check_crypto_available()

    class_keys: dict[int, bytes] = {}

    for key in keybag.keys:
        if not key.wrapped_key or key.protection_class == 0:
            continue

        try:
            unwrapped = aes_key_unwrap(derived_key, key.wrapped_key)
            key.unwrapped_key = unwrapped
            class_keys[key.protection_class] = unwrapped
        except Exception:
            # Key unwrap failure usually means wrong password
            continue

    if not class_keys:
        raise WrongPasswordError("Failed to unwrap any class keys. Wrong password?")

    return class_keys


def decrypt_file(
    encrypted_data: bytes,
    encryption_key: bytes,
    protection_class: int,
    class_keys: dict[int, bytes],
) -> bytes:
    """Decrypt a single file from an encrypted backup.

    Files are encrypted with AES-256-CBC using a per-file key.
    The per-file key is itself wrapped with the class key.

    Args:
        encrypted_data: The encrypted file content.
        encryption_key: The per-file EncryptionKey from Manifest.db.
        protection_class: The file's protection class.
        class_keys: Unwrapped class keys from the keybag.

    Returns:
        Decrypted file content.
    """
    check_crypto_available()

    class_key = class_keys.get(protection_class)
    if not class_key:
        raise CryptoError(
            f"No class key for protection class {protection_class}"
        )

    # Strip 4-byte protection class prefix if present
    wrapped_file_key = encryption_key[4:] if len(encryption_key) >= 4 else encryption_key

    try:
        file_key = aes_key_unwrap(class_key, wrapped_file_key)
    except Exception as e:
        raise CryptoError(f"Failed to unwrap file key: {e}") from e

    # AES-256-CBC decryption with zero IV
    iv = b"\x00" * 16
    cipher = Cipher(algorithms.AES(file_key), modes.CBC(iv))
    decryptor = cipher.decryptor()
    decrypted = decryptor.update(encrypted_data) + decryptor.finalize()

    # Remove PKCS7 padding
    if decrypted:
        pad_len = decrypted[-1]
        if 0 < pad_len <= 16 and all(b == pad_len for b in decrypted[-pad_len:]):
            decrypted = decrypted[:-pad_len]

    return decrypted


def encrypt_file(
    plaintext: bytes,
    encryption_key: bytes,
    protection_class: int,
    class_keys: dict[int, bytes],
) -> bytes:
    """Re-encrypt a file for an encrypted backup.

    Uses the same per-file key (from encryption_key) and class key.

    Args:
        plaintext: The plaintext file content.
        encryption_key: The per-file EncryptionKey from Manifest.db.
        protection_class: The file's protection class.
        class_keys: Unwrapped class keys from the keybag.

    Returns:
        Encrypted file content.
    """
    check_crypto_available()

    class_key = class_keys.get(protection_class)
    if not class_key:
        raise CryptoError(
            f"No class key for protection class {protection_class}"
        )

    wrapped_file_key = encryption_key[4:] if len(encryption_key) >= 4 else encryption_key

    try:
        file_key = aes_key_unwrap(class_key, wrapped_file_key)
    except Exception as e:
        raise CryptoError(f"Failed to unwrap file key for encryption: {e}") from e

    # PKCS7 padding
    pad_len = 16 - (len(plaintext) % 16)
    padded = plaintext + bytes([pad_len] * pad_len)

    # AES-256-CBC encryption with zero IV
    iv = b"\x00" * 16
    cipher = Cipher(algorithms.AES(file_key), modes.CBC(iv))
    encryptor = cipher.encryptor()
    return encryptor.update(padded) + encryptor.finalize()


class EncryptedBackup:
    """High-level interface for working with encrypted iPhone backups."""

    def __init__(self, backup_path: Path, password: str):
        check_crypto_available()
        self.backup_path = backup_path
        self.password = password
        self.keybag: Keybag | None = None
        self.class_keys: dict[int, bytes] = {}
        self._manifest_key: bytes = b""
        self._manifest_class: int = 0

    def unlock(self) -> None:
        """Parse keybag, derive key, and unwrap class keys.

        Raises:
            WrongPasswordError: If the password is incorrect.
            CryptoError: If the backup cannot be decrypted.
        """
        manifest_plist_path = self.backup_path / "Manifest.plist"
        if not manifest_plist_path.exists():
            raise CryptoError("Manifest.plist not found")

        with open(manifest_plist_path, "rb") as f:
            manifest_plist = plistlib.load(f)

        if not manifest_plist.get("IsEncrypted"):
            raise CryptoError("Backup is not encrypted")

        keybag_data = manifest_plist.get("BackupKeyBag")
        if not keybag_data:
            raise CryptoError("No BackupKeyBag in Manifest.plist")

        self.keybag = parse_keybag(keybag_data)
        derived_key = derive_key_from_password(self.password, self.keybag)
        self.class_keys = unwrap_class_keys(self.keybag, derived_key)

        # Store the ManifestKey for Manifest.db decryption
        manifest_key_data = manifest_plist.get("ManifestKey")
        if manifest_key_data:
            if len(manifest_key_data) >= 4:
                self._manifest_class = struct.unpack("<I", manifest_key_data[:4])[0]
                self._manifest_key = manifest_key_data
            else:
                self._manifest_key = manifest_key_data

        logger.info(
            "Unlocked encrypted backup: %d class keys unwrapped",
            len(self.class_keys),
        )

    def decrypt_manifest_db(self) -> Path:
        """Decrypt Manifest.db to a temporary file.

        Returns:
            Path to the decrypted Manifest.db.
        """
        encrypted_path = self.backup_path / "Manifest.db"
        encrypted_data = encrypted_path.read_bytes()

        decrypted = decrypt_file(
            encrypted_data,
            self._manifest_key,
            self._manifest_class,
            self.class_keys,
        )

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            tmp.write(decrypted)
        return Path(tmp.name)

    def decrypt_db_file(
        self,
        file_data: bytes,
        encryption_key: bytes,
        protection_class: int,
    ) -> bytes:
        """Decrypt an individual database file."""
        return decrypt_file(file_data, encryption_key, protection_class, self.class_keys)

    def encrypt_db_file(
        self,
        plaintext: bytes,
        encryption_key: bytes,
        protection_class: int,
    ) -> bytes:
        """Re-encrypt a modified database file."""
        return encrypt_file(plaintext, encryption_key, protection_class, self.class_keys)

    def re_encrypt_manifest_db(self, decrypted_path: Path) -> None:
        """Re-encrypt a modified Manifest.db back into the backup."""
        plaintext = decrypted_path.read_bytes()
        encrypted = encrypt_file(
            plaintext,
            self._manifest_key,
            self._manifest_class,
            self.class_keys,
        )
        (self.backup_path / "Manifest.db").write_bytes(encrypted)
