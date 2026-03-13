"""Encrypted iPhone backup support.

Handles decryption and re-encryption of encrypted iPhone backups:
- Parse keybag from Manifest.plist (BackupKeyBag)
- Derive encryption key from user password via PBKDF2
- Unwrap class keys using AES key unwrap (RFC3394)
- Decrypt/re-encrypt Manifest.db and individual files (sms.db)

Requires the `cryptography` package (default dependency).
Install via: pip install green2blue
"""

from __future__ import annotations

import hashlib
import logging
import os
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
    from cryptography.hazmat.primitives.keywrap import aes_key_unwrap, aes_key_wrap

    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False


def check_crypto_available() -> None:
    """Raise if the cryptography package is not installed."""
    if not HAS_CRYPTO:
        raise CryptoDependencyError("The 'cryptography' package is required for encrypted backups.")


# --- Keybag parsing ---


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
        raise CryptoError(f"No class key for protection class {protection_class}")

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


def decrypt_file_to_path(
    encrypted_path: Path,
    dest_path: Path,
    encryption_key: bytes,
    protection_class: int,
    class_keys: dict[int, bytes],
    *,
    chunk_size: int = 1024 * 1024,
) -> int:
    """Decrypt a backup file from disk into a destination path.

    Returns:
        Plaintext size in bytes.
    """
    check_crypto_available()

    class_key = class_keys.get(protection_class)
    if not class_key:
        raise CryptoError(f"No class key for protection class {protection_class}")

    wrapped_file_key = encryption_key[4:] if len(encryption_key) >= 4 else encryption_key

    try:
        file_key = aes_key_unwrap(class_key, wrapped_file_key)
    except Exception as e:
        raise CryptoError(f"Failed to unwrap file key: {e}") from e

    iv = b"\x00" * 16
    cipher = Cipher(algorithms.AES(file_key), modes.CBC(iv))
    decryptor = cipher.decryptor()

    plaintext_size = 0
    buffered = b""

    with encrypted_path.open("rb") as src, dest_path.open("wb") as dst:
        while True:
            chunk = src.read(chunk_size)
            if not chunk:
                break
            buffered += chunk
            full_len = len(buffered) - 16
            if full_len <= 0:
                continue
            full_len -= full_len % 16
            if full_len <= 0:
                continue
            decrypted_chunk = decryptor.update(buffered[:full_len])
            buffered = buffered[full_len:]
            if decrypted_chunk:
                dst.write(decrypted_chunk)
                plaintext_size += len(decrypted_chunk)

        decrypted_tail = decryptor.update(buffered) + decryptor.finalize()
        if decrypted_tail:
            pad_len = decrypted_tail[-1]
            if 0 < pad_len <= 16 and all(b == pad_len for b in decrypted_tail[-pad_len:]):
                decrypted_tail = decrypted_tail[:-pad_len]
            if decrypted_tail:
                dst.write(decrypted_tail)
                plaintext_size += len(decrypted_tail)

    return plaintext_size


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
        raise CryptoError(f"No class key for protection class {protection_class}")

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


def encrypt_file_from_path(
    plaintext_path: Path,
    dest_path: Path,
    encryption_key: bytes,
    protection_class: int,
    class_keys: dict[int, bytes],
    *,
    chunk_size: int = 1024 * 1024,
) -> tuple[int, bytes]:
    """Encrypt a plaintext file from disk into a destination path.

    Returns:
        Tuple of (plaintext_size, ciphertext_sha1_digest).
    """
    check_crypto_available()

    class_key = class_keys.get(protection_class)
    if not class_key:
        raise CryptoError(f"No class key for protection class {protection_class}")

    wrapped_file_key = encryption_key[4:] if len(encryption_key) >= 4 else encryption_key

    try:
        file_key = aes_key_unwrap(class_key, wrapped_file_key)
    except Exception as e:
        raise CryptoError(f"Failed to unwrap file key for encryption: {e}") from e

    iv = b"\x00" * 16
    cipher = Cipher(algorithms.AES(file_key), modes.CBC(iv))
    encryptor = cipher.encryptor()

    digest_hasher = hashlib.sha1()
    plaintext_size = 0
    buffered = b""

    with plaintext_path.open("rb") as src, dest_path.open("wb") as dst:
        while True:
            chunk = src.read(chunk_size)
            if not chunk:
                break
            plaintext_size += len(chunk)
            buffered += chunk
            full_len = len(buffered) - (len(buffered) % 16)
            if full_len == 0:
                continue
            encrypted_chunk = encryptor.update(buffered[:full_len])
            buffered = buffered[full_len:]
            if encrypted_chunk:
                dst.write(encrypted_chunk)
                digest_hasher.update(encrypted_chunk)

        pad_len = 16 - (len(buffered) % 16)
        if pad_len == 0:
            pad_len = 16
        padded = buffered + bytes([pad_len] * pad_len)
        encrypted_tail = encryptor.update(padded) + encryptor.finalize()
        if encrypted_tail:
            dst.write(encrypted_tail)
            digest_hasher.update(encrypted_tail)

    return plaintext_size, digest_hasher.digest()


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
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            dest_path = Path(tmp.name)

        decrypt_file_to_path(
            encrypted_path,
            dest_path,
            self._manifest_key,
            self._manifest_class,
            self.class_keys,
        )
        return dest_path

    def decrypt_db_file(
        self,
        file_data: bytes,
        encryption_key: bytes,
        protection_class: int,
    ) -> bytes:
        """Decrypt an individual database file."""
        return decrypt_file(file_data, encryption_key, protection_class, self.class_keys)

    def decrypt_db_file_to_path(
        self,
        encrypted_path: Path,
        encryption_key: bytes,
        protection_class: int,
        dest_path: Path,
    ) -> int:
        """Decrypt an individual database file from disk to a destination path."""
        return decrypt_file_to_path(
            encrypted_path,
            dest_path,
            encryption_key,
            protection_class,
            self.class_keys,
        )

    def encrypt_db_file(
        self,
        plaintext: bytes,
        encryption_key: bytes,
        protection_class: int,
    ) -> bytes:
        """Re-encrypt a modified database file."""
        return encrypt_file(plaintext, encryption_key, protection_class, self.class_keys)

    def encrypt_db_file_from_path(
        self,
        plaintext_path: Path,
        encryption_key: bytes,
        protection_class: int,
        dest_path: Path,
    ) -> tuple[int, bytes]:
        """Re-encrypt a modified database file from disk into the backup."""
        return encrypt_file_from_path(
            plaintext_path,
            dest_path,
            encryption_key,
            protection_class,
            self.class_keys,
        )

    def generate_file_key(self, protection_class: int = 3) -> bytes:
        """Generate a new per-file encryption key for an encrypted backup.

        Creates a random AES-256 key, wraps it with the class key, and
        prepends the 4-byte little-endian protection class prefix.

        Args:
            protection_class: The iOS protection class (default: 3).

        Returns:
            The full wrapped key blob (same format as EncryptionKey in Manifest.db).
        """
        check_crypto_available()

        class_key = self.class_keys.get(protection_class)
        if not class_key:
            raise CryptoError(f"No class key for protection class {protection_class}")

        file_key = os.urandom(32)
        wrapped = aes_key_wrap(class_key, file_key)
        return struct.pack("<I", protection_class) + wrapped

    def encrypt_new_file(self, plaintext: bytes, protection_class: int = 3) -> tuple[bytes, bytes]:
        """Encrypt a new file for inclusion in the encrypted backup.

        Generates a fresh per-file key, encrypts the data, and returns
        both the encrypted data and the wrapped key blob.

        Args:
            plaintext: The plaintext file content.
            protection_class: The iOS protection class (default: 3).

        Returns:
            Tuple of (encrypted_data, encryption_key_blob).
        """
        enc_key_blob = self.generate_file_key(protection_class)
        encrypted = encrypt_file(
            plaintext,
            enc_key_blob,
            protection_class,
            self.class_keys,
        )
        return encrypted, enc_key_blob

    def encrypt_new_file_to_path(
        self,
        source_path: Path,
        dest_path: Path,
        protection_class: int = 3,
        chunk_size: int = 1024 * 1024,
    ) -> tuple[int, bytes, bytes]:
        """Encrypt a new file from disk directly into the backup path.

        This avoids loading large attachment payloads fully into memory before
        writing them into the encrypted backup.

        Returns:
            Tuple of (plaintext_size, encrypted_digest, encryption_key_blob).
        """
        check_crypto_available()

        class_key = self.class_keys.get(protection_class)
        if not class_key:
            raise CryptoError(f"No class key for protection class {protection_class}")

        file_key = os.urandom(32)
        wrapped = aes_key_wrap(class_key, file_key)
        enc_key_blob = struct.pack("<I", protection_class) + wrapped

        iv = b"\x00" * 16
        cipher = Cipher(algorithms.AES(file_key), modes.CBC(iv))
        encryptor = cipher.encryptor()

        digest_hasher = hashlib.sha1()
        plaintext_size = 0
        buffered = b""

        with source_path.open("rb") as src, dest_path.open("wb") as dst:
            while True:
                chunk = src.read(chunk_size)
                if not chunk:
                    break
                plaintext_size += len(chunk)
                buffered += chunk
                full_len = len(buffered) - (len(buffered) % 16)
                if full_len == 0:
                    continue
                encrypted_chunk = encryptor.update(buffered[:full_len])
                buffered = buffered[full_len:]
                if encrypted_chunk:
                    dst.write(encrypted_chunk)
                    digest_hasher.update(encrypted_chunk)

            pad_len = 16 - (len(buffered) % 16)
            if pad_len == 0:
                pad_len = 16
            padded = buffered + bytes([pad_len] * pad_len)
            encrypted_tail = encryptor.update(padded) + encryptor.finalize()
            if encrypted_tail:
                dst.write(encrypted_tail)
                digest_hasher.update(encrypted_tail)

        return plaintext_size, digest_hasher.digest(), enc_key_blob

    def re_encrypt_manifest_db(self, decrypted_path: Path) -> None:
        """Re-encrypt a modified Manifest.db back into the backup."""
        encrypt_file_from_path(
            decrypted_path,
            self.backup_path / "Manifest.db",
            self._manifest_key,
            self._manifest_class,
            self.class_keys,
        )
