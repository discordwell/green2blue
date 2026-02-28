"""Tests for encrypted backup support.

Tests that don't require the cryptography package use mocking.
Tests that do require it are skipped if the package is not installed.
"""

from __future__ import annotations

import struct

import pytest

from green2blue.ios.crypto import HAS_CRYPTO, Keybag, parse_keybag

# Skip all crypto-dependent tests if cryptography not installed
crypto_required = pytest.mark.skipif(
    not HAS_CRYPTO, reason="cryptography package not installed"
)


def _build_keybag_tlv(tag: bytes, value: bytes) -> bytes:
    """Build a single TLV record for a keybag."""
    return tag + struct.pack(">I", len(value)) + value


def _build_test_keybag() -> bytes:
    """Build a minimal test keybag binary blob."""
    data = b""
    data += _build_keybag_tlv(b"VERS", struct.pack(">I", 5))
    data += _build_keybag_tlv(b"TYPE", struct.pack(">I", 1))
    data += _build_keybag_tlv(b"UUID", b"\x00" * 16)
    data += _build_keybag_tlv(b"SALT", b"\x01" * 20)
    data += _build_keybag_tlv(b"ITER", struct.pack(">I", 10000))
    # Add a class key entry
    data += _build_keybag_tlv(b"UUID", b"\x02" * 16)  # Start of key
    data += _build_keybag_tlv(b"CLAS", struct.pack(">I", 3))
    data += _build_keybag_tlv(b"WPKY", b"\x03" * 40)
    data += _build_keybag_tlv(b"KTYP", struct.pack(">I", 1))
    return data


class TestKeybagParsing:
    def test_parse_basic_keybag(self):
        data = _build_test_keybag()
        keybag = parse_keybag(data)
        assert keybag.version == 5
        assert keybag.type == 1
        assert keybag.salt == b"\x01" * 20
        assert keybag.iterations == 10000

    def test_parse_keybag_keys(self):
        data = _build_test_keybag()
        keybag = parse_keybag(data)
        # First UUID is the keybag-level UUID, second starts a class key
        assert len(keybag.keys) == 1
        key = keybag.keys[0]
        assert key.protection_class == 3
        assert key.wrapped_key == b"\x03" * 40
        assert key.key_type == 1

    def test_keybag_uuid_not_phantom_key(self):
        """First UUID should set keybag.uuid, not create a phantom key."""
        data = _build_test_keybag()
        keybag = parse_keybag(data)
        assert keybag.uuid == b"\x00" * 16
        # No phantom empty key — all keys have real data
        for key in keybag.keys:
            assert key.protection_class > 0
            assert key.wrapped_key != b""

    def test_parse_empty(self):
        keybag = parse_keybag(b"")
        assert keybag.version == 0
        assert len(keybag.keys) == 0

    def test_parse_with_dpic(self):
        data = _build_test_keybag()
        data += _build_keybag_tlv(b"DPIC", struct.pack(">I", 10000000))
        data += _build_keybag_tlv(b"DPSL", b"\x04" * 20)
        keybag = parse_keybag(data)
        assert keybag.dpic == 10000000
        assert keybag.dpsl == b"\x04" * 20


@crypto_required
class TestKeyDerivation:
    def test_derive_key_basic(self):
        from green2blue.ios.crypto import derive_key_from_password

        keybag = Keybag()
        keybag.salt = b"\x00" * 20
        keybag.iterations = 1  # Low for testing

        key = derive_key_from_password("testpassword", keybag)
        assert len(key) == 32
        assert isinstance(key, bytes)

    def test_derive_key_deterministic(self):
        from green2blue.ios.crypto import derive_key_from_password

        keybag = Keybag()
        keybag.salt = b"\xaa" * 20
        keybag.iterations = 1

        key1 = derive_key_from_password("password", keybag)
        key2 = derive_key_from_password("password", keybag)
        assert key1 == key2

    def test_different_passwords_different_keys(self):
        from green2blue.ios.crypto import derive_key_from_password

        keybag = Keybag()
        keybag.salt = b"\xbb" * 20
        keybag.iterations = 1

        key1 = derive_key_from_password("password1", keybag)
        key2 = derive_key_from_password("password2", keybag)
        assert key1 != key2


@crypto_required
class TestEncryptDecrypt:
    def test_encrypt_decrypt_round_trip(self):
        """Test that encrypt → decrypt returns original data."""
        from cryptography.hazmat.primitives.keywrap import aes_key_wrap

        from green2blue.ios.crypto import decrypt_file, encrypt_file

        # Create a test class key (32 bytes)
        class_key = b"\xaa" * 32

        # Create a test file key (32 bytes) and wrap it
        file_key = b"\xbb" * 32
        wrapped_file_key = aes_key_wrap(class_key, file_key)

        # Build encryption_key: 4-byte class prefix + wrapped key
        encryption_key = struct.pack("<I", 3) + wrapped_file_key

        class_keys = {3: class_key}
        protection_class = 3

        original = b"Hello, this is test data for sms.db encryption!"

        encrypted = encrypt_file(original, encryption_key, protection_class, class_keys)
        assert encrypted != original

        decrypted = decrypt_file(encrypted, encryption_key, protection_class, class_keys)
        assert decrypted == original

    def test_encrypt_decrypt_various_sizes(self):
        """Test encryption with data of various sizes (padding edge cases)."""
        from cryptography.hazmat.primitives.keywrap import aes_key_wrap

        from green2blue.ios.crypto import decrypt_file, encrypt_file

        class_key = b"\xcc" * 32
        file_key = b"\xdd" * 32
        wrapped = aes_key_wrap(class_key, file_key)
        enc_key = struct.pack("<I", 3) + wrapped
        class_keys = {3: class_key}

        for size in [1, 15, 16, 17, 31, 32, 100, 1024]:
            data = bytes(range(256)) * (size // 256 + 1)
            data = data[:size]
            encrypted = encrypt_file(data, enc_key, 3, class_keys)
            decrypted = decrypt_file(encrypted, enc_key, 3, class_keys)
            assert decrypted == data, f"Round-trip failed for size {size}"


@crypto_required
class TestGenerateFileKey:
    """Test per-file key generation for encrypted backups."""

    def test_generate_file_key_format(self):
        """Generated key blob has 4-byte class prefix + wrapped key."""
        from green2blue.ios.crypto import EncryptedBackup

        backup = EncryptedBackup.__new__(EncryptedBackup)
        backup.class_keys = {3: b"\xaa" * 32}

        key_blob = backup.generate_file_key(protection_class=3)

        # 4-byte prefix + 40-byte wrapped key (32-byte key + 8 bytes wrap overhead)
        assert len(key_blob) == 4 + 40
        prefix_class = struct.unpack("<I", key_blob[:4])[0]
        assert prefix_class == 3

    def test_generate_file_key_can_decrypt(self):
        """A generated key should be usable for encrypt/decrypt round-trip."""
        from green2blue.ios.crypto import EncryptedBackup, decrypt_file, encrypt_file

        backup = EncryptedBackup.__new__(EncryptedBackup)
        backup.class_keys = {3: b"\xaa" * 32}

        key_blob = backup.generate_file_key(protection_class=3)

        original = b"test data for encryption"
        encrypted = encrypt_file(original, key_blob, 3, backup.class_keys)
        decrypted = decrypt_file(encrypted, key_blob, 3, backup.class_keys)
        assert decrypted == original

    def test_generate_file_key_unique(self):
        """Each call should generate a different random key."""
        from green2blue.ios.crypto import EncryptedBackup

        backup = EncryptedBackup.__new__(EncryptedBackup)
        backup.class_keys = {3: b"\xaa" * 32}

        key1 = backup.generate_file_key(3)
        key2 = backup.generate_file_key(3)
        assert key1 != key2


@crypto_required
class TestEncryptNewFile:
    """Test the high-level encrypt_new_file method."""

    def test_encrypt_new_file_round_trip(self):
        """encrypt_new_file output should be decryptable."""
        from green2blue.ios.crypto import EncryptedBackup, decrypt_file

        backup = EncryptedBackup.__new__(EncryptedBackup)
        backup.class_keys = {3: b"\xbb" * 32}

        original = b"This is a test attachment file content"
        encrypted_data, enc_key_blob = backup.encrypt_new_file(original, 3)

        assert encrypted_data != original
        assert len(enc_key_blob) == 4 + 40

        decrypted = decrypt_file(encrypted_data, enc_key_blob, 3, backup.class_keys)
        assert decrypted == original

    def test_encrypt_new_file_various_sizes(self):
        """Test encrypt_new_file with various data sizes."""
        from green2blue.ios.crypto import EncryptedBackup, decrypt_file

        backup = EncryptedBackup.__new__(EncryptedBackup)
        backup.class_keys = {3: b"\xcc" * 32}

        for size in [0, 1, 15, 16, 17, 100, 1024, 4096]:
            data = bytes(range(256)) * (size // 256 + 1)
            data = data[:size]
            encrypted, key_blob = backup.encrypt_new_file(data, 3)
            decrypted = decrypt_file(encrypted, key_blob, 3, backup.class_keys)
            assert decrypted == data, f"Round-trip failed for size {size}"


@crypto_required
class TestEncryptedBackupUnlock:
    """Test EncryptedBackup.unlock with synthetic keybag data."""

    def _create_synthetic_encrypted_backup(self, tmp_path, password="test"):
        """Create a minimal synthetic encrypted backup for testing."""
        import plistlib

        from cryptography.hazmat.primitives.keywrap import aes_key_wrap

        from green2blue.ios.crypto import derive_key_from_password

        # Build a keybag with a known class key and low iteration counts
        class_key = b"\xee" * 32

        keybag_data = b""
        keybag_data += _build_keybag_tlv(b"VERS", struct.pack(">I", 5))
        keybag_data += _build_keybag_tlv(b"TYPE", struct.pack(">I", 1))
        keybag_data += _build_keybag_tlv(b"UUID", b"\x00" * 16)
        keybag_data += _build_keybag_tlv(b"SALT", b"\x01" * 20)
        keybag_data += _build_keybag_tlv(b"ITER", struct.pack(">I", 1))

        # Derive key from password with these params to wrap the class key
        keybag = Keybag()
        keybag.salt = b"\x01" * 20
        keybag.iterations = 1
        derived_key = derive_key_from_password(password, keybag)

        # Wrap the class key
        wrapped_class_key = aes_key_wrap(derived_key, class_key)

        # Add class key entry to keybag
        keybag_data += _build_keybag_tlv(b"UUID", b"\x02" * 16)
        keybag_data += _build_keybag_tlv(b"CLAS", struct.pack(">I", 3))
        keybag_data += _build_keybag_tlv(b"WPKY", wrapped_class_key)
        keybag_data += _build_keybag_tlv(b"KTYP", struct.pack(">I", 1))

        # Create a manifest key (encrypt Manifest.db with it)
        manifest_file_key = b"\xff" * 32
        wrapped_manifest_key = aes_key_wrap(class_key, manifest_file_key)
        manifest_key_data = struct.pack("<I", 3) + wrapped_manifest_key

        # Write Manifest.plist
        backup_dir = tmp_path / "encrypted_backup"
        backup_dir.mkdir()
        (backup_dir / "Manifest.plist").write_bytes(plistlib.dumps({
            "IsEncrypted": True,
            "BackupKeyBag": keybag_data,
            "ManifestKey": manifest_key_data,
            "Version": "3.3",
        }))

        return backup_dir, class_key, manifest_key_data

    def test_unlock_success(self, tmp_dir):
        from green2blue.ios.crypto import EncryptedBackup

        backup_dir, class_key, _ = self._create_synthetic_encrypted_backup(tmp_dir)
        eb = EncryptedBackup(backup_dir, "test")
        eb.unlock()

        assert len(eb.class_keys) > 0
        assert 3 in eb.class_keys
        assert eb.class_keys[3] == class_key

    def test_unlock_wrong_password(self, tmp_dir):
        from green2blue.exceptions import WrongPasswordError
        from green2blue.ios.crypto import EncryptedBackup

        backup_dir, _, _ = self._create_synthetic_encrypted_backup(tmp_dir)
        eb = EncryptedBackup(backup_dir, "wrong_password")

        with pytest.raises(WrongPasswordError):
            eb.unlock()

    def test_decrypt_re_encrypt_manifest_round_trip(self, tmp_dir):
        """Test decrypt_manifest_db → re_encrypt_manifest_db round trip."""
        import sqlite3

        from green2blue.ios.crypto import EncryptedBackup, encrypt_file

        backup_dir, class_key, manifest_key_data = (
            self._create_synthetic_encrypted_backup(tmp_dir)
        )

        # Create a real Manifest.db, encrypt it, and write to backup
        manifest_db_path = tmp_dir / "manifest_plain.db"
        conn = sqlite3.connect(manifest_db_path)
        conn.execute("""
            CREATE TABLE Files (
                fileID TEXT PRIMARY KEY,
                domain TEXT,
                relativePath TEXT,
                flags INTEGER,
                file BLOB
            )
        """)
        conn.execute(
            "INSERT INTO Files VALUES (?, ?, ?, ?, ?)",
            ("abc123", "HomeDomain", "Library/SMS/sms.db", 1, b"test_blob"),
        )
        conn.commit()
        conn.close()

        # Encrypt and write to backup
        plaintext = manifest_db_path.read_bytes()
        class_keys = {3: class_key}
        encrypted = encrypt_file(plaintext, manifest_key_data, 3, class_keys)
        (backup_dir / "Manifest.db").write_bytes(encrypted)

        # Now test the round-trip
        eb = EncryptedBackup(backup_dir, "test")
        eb.unlock()

        decrypted_path = eb.decrypt_manifest_db()
        assert decrypted_path.exists()

        # Verify decrypted Manifest.db is valid SQLite
        conn = sqlite3.connect(decrypted_path)
        cursor = conn.execute("SELECT fileID FROM Files")
        assert cursor.fetchone()[0] == "abc123"
        conn.close()

        # Re-encrypt and verify we can decrypt again
        eb.re_encrypt_manifest_db(decrypted_path)
        decrypted_path2 = eb.decrypt_manifest_db()

        conn = sqlite3.connect(decrypted_path2)
        cursor = conn.execute("SELECT fileID FROM Files")
        assert cursor.fetchone()[0] == "abc123"
        conn.close()

        # Clean up
        decrypted_path.unlink(missing_ok=True)
        decrypted_path2.unlink(missing_ok=True)


@crypto_required
class TestPKCS7Padding:
    """Test PKCS7 padding behavior for various data sizes."""

    def test_padding_exact_block_size(self):
        """Data that's exactly a block size should get a full block of padding."""
        from cryptography.hazmat.primitives.keywrap import aes_key_wrap

        from green2blue.ios.crypto import decrypt_file, encrypt_file

        class_key = b"\xaa" * 32
        file_key = b"\xbb" * 32
        wrapped = aes_key_wrap(class_key, file_key)
        enc_key = struct.pack("<I", 3) + wrapped
        class_keys = {3: class_key}

        # 16 bytes = exactly 1 block
        data = b"\x42" * 16
        encrypted = encrypt_file(data, enc_key, 3, class_keys)
        # Should be 32 bytes: 16 data + 16 padding
        assert len(encrypted) == 32
        decrypted = decrypt_file(encrypted, enc_key, 3, class_keys)
        assert decrypted == data

    def test_padding_one_byte(self):
        """1 byte of data should get 15 bytes of padding."""
        from cryptography.hazmat.primitives.keywrap import aes_key_wrap

        from green2blue.ios.crypto import decrypt_file, encrypt_file

        class_key = b"\xaa" * 32
        file_key = b"\xbb" * 32
        wrapped = aes_key_wrap(class_key, file_key)
        enc_key = struct.pack("<I", 3) + wrapped
        class_keys = {3: class_key}

        data = b"\x42"
        encrypted = encrypt_file(data, enc_key, 3, class_keys)
        assert len(encrypted) == 16
        decrypted = decrypt_file(encrypted, enc_key, 3, class_keys)
        assert decrypted == data


class TestCryptoDependencyCheck:
    def test_has_crypto_flag(self):
        # This just checks the flag is a bool
        assert isinstance(HAS_CRYPTO, bool)

    def test_missing_crypto_raises(self):
        """Verify CryptoDependencyError when HAS_CRYPTO is False."""
        from unittest.mock import patch

        from green2blue.exceptions import CryptoDependencyError
        from green2blue.ios.crypto import check_crypto_available

        with patch("green2blue.ios.crypto.HAS_CRYPTO", False), pytest.raises(
            CryptoDependencyError
        ):
            check_crypto_available()
