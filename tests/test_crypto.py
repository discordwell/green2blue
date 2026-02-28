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


class TestCryptoDependencyCheck:
    def test_has_crypto_flag(self):
        # This just checks the flag is a bool
        assert isinstance(HAS_CRYPTO, bool)
