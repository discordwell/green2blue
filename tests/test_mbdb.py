"""Tests for Manifest.mbdb binary format (no device needed)."""

from __future__ import annotations

import hashlib
import plistlib
import struct

import pytest

from green2blue.ios.mbdb import (
    MBDB_HEADER,
    MODE_DIR_755,
    MODE_FILE_644,
    SYNTHETIC_BACKUP_KEYBAG,
    Mbdb,
    MbdbRecord,
    SyntheticBackup,
    _decode_string,
    _encode_string,
    directory_record,
    file_record,
)

# --- String encoding tests ---


class TestStringEncoding:
    def test_encode_null(self):
        assert _encode_string(None) == b"\xff\xff"

    def test_encode_empty(self):
        assert _encode_string(b"") == b"\x00\x00"

    def test_encode_short(self):
        result = _encode_string(b"hello")
        assert result == b"\x00\x05hello"

    def test_encode_unicode(self):
        data = "caf\u00e9".encode()
        result = _encode_string(data)
        length = struct.unpack(">H", result[:2])[0]
        assert length == len(data)
        assert result[2:] == data

    def test_decode_null(self):
        value, offset = _decode_string(b"\xff\xff", 0)
        assert value is None
        assert offset == 2

    def test_decode_empty(self):
        value, offset = _decode_string(b"\x00\x00", 0)
        assert value == b""
        assert offset == 2

    def test_decode_short(self):
        value, offset = _decode_string(b"\x00\x05hello", 0)
        assert value == b"hello"
        assert offset == 7

    def test_roundtrip_null(self):
        encoded = _encode_string(None)
        decoded, _ = _decode_string(encoded, 0)
        assert decoded is None

    def test_roundtrip_data(self):
        original = b"test data 123"
        encoded = _encode_string(original)
        decoded, _ = _decode_string(encoded, 0)
        assert decoded == original

    def test_decode_at_offset(self):
        """Decode should work at non-zero offsets."""
        data = b"\x00\x00\x00\x03abc"
        value, offset = _decode_string(data, 2)
        assert value == b"abc"
        assert offset == 7


# --- MbdbRecord tests ---


class TestMbdbRecord:
    def test_basic_roundtrip(self):
        record = MbdbRecord(
            domain="HomeDomain",
            filename="Library/SMS/sms.db",
            data_hash=b"\x01" * 20,
            mode=int(MODE_FILE_644),
            uid=501,
            gid=501,
            mtime=1700000000,
            atime=1700000000,
            ctime=1700000000,
            size=1024,
            flags=4,
        )
        data = record.to_bytes()
        parsed, offset = MbdbRecord.from_bytes(data, 0)

        assert parsed.domain == "HomeDomain"
        assert parsed.filename == "Library/SMS/sms.db"
        assert parsed.data_hash == b"\x01" * 20
        assert parsed.mode == int(MODE_FILE_644)
        assert parsed.uid == 501
        assert parsed.gid == 501
        assert parsed.mtime == 1700000000
        assert parsed.size == 1024
        assert parsed.flags == 4
        assert offset == len(data)

    def test_null_optional_fields(self):
        record = MbdbRecord(
            domain="HomeDomain",
            filename="test.txt",
        )
        assert record.link_target is None
        assert record.data_hash is None
        assert record.encryption_key is None

        data = record.to_bytes()
        parsed, _ = MbdbRecord.from_bytes(data, 0)
        assert parsed.link_target is None
        assert parsed.data_hash is None
        assert parsed.encryption_key is None

    def test_with_properties(self):
        record = MbdbRecord(
            domain="HomeDomain",
            filename="test.txt",
            properties={"key1": b"value1", "key2": b"value2"},
        )
        data = record.to_bytes()
        parsed, _ = MbdbRecord.from_bytes(data, 0)
        assert parsed.properties == {"key1": b"value1", "key2": b"value2"}

    def test_directory_record_mode(self):
        record = MbdbRecord(
            domain="HomeDomain",
            filename="Library/SMS",
            mode=int(MODE_DIR_755),
            flags=0,
        )
        data = record.to_bytes()
        parsed, _ = MbdbRecord.from_bytes(data, 0)
        assert parsed.mode == int(MODE_DIR_755)
        assert parsed.flags == 0

    def test_with_encryption_key(self):
        key = b"\xaa" * 32
        record = MbdbRecord(
            domain="HomeDomain",
            filename="encrypted.dat",
            encryption_key=key,
        )
        data = record.to_bytes()
        parsed, _ = MbdbRecord.from_bytes(data, 0)
        assert parsed.encryption_key == key


# --- Mbdb container tests ---


class TestMbdb:
    def test_empty_mbdb(self):
        mbdb = Mbdb()
        data = mbdb.to_bytes()
        assert data == MBDB_HEADER

    def test_roundtrip_single_record(self):
        record = MbdbRecord(
            domain="HomeDomain",
            filename="Library/SMS/sms.db",
            data_hash=b"\x42" * 20,
            mode=int(MODE_FILE_644),
            size=2048,
        )
        mbdb = Mbdb(records=[record])
        data = mbdb.to_bytes()

        parsed = Mbdb.from_bytes(data)
        assert len(parsed.records) == 1
        assert parsed.records[0].domain == "HomeDomain"
        assert parsed.records[0].filename == "Library/SMS/sms.db"
        assert parsed.records[0].data_hash == b"\x42" * 20
        assert parsed.records[0].size == 2048

    def test_roundtrip_multiple_records(self):
        records = [
            MbdbRecord(domain="HomeDomain", filename="Library/SMS", mode=int(MODE_DIR_755)),
            MbdbRecord(
                domain="HomeDomain",
                filename="Library/SMS/sms.db",
                data_hash=b"\x01" * 20,
                size=4096,
            ),
            MbdbRecord(
                domain="MediaDomain",
                filename="Library/SMS/Attachments/test.jpg",
                data_hash=b"\x02" * 20,
                size=1000,
            ),
        ]
        mbdb = Mbdb(records=records)
        data = mbdb.to_bytes()

        parsed = Mbdb.from_bytes(data)
        assert len(parsed.records) == 3
        assert parsed.records[0].filename == "Library/SMS"
        assert parsed.records[1].filename == "Library/SMS/sms.db"
        assert parsed.records[2].domain == "MediaDomain"

    def test_invalid_header(self):
        with pytest.raises(ValueError, match="Invalid MBDB header"):
            Mbdb.from_bytes(b"not-mbdb-data")

    def test_starts_with_header(self):
        mbdb = Mbdb()
        data = mbdb.to_bytes()
        assert data[:4] == b"mbdb"
        assert data[4:6] == b"\x05\x00"


# --- Helper function tests ---


class TestFileRecord:
    def test_creates_record_with_hash(self):
        contents = b"Hello, world!"
        expected_hash = hashlib.sha1(contents).digest()

        record = file_record("HomeDomain", "test.txt", contents)
        assert record.domain == "HomeDomain"
        assert record.filename == "test.txt"
        assert record.data_hash == expected_hash
        assert record.size == len(contents)
        assert record.mode == int(MODE_FILE_644)
        assert record.uid == 501
        assert record.gid == 501
        assert record.flags == 4

    def test_custom_permissions(self):
        record = file_record("HomeDomain", "test.txt", b"data", mode=int(MODE_FILE_644), flags=0)
        assert record.mode == int(MODE_FILE_644)
        assert record.flags == 0

    def test_timestamps_set(self):
        record = file_record("HomeDomain", "test.txt", b"data")
        assert record.mtime > 0
        assert record.atime > 0
        assert record.ctime > 0


class TestDirectoryRecord:
    def test_creates_directory(self):
        record = directory_record("HomeDomain", "Library/SMS")
        assert record.domain == "HomeDomain"
        assert record.filename == "Library/SMS"
        assert record.mode == int(MODE_DIR_755)
        assert record.size == 0
        assert record.flags == 0


# --- SyntheticBackup tests ---


class TestSyntheticBackup:
    def test_write_basic_backup(self, tmp_path):
        sb = SyntheticBackup(
            device_name="Test iPhone",
            udid="abc123",
            product_version="18.0",
        )
        sb.add_file("HomeDomain", "Library/SMS/sms.db", b"fake sms data")

        output_dir = tmp_path / "backup"
        sb.write_to_directory(output_dir)

        # Check all required files exist
        assert (output_dir / "Manifest.mbdb").exists()
        assert (output_dir / "Status.plist").exists()
        assert (output_dir / "Info.plist").exists()
        assert (output_dir / "Manifest.plist").exists()

        # Check the data file was written at the correct hash path
        file_id = hashlib.sha1(b"HomeDomain-Library/SMS/sms.db").hexdigest()
        data_path = output_dir / file_id[:2] / file_id
        assert data_path.exists()
        assert data_path.read_bytes() == b"fake sms data"

    def test_status_plist_content(self, tmp_path):
        import datetime

        sb = SyntheticBackup()
        output_dir = tmp_path / "backup"
        sb.write_to_directory(output_dir)

        status = plistlib.loads((output_dir / "Status.plist").read_bytes())
        assert status["BackupState"] == "new"
        assert status["IsFullBackup"] is False
        assert status["SnapshotState"] == "finished"
        assert status["Version"] == "2.4"
        assert isinstance(status["Date"], datetime.datetime)

    def test_info_plist_content(self, tmp_path):
        sb = SyntheticBackup(
            device_name="My iPhone",
            udid="test-udid-123",
            product_version="17.5",
        )
        output_dir = tmp_path / "backup"
        sb.write_to_directory(output_dir)

        info = plistlib.loads((output_dir / "Info.plist").read_bytes())
        assert info["Device Name"] == "My iPhone"
        assert info["Target Identifier"] == "test-udid-123"
        assert info["Product Version"] == "17.5"

    def test_manifest_plist_content(self, tmp_path):
        sb = SyntheticBackup()
        output_dir = tmp_path / "backup"
        sb.write_to_directory(output_dir)

        manifest = plistlib.loads((output_dir / "Manifest.plist").read_bytes())
        assert manifest["IsEncrypted"] is False
        assert manifest["BackupKeyBag"] == SYNTHETIC_BACKUP_KEYBAG

    def test_auto_creates_parent_directories(self, tmp_path):
        sb = SyntheticBackup()
        sb.add_file("HomeDomain", "Library/SMS/sms.db", b"data")

        output_dir = tmp_path / "backup"
        sb.write_to_directory(output_dir)

        mbdb_data = (output_dir / "Manifest.mbdb").read_bytes()
        mbdb = Mbdb.from_bytes(mbdb_data)

        # Should have directory records for Library and Library/SMS
        domains_and_names = [(r.domain, r.filename) for r in mbdb.records]
        assert ("HomeDomain", "Library") in domains_and_names
        assert ("HomeDomain", "Library/SMS") in domains_and_names

    def test_mbdb_roundtrip(self, tmp_path):
        sb = SyntheticBackup()
        sb.add_file("HomeDomain", "Library/SMS/sms.db", b"sms data")
        sb.add_file("MediaDomain", "Library/SMS/Attachments/img.jpg", b"\xff\xd8\xff\xe0")
        sb.add_directory("HomeDomain", "Library")

        output_dir = tmp_path / "backup"
        sb.write_to_directory(output_dir)

        # Parse the MBDB and verify
        mbdb_data = (output_dir / "Manifest.mbdb").read_bytes()
        mbdb = Mbdb.from_bytes(mbdb_data)

        file_records = [r for r in mbdb.records if r.mode == int(MODE_FILE_644)]
        dir_records = [r for r in mbdb.records if r.mode == int(MODE_DIR_755)]

        assert len(file_records) == 2
        assert len(dir_records) >= 2  # At least Library and Library/SMS

    def test_multiple_files_same_domain(self, tmp_path):
        sb = SyntheticBackup()
        sb.add_file("HomeDomain", "a.txt", b"aaa")
        sb.add_file("HomeDomain", "b.txt", b"bbb")
        sb.add_file("HomeDomain", "c.txt", b"ccc")

        output_dir = tmp_path / "backup"
        sb.write_to_directory(output_dir)

        # All three files should exist at their hash paths
        for name, content in [("a.txt", b"aaa"), ("b.txt", b"bbb"), ("c.txt", b"ccc")]:
            file_id = hashlib.sha1(f"HomeDomain-{name}".encode()).hexdigest()
            assert (output_dir / file_id[:2] / file_id).read_bytes() == content

    def test_empty_file(self, tmp_path):
        sb = SyntheticBackup()
        sb.add_file("HomeDomain", "empty.txt", b"")

        output_dir = tmp_path / "backup"
        sb.write_to_directory(output_dir)

        file_id = hashlib.sha1(b"HomeDomain-empty.txt").hexdigest()
        assert (output_dir / file_id[:2] / file_id).read_bytes() == b""

        mbdb_data = (output_dir / "Manifest.mbdb").read_bytes()
        mbdb = Mbdb.from_bytes(mbdb_data)
        file_rec = [r for r in mbdb.records if r.filename == "empty.txt"][0]
        assert file_rec.size == 0
        assert file_rec.data_hash == hashlib.sha1(b"").digest()
