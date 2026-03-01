"""Manifest.mbdb binary format for iOS synthetic/partial backups.

The Manifest.mbdb format (version 2.4) is used by partial/overlay backup tools
like TrollRestore and Nugget. It stores file metadata in a compact binary format:

  Header: b"mbdb\\x05\\x00"
  Records: Sequence of MbdbRecord entries, each containing:
    - Domain + filename (string pair)
    - Symlink target, hash, encryption key (optional)
    - Mode, inode, uid, gid, timestamps, size, flags
    - Property count + key/value pairs

Strings use a 2-byte big-endian length prefix; 0xFFFF means NULL.
"""

from __future__ import annotations

import datetime
import hashlib
import plistlib
import struct
import time
from dataclasses import dataclass, field
from enum import IntFlag
from pathlib import Path


class FileMode(IntFlag):
    """POSIX file type and permission bits."""

    S_IFREG = 0o100000
    S_IFDIR = 0o040000
    S_IFLNK = 0o120000

    # Common permission combos
    OWNER_RWX = 0o700
    OWNER_RW = 0o600
    OWNER_R = 0o400
    GROUP_RX = 0o050
    GROUP_R = 0o040
    OTHER_RX = 0o005
    OTHER_R = 0o004


# Standard file permissions
MODE_FILE_644 = FileMode.S_IFREG | 0o644
MODE_DIR_755 = FileMode.S_IFDIR | 0o755


def _encode_string(value: bytes | None) -> bytes:
    """Encode a string for MBDB format: 2-byte BE length prefix, or 0xFFFF for NULL."""
    if value is None:
        return b"\xff\xff"
    return struct.pack(">H", len(value)) + value


def _decode_string(data: bytes, offset: int) -> tuple[bytes | None, int]:
    """Decode an MBDB string. Returns (value, new_offset)."""
    length = struct.unpack(">H", data[offset : offset + 2])[0]
    offset += 2
    if length == 0xFFFF:
        return None, offset
    value = data[offset : offset + length]
    return value, offset + length


@dataclass
class MbdbRecord:
    """A single file/directory record in Manifest.mbdb."""

    domain: str
    filename: str
    link_target: bytes | None = None
    data_hash: bytes | None = None
    encryption_key: bytes | None = None
    mode: int = int(MODE_FILE_644)
    inode: int = 0
    uid: int = 501
    gid: int = 501
    mtime: int = 0
    atime: int = 0
    ctime: int = 0
    size: int = 0
    flags: int = 4
    properties: dict[str, bytes] = field(default_factory=dict)

    def to_bytes(self) -> bytes:
        """Serialize this record to MBDB binary format."""
        parts = []

        # Domain + filename
        parts.append(_encode_string(self.domain.encode("utf-8")))
        parts.append(_encode_string(self.filename.encode("utf-8")))

        # Link target, hash, encryption key
        parts.append(_encode_string(self.link_target))
        parts.append(_encode_string(self.data_hash))
        parts.append(_encode_string(self.encryption_key))

        # Fixed fields: mode(u16), inode(u64), uid(u32), gid(u32),
        # mtime(u32), atime(u32), ctime(u32), size(u64), flags(u8)
        parts.append(
            struct.pack(
                ">HQIIIIIQB",
                self.mode,
                self.inode,
                self.uid,
                self.gid,
                self.mtime,
                self.atime,
                self.ctime,
                self.size,
                self.flags,
            )
        )

        # Property count + key/value pairs
        parts.append(struct.pack(">B", len(self.properties)))
        for key, value in self.properties.items():
            parts.append(_encode_string(key.encode("utf-8")))
            parts.append(_encode_string(value))

        return b"".join(parts)

    @classmethod
    def from_bytes(cls, data: bytes, offset: int) -> tuple[MbdbRecord, int]:
        """Deserialize a record from MBDB binary data. Returns (record, new_offset)."""
        domain_bytes, offset = _decode_string(data, offset)
        filename_bytes, offset = _decode_string(data, offset)
        link_target, offset = _decode_string(data, offset)
        data_hash, offset = _decode_string(data, offset)
        encryption_key, offset = _decode_string(data, offset)

        _fmt = ">HQIIIIIQB"
        _fmt_size = struct.calcsize(_fmt)
        fields = struct.unpack(_fmt, data[offset : offset + _fmt_size])
        offset += _fmt_size

        mode, inode, uid, gid, mtime, atime, ctime, size, flags = fields

        prop_count = struct.unpack(">B", data[offset : offset + 1])[0]
        offset += 1

        properties: dict[str, bytes] = {}
        for _ in range(prop_count):
            key_bytes, offset = _decode_string(data, offset)
            val_bytes, offset = _decode_string(data, offset)
            if key_bytes is not None and val_bytes is not None:
                properties[key_bytes.decode("utf-8")] = val_bytes

        record = cls(
            domain=domain_bytes.decode("utf-8") if domain_bytes else "",
            filename=filename_bytes.decode("utf-8") if filename_bytes else "",
            link_target=link_target,
            data_hash=data_hash,
            encryption_key=encryption_key,
            mode=mode,
            inode=inode,
            uid=uid,
            gid=gid,
            mtime=mtime,
            atime=atime,
            ctime=ctime,
            size=size,
            flags=flags,
            properties=properties,
        )
        return record, offset


MBDB_HEADER = b"mbdb\x05\x00"


@dataclass
class Mbdb:
    """Container for a Manifest.mbdb file."""

    records: list[MbdbRecord] = field(default_factory=list)

    def to_bytes(self) -> bytes:
        """Serialize the full MBDB (header + all records)."""
        parts = [MBDB_HEADER]
        for record in self.records:
            parts.append(record.to_bytes())
        return b"".join(parts)

    @classmethod
    def from_bytes(cls, data: bytes) -> Mbdb:
        """Parse an MBDB file from raw bytes."""
        if not data.startswith(MBDB_HEADER):
            msg = f"Invalid MBDB header: {data[:6]!r}"
            raise ValueError(msg)

        records = []
        offset = len(MBDB_HEADER)
        while offset < len(data):
            record, offset = MbdbRecord.from_bytes(data, offset)
            records.append(record)
        return cls(records=records)


def file_record(
    domain: str,
    filename: str,
    contents: bytes,
    *,
    mode: int = int(MODE_FILE_644),
    uid: int = 501,
    gid: int = 501,
    flags: int = 4,
) -> MbdbRecord:
    """Create an MbdbRecord for a regular file with its SHA1 hash."""
    now = int(time.time())
    return MbdbRecord(
        domain=domain,
        filename=filename,
        data_hash=hashlib.sha1(contents).digest(),
        mode=mode,
        uid=uid,
        gid=gid,
        mtime=now,
        atime=now,
        ctime=now,
        size=len(contents),
        flags=flags,
    )


def directory_record(
    domain: str,
    filename: str,
    *,
    uid: int = 501,
    gid: int = 501,
) -> MbdbRecord:
    """Create an MbdbRecord for a directory."""
    now = int(time.time())
    return MbdbRecord(
        domain=domain,
        filename=filename,
        mode=int(MODE_DIR_755),
        uid=uid,
        gid=gid,
        mtime=now,
        atime=now,
        ctime=now,
        size=0,
        flags=0,
    )


# Static BackupKeyBag blob from TrollRestore — valid for unencrypted synthetic backups.
# This is the minimal keybag that iOS accepts during restore of an unencrypted backup.
SYNTHETIC_BACKUP_KEYBAG = bytes.fromhex(
    "56455253"  # VERS
    "00000004"  # length 4
    "00000005"  # version 5
    "54595045"  # TYPE
    "00000004"  # length 4
    "00000001"  # type 1 (backup)
    "55554944"  # UUID
    "00000010"  # length 16
    "00000000000000000000000000000000"
    "484d434b"  # HMCK
    "00000028"  # length 40
    "0000000000000000000000000000000000000000"
    "0000000000000000000000000000000000000000"
    "57524150"  # WRAP
    "00000004"  # length 4
    "00000000"  # wrap 0 (none)
    "534c5400"  # SLT\0 (SALT)
    "00000014"  # length 20
    "0000000000000000000000000000000000000000"
    "49544552"  # ITER
    "00000004"  # length 4
    "00000000"  # iterations 0
)


class SyntheticBackup:
    """Build a version 2.4 synthetic/partial backup directory.

    Usage:
        sb = SyntheticBackup(device_name="iPhone", udid="abc123")
        sb.add_file("HomeDomain", "Library/SMS/sms.db", sms_data)
        sb.write_to_directory(Path("/tmp/backup"))
    """

    def __init__(
        self,
        device_name: str = "iPhone",
        udid: str = "0000000000000000000000000000000000000000",
        product_version: str = "18.0",
    ):
        self.device_name = device_name
        self.udid = udid
        self.product_version = product_version
        self._files: list[tuple[str, str, bytes]] = []  # (domain, path, contents)
        self._directories: set[tuple[str, str]] = set()  # (domain, path)

    def add_file(self, domain: str, path: str, contents: bytes) -> None:
        """Add a file to the synthetic backup."""
        self._files.append((domain, path, contents))
        # Auto-create parent directory entries
        parts = path.split("/")
        for i in range(1, len(parts)):
            parent = "/".join(parts[:i])
            self._directories.add((domain, parent))

    def add_directory(self, domain: str, path: str) -> None:
        """Add a directory entry to the synthetic backup."""
        self._directories.add((domain, path))

    def write_to_directory(self, output_dir: Path) -> None:
        """Write the complete synthetic backup to a directory."""
        output_dir.mkdir(parents=True, exist_ok=True)

        # Build MBDB records
        mbdb = Mbdb()

        # Add directory records first
        for domain, path in sorted(self._directories):
            mbdb.records.append(directory_record(domain, path))

        # Add file records and write file data
        for domain, path, contents in self._files:
            mbdb.records.append(file_record(domain, path, contents))

            # Write file data to hash-named location
            file_id = hashlib.sha1(f"{domain}-{path}".encode()).hexdigest()
            file_path = output_dir / file_id[:2] / file_id
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_bytes(contents)

        # Write Manifest.mbdb
        (output_dir / "Manifest.mbdb").write_bytes(mbdb.to_bytes())

        # Write Status.plist
        status = {
            "BackupState": "new",
            "IsFullBackup": False,
            "SnapshotState": "finished",
            "Version": "2.4",
            "Date": datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None),
        }
        (output_dir / "Status.plist").write_bytes(plistlib.dumps(status, fmt=plistlib.FMT_BINARY))

        # Write Info.plist
        info = {
            "Device Name": self.device_name,
            "Display Name": self.device_name,
            "Product Version": self.product_version,
            "Target Identifier": self.udid,
            "Unique Identifier": self.udid,
            "UDID": self.udid,
        }
        (output_dir / "Info.plist").write_bytes(plistlib.dumps(info, fmt=plistlib.FMT_BINARY))

        # Write Manifest.plist with keybag
        manifest = {
            "BackupKeyBag": SYNTHETIC_BACKUP_KEYBAG,
            "IsEncrypted": False,
            "Version": 9.1,
            "SystemDomainsVersion": "20.0",
        }
        (output_dir / "Manifest.plist").write_bytes(
            plistlib.dumps(manifest, fmt=plistlib.FMT_BINARY)
        )
