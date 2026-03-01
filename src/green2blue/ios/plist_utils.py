"""NSKeyedArchiver binary plist construction for Manifest.db MBFile objects.

iPhone backups store file metadata in Manifest.db as NSKeyedArchiver-encoded
binary plists. We use a "clone and patch" strategy: read an existing blob,
deserialize, patch Size/LastModified, re-serialize. If no existing blob is
available, we build a minimal one from scratch.
"""

from __future__ import annotations

import plistlib
import time
from typing import Any


def patch_mbfile_blob(
    existing_blob: bytes,
    new_size: int,
    new_mtime: float | None = None,
    new_digest: bytes | None = None,
) -> bytes:
    """Patch an existing MBFile NSKeyedArchiver blob with new size/mtime/digest.

    Uses raw binary patching to avoid plistlib roundtrip corruption.
    plistlib re-serializes binary plists with different encoding (key order,
    offset table layout), which iOS rejects as a corrupt backup.

    Falls back to plistlib-based patching only if raw patching fails.

    Args:
        existing_blob: The original binary plist blob from Manifest.db.
        new_size: New file size in bytes.
        new_mtime: New modification time as Unix timestamp. Defaults to now.
        new_digest: New SHA1 digest (20 bytes). If provided and the blob has a
            Digest field, it will be updated.

    Returns:
        Updated binary plist blob.
    """
    if new_mtime is None:
        new_mtime = time.time()

    result = _try_raw_patch(existing_blob, new_size, int(new_mtime), new_digest)
    if result is not None:
        return result

    # Fallback: plistlib roundtrip (may not work for real iOS backups)
    try:
        plist = plistlib.loads(existing_blob)
    except Exception:
        return build_mbfile_blob(new_size, new_mtime)

    objects = plist.get("$objects")
    if not objects:
        return build_mbfile_blob(new_size, new_mtime)

    for obj in objects:
        if isinstance(obj, dict):
            if "Size" in obj:
                obj["Size"] = new_size
            if "LastModified" in obj:
                obj["LastModified"] = int(new_mtime)

    # Patch digest in fallback path
    if new_digest is not None:
        _patch_digest_plistlib(objects, new_digest)

    return plistlib.dumps(plist, fmt=plistlib.FMT_BINARY)


def _try_raw_patch(
    blob: bytes, new_size: int, new_mtime: int, new_digest: bytes | None = None
) -> bytes | None:
    """Attempt to patch Size, LastModified, and Digest directly in the raw binary plist.

    Binary plists store integers as type 0x1X where X encodes byte width
    (0=1B, 1=2B, 2=4B, 3=8B). We locate the fields by first finding their
    key strings, then patching the integer objects they reference via the
    offset table.

    Returns patched blob, or None if raw patching is not feasible.
    """
    if not blob.startswith(b"bplist00"):
        return None

    try:
        plist = plistlib.loads(blob)
        objects = plist.get("$objects")
        if not objects:
            return None
    except Exception:
        return None

    # Find the MBFile dict and read current values
    mbfile = None
    for obj in objects:
        if isinstance(obj, dict) and "Size" in obj:
            mbfile = obj
            break
    if mbfile is None:
        return None

    old_size = mbfile["Size"]
    old_mtime = mbfile.get("LastModified")

    patched = bytearray(blob)

    # Patch Size
    if not _replace_int_value(patched, old_size, new_size):
        return None

    # Patch LastModified
    if old_mtime is not None:
        if not _replace_int_value(patched, old_mtime, new_mtime):
            return None

    # Patch Digest
    if new_digest is not None:
        old_digest = _extract_digest(objects, mbfile)
        if old_digest is not None and not _replace_data_value(
            patched, old_digest, new_digest
        ):
            return None

    return bytes(patched)


def _replace_int_value(blob: bytearray, old_val: int, new_val: int) -> bool:
    """Replace a binary plist integer value in-place.

    Finds the old value encoded as a bplist integer (type marker 0x1X + BE bytes)
    and replaces it with the new value using the same byte width.

    Returns True on success, False if the value wasn't found or can't fit.
    """
    # Determine the byte width the old value uses
    for width_power in range(4):  # 1, 2, 4, 8 bytes
        width = 1 << width_power
        if old_val >= (1 << (8 * width)):
            continue
        type_marker = 0x10 | width_power
        needle = bytes([type_marker]) + old_val.to_bytes(width, "big")
        pos = blob.find(needle)
        if pos != -1:
            # Check the new value fits in the same width
            if new_val >= (1 << (8 * width)):
                return False
            replacement = bytes([type_marker]) + new_val.to_bytes(width, "big")
            blob[pos : pos + len(needle)] = replacement
            return True
    return False


def _extract_digest(objects: list, mbfile: dict) -> bytes | None:
    """Extract the Digest bytes from an MBFile dict's $objects array.

    The Digest field in the MBFile dict is a plistlib.UID referencing
    a bytes object in the $objects array.

    Returns the digest bytes, or None if no Digest field exists.
    """
    digest_ref = mbfile.get("Digest")
    if digest_ref is None:
        return None

    if isinstance(digest_ref, plistlib.UID):
        resolved = objects[digest_ref]
        if isinstance(resolved, bytes):
            return resolved
    elif isinstance(digest_ref, bytes):
        return digest_ref

    return None


def _replace_data_value(blob: bytearray, old_data: bytes, new_data: bytes) -> bool:
    """Replace a binary plist data value in-place.

    Both old and new data must be the same length (e.g., 20-byte SHA1 hashes).

    Returns True on success, False if the data wasn't found or lengths differ.
    """
    if len(old_data) != len(new_data):
        return False

    pos = blob.find(old_data)
    if pos == -1:
        return False

    blob[pos : pos + len(old_data)] = new_data
    return True


def _patch_digest_plistlib(objects: list, new_digest: bytes) -> None:
    """Patch digest in plistlib-parsed objects (fallback path)."""
    for obj in objects:
        if isinstance(obj, dict) and "Digest" in obj:
            digest_ref = obj["Digest"]
            if isinstance(digest_ref, plistlib.UID):
                objects[digest_ref] = new_digest
            else:
                obj["Digest"] = new_digest
            break


def build_mbfile_blob(
    size: int,
    mtime: float | None = None,
    mode: int = 0o100644,
    encryption_key: bytes | None = None,
    protection_class: int = 3,
) -> bytes:
    """Build a minimal MBFile NSKeyedArchiver binary plist from scratch.

    This creates a simplified but valid NSKeyedArchiver-compatible structure.
    iOS uses this to track file metadata in Manifest.db.

    Args:
        size: File size in bytes.
        mtime: Modification time as Unix timestamp.
        mode: POSIX file mode (default: regular file, 0644).
        encryption_key: Per-file wrapped encryption key blob (for encrypted backups).
        protection_class: iOS protection class (default: 3).

    Returns:
        Binary plist blob.
    """
    if mtime is None:
        mtime = time.time()

    mbfile_dict: dict[str, Any] = {
        "$class": plistlib.UID(2),
        "Size": size,
        "Mode": mode,
        "LastModified": int(mtime),
        "Birth": int(mtime),
        "UserID": 501,
        "GroupID": 501,
        "InodeNumber": 0,
        "Flags": 0,
        "ProtectionClass": protection_class,
    }

    if encryption_key is not None:
        mbfile_dict["EncryptionKey"] = encryption_key

    # Build NSKeyedArchiver structure
    # $objects[0] = "$null" sentinel
    # $objects[1] = the MBFile dictionary
    plist: dict[str, Any] = {
        "$version": 100000,
        "$archiver": "NSKeyedArchiver",
        "$top": {"root": plistlib.UID(1)},
        "$objects": [
            "$null",
            mbfile_dict,
            {
                "$classes": ["MBFile", "NSObject"],
                "$classname": "MBFile",
            },
        ],
    }

    return plistlib.dumps(plist, fmt=plistlib.FMT_BINARY)
