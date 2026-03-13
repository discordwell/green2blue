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

    When only patching Size/LastModified, uses raw binary patching to preserve
    the exact byte layout of real iOS blobs.

    When patching Digest, always uses plistlib roundtrip to avoid the risk of
    blind byte search matching Size/LastModified patterns inside the 20-byte
    digest data and corrupting it.

    Args:
        existing_blob: The original binary plist blob from Manifest.db.
        new_size: New file size in bytes.
        new_mtime: New modification time as Unix timestamp. Defaults to now.
        new_digest: New SHA1 digest (20 bytes). If provided, the Digest field
            will be updated or added.

    Returns:
        Updated binary plist blob.
    """
    if new_mtime is None:
        new_mtime = time.time()

    # When digest patching is needed, use plistlib roundtrip to avoid
    # corruption from blind byte search in raw patching.
    if new_digest is not None:
        return _patch_via_plistlib(
            existing_blob,
            new_size,
            int(new_mtime),
            new_digest,
        )

    # Size/LastModified only: try raw patching first
    result = _try_raw_patch(existing_blob, new_size, int(new_mtime))
    if result is not None:
        return result

    # Fallback: plistlib roundtrip
    return _patch_via_plistlib(existing_blob, new_size, int(new_mtime), None)


def clone_mbfile_blob(
    existing_blob: bytes,
    new_size: int,
    *,
    new_mtime: float | None = None,
    new_relative_path: str | None = None,
    new_encryption_key: bytes | None = None,
    new_digest: bytes | None = None,
    new_mode: int | None = None,
    new_protection_class: int | None = None,
) -> bytes:
    """Clone a real MBFile blob and patch selected fields via plistlib.

    This preserves the richer NSKeyedArchiver object graph used by real iOS
    backups, including UID-backed RelativePath and EncryptionKey objects.
    """
    if new_mtime is None:
        new_mtime = time.time()

    try:
        plist = plistlib.loads(existing_blob)
    except Exception:
        return build_mbfile_blob(
            new_size,
            float(new_mtime),
            mode=new_mode or 0o100644,
            encryption_key=new_encryption_key,
            protection_class=new_protection_class or 3,
            digest=new_digest,
        )

    objects = plist.get("$objects")
    if not objects:
        return build_mbfile_blob(
            new_size,
            float(new_mtime),
            mode=new_mode or 0o100644,
            encryption_key=new_encryption_key,
            protection_class=new_protection_class or 3,
            digest=new_digest,
        )

    mbfile = None
    for obj in objects:
        if isinstance(obj, dict) and "Size" in obj:
            mbfile = obj
            break

    if mbfile is None:
        return build_mbfile_blob(
            new_size,
            float(new_mtime),
            mode=new_mode or 0o100644,
            encryption_key=new_encryption_key,
            protection_class=new_protection_class or 3,
            digest=new_digest,
        )

    timestamp = int(new_mtime)
    mbfile["Size"] = new_size
    if "Birth" in mbfile:
        mbfile["Birth"] = timestamp
    if "LastModified" in mbfile:
        mbfile["LastModified"] = timestamp
    if "LastStatusChange" in mbfile:
        mbfile["LastStatusChange"] = timestamp
    if new_mode is not None:
        mbfile["Mode"] = new_mode
    if new_protection_class is not None:
        mbfile["ProtectionClass"] = new_protection_class

    if new_relative_path is not None:
        _patch_or_add_relative_path(objects, mbfile, new_relative_path)
    if new_encryption_key is not None:
        _patch_or_add_encryption_key(objects, mbfile, new_encryption_key)
    if new_digest is not None:
        _patch_or_add_digest(plist, objects, new_digest)

    return plistlib.dumps(plist, fmt=plistlib.FMT_BINARY)


def _patch_via_plistlib(
    existing_blob: bytes,
    new_size: int,
    new_mtime: int,
    new_digest: bytes | None,
) -> bytes:
    """Patch an MBFile blob using plistlib parse/serialize roundtrip.

    This is safe for Manifest.db MBFile blobs. It correctly handles adding
    or updating the Digest field.
    """
    try:
        plist = plistlib.loads(existing_blob)
    except Exception:
        return build_mbfile_blob(new_size, float(new_mtime), digest=new_digest)

    objects = plist.get("$objects")
    if not objects:
        return build_mbfile_blob(new_size, float(new_mtime), digest=new_digest)

    for obj in objects:
        if isinstance(obj, dict):
            if "Size" in obj:
                obj["Size"] = new_size
            if "LastModified" in obj:
                obj["LastModified"] = new_mtime

    if new_digest is not None:
        _patch_or_add_digest(plist, objects, new_digest)

    return plistlib.dumps(plist, fmt=plistlib.FMT_BINARY)


def _try_raw_patch(
    blob: bytes,
    new_size: int,
    new_mtime: int,
) -> bytes | None:
    """Attempt to patch Size and LastModified directly in the raw binary plist.

    Binary plists store integers as type 0x1X where X encodes byte width
    (0=1B, 1=2B, 2=4B, 3=8B). We locate the fields by finding their values
    via plistlib, then replacing the encoded bytes.

    Only used for Size/LastModified patching. Digest patching always goes
    through _patch_via_plistlib to avoid blind byte search corruption.

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
    if old_mtime is not None and not _replace_int_value(patched, old_mtime, new_mtime):
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


def extract_mbfile_digest(blob: bytes) -> bytes | None:
    """Extract the Digest field from an MBFile NSKeyedArchiver blob.

    Returns the SHA1 digest bytes (20 bytes), or None if no Digest field.
    """
    try:
        plist = plistlib.loads(blob)
        objects = plist.get("$objects")
        if not objects:
            return None
    except Exception:
        return None
    for obj in objects:
        if isinstance(obj, dict) and "Digest" in obj:
            return _resolve_digest_ref(objects, obj)
    return None


def _resolve_digest_ref(objects: list, mbfile: dict) -> bytes | None:
    """Resolve a Digest field reference in an MBFile dict."""
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


def _patch_or_add_digest(
    plist: dict,
    objects: list,
    new_digest: bytes,
) -> None:
    """Patch or add Digest in plistlib-parsed objects.

    If the MBFile dict already has a Digest field, update it.
    If not, add the digest as a new object and reference it via UID.
    """
    for obj in objects:
        if isinstance(obj, dict) and "Digest" in obj:
            digest_ref = obj["Digest"]
            if isinstance(digest_ref, plistlib.UID):
                objects[digest_ref] = new_digest
            else:
                obj["Digest"] = new_digest
            return

    # No Digest field found — add one to the MBFile dict
    for obj in objects:
        if isinstance(obj, dict) and "Size" in obj:
            # Insert digest bytes as a new object in $objects
            new_uid = len(objects)
            objects.append(new_digest)
            obj["Digest"] = plistlib.UID(new_uid)
            return


def _patch_or_add_relative_path(
    objects: list,
    mbfile: dict,
    new_relative_path: str,
) -> None:
    """Patch or add RelativePath in an MBFile dict."""
    relative_path_ref = mbfile.get("RelativePath")
    if isinstance(relative_path_ref, plistlib.UID):
        objects[relative_path_ref] = new_relative_path
    else:
        new_uid = len(objects)
        objects.append(new_relative_path)
        mbfile["RelativePath"] = plistlib.UID(new_uid)


def _patch_or_add_encryption_key(
    objects: list,
    mbfile: dict,
    new_encryption_key: bytes,
) -> None:
    """Patch or add EncryptionKey, preserving NSData wrapper when present."""
    encryption_key_ref = mbfile.get("EncryptionKey")
    if isinstance(encryption_key_ref, plistlib.UID):
        resolved = objects[encryption_key_ref]
        if isinstance(resolved, dict) and "NS.data" in resolved:
            resolved["NS.data"] = new_encryption_key
            return
        objects[encryption_key_ref] = new_encryption_key
        return

    if isinstance(encryption_key_ref, bytes):
        mbfile["EncryptionKey"] = new_encryption_key
        return

    data_uid = len(objects)
    class_uid = data_uid + 1
    objects.extend(
        [
            {
                "NS.data": new_encryption_key,
                "$class": plistlib.UID(class_uid),
            },
            {
                "$classname": "NSMutableData",
                "$classes": ["NSMutableData", "NSData", "NSObject"],
            },
        ]
    )
    mbfile["EncryptionKey"] = plistlib.UID(data_uid)


def build_mbfile_blob(
    size: int,
    mtime: float | None = None,
    mode: int = 0o100644,
    encryption_key: bytes | None = None,
    protection_class: int = 3,
    digest: bytes | None = None,
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
        digest: SHA1 digest of the file (20 bytes). Included if provided.

    Returns:
        Binary plist blob.
    """
    if mtime is None:
        mtime = time.time()

    # Class reference UID depends on how many objects we'll add
    # $objects layout: [0]="$null", [1]=MBFile dict, [2+]=optional data, [N]=class dict
    extra_objects: list[Any] = []

    mbfile_dict: dict[str, Any] = {
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

    if digest is not None:
        digest_uid = 2 + len(extra_objects)
        extra_objects.append(digest)
        mbfile_dict["Digest"] = plistlib.UID(digest_uid)

    if encryption_key is not None:
        mbfile_dict["EncryptionKey"] = encryption_key

    class_uid = 2 + len(extra_objects)
    mbfile_dict["$class"] = plistlib.UID(class_uid)

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
            *extra_objects,
            {
                "$classes": ["MBFile", "NSObject"],
                "$classname": "MBFile",
            },
        ],
    }

    return plistlib.dumps(plist, fmt=plistlib.FMT_BINARY)
