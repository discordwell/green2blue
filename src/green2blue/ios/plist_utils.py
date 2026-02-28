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


def patch_mbfile_blob(existing_blob: bytes, new_size: int, new_mtime: float | None = None) -> bytes:
    """Patch an existing MBFile NSKeyedArchiver blob with new size/mtime.

    Args:
        existing_blob: The original binary plist blob from Manifest.db.
        new_size: New file size in bytes.
        new_mtime: New modification time as Unix timestamp. Defaults to now.

    Returns:
        Updated binary plist blob.
    """
    if new_mtime is None:
        new_mtime = time.time()

    try:
        plist = plistlib.loads(existing_blob)
    except Exception:
        # If we can't parse the existing blob, build from scratch
        return build_mbfile_blob(new_size, new_mtime)

    # NSKeyedArchiver stores objects in $objects array
    objects = plist.get("$objects")
    if not objects:
        return build_mbfile_blob(new_size, new_mtime)

    # Walk the objects looking for Size and LastModified keys
    for obj in objects:
        if isinstance(obj, dict):
            if "Size" in obj:
                obj["Size"] = new_size
            if "LastModified" in obj:
                obj["LastModified"] = int(new_mtime)

    return plistlib.dumps(plist, fmt=plistlib.FMT_BINARY)


def build_mbfile_blob(size: int, mtime: float | None = None, mode: int = 0o100644) -> bytes:
    """Build a minimal MBFile NSKeyedArchiver binary plist from scratch.

    This creates a simplified but valid NSKeyedArchiver-compatible structure.
    iOS uses this to track file metadata in Manifest.db.

    Args:
        size: File size in bytes.
        mtime: Modification time as Unix timestamp.
        mode: POSIX file mode (default: regular file, 0644).

    Returns:
        Binary plist blob.
    """
    if mtime is None:
        mtime = time.time()

    # Build NSKeyedArchiver structure
    # $objects[0] = "$null" sentinel
    # $objects[1] = the MBFile dictionary
    plist: dict[str, Any] = {
        "$version": 100000,
        "$archiver": "NSKeyedArchiver",
        "$top": {"root": plistlib.UID(1)},
        "$objects": [
            "$null",
            {
                "$class": plistlib.UID(2),
                "Size": size,
                "Mode": mode,
                "LastModified": int(mtime),
                "Birth": int(mtime),
                "UserID": 501,
                "GroupID": 501,
                "InodeNumber": 0,
                "Flags": 0,
                "ProtectionClass": 3,
            },
            {
                "$classes": ["MBFile", "NSObject"],
                "$classname": "MBFile",
            },
        ],
    }

    return plistlib.dumps(plist, fmt=plistlib.FMT_BINARY)
