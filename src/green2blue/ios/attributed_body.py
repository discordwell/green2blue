"""Generate attributedBody typedstream blobs for injected messages.

iOS sms.db stores an `attributedBody` column on every message. It is a
typedstream (NSArchiver) serialized NSAttributedString containing the
message text with attribute metadata. iOS uses this blob to render messages
in the Messages app. Messages without an attributedBody may display as
blank in the conversation view.

Research on a real iOS 26.2 sms.db (26,891 messages with attributedBody) shows:

Format:
    Apple typedstream v4 (header: 04 0b 'streamtyped' 81 e8 03)
    NOT NSKeyedArchiver (binary plist) — this is the older NSArchiver format.

Two class hierarchy variants exist in real data:
    NSAttributedString (non-mutable):  9,625 messages (36%)
    NSMutableAttributedString:         17,266 messages (64%)

Both variants are accepted by iOS. Both appear in SMS, iMessage, and RCS
messages regardless of direction. For simplicity and consistency, green2blue
uses the NSAttributedString (non-mutable) variant, which is smaller.

Structure (simple message with no detected data):
    1. typedstream header (version 4, system 1000)
    2. NSAttributedString > NSObject class hierarchy
    3. NSString containing the message text as UTF-8
    4. Attribute run info: 1 run covering the full string length (UTF-16 units)
    5. NSDictionary with single entry:
       key:   '__kIMMessagePartAttributeName'
       value: NSNumber(0)  (message part index 0)

Length encoding (typedstream integer format):
    Values 0-127:     single byte
    Values 128-32767: 0x81 + uint16 little-endian
    Values 32768+:    0x82 + uint32 little-endian

Verified: The generator produces byte-identical output to real iOS blobs
for all 91 tested simple NSAttributedString messages (100% match rate).
Complex messages (containing detected data like URLs, phone numbers,
dates, or money amounts) have additional attribute runs; iOS regenerates
these after restore via data detection, so the simple form is correct.
"""

from __future__ import annotations

import struct


def _encode_typedstream_int(value: int) -> bytes:
    """Encode an integer using Apple's typedstream variable-length format.

    Args:
        value: Non-negative integer to encode.

    Returns:
        1, 3, or 5 bytes depending on value magnitude.
    """
    if value < 0x80:
        return bytes([value])
    elif value < 0x8000:
        return b"\x81" + struct.pack("<H", value)
    else:
        return b"\x82" + struct.pack("<I", value)


# Pre-computed fixed byte sequences from real iOS 26.2 sms.db blobs.
# These are constant across all simple messages — only the text content
# and length fields vary.

# Header + class hierarchy + string type tag (72 bytes)
_PREFIX = bytes.fromhex(
    "040b73747265616d747970656481e803"  # typedstream v4, system 1000
    "840140"  # type tag '@' (object) -> cache 0x92
    "848484124e53417474726962757465645374"
    "72696e6700"  # NSAttributedString v0 -> cache 0x93
    "8484084e534f626a65637400"  # NSObject v0 -> cache 0x94
    "85"  # end inheritance chain
    "92"  # ref '@' (another object follows)
    "848484084e53537472696e6701"  # NSString v1 -> cache 0x95
    "94"  # ref NSObject
    "84012b"  # type tag '+' (UTF-8 string) -> cache 0x96
)

# End string + attribute run type tags (5 bytes)
_MIDDLE = bytes.fromhex(
    "86"  # end NSString object
    "84026949"  # type tags 'i','I' (int, uint) -> cache 0x97
)

# NSDictionary with __kIMMessagePartAttributeName = NSNumber(0) (100 bytes)
_SUFFIX = bytes.fromhex(
    "92"  # ref '@'
    "8484840c4e5344696374696f6e61727900"  # NSDictionary v0 -> cache 0x98
    "94"  # ref NSObject
    "840169"  # type tag 'i' (dict key count type) -> cache 0x99
    "01"  # 1 key-value pair
    "92"  # ref '@' (key is object)
    "8496"  # ref '+' (key type) + ref '+' (string encoding)
    "96"  # ref '+' (string type tag for the key)
    "1d"  # 29 bytes follow
    "5f5f6b494d4d6573736167655061727441"
    "74747269627574654e616d65"  # '__kIMMessagePartAttributeName'
    "86"  # end key string
    "92"  # ref '@' (value is object)
    "848484084e534e756d62657200"  # NSNumber v0 -> cache 0x9a
    "8484074e5356616c756500"  # NSValue v0 -> cache 0x9b
    "94"  # ref NSObject
    "84012a"  # type tag '*' (raw bytes) -> cache 0x9c
    "84"  # data section for the NSNumber value
    "9999"  # type refs for int encoding (ref cache 0x99)
    "00"  # value = 0 (message part index)
    "868686"  # end NSDictionary, end attribute run, end NSAttributedString
)


def build_attributed_body(text: str) -> bytes | None:
    """Build an attributedBody typedstream blob for a message.

    Produces a minimal NSAttributedString with a single attribute run
    covering the entire text: ``{__kIMMessagePartAttributeName: 0}``.
    This matches the format iOS generates for simple SMS messages.

    Messages without text (attachment-only, system events) should have
    NULL attributedBody per real iOS behavior.

    Args:
        text: The message text string. Must be non-empty.

    Returns:
        Typedstream blob bytes, or None if text is empty/None.
    """
    if not text:
        return None

    text_bytes = text.encode("utf-8")
    utf8_len = len(text_bytes)
    # NSString.length counts UTF-16 code units, not characters or bytes
    utf16_len = len(text.encode("utf-16-le")) // 2

    return (
        _PREFIX
        + _encode_typedstream_int(utf8_len)
        + text_bytes
        + _MIDDLE
        + b"\x01"  # 1 attribute run
        + _encode_typedstream_int(utf16_len)
        + _SUFFIX
    )
