"""Timestamp conversion between Android and iOS formats.

Android SMS: Unix epoch in milliseconds (ms since 1970-01-01 00:00:00 UTC).
Android MMS: Unix epoch in seconds.
iOS sms.db: CoreData / "Apple Cocoa Core Data" nanoseconds since 2001-01-01 00:00:00 UTC.

All conversions use integer arithmetic to avoid floating-point precision loss.
"""

# Seconds between Unix epoch (1970-01-01) and Apple epoch (2001-01-01)
APPLE_EPOCH_OFFSET = 978_307_200

NANOSECONDS = 1_000_000_000
NS_PER_MS = 1_000_000


def unix_ms_to_ios_ns(unix_ms: int) -> int:
    """Convert Unix milliseconds to iOS CoreData nanoseconds.

    Args:
        unix_ms: Milliseconds since 1970-01-01 00:00:00 UTC.

    Returns:
        Nanoseconds since 2001-01-01 00:00:00 UTC.
    """
    apple_ms = unix_ms - APPLE_EPOCH_OFFSET * 1000
    return apple_ms * NS_PER_MS


def unix_s_to_ios_ns(unix_s: int) -> int:
    """Convert Unix seconds to iOS CoreData nanoseconds.

    Used for MMS timestamps which are in seconds, not milliseconds.

    Args:
        unix_s: Seconds since 1970-01-01 00:00:00 UTC.

    Returns:
        Nanoseconds since 2001-01-01 00:00:00 UTC.
    """
    apple_seconds = unix_s - APPLE_EPOCH_OFFSET
    return apple_seconds * NANOSECONDS


def ios_ns_to_unix_ms(ios_ns: int) -> int:
    """Convert iOS CoreData nanoseconds to Unix milliseconds.

    Args:
        ios_ns: Nanoseconds since 2001-01-01 00:00:00 UTC.

    Returns:
        Milliseconds since 1970-01-01 00:00:00 UTC.
    """
    apple_ms = ios_ns // NS_PER_MS
    return apple_ms + APPLE_EPOCH_OFFSET * 1000
