"""Tests for timestamp conversion."""

from green2blue.converter.timestamp import (
    APPLE_EPOCH_OFFSET,
    ios_ns_to_unix_ms,
    unix_ms_to_ios_ns,
    unix_s_to_ios_ns,
)


class TestUnixMsToIosNs:
    def test_epoch_boundary(self):
        """Unix ms for 2001-01-01 00:00:00 UTC should map to iOS ns 0."""
        unix_ms = APPLE_EPOCH_OFFSET * 1000
        assert unix_ms_to_ios_ns(unix_ms) == 0

    def test_known_date(self):
        """2024-01-01 00:00:00 UTC = Unix 1704067200000 ms."""
        unix_ms = 1_704_067_200_000
        ios_ns = unix_ms_to_ios_ns(unix_ms)
        expected_apple_seconds = 1_704_067_200 - APPLE_EPOCH_OFFSET
        assert ios_ns == expected_apple_seconds * 1_000_000_000

    def test_before_apple_epoch(self):
        """Dates before 2001-01-01 produce negative iOS timestamps."""
        unix_ms = 946684800000  # 2000-01-01 00:00:00 UTC
        ios_ns = unix_ms_to_ios_ns(unix_ms)
        assert ios_ns < 0

    def test_round_trip(self):
        """Converting to iOS and back should return the original value."""
        original = 1_700_000_000_000
        assert ios_ns_to_unix_ms(unix_ms_to_ios_ns(original)) == original

    def test_zero(self):
        """Unix epoch 0 should produce a negative iOS timestamp."""
        ios_ns = unix_ms_to_ios_ns(0)
        assert ios_ns == -APPLE_EPOCH_OFFSET * 1_000_000_000


class TestUnixSToIosNs:
    def test_mms_timestamp(self):
        """MMS timestamps are in seconds, not milliseconds."""
        unix_s = 1_700_000_000
        ios_ns = unix_s_to_ios_ns(unix_s)
        expected = (unix_s - APPLE_EPOCH_OFFSET) * 1_000_000_000
        assert ios_ns == expected

    def test_matches_ms_conversion(self):
        """unix_s_to_ios_ns(s) should match unix_ms_to_ios_ns(s * 1000)."""
        unix_s = 1_700_000_000
        assert unix_s_to_ios_ns(unix_s) == unix_ms_to_ios_ns(unix_s * 1000)


class TestIosNsToUnixMs:
    def test_zero_maps_to_apple_epoch(self):
        """iOS ns 0 should map to the Apple epoch in Unix ms."""
        assert ios_ns_to_unix_ms(0) == APPLE_EPOCH_OFFSET * 1000

    def test_positive_value(self):
        ios_ns = 725_760_000_000_000_000  # ~23 years after 2001
        unix_ms = ios_ns_to_unix_ms(ios_ns)
        assert unix_ms > APPLE_EPOCH_OFFSET * 1000
