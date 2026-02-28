"""Tests for phone number normalization."""

import pytest

from green2blue.converter.phone import normalize_phone, strip_to_digits
from green2blue.exceptions import PhoneNormalizationError


class TestUSNumbers:
    def test_ten_digit(self):
        assert normalize_phone("2025551234", "US") == "+12025551234"

    def test_with_country_code(self):
        assert normalize_phone("12025551234", "US") == "+12025551234"

    def test_with_plus_country_code(self):
        assert normalize_phone("+12025551234", "US") == "+12025551234"

    def test_parentheses_format(self):
        assert normalize_phone("(202) 555-1234", "US") == "+12025551234"

    def test_dashes_format(self):
        assert normalize_phone("202-555-1234", "US") == "+12025551234"

    def test_spaces_format(self):
        assert normalize_phone("202 555 1234", "US") == "+12025551234"

    def test_dots_format(self):
        assert normalize_phone("202.555.1234", "US") == "+12025551234"

    def test_mixed_format(self):
        assert normalize_phone("(202) 555.1234", "US") == "+12025551234"


class TestInternational:
    def test_uk_number(self):
        assert normalize_phone("+442079460958") == "+442079460958"

    def test_uk_national(self):
        assert normalize_phone("2079460958", "GB") == "+442079460958"

    def test_australian_number(self):
        assert normalize_phone("+61412345678") == "+61412345678"

    def test_australian_national(self):
        assert normalize_phone("412345678", "AU") == "+61412345678"

    def test_german_number(self):
        assert normalize_phone("+4915123456789") == "+4915123456789"

    def test_french_national(self):
        assert normalize_phone("612345678", "FR") == "+33612345678"

    def test_indian_national(self):
        assert normalize_phone("9876543210", "IN") == "+919876543210"

    def test_japanese_number(self):
        assert normalize_phone("+819012345678") == "+819012345678"


class TestShortCodes:
    def test_five_digit(self):
        assert normalize_phone("12345") == "12345"

    def test_six_digit(self):
        assert normalize_phone("123456") == "123456"

    def test_short_code_not_e164(self):
        """Short codes should not get a + prefix."""
        result = normalize_phone("55555")
        assert not result.startswith("+")

    def test_seven_digit_local_us(self):
        """7-digit local numbers (no area code) should pass through."""
        assert normalize_phone("5551234") == "5551234"

    def test_seven_digit_local_ca(self):
        assert normalize_phone("5551234", "CA") == "5551234"

    def test_seven_digit_not_e164(self):
        """7-digit local numbers should not get a + prefix."""
        result = normalize_phone("5551234")
        assert not result.startswith("+")


class TestEdgeCases:
    def test_empty_string(self):
        with pytest.raises(PhoneNormalizationError):
            normalize_phone("")

    def test_whitespace_only(self):
        with pytest.raises(PhoneNormalizationError):
            normalize_phone("   ")

    def test_no_digits(self):
        with pytest.raises(PhoneNormalizationError):
            normalize_phone("abc")

    def test_plus_only(self):
        with pytest.raises(PhoneNormalizationError):
            normalize_phone("+")

    def test_unknown_country(self):
        with pytest.raises(PhoneNormalizationError):
            normalize_phone("12345678", "XX")

    def test_too_short_international(self):
        with pytest.raises(PhoneNormalizationError):
            normalize_phone("+123")

    def test_too_long_international(self):
        with pytest.raises(PhoneNormalizationError):
            normalize_phone("+1234567890123456")

    def test_default_country_is_us(self):
        assert normalize_phone("2025551234") == "+12025551234"

    def test_leading_trailing_whitespace(self):
        assert normalize_phone("  +12025551234  ") == "+12025551234"


class TestStripToDigits:
    def test_basic(self):
        assert strip_to_digits("+1 (202) 555-1234") == "12025551234"

    def test_already_digits(self):
        assert strip_to_digits("2025551234") == "2025551234"

    def test_empty(self):
        assert strip_to_digits("") == ""
